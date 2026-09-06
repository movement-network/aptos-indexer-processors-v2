use self::{
    evm_connection::{EvmConnecting, EvmConnectionState, EvmScreening, HypernativeState},
    hypernative::{EvmScreeningDb, HypernativeClient},
    lz_enricher::LzEnricher,
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{sync::mpsc::UnboundedReceiver, time::MissedTickBehavior};
use tracing::{error, info, warn};

mod evm_connection;
pub mod evm_storer;
pub mod hypernative;
pub mod lz_enricher;
pub mod lz_payload;
pub mod lz_storer;

/// Maximum number of attempts per item (first attempt + this many retries).
const MAX_RETRIES: u32 = 5;

/// How long a retry future sleeps before its next attempt.
const RETRY_DELAY: Duration = Duration::from_secs(30);

/// How long to wait between Hypernative connection attempts.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(60);

pub struct EnricherLoop {
    ctx: LoopCtx,
    lz: Arc<LzEnricher>,
    guid_rx: UnboundedReceiver<String>,
    evm_rx: UnboundedReceiver<String>,
    interval: Duration,
}

impl EnricherLoop {
    pub fn new(
        db: Arc<dyn EvmScreeningDb>,
        guid_rx: UnboundedReceiver<String>,
        evm_rx: UnboundedReceiver<String>,
        lz: LzEnricher,
        hypernative: HypernativeClient,
        interval_ms: u64,
    ) -> Self {
        Self {
            ctx: LoopCtx {
                hypernative: Arc::new(hypernative),
                db,
                retry_delay: RETRY_DELAY,
                reconnect_interval: RECONNECT_INTERVAL,
            },
            lz: Arc::new(lz),
            guid_rx,
            evm_rx,
            interval: Duration::from_millis(interval_ms),
        }
    }

    pub fn with_retry_delay(mut self, d: Duration) -> Self {
        self.ctx.retry_delay = d;
        self
    }

    pub fn with_reconnect_interval(mut self, d: Duration) -> Self {
        self.ctx.reconnect_interval = d;
        self
    }

    pub async fn run(mut self) {
        let mut guid_queue: VecDeque<String> = self.lz.load_pending_guids().await;

        let mut evm_screening_state: Box<dyn EvmConnectionState> = Box::new(HypernativeState {
            state: EvmConnecting { last_ping: None },
        });

        info!(pending_guids = guid_queue.len(), "evm_fetch_loop: started");

        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut pending: FuturesUnordered<BoxFuture<'static, Option<RetryItem>>> =
            FuturesUnordered::new();
        // Ping runs as a non-blocking future so the select loop stays live
        // during network timeouts. At most one ping is in flight at a time.
        let mut ping_tasks: FuturesUnordered<BoxFuture<'static, anyhow::Result<()>>> =
            FuturesUnordered::new();

        loop {
            tokio::select! {
                msg = self.guid_rx.recv() => match msg {
                    Some(guid) => guid_queue.push_back(guid),
                    None => {
                        warn!("evm_fetch_loop: guid channel closed, end loop");
                        break;
                    },
                },
                msg = self.evm_rx.recv() => match msg {
                    Some(evm) => {
                        evm_screening_state.new_evm(evm, &self.ctx).await;
                    },
                    None => {
                        warn!("evm_fetch_loop: evm channel closed, end loop");
                        break;
                    },
                },
                // Ping result: set connected and seed the queue on success.
                Some(result) = ping_tasks.next() => {
                    match result {
                        Ok(()) => {
                            info!("evm_fetch_loop: Hypernative connected; loading pending EVMs");
                            let evm_queue = self.ctx.db.load_pending_evms().await;
                            info!(pending_evms = evm_queue.len(), "evm_fetch_loop: pending EVMs loaded");
                            evm_screening_state = Box::new(HypernativeState { state: EvmScreening{ evm_queue }});
                        },
                        Err(e) => warn!(
                            err = %e,
                            retry_secs = self.ctx.reconnect_interval.as_secs(),
                            "evm_fetch_loop: Hypernative ping failed, will retry"
                        ),
                    }
                },
                // `FuturesUnordered::next()` resolves immediately with None
                // when the set is empty, so we guard to avoid a busy-loop.
                Some(Some(item)) = pending.next() => {
                    match item {
                        RetryItem::Guid { guid, count } => {
                            if count < MAX_RETRIES {
                                info!(lz_guid = %guid, attempt = count + 1, "evm_fetch_loop: retrying GUID");
                                let lz = Arc::clone(&self.lz);
                                let retry_delay = self.ctx.retry_delay;
                                pending.push(Box::pin(
                                    tokio::time::sleep(retry_delay)
                                        .then(move |_| guid_future(lz, guid, count))
                                ));
                            } else {
                                error!(lz_guid = %guid, "evm_fetch_loop: GUID exhausted max retries, dropping");
                            }
                        },
                        RetryItem::Evm { evms, count } => {
                            evm_screening_state.process_evms(evms,count, &mut pending, &self.ctx).await;
                        },
                    }
                },
                _ = ticker.tick() => {
                    // Spawn a ping if not yet connected and none is already in flight.
                    if ping_tasks.is_empty() {
                        if let Some(task) = evm_screening_state.connect(&self.ctx) {
                            ping_tasks.push(task);
                        }
                    }

                    while let Some(guid) = guid_queue.pop_front() {
                        pending.push(Box::pin(guid_future(Arc::clone(&self.lz), guid, 0)));
                    }

                    // Process pinfing evm addresses
                    evm_screening_state.process_evms(vec![],0, &mut pending, &self.ctx).await;
                }
            }
        }
        warn!("evm_fetch_loop: exited");
    }
}

/// Returned by a future that failed transiently. `count` is the number of
/// attempts already made; the select branch uses it to enforce `MAX_RETRIES`.
enum RetryItem {
    Guid { guid: String, count: u32 },
    Evm { evms: Vec<String>, count: u32 },
}

// ---------------------------------------------------------------------------
// Shared context threaded into state methods
// ---------------------------------------------------------------------------
struct LoopCtx {
    hypernative: Arc<HypernativeClient>,
    db: Arc<dyn EvmScreeningDb>,
    retry_delay: Duration,
    reconnect_interval: Duration,
}

// ---------------------------------------------------------------------------
// Futures pushed into FuturesUnordered — return None (done) or Some(RetryItem)
// ---------------------------------------------------------------------------

fn guid_future(
    lz: Arc<LzEnricher>,
    guid: String,
    count: u32,
) -> BoxFuture<'static, Option<RetryItem>> {
    Box::pin(async move {
        match lz.process_guid(&guid).await {
            Ok(Some(evm)) => {
                // GUID resolved — hand the EVM off as a fresh first attempt.
                Some(RetryItem::Evm {
                    evms: vec![evm],
                    count: 0,
                })
            },
            Ok(None) => {
                // No EVM to screen (unused: fetch failures are Err and retry).
                None
            },
            Err(_) => Some(RetryItem::Guid {
                guid,
                count: count + 1,
            }),
        }
    })
}
