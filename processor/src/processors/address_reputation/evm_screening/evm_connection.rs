use super::hypernative::{error_score, screen_evms, EvmScreeningDb, HypernativeClient};
use super::{LoopCtx, RetryItem, MAX_RETRIES};
use async_trait::async_trait;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Connection state trait
// ---------------------------------------------------------------------------

#[async_trait]
pub(super) trait EvmConnectionState: Send {
    // If not connected return the connection task
    fn connect(&mut self, ctx: &LoopCtx) -> Option<BoxFuture<'static, anyhow::Result<()>>>;
    /// Handle EVMs resolved from a GUID; performs its own freshness check.
    async fn process_evms(
        &mut self,
        evms: Vec<String>,
        count: u32,
        pending: &mut FuturesUnordered<BoxFuture<'static, Option<RetryItem>>>,
        ctx: &LoopCtx,
    );
    /// Handle new evm addresses received.
    async fn new_evm(&mut self, evm: String, ctx: &LoopCtx);
}

// ---------------------------------------------------------------------------
// Connecting: save error scores, schedule pings until Hypernative is reachable
// ---------------------------------------------------------------------------

pub(super) struct EvmConnecting {
    pub(super) last_ping: Option<std::time::Instant>,
}
// ---------------------------------------------------------------------------
// Screening: queue EVMs and flush them to Hypernative each tick
// ---------------------------------------------------------------------------

pub(super) struct EvmScreening {
    pub(super) evm_queue: VecDeque<String>,
}

pub(super) struct HypernativeState<S> {
    pub(super) state: S,
}

#[async_trait]
impl EvmConnectionState for HypernativeState<EvmConnecting> {
    fn connect(&mut self, ctx: &LoopCtx) -> Option<BoxFuture<'static, anyhow::Result<()>>> {
        self.state
            .last_ping
            .map_or(true, |t| t.elapsed() >= ctx.reconnect_interval)
            .then(|| {
                self.state.last_ping = Some(std::time::Instant::now());
                let hn = Arc::clone(&ctx.hypernative);
                Box::pin(async move { hn.ping().await }) as BoxFuture<'static, anyhow::Result<()>>
            })
    }

    async fn process_evms(
        &mut self,
        evms: Vec<String>,
        count: u32,
        _pending: &mut FuturesUnordered<BoxFuture<'static, Option<RetryItem>>>,
        ctx: &LoopCtx,
    ) {
        for evm in &evms {
            if count == 0 && !ctx.db.is_fresh_in_db(evm).await {
                let score = error_score(evm);
                if let Err(e) = ctx.db.save(&score).await {
                    warn!(evm_address = %evm, err = %e, "evm_fetch_loop: failed to save pre-connection error score");
                } else {
                    info!(evm_address = %evm, "evm_fetch_loop: saved error score (Hypernative not yet connected)");
                }
            }
        }
    }

    async fn new_evm(&mut self, evm: String, ctx: &LoopCtx) {
        if ctx.db.is_fresh_in_db(&evm).await {
            info!(evm_address = %evm, "evm_fetch_loop: skipping evm, already screened within TTL");
        } else {
            // Hypernative not yet connected: persist an error score so
            // load_pending_evms re-queues the address after connection.
            let score = error_score(&evm);
            if let Err(e) = ctx.db.save(&score).await {
                warn!(evm_address = %evm, err = %e, "evm_fetch_loop: failed to save pre-connection error score");
            } else {
                info!(evm_address = %evm, "evm_fetch_loop: saved error score (Hypernative not yet connected)");
            }
        }
    }
}

#[async_trait]
impl EvmConnectionState for HypernativeState<EvmScreening> {
    fn connect(&mut self, _ctx: &LoopCtx) -> Option<BoxFuture<'static, anyhow::Result<()>>> {
        None
    }

    async fn process_evms(
        &mut self,
        evms: Vec<String>,
        count: u32,
        pending: &mut FuturesUnordered<BoxFuture<'static, Option<RetryItem>>>,
        ctx: &LoopCtx,
    ) {
        let seen: std::collections::HashSet<String> =
            evms.iter().map(|e| e.to_lowercase()).collect();
        let mut evms = evms;
        evms.extend(
            self.state
                .evm_queue
                .drain(..)
                .filter(|e| !seen.contains(&e.to_lowercase())),
        );
        let mut to_screen = Vec::with_capacity(evms.len());
        let evms_count = evms.len();
        for evm in evms {
            if count == 0 && ctx.db.is_fresh_in_db(&evm).await {
                info!(evm_address = %evm, "evm_fetch_loop: skipping guid-resolved evm, already screened within TTL");
            } else {
                to_screen.push(evm);
            }
        }
        if !to_screen.is_empty() {
            if count < MAX_RETRIES {
                let delay = match count {
                    0 => Duration::from_secs(0),
                    _ => {
                        info!(
                            evm_count = evms_count,
                            attempt = count + 1,
                            "evm_fetch_loop: retrying EVM batch"
                        );
                        ctx.retry_delay
                    },
                };
                let hn = Arc::clone(&ctx.hypernative);
                let db = Arc::clone(&ctx.db);
                pending.push(Box::pin(
                    tokio::time::sleep(delay).then(move |_| evm_future(hn, db, to_screen, count)),
                ));
            } else {
                warn!(
                    evm_count = to_screen.len(),
                    "evm_fetch_loop: EVM batch exhausted max retries, saving error scores"
                );
                for evm in &to_screen {
                    let score = error_score(evm);
                    if let Err(e) = ctx.db.save(&score).await {
                        warn!(evm_address = %evm, err = %e, "evm_fetch_loop: failed to save error score after max retries");
                    }
                }
            }
        }
    }

    async fn new_evm(&mut self, evm: String, _ctx: &LoopCtx) {
        self.state.evm_queue.push_back(evm);
    }
}

fn evm_future(
    hn: Arc<HypernativeClient>,
    db: Arc<dyn EvmScreeningDb>,
    evms: Vec<String>,
    count: u32,
) -> BoxFuture<'static, Option<RetryItem>> {
    Box::pin(async move {
        if screen_evms(&hn, db.as_ref(), &evms).await {
            Some(RetryItem::Evm {
                evms,
                count: count + 1,
            })
        } else {
            None
        }
    })
}
