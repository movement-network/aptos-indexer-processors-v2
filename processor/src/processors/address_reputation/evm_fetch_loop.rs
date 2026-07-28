// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Background EVM fetch loop for the address_reputation processor.
//!
//! ## Concurrency model
//! The `select!` loop never awaits a network call directly. On each tick it
//! pops one item from a queue and pushes a future into `pending`
//! (`FuturesUnordered`). The futures run concurrently; the select polls them
//! in a dedicated branch so completed results are handled promptly without
//! blocking the recv branches.
//!
//! ## Retry
//! All retry logic lives in the `pending.next()` branch. A future returns
//! `Some(RetryItem)` carrying a `count` when it fails transiently. The branch
//! increments the count and re-pushes a new future only if `count < MAX_RETRIES`;
//! otherwise it logs a final error and drops the item. Futures sleep
//! `RETRY_DELAY` at the start when `count > 0` so retries are not immediate.
//! Permanent failures (LZ 404) return `None` and are never retried.

use super::{
    address_reputation_model::EvmRiskScore,
    hypernative::{HypernativeClient, HypernativeResult},
    lz_enricher::{LzEnricher, EVM_NULL_SENTINEL},
};
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{Numeric, Timestamp, Varchar},
};
use diesel_async::RunQueryDsl;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{collections::VecDeque, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc::UnboundedReceiver, time::MissedTickBehavior};
use tracing::{error, info, warn};

#[derive(diesel::QueryableByName)]
struct EvmRow {
    #[diesel(sql_type = Varchar)]
    evm_source: String,
}

/// Returned by a future that failed transiently. `count` is the number of
/// attempts already made; the select branch uses it to enforce `MAX_RETRIES`.
enum RetryItem {
    Guid { guid: String, count: u32 },
    Evm { evm: String, count: u32 },
}

/// Maximum number of attempts per item (first attempt + this many retries).
const MAX_RETRIES: u32 = 5;

/// How long a retry future sleeps before its next attempt.
const RETRY_DELAY: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Score persistence — trait for dependency injection (enables mocking in tests)
// ---------------------------------------------------------------------------

/// Persists a computed `EvmRiskScore`. The production implementation writes to
/// PostgreSQL; tests inject a `CapturingSaver` that records scores in memory.
#[async_trait]
pub trait ScoreSaver: Send + Sync + 'static {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()>;
}

/// Production implementation: delegates to `save_risk_score`.
pub struct DbScoreSaver {
    pool: ArcDbPool,
}

impl DbScoreSaver {
    pub fn new(pool: ArcDbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ScoreSaver for DbScoreSaver {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()> {
        save_risk_score(&self.pool, score).await
    }
}

pub struct EnricherLoop {
    db_pool: Option<ArcDbPool>,
    guid_rx: UnboundedReceiver<String>,
    evm_rx: UnboundedReceiver<String>,
    lz: Option<Arc<LzEnricher>>,
    hypernative: Option<Arc<HypernativeClient>>,
    interval: Duration,
    saver: Arc<dyn ScoreSaver>,
}

impl EnricherLoop {
    pub fn new(
        db_pool: Option<ArcDbPool>,
        guid_rx: UnboundedReceiver<String>,
        evm_rx: UnboundedReceiver<String>,
        lz: Option<LzEnricher>,
        hypernative: Option<HypernativeClient>,
        interval_ms: u64,
        saver: Arc<dyn ScoreSaver>,
    ) -> Self {
        Self {
            db_pool,
            guid_rx,
            evm_rx,
            lz: lz.map(Arc::new),
            hypernative: hypernative.map(Arc::new),
            interval: Duration::from_millis(interval_ms),
            saver,
        }
    }

    pub async fn run(mut self) {
        let mut guid_queue: VecDeque<String> = if let Some(lz) = self.lz.as_ref() {
            lz.load_pending_guids().await
        } else {
            VecDeque::new()
        };
        let mut evm_queue: VecDeque<String> = self.load_pending_evms().await;
        info!(
            pending_guids = guid_queue.len(),
            pending_evms = evm_queue.len(),
            "evm_fetch_loop: started"
        );

        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut pending: FuturesUnordered<BoxFuture<'static, Option<RetryItem>>> =
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
                    Some(evm) => evm_queue.push_back(evm),
                    None => {
                        warn!("evm_fetch_loop: evm channel closed, end loop");
                        break;
                    },
                },
                // `FuturesUnordered::next()` resolves immediately with None
                // when the set is empty, so we guard to avoid a busy-loop.
                Some(Some(item)) = pending.next() => {
                    match item {
                        RetryItem::Guid { guid, count } => {
                            if count < MAX_RETRIES {
                                info!(lz_guid = %guid, attempt = count + 1, "evm_fetch_loop: retrying GUID");
                                if let Some(lz) = self.lz.as_ref() {
                                    let lz = Arc::clone(lz);
                                    pending.push(Box::pin(
                                        tokio::time::sleep(RETRY_DELAY)
                                            .then(move |_| guid_future(lz, guid, count))
                                    ));
                                }
                            } else {
                                error!(lz_guid = %guid, "evm_fetch_loop: GUID exhausted max retries, dropping");
                            }
                        },
                        RetryItem::Evm { evm, count } => {
                            if let Some(hn) = self.hypernative.as_ref() {
                                if count == 0 {
                                    // Fresh EVM from a resolved GUID — first attempt, no delay.
                                    pending.push(Box::pin(evm_future(Arc::clone(hn), Arc::clone(&self.saver), evm, 0)));
                                } else if count < MAX_RETRIES {
                                    info!(evm_address = %evm, attempt = count + 1, "evm_fetch_loop: retrying EVM");
                                    let hn = Arc::clone(hn);
                                    let saver = Arc::clone(&self.saver);
                                    pending.push(Box::pin(
                                        tokio::time::sleep(RETRY_DELAY)
                                            .then(move |_| evm_future(hn, saver, evm, count))
                                    ));
                                } else {
                                    error!(evm_address = %evm, "evm_fetch_loop: EVM exhausted max retries, dropping");
                                }
                            }
                        },
                    }
                },
                _ = ticker.tick() => {
                    if let Some(lz) = self.lz.as_ref() {
                        while let Some(guid) = guid_queue.pop_front() {
                            pending.push(Box::pin(guid_future(Arc::clone(lz), guid, 0)));
                        }
                    }
                    if let Some(hn) = self.hypernative.as_ref() {
                        while let Some(evm) = evm_queue.pop_front() {
                            pending.push(Box::pin(evm_future(Arc::clone(hn), Arc::clone(&self.saver), evm, 0)));
                        }
                    }
                }
            }
        }
        warn!("evm_fetch_loop: exited");
    }

    // -----------------------------------------------------------------------
    // Startup seed
    // -----------------------------------------------------------------------

    async fn load_pending_evms(&self) -> VecDeque<String> {
        let pool = match self.db_pool.as_ref() {
            Some(p) => p,
            None => return VecDeque::new(),
        };
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(err = ?e, "evm_fetch_loop: failed to get DB connection for EVM seed");
                return VecDeque::new();
            },
        };
        match sql_query(
            // Unscreened: in bridge_inflows but absent from the risk-score table.
            "SELECT DISTINCT evm_source \
               FROM bridge_inflows \
              WHERE evm_source IS NOT NULL \
                AND evm_source != $1 \
                AND evm_source NOT IN (SELECT evm_address FROM evm_address_risk_scores) \
             UNION \
             -- Previously failed: risk_score = 0 means the Hypernative fetch errored. \
             SELECT evm_address AS evm_source \
               FROM evm_address_risk_scores \
              WHERE risk_score = 0",
        )
        .bind::<Varchar, _>(EVM_NULL_SENTINEL)
        .get_results::<EvmRow>(&mut conn)
        .await
        {
            Ok(rows) => rows.into_iter().map(|r| r.evm_source).collect(),
            Err(e) => {
                error!(err = ?e, "evm_fetch_loop: failed to load pending EVMs; starting empty");
                VecDeque::new()
            },
        }
    }
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
                Some(RetryItem::Evm { evm, count: 0 })
            },
            Ok(None) => {
                // Permanent 404: sentinel written to DB, no retry.
                None
            },
            Err(_) => Some(RetryItem::Guid {
                guid,
                count: count + 1,
            }),
        }
    })
}

fn evm_future(
    hn: Arc<HypernativeClient>,
    saver: Arc<dyn ScoreSaver>,
    evm: String,
    count: u32,
) -> BoxFuture<'static, Option<RetryItem>> {
    Box::pin(async move {
        if screen_evm(&hn, saver.as_ref(), &evm).await {
            Some(RetryItem::Evm {
                evm,
                count: count + 1,
            })
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// Score computation — pure function, no I/O, safe to unit-test without a DB
// ---------------------------------------------------------------------------

/// Compute a `EvmRiskScore` from a Hypernative response (or `None` on failure).
///
/// Score formula: `recommendation_value + severity_value`
/// - deny:   1.0  (→ total always > 1)
/// - approve: 0.1 (→ total < 1 except when severity is high)
/// - high:   0.9, medium: 0.6, low: 0.4, info / unknown: 0.1
/// - On fetch failure or no data: score = 0, fields = "not available"
pub fn compute_risk_score(
    evm: &str,
    result: Option<&HypernativeResult>,
    source: &str,
) -> EvmRiskScore {
    let (recommendation, severity, risk_score) = match result {
        Some(r) => {
            let rec = if r.recommendation.eq_ignore_ascii_case("deny") {
                BigDecimal::from_str("1.0").unwrap()
            } else {
                BigDecimal::from_str("0.1").unwrap()
            };
            let sev = match r.severity.to_lowercase().as_str() {
                "high" => BigDecimal::from_str("0.9").unwrap(),
                "medium" => BigDecimal::from_str("0.6").unwrap(),
                "low" => BigDecimal::from_str("0.4").unwrap(),
                _ => BigDecimal::from_str("0.1").unwrap(),
            };
            (r.recommendation.clone(), r.severity.clone(), rec + sev)
        },
        None => (
            "not available".to_string(),
            "not available".to_string(),
            BigDecimal::from(0),
        ),
    };
    EvmRiskScore {
        evm_address: evm.to_string(),
        risk_score,
        risk_label: "hypernative".to_string(),
        source: source.to_string(),
        recommendation,
        severity,
        fetched_at: chrono::Utc::now().naive_utc(),
    }
}

// ---------------------------------------------------------------------------
// DB persistence — separated from computation so tests can mock this layer
// ---------------------------------------------------------------------------

pub async fn save_risk_score(pool: &ArcDbPool, score: &EvmRiskScore) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    sql_query(
        "INSERT INTO evm_address_risk_scores \
            (evm_address, risk_score, risk_label, source, recommendation, severity, fetched_at, inserted_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) \
         ON CONFLICT (evm_address) DO UPDATE SET \
            risk_score     = EXCLUDED.risk_score, \
            risk_label     = EXCLUDED.risk_label, \
            source         = EXCLUDED.source, \
            recommendation = EXCLUDED.recommendation, \
            severity       = EXCLUDED.severity, \
            fetched_at     = EXCLUDED.fetched_at",
    )
    .bind::<Varchar, _>(&score.evm_address)
    .bind::<Numeric, _>(&score.risk_score)
    .bind::<Varchar, _>(&score.risk_label)
    .bind::<Varchar, _>(&score.source)
    .bind::<Varchar, _>(&score.recommendation)
    .bind::<Varchar, _>(&score.severity)
    .bind::<Timestamp, _>(score.fetched_at)
    .execute(&mut conn)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Orchestration — fetch → compute → save → log
// ---------------------------------------------------------------------------

/// Fetch the Hypernative reputation for `evm`, compute its score, persist it,
/// and log the result. Returns `true` if the fetch failed and should be retried.
async fn screen_evm(hn: &HypernativeClient, saver: &dyn ScoreSaver, evm: &str) -> bool {
    if evm == EVM_NULL_SENTINEL {
        return false;
    }

    let (result, should_retry) = match hn.fetch(evm).await {
        Ok(r) => (r, false),
        Err(e) => {
            warn!(evm_address = %evm, err = %e, "evm_fetch_loop: hypernative fetch failed");
            (None, true)
        },
    };

    let score = compute_risk_score(evm, result.as_ref(), hn.screener_url());

    if let Err(e) = saver.save(&score).await {
        warn!(evm_address = %evm, err = %e, "evm_fetch_loop: failed to save risk score");
    }

    info!(
        evm_address    = %score.evm_address,
        risk_score     = %score.risk_score,
        risk_label     = %score.risk_label,
        recommendation = %score.recommendation,
        severity       = %score.severity,
        source         = %score.source,
        fetched_at     = %score.fetched_at,
        "evm_fetch_loop: risk score saved"
    );

    should_retry
}
