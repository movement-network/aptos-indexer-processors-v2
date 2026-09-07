// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Hypernative address-reputation screener client, score computation, and persistence.
//!
//! ## Rate limiting
//! The client enforces two independent limits:
//! - **Concurrency** (`max_concurrent_requests`): at most N in-flight HTTP calls at once.
//! - **RPS** (`max_rps`): at most N requests per second via a 1-second sliding window.
//!
//! On an HTTP 429 the client drops its concurrency permit, then spawns a background
//! task that drains the semaphore (`acquire_many(max_concurrent)`) and sleeps for
//! `BACKOFF_429` before releasing. Any subsequent `fetch_batch` call blocks until the
//! drain completes and the sleep ends, providing a hard server-side back-pressure signal.
//!
//! ## Duplicate / TTL deduplication
//! The client keeps a bounded `moka` cache of recently-screened addresses (keyed by
//! lowercase address, value `()`). After the rate-limiter permit is acquired, every
//! address in the batch is checked against the cache:
//! - **In cache** → marked `duplicate = true`; a placeholder result is returned so
//!   callers know to skip saving (not retry). No API call is made.
//! - **Not in cache** → included in the Hypernative request.
//!
//! Entries are inserted only after a successful parse, and only for addresses
//! that Hypernative actually returned in `data`. A failed request or an address
//! absent from the response stays uncached and retryable. `moka` enforces the
//! TTL expiry and a maximum capacity automatically — no manual eviction needed.

pub use super::evm_storer::{DbScoreSaver, EvmScreeningDb};
use super::{super::address_reputation_model::EvmRiskScore, lz_enricher::EVM_NULL_SENTINEL};
use bigdecimal::BigDecimal;
use std::{
    collections::VecDeque,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

/// Pause duration after an HTTP 429 response (production default).
const BACKOFF_429: Duration = Duration::from_secs(60);
/// Maximum number of entries held in the dedup cache at any time.
const CACHE_MAX_CAPACITY: u64 = 100_000;

// ---------------------------------------------------------------------------
// Rate limiter
// ---------------------------------------------------------------------------

struct RateLimiter {
    /// Semaphore that bounds in-flight requests. Draining it (acquire_many)
    /// pauses all subsequent callers until the drain is released.
    semaphore: Arc<Semaphore>,
    /// Total permits issued at construction (= effective max_concurrent).
    max_concurrent: u32,
    /// Rolling window of request start timestamps for RPS enforcement.
    rps_window: Mutex<VecDeque<Instant>>,
    /// Maximum requests per second; 0 means unlimited.
    max_rps: u32,
}

impl RateLimiter {
    fn new(max_concurrent: u32, max_rps: u32) -> Self {
        // 0 → effectively unlimited.
        let permits = if max_concurrent == 0 {
            10_000
        } else {
            max_concurrent
        };
        Self {
            semaphore: Arc::new(Semaphore::new(permits as usize)),
            max_concurrent: permits,
            rps_window: Mutex::new(VecDeque::new()),
            max_rps,
        }
    }

    /// Wait for both an RPS slot and a concurrency permit, then return the permit.
    /// The returned permit must be dropped to release the concurrency slot.
    async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        if self.max_rps > 0 {
            self.wait_for_rps_slot().await;
        }
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("Hypernative semaphore was closed")
    }

    /// Block until the sliding window has room for one more request this second,
    /// then record the request timestamp and return.
    async fn wait_for_rps_slot(&self) {
        loop {
            let mut window = self.rps_window.lock().await;
            let now = Instant::now();
            // Evict timestamps that have left the 1-second window.
            while window
                .front()
                .is_some_and(|&t| now.duration_since(t) >= Duration::from_secs(1))
            {
                window.pop_front();
            }
            if (window.len() as u32) < self.max_rps {
                window.push_back(now);
                return;
            }
            // Sleep until the oldest timestamp rolls out of the window.
            let oldest = *window.front().unwrap();
            let sleep = Duration::from_secs(1)
                .checked_sub(now.duration_since(oldest))
                .unwrap_or(Duration::from_millis(1));
            drop(window);
            tokio::time::sleep(sleep).await;
        }
    }

    /// Drain the semaphore for `duration`, blocking all new requests.
    ///
    /// Waits until all currently in-flight requests release their permits
    /// (via `acquire_many`), sleeps for `duration`, then releases all permits.
    /// Callers waiting on `acquire()` resume automatically when the drain ends.
    async fn pause(&self, duration: Duration) {
        let _all = Arc::clone(&self.semaphore)
            .acquire_many_owned(self.max_concurrent)
            .await
            .expect("Hypernative semaphore was closed");
        tokio::time::sleep(duration).await;
        // _all is dropped here, restoring all permits.
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Fields extracted from a Hypernative screening response.
///
/// When `duplicate` is `true` the address was already screened within the TTL
/// window: no actual API call was made for it, and all other fields are empty.
/// Callers should skip computing / saving a score for duplicate results.
#[derive(Debug)]
pub struct HypernativeResult {
    pub address: String,
    pub recommendation: String,
    pub severity: String,
    pub total_incoming_usd: Option<f64>,
    pub total_outgoing_usd: Option<f64>,
    pub policy_id: Option<String>,
    pub screened_at: Option<String>,
    /// `true` if this address was suppressed by the TTL cache.
    pub duplicate: bool,
}

pub struct HypernativeClient {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    screener_policy_id: Option<String>,
    screener_url: String,
    rate_limiter: Arc<RateLimiter>,
    /// Bounded TTL cache of successfully-screened addresses (lowercase key, unit value).
    /// Entries are inserted only after a confirmed successful API response; `moka`
    /// handles expiry (TTL) and capacity eviction automatically.
    cache: moka::future::Cache<String, ()>,
    backoff_429: Duration,
}

impl HypernativeClient {
    pub fn screener_url(&self) -> &str {
        &self.screener_url
    }

    pub fn new(
        client_id: String,
        client_secret: String,
        screener_policy_id: Option<String>,
        screener_url: String,
        max_concurrent_requests: u32,
        max_rps: u32,
        ttl_secs: u64,
    ) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("failed to build reqwest::Client for Hypernative"),
            client_id,
            client_secret,
            screener_policy_id,
            screener_url,
            rate_limiter: Arc::new(RateLimiter::new(max_concurrent_requests, max_rps)),
            cache: moka::future::Cache::builder()
                .max_capacity(CACHE_MAX_CAPACITY)
                .time_to_live(Duration::from_secs(ttl_secs))
                // Evict expired address during get.
                .time_to_idle(Duration::from_secs(ttl_secs))
                .build(),
            backoff_429: BACKOFF_429,
        }
    }

    /// Override the 429 backoff duration (useful in tests to avoid 60 s waits).
    pub fn with_backoff_429(mut self, dur: Duration) -> Self {
        self.backoff_429 = dur;
        self
    }

    /// Check that the API is reachable and credentials are valid.
    /// Returns Ok on any HTTP response (even a non-200), Err on network failure
    /// or authentication rejection (401/403). Called once before the main loop
    /// starts; subsequent errors after a successful ping use the retry mechanism.
    pub async fn ping(&self) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(&self.screener_url)
            .header("Content-Type", "application/json")
            .header("x-client-id", &self.client_id)
            .header("x-client-secret", &self.client_secret)
            .json(&serde_json::json!({"addresses": ["0x0000000000000000000000000000000000000000"]}))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("network error: {e}"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            anyhow::bail!("Hypernative authentication failed ({status})");
        }
        Ok(())
    }

    /// Screen a batch of EVM addresses in one API call.
    ///
    /// After acquiring a rate-limiter slot, addresses already present in the TTL
    /// cache are returned as `HypernativeResult { duplicate: true }` without hitting
    /// the API. Only uncached addresses are sent to Hypernative. On success, only
    /// addresses present in the parsed `data` array are cached; addresses the API
    /// omitted stay uncached so they can be retried. On any failure the whole
    /// requested set remains uncached.
    pub async fn fetch_batch(&self, addresses: &[&str]) -> anyhow::Result<Vec<HypernativeResult>> {
        let permit = self.rate_limiter.acquire().await;

        // Single pass: cache hits become duplicate placeholders, misses go to the API.
        let mut results: Vec<HypernativeResult> = Vec::new();
        let mut to_process: Vec<&str> = Vec::new();
        for &addr in addresses {
            if self.cache.get(&addr.to_lowercase()).await.is_some() {
                results.push(HypernativeResult {
                    address: addr.to_string(),
                    recommendation: String::new(),
                    severity: String::new(),
                    total_incoming_usd: None,
                    total_outgoing_usd: None,
                    policy_id: None,
                    screened_at: None,
                    duplicate: true,
                });
            } else {
                to_process.push(addr);
            }
        }

        if to_process.is_empty() {
            return Ok(results);
        }

        let mut body = serde_json::json!({ "addresses": to_process });
        if let Some(ref policy_id) = self.screener_policy_id {
            body["screenerPolicyId"] = serde_json::Value::String(policy_id.clone());
        }

        let resp = match self
            .http
            .post(&self.screener_url)
            .header("Content-Type", "application/json")
            .header("x-client-id", &self.client_id)
            .header("x-client-secret", &self.client_secret)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return Err(anyhow::anyhow!("network: {e}")),
        };

        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            drop(permit); // Release before draining so the pause task can proceed.
            let rl = Arc::clone(&self.rate_limiter);
            let backoff = self.backoff_429;
            tokio::spawn(async move {
                warn!(
                    backoff_secs = backoff.as_secs_f64(),
                    "hypernative: 429 Too Many Requests — pausing all requests"
                );
                rl.pause(backoff).await;
            });
            anyhow::bail!("HTTP 429 Too Many Requests");
        }

        if !status.is_success() {
            anyhow::bail!("HTTP {status}");
        }

        let json = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| anyhow::anyhow!("json: {e}"))?;

        let data = match json.get("data").and_then(|d| d.as_array()) {
            Some(arr) => arr,
            None => {
                warn!(
                    evm_count = to_process.len(),
                    "hypernative: response missing 'data' array — dropping batch without retry"
                );
                return Ok(results);
            },
        };

        // Parse all entries before caching: if any entry is malformed we return
        // Err without caching so the addresses remain retryable.
        let mut new_results = Vec::with_capacity(data.len());
        for entry in data {
            let get_str = |key: &str| entry.get(key).and_then(|v| v.as_str()).map(str::to_owned);
            let get_f64 = |key: &str| entry.get(key).and_then(|v| v.as_f64());
            let address = get_str("address").unwrap_or_default();
            let recommendation = get_str("recommendation").ok_or_else(|| {
                anyhow::anyhow!(
                    "malformed Hypernative response: 'recommendation' missing for {address}"
                )
            })?;
            let severity = get_str("severity").ok_or_else(|| {
                anyhow::anyhow!("malformed Hypernative response: 'severity' missing for {address}")
            })?;
            new_results.push(HypernativeResult {
                address,
                recommendation,
                severity,
                total_incoming_usd: get_f64("totalIncomingUsd"),
                total_outgoing_usd: get_f64("totalOutgoingUsd"),
                policy_id: get_str("policyId"),
                screened_at: get_str("timestamp"),
                duplicate: false,
            });
        }

        // Cache only addresses Hypernative actually returned. Caching the whole
        // requested set would suppress retries for addresses omitted from `data`.
        for result in &new_results {
            if !result.address.is_empty() {
                self.cache.insert(result.address.to_lowercase(), ()).await;
            }
        }
        results.extend(new_results);

        // permit drops here, freeing the concurrency slot.
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Score computation — pure function, no I/O, safe to unit-test without a DB
// ---------------------------------------------------------------------------

/// Compute a `EvmRiskScore` from a Hypernative response (or `None` on failure).
///
/// Build an `EvmRiskScore` with `risk_score = 0` representing a fetch error
/// (Hypernative unavailable at the time the address was received).
pub fn error_score(evm: &str) -> EvmRiskScore {
    EvmRiskScore {
        evm_address: evm.to_string(),
        risk_score: BigDecimal::from(0),
        risk_label: "hypernative".to_string(),
        source: "not available".to_string(),
        recommendation: "not available".to_string(),
        severity: "not available".to_string(),
        fetched_at: chrono::Utc::now().naive_utc(),
        to_be_updated: true,
    }
}

/// Score formula: `recommendation_value + severity_value`
/// - deny: 1.0, approve/other: 0.1, not available: 0.0
/// - high: 0.9, medium: 0.6, low: 0.4, other/not available: 0.1
/// - N/A recommendation from Hypernative: 0.0 + 0.1 = 0.1
/// - address absent from the response uses `error_score` (risk_score = 0) instead
/// - error (stored separately as risk_score = 0 in DB)
pub fn compute_risk_score(evm: &str, result: &HypernativeResult, source: &str) -> EvmRiskScore {
    let rec = if result.recommendation.eq_ignore_ascii_case("deny") {
        BigDecimal::from_str("1.0").unwrap()
    } else if result.recommendation.eq_ignore_ascii_case("not available") {
        BigDecimal::from(0)
    } else {
        BigDecimal::from_str("0.1").unwrap()
    };
    let sev = match result.severity.to_lowercase().as_str() {
        "high" => BigDecimal::from_str("0.9").unwrap(),
        "medium" => BigDecimal::from_str("0.6").unwrap(),
        "low" => BigDecimal::from_str("0.4").unwrap(),
        _ => BigDecimal::from_str("0.1").unwrap(),
    };
    EvmRiskScore {
        evm_address: evm.to_string(),
        risk_score: rec + sev,
        risk_label: "hypernative".to_string(),
        source: source.to_string(),
        recommendation: result.recommendation.clone(),
        severity: result.severity.clone(),
        fetched_at: chrono::Utc::now().naive_utc(),
        to_be_updated: false,
    }
}

// ---------------------------------------------------------------------------
// Orchestration — fetch → compute → save → log
// ---------------------------------------------------------------------------

/// Screen a batch of EVM addresses: one Hypernative API call, compute and
/// persist a score for each address in the batch, log every result.
/// Returns `true` if the API call itself failed and the whole batch should be
/// retried. Addresses absent from the response get `error_score` (`risk_score = 0`,
/// `to_be_updated = true`) so they stay pending instead of a finished 0.1 score.
pub async fn screen_evms(
    hn: &HypernativeClient,
    saver: &dyn EvmScreeningDb,
    evms: &[String],
) -> bool {
    let addrs: Vec<&str> = evms
        .iter()
        .filter(|e| e.as_str() != EVM_NULL_SENTINEL)
        .map(String::as_str)
        .collect();

    if addrs.is_empty() {
        return false;
    }

    let results = match hn.fetch_batch(&addrs).await {
        Ok(r) => r,
        Err(e) => {
            warn!(evm_count = addrs.len(), err = %e, "evm_fetch_loop: hypernative batch fetch failed");
            return true;
        },
    };

    for evm in &addrs {
        let result = results.iter().find(|r| r.address.eq_ignore_ascii_case(evm));

        // Skip addresses suppressed by the TTL cache.
        if result.is_some_and(|r| r.duplicate) {
            info!(evm_address = %evm, "evm_fetch_loop: skipping in-flight duplicate");
            continue;
        }

        let score = match result {
            Some(r) => compute_risk_score(evm, r, hn.screener_url()),
            None => {
                warn!(
                    evm_address = %evm,
                    "evm_fetch_loop: evm address not return by Hypernative API call."
                );
                error_score(evm)
            },
        };
        if let Err(e) = saver.save(&score).await {
            tracing::error!(evm_address = %evm, err = %e, "evm_fetch_loop: failed to save risk score");
        } else {
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
        }
    }

    false
}
