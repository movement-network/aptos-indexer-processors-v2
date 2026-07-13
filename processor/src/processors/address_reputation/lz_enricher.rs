// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Background task that resolves LayerZero GUIDs → EVM depositor addresses.
//!
//! At startup the queue is seeded from DB rows where `lz_guid IS NOT NULL AND
//! evm_source IS NULL`. New GUIDs stream in via an mpsc channel from the
//! extractor. Items are dequeued at a configurable rate and resolved against the
//! LZ Scan API (`https://scan.layerzero-api.com/v1/messages/guid/{guid}`).
//!
//! Resolution outcomes:
//! - `Ok(Some(evm))` → update `bridge_inflows.evm_source`; seed `address_evm_sources`.
//! - `Ok(None)`      → GUID absent in LZ DB (HTTP 404); write the NULL sentinel
//!   (`0x000…000`) so the row is never retried.
//! - `Err(_)`        → transient failure; retry up to `max_retries` times with
//!   exponential backoff. After max retries, log and leave `evm_source` NULL so
//!   the next processor startup re-enqueues the row.

use super::address_reputation_storer::{seen_ord, upsert_bridge_seed};
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{BigInt, Nullable, Numeric, Varchar},
};
use diesel_async::RunQueryDsl;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info, warn};

const LZ_SCAN_BASE: &str = "https://scan.layerzero-api.com/v1/messages/guid";

/// Sentinel written when LZ confirms the GUID does not exist (HTTP 404).
/// Prevents infinite retries for packets that never existed in the LZ DB.
const EVM_NULL_SENTINEL: &str = "0x0000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// Diesel query result types
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName)]
struct GuidRow {
    #[diesel(sql_type = Varchar)]
    lz_guid: String,
}

#[derive(diesel::QueryableByName)]
struct InflowRow {
    #[diesel(sql_type = Varchar)]
    aptos_recipient: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    asset_type: Option<String>,
    #[diesel(sql_type = Numeric)]
    amount: BigDecimal,
    #[diesel(sql_type = BigInt)]
    transaction_version: i64,
    #[diesel(sql_type = BigInt)]
    event_index: i64,
}

// ---------------------------------------------------------------------------
// LzEnricher
// ---------------------------------------------------------------------------

pub struct LzEnricher {
    db_pool: ArcDbPool,
    receiver: UnboundedReceiver<String>,
    http: reqwest::Client,
    interval: Duration,
    max_retries: u32,
    propagate_evm: bool,
}

impl LzEnricher {
    pub fn new(
        db_pool: ArcDbPool,
        receiver: UnboundedReceiver<String>,
        interval_ms: u64,
        max_retries: u32,
        propagate_evm: bool,
    ) -> Self {
        Self {
            db_pool,
            receiver,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("failed to build reqwest::Client"),
            interval: Duration::from_millis(interval_ms),
            max_retries,
            propagate_evm,
        }
    }

    pub async fn run(mut self) {
        let mut queue: VecDeque<String> = self.load_pending().await;
        info!(
            pending = queue.len(),
            "lz_enricher: started, seeded from DB"
        );

        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                msg = self.receiver.recv() => match msg {
                    Some(guid) => queue.push_back(guid),
                    None => {
                        // Sender dropped — process whatever is left then exit.
                        info!(remaining = queue.len(), "lz_enricher: channel closed, draining queue");
                        while let Some(guid) = queue.pop_front() {
                            self.process_guid(&guid).await;
                        }
                        info!("lz_enricher: done");
                        return;
                    }
                },
                _ = ticker.tick() => {
                    if let Some(guid) = queue.pop_front() {
                        self.process_guid(&guid).await;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Per-GUID processing
    // -----------------------------------------------------------------------

    async fn process_guid(&self, guid: &str) {
        let mut last_err = String::new();
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                // 1 s, 2 s, 4 s backoff
                tokio::time::sleep(Duration::from_secs(1u64 << (attempt - 1))).await;
            }
            match self.fetch_evm(guid).await {
                Ok(Some(evm)) => {
                    self.write_evm(guid, &evm).await;
                    return;
                },
                Ok(None) => {
                    // GUID definitively absent in LZ — record sentinel.
                    self.write_evm(guid, EVM_NULL_SENTINEL).await;
                    return;
                },
                Err(e) => {
                    last_err = e.to_string();
                    warn!(lz_guid = guid, attempt, err = %last_err, "lz_enricher: transient error");
                },
            }
        }
        error!(
            lz_guid = guid,
            err = %last_err,
            retries = self.max_retries,
            "lz_enricher: giving up after max retries; evm_source stays NULL"
        );
    }

    // -----------------------------------------------------------------------
    // LZ Scan API fetch
    // -----------------------------------------------------------------------

    /// Returns:
    /// - `Ok(Some(evm))` — resolved depositor EVM address
    /// - `Ok(None)`      — HTTP 404: GUID not in LZ DB (write sentinel)
    /// - `Err(_)`        — transient failure (network, 5xx, 429, empty data) → retry
    async fn fetch_evm(&self, guid: &str) -> anyhow::Result<Option<String>> {
        let url = format!("{}/{}", LZ_SCAN_BASE, guid);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("network: {e}"))?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            anyhow::bail!("HTTP {}", status);
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("json: {e}"))?;

        // Response shape: { "data": [ { "source": { "tx": { "from": "0x…" } }, … } ] }
        let evm = body
            .get("data")
            .and_then(|d| d.get(0))
            .and_then(|msg| msg.get("source"))
            .and_then(|s| s.get("tx"))
            .and_then(|tx| tx.get("from"))
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        match evm {
            Some(addr) => Ok(Some(addr)),
            // data[0].source.tx.from missing — packet may be in-flight or
            // the API schema changed. Treat as transient so we retry.
            None => anyhow::bail!("sender field absent in LZ response (packet may be undelivered)"),
        }
    }

    // -----------------------------------------------------------------------
    // DB write
    // -----------------------------------------------------------------------

    async fn write_evm(&self, guid: &str, evm: &str) {
        let mut conn = match self.db_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to get DB connection");
                return;
            },
        };

        // Atomically update bridge_inflows and return the affected rows so we
        // can seed address_evm_sources in the same DB round-trip.
        let rows: Vec<InflowRow> = match sql_query(
            "UPDATE bridge_inflows \
               SET evm_source = $1 \
             WHERE lz_guid = $2 AND evm_source IS NULL \
             RETURNING aptos_recipient, asset_type, amount, \
                       transaction_version, event_index",
        )
        .bind::<Varchar, _>(evm)
        .bind::<Varchar, _>(guid)
        .get_results(&mut conn)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to update bridge_inflows");
                return;
            },
        };

        if rows.is_empty() {
            // Already resolved by a concurrent enricher or the row is gone.
            return;
        }

        if !self.propagate_evm || evm == EVM_NULL_SENTINEL {
            return;
        }

        // Seed address_evm_sources for each updated inflow that has a known asset.
        for row in &rows {
            let Some(ref asset) = row.asset_type else {
                continue;
            };
            let ord = seen_ord(row.transaction_version, row.event_index);
            if let Err(e) = upsert_bridge_seed(
                &mut conn,
                &row.aptos_recipient,
                asset,
                evm,
                &row.amount,
                ord,
            )
            .await
            {
                warn!(lz_guid = guid, err = ?e, "lz_enricher: failed to seed address_evm_sources");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Startup seed
    // -----------------------------------------------------------------------

    async fn load_pending(&self) -> VecDeque<String> {
        let mut conn = match self.db_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(err = ?e, "lz_enricher: failed to get DB connection for startup seed");
                return VecDeque::new();
            },
        };
        match sql_query(
            "SELECT lz_guid FROM bridge_inflows \
              WHERE lz_guid IS NOT NULL AND evm_source IS NULL",
        )
        .get_results::<GuidRow>(&mut conn)
        .await
        {
            Ok(rows) => rows.into_iter().map(|r| r.lz_guid).collect(),
            Err(e) => {
                error!(err = ?e, "lz_enricher: failed to load pending GUIDs; starting empty");
                VecDeque::new()
            },
        }
    }
}
