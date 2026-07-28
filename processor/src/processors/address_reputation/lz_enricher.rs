// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! LayerZero GUID → EVM depositor resolver.
//!
//! Resolves a 32-byte LZ message GUID to the EVM address that initiated the
//! cross-chain transfer by querying the LZ Scan API.
//!
//! Resolution outcomes:
//! - `Ok(Some(evm))` → update `bridge_inflows.evm_source`; seed `address_evm_sources`.
//! - `Ok(None)`      → GUID absent in LZ DB (HTTP 404); write the NULL sentinel
//!   (`EVM_NULL_SENTINEL`) so the row is never retried.
//! - `Err(_)`        → transient failure; retry up to `max_retries` times with
//!   exponential backoff.
//!
//! The main event loop lives in `evm_fetch_loop.rs`; this module is pure LZ logic.

use super::address_reputation_storer::{seen_ord, upsert_bridge_seed};
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{BigInt, Nullable, Numeric, Varchar},
};
use diesel_async::RunQueryDsl;
use std::{collections::VecDeque, time::Duration};
use tracing::{error, warn};

/// Sentinel written when LZ confirms the GUID does not exist (HTTP 404).
/// Prevents infinite retries for packets that never existed in the LZ DB.
pub const EVM_NULL_SENTINEL: &str = "0x0000000000000000000000000000000000000000";

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
// LzEnricher — stateless helper used by the enricher loop
// ---------------------------------------------------------------------------

pub struct LzEnricher {
    db_pool: Option<ArcDbPool>,
    http: reqwest::Client,
    propagate_evm: bool,
    scan_api_base_url: String,
}

impl LzEnricher {
    pub fn new(db_pool: Option<ArcDbPool>, propagate_evm: bool, scan_api_base_url: String) -> Self {
        Self {
            db_pool,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .expect("failed to build reqwest::Client"),
            propagate_evm,
            scan_api_base_url,
        }
    }

    /// Resolve a GUID: query LZ Scan API, update `bridge_inflows`, seed
    /// `address_evm_sources`. Single attempt — retry logic lives in the caller.
    ///
    /// Returns:
    /// - `Ok(Some(evm))` — resolved; caller may forward EVM to Hypernative.
    /// - `Ok(None)`      — permanent 404; sentinel written; no retry needed.
    /// - `Err(_)`        — transient failure; caller should schedule a retry.
    pub async fn process_guid(&self, guid: &str) -> anyhow::Result<Option<String>> {
        match self.fetch_evm(guid).await {
            Ok(Some(evm)) => {
                self.write_evm(guid, &evm).await;
                Ok(Some(evm))
            },
            Ok(None) => {
                self.write_evm(guid, EVM_NULL_SENTINEL).await;
                Ok(None)
            },
            Err(e) => {
                warn!(lz_guid = guid, err = %e, "lz_enricher: fetch failed");
                Err(e)
            },
        }
    }

    /// Load all unresolved LZ GUIDs from DB at startup.
    pub async fn load_pending_guids(&self) -> VecDeque<String> {
        let pool = match self.db_pool.as_ref() {
            Some(p) => p,
            None => return VecDeque::new(),
        };
        let mut conn = match pool.get().await {
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

    // -----------------------------------------------------------------------
    // LZ Scan API
    // -----------------------------------------------------------------------

    async fn fetch_evm(&self, guid: &str) -> anyhow::Result<Option<String>> {
        let url = format!("{}/{}", self.scan_api_base_url, guid);
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
            anyhow::bail!("LZ HTTP NOK status {} for GUI {}", status, guid);
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("json: {e}"))?;

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
            None => anyhow::bail!("sender field absent in LZ response (packet may be undelivered)"),
        }
    }

    // -----------------------------------------------------------------------
    // DB write
    // -----------------------------------------------------------------------

    async fn write_evm(&self, guid: &str, evm: &str) {
        let pool = match self.db_pool.as_ref() {
            Some(p) => p,
            None => return,
        };
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to get DB connection");
                return;
            },
        };

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

        if rows.is_empty() || !self.propagate_evm || evm == EVM_NULL_SENTINEL {
            return;
        }

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
}
