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

pub use super::lz_storer::LzDb;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tracing::warn;

/// Sentinel written when LZ confirms the GUID does not exist (HTTP 404).
/// Prevents infinite retries for packets that never existed in the LZ DB.
pub const EVM_NULL_SENTINEL: &str = "0x0000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// LzEnricher — stateless helper used by the enricher loop
// ---------------------------------------------------------------------------

pub struct LzEnricher {
    db: Arc<dyn LzDb>,
    http: reqwest::Client,
    scan_api_base_url: String,
}

impl LzEnricher {
    pub fn new(db: Arc<dyn LzDb>, scan_api_base_url: String) -> Self {
        Self {
            db,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()
                .expect("failed to build reqwest::Client"),
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
                self.db.write_evm(guid, &evm).await;
                Ok(Some(evm))
            },
            Ok(None) => {
                self.db.write_evm(guid, EVM_NULL_SENTINEL).await;
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
        self.db.load_pending_guids().await
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
            warn!(lz_guid = guid, url = %url, "lz_enricher: GUID not found (404) — sentinel will be written");
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
            .map(crate::processors::address_reputation::standardize_evm_address);

        match evm {
            Some(addr) => Ok(Some(addr)),
            None => anyhow::bail!("sender field absent in LZ response (packet may be undelivered)"),
        }
    }
}
