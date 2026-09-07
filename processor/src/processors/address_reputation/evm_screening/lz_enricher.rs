// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! LayerZero GUID → EVM depositor resolver.
//!
//! Resolves a 32-byte LZ message GUID to the EVM address that initiated the
//! cross-chain transfer by querying the LZ Scan API.
//!
//! Resolution outcomes:
//! - `Ok(Some(evm))` → update `bridge_inflows.evm_source`; seed `address_evm_sources`.
//! - `Err(_)`        → transient failure (HTTP errors including 404, network,
//!   missing sender); retry up to `max_retries` times.
//!
//! HTTP 404 is **not** permanent. GUIDs are extracted from a Movement
//! `lz_receive`, so the packet exists; Scan often 404s until it indexes.
//! Writing `EVM_NULL_SENTINEL` on 404 would mark `evm_source` and
//! `load_pending_guids` would never retry.
//!
//! The main event loop lives in `evm_fetch_loop.rs`; this module is pure LZ logic.

pub use super::lz_storer::LzDb;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tracing::warn;

/// Sentinel stored in `bridge_inflows.evm_source` when a GUID is known to
/// have no EVM depositor. Must not be written on a transient LZ Scan 404 —
/// those packets exist (we just indexed the Movement receive) and will
/// resolve once Scan catches up.
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
    /// - `Err(_)`        — transient failure (including Scan 404); caller retries.
    pub async fn process_guid(&self, guid: &str) -> anyhow::Result<Option<String>> {
        match self.fetch_evm(guid).await {
            Ok(evm) => {
                self.db.write_evm(guid, &evm).await;
                Ok(Some(evm))
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

    async fn fetch_evm(&self, guid: &str) -> anyhow::Result<String> {
        let url = format!("{}/{}", self.scan_api_base_url, guid);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("network: {e}"))?;

        let status = resp.status();
        if !status.is_success() {
            // 404 included: Scan lags the Movement receive we just indexed.
            anyhow::bail!("LZ HTTP NOK status {} for GUID {}", status, guid);
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
            Some(addr) => Ok(addr),
            None => anyhow::bail!("sender field absent in LZ response (packet may be undelivered)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use wiremock::{
        matchers::{method, path_regex},
        Mock, MockServer, ResponseTemplate,
    };

    struct RecordingLzDb {
        writes: Mutex<Vec<(String, String)>>,
    }

    impl RecordingLzDb {
        fn new() -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
            }
        }

        fn writes(&self) -> Vec<(String, String)> {
            self.writes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LzDb for RecordingLzDb {
        async fn load_pending_guids(&self) -> VecDeque<String> {
            VecDeque::new()
        }

        async fn write_evm(&self, guid: &str, evm: &str) {
            self.writes
                .lock()
                .unwrap()
                .push((guid.to_string(), evm.to_string()));
        }
    }

    const TEST_GUID: &str = "0xe97fc9204872ba072f8d1a647d7045b881d20ff69aff1f050a0b05e8fb83228e";
    const TEST_EVM: &str = "0x97e6a34897a32e7103f3cf260f0c9ca5ca1fb90b";

    fn lz_success_body(evm: &str) -> serde_json::Value {
        serde_json::json!({
            "data": [{
                "source": { "tx": { "from": evm } }
            }]
        })
    }

    #[tokio::test]
    async fn process_guid_404_is_retryable_err_and_does_not_write_sentinel() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(".*"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let db = Arc::new(RecordingLzDb::new());
        let lz = LzEnricher::new(db.clone(), server.uri());
        let err = lz
            .process_guid(TEST_GUID)
            .await
            .expect_err("404 must be a retryable Err, not Ok(None)");
        assert!(
            err.to_string().contains("404"),
            "error should mention 404, got {err}"
        );
        assert!(
            db.writes().is_empty(),
            "404 must not write evm_source (sentinel would block load_pending_guids): {:?}",
            db.writes()
        );
    }

    #[tokio::test]
    async fn process_guid_success_writes_resolved_evm() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(".*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lz_success_body(TEST_EVM)))
            .mount(&server)
            .await;

        let db = Arc::new(RecordingLzDb::new());
        let lz = LzEnricher::new(db.clone(), server.uri());
        let evm = lz
            .process_guid(TEST_GUID)
            .await
            .expect("200 with sender must succeed")
            .expect("resolved EVM");
        assert_eq!(evm, TEST_EVM);
        assert_eq!(db.writes(), vec![(
            TEST_GUID.to_string(),
            TEST_EVM.to_string()
        )]);
    }

    #[tokio::test]
    async fn process_guid_404_then_success_writes_evm_not_sentinel() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(".*"))
            .respond_with(ResponseTemplate::new(404))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(".*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(lz_success_body(TEST_EVM)))
            .mount(&server)
            .await;

        let db = Arc::new(RecordingLzDb::new());
        let lz = LzEnricher::new(db.clone(), server.uri());

        lz.process_guid(TEST_GUID)
            .await
            .expect_err("first 404 is retryable");
        assert!(db.writes().is_empty());

        let evm = lz
            .process_guid(TEST_GUID)
            .await
            .expect("retry after index lag must succeed")
            .expect("resolved EVM");
        assert_eq!(evm, TEST_EVM);
        assert_eq!(db.writes(), vec![(
            TEST_GUID.to_string(),
            TEST_EVM.to_string()
        )]);
        assert!(
            !db.writes().iter().any(|(_, evm)| evm == EVM_NULL_SENTINEL),
            "must never persist the null sentinel on a 404-then-success path"
        );
    }
}
