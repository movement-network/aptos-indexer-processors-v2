// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Integration test for `evm_fetch_loop::EnricherLoop`.
//!
//! Exercises the full loop with real HTTP calls to the LZ Scan API and the
//! Hypernative screener. The DB save step is replaced by a `CapturingSaver`
//! that writes scores into an in-memory channel so the test can inspect them
//! without a running PostgreSQL instance.
//!
//! ## Running
//! This test is marked `#[ignore]` because it requires network access and
//! Hypernative credentials.  Run it explicitly with:
//!
//! ```sh
//! HYPERNATIVE_CLIENT_ID=<id> \
//! HYPERNATIVE_CLIENT_SECRET=<secret> \
//! cargo test -p processor --test evm_fetch_loop_integration -- --ignored --nocapture
//! ```
//!
//! ## Addresses under test
//! | Input | Kind | Expected |
//! |---|---|---|
//! | `0x5e87d7e75b272fb7150b4d1a05afb6bd71474950` | EVM | Approve → score < 1.0 |
//! | `0x31c05d73f2333b5a176cfdbb7c5ef96ec7bb04ac` | EVM | Deny → score > 1.0 |
//! | `0xe97fc9204872ba072f8d1a647d7045b881d20ff69aff1f050a0b05e8fb83228e` | LZ GUID | resolves → any valid score |

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use processor::processors::address_reputation::{
    address_reputation_model::EvmRiskScore,
    evm_fetch_loop::{EnricherLoop, ScoreSaver},
    hypernative::HypernativeClient,
    lz_enricher::LzEnricher,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{self, UnboundedSender};

// ---------------------------------------------------------------------------
// CapturingSaver — test double for ScoreSaver
// ---------------------------------------------------------------------------

struct CapturingSaver {
    tx: UnboundedSender<EvmRiskScore>,
}

impl CapturingSaver {
    fn new(tx: UnboundedSender<EvmRiskScore>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl ScoreSaver for CapturingSaver {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()> {
        let _ = self.tx.send(score.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Integration test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires network access and HYPERNATIVE_CLIENT_ID / HYPERNATIVE_CLIENT_SECRET env vars"]
async fn evm_fetch_loop_integration() {
    // Initialize tracing so loop logs appear on stdout with --nocapture.
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter("processor=debug,info")
    //     .try_init();

    let client_id = match std::env::var("HYPERNATIVE_CLIENT_ID")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(v) => v,
        None => {
            eprintln!("SKIP: HYPERNATIVE_CLIENT_ID not set");
            return;
        },
    };
    let client_secret = match std::env::var("HYPERNATIVE_CLIENT_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(v) => v,
        None => {
            eprintln!("SKIP: HYPERNATIVE_CLIENT_SECRET not set");
            return;
        },
    };
    let screener_url = std::env::var("HYPERNATIVE_SCREENER_URL")
        .unwrap_or_else(|_| "https://api.hypernative.xyz/screener/reputation".to_string());
    let lz_scan_base = std::env::var("LZ_SCAN_BASE_URL")
        .unwrap_or_else(|_| "https://scan.layerzero-api.com/v1/messages/guid".to_string());

    const EVM_APPROVE: &str = "0x5e87d7e75b272fb7150b4d1a05afb6bd71474950";
    const EVM_DENY: &str = "0x31c05d73f2333b5a176cfdbb7c5ef96ec7bb04ac";
    const LZ_GUID: &str = "0xe97fc9204872ba072f8d1a647d7045b881d20ff69aff1f050a0b05e8fb83228e";
    // Sender address that the GUID above resolves to (verified against LZ Scan API).
    const GUID_RESOLVED_EVM: &str = "0x97e6a34897a32e7103f3cf260f0c9ca5ca1fb90b";

    // No database needed: pass None so all DB paths are skipped immediately.
    let lz = LzEnricher::new(None, false, lz_scan_base);
    let hn = HypernativeClient::new(client_id, client_secret, None, screener_url);

    let (guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();
    let (score_tx, mut score_rx) = mpsc::unbounded_channel::<EvmRiskScore>();

    let saver = Arc::new(CapturingSaver::new(score_tx));

    // Tick interval 50 ms so items are dispatched quickly in the test.
    let loop_ = EnricherLoop::new(None, guid_rx, evm_rx, Some(lz), Some(hn), 50, saver);
    tokio::spawn(loop_.run());

    // Send test inputs.
    evm_tx.send(EVM_APPROVE.to_string()).unwrap();
    evm_tx.send(EVM_DENY.to_string()).unwrap();
    guid_tx.send(LZ_GUID.to_string()).unwrap();

    eprintln!("[test] sent 2 EVMs + 1 GUID, waiting for scores (30 s per score)...");

    // Collect up to 3 scores. The GUID may not resolve (LZ 404) so we accept
    // 2 or 3. Each score has a 30 s timeout; give up as soon as one times out.
    let mut scores: Vec<EvmRiskScore> = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(30), score_rx.recv()).await {
            Ok(Some(s)) => {
                eprintln!(
                    "[test] score: {} | rec={} sev={} risk={}",
                    s.evm_address, s.recommendation, s.severity, s.risk_score
                );
                scores.push(s);
            },
            Ok(None) => break, // channel closed
            Err(_) => {
                eprintln!(
                    "[test] timeout waiting for next score (got {} so far)",
                    scores.len()
                );
                break;
            },
        }
    }

    assert_eq!(
        scores.len(),
        3,
        "expected 3 scores (2 direct EVMs + 1 from GUID), got {}",
        scores.len()
    );

    // --- approve address: Approve / N/A → score = 0.1 + 0.1 = 0.2 ----------
    let approve = scores
        .iter()
        .find(|s| s.evm_address.eq_ignore_ascii_case(EVM_APPROVE))
        .expect("approve address score not captured");

    assert_eq!(approve.evm_address.to_lowercase(), EVM_APPROVE);
    assert_eq!(approve.risk_label, "hypernative");
    assert!(
        approve.source.contains("hypernative"),
        "source: {}",
        approve.source
    );
    assert!(
        approve.recommendation.eq_ignore_ascii_case("approve"),
        "rec: {}",
        approve.recommendation
    );
    assert!(
        approve.risk_score < BigDecimal::from(1),
        "score should be < 1.0, got {}",
        approve.risk_score
    );

    // --- deny address: Deny / High → score = 1.0 + 0.9 = 1.9 ---------------
    let deny = scores
        .iter()
        .find(|s| s.evm_address.eq_ignore_ascii_case(EVM_DENY))
        .expect("deny address score not captured");

    assert_eq!(deny.evm_address.to_lowercase(), EVM_DENY);
    assert_eq!(deny.risk_label, "hypernative");
    assert!(
        deny.recommendation.eq_ignore_ascii_case("deny"),
        "rec: {}",
        deny.recommendation
    );
    assert!(
        deny.severity.eq_ignore_ascii_case("high"),
        "sev: {}",
        deny.severity
    );
    assert!(
        deny.risk_score > BigDecimal::from(1),
        "score should be > 1.0, got {}",
        deny.risk_score
    );

    // --- GUID-resolved address: known sender → Approve / N/A → 0.2 ---------
    let guid_score = scores
        .iter()
        .find(|s| s.evm_address.eq_ignore_ascii_case(GUID_RESOLVED_EVM))
        .expect("GUID-resolved address score not captured");

    assert_eq!(guid_score.evm_address.to_lowercase(), GUID_RESOLVED_EVM);
    assert_eq!(guid_score.risk_label, "hypernative");
    assert!(
        guid_score.recommendation.eq_ignore_ascii_case("approve"),
        "rec: {}",
        guid_score.recommendation
    );
    assert!(
        guid_score.risk_score < BigDecimal::from(1),
        "score should be < 1.0, got {}",
        guid_score.risk_score
    );

    eprintln!("[test] PASSED");
}
