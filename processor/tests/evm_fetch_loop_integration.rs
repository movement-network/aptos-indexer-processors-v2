// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Integration test for `evm_fetch_loop::EnricherLoop`.
//!
//! Exercises the full loop with real HTTP calls to the LZ Scan API and the
//! Hypernative screener. The DB layer is replaced by a `MockScreeningDb` that
//! stores error scores in memory so they can be re-queued after connection,
//! without requiring a running PostgreSQL instance.
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
    evm_screening::{
        hypernative::{EvmScreeningDb, HypernativeClient},
        lz_enricher::{LzDb, LzEnricher},
        EnricherLoop,
    },
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use wiremock::{
    matchers::{body_string_contains, method},
    Mock, MockServer, ResponseTemplate,
};

// ---------------------------------------------------------------------------
// MockLzDb — no-op LzDb for tests that don't need DB persistence
// ---------------------------------------------------------------------------

struct MockLzDb;

#[async_trait]
impl LzDb for MockLzDb {
    async fn load_pending_guids(&self) -> VecDeque<String> {
        VecDeque::new()
    }

    async fn write_evm(&self, _guid: &str, _evm: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockScreeningDb — in-memory EvmScreeningDb for tests
// ---------------------------------------------------------------------------

struct MockScreeningDb {
    /// EVMs saved with error score (score=0) — pending re-queue.
    pending: Mutex<Vec<String>>,
    /// Channel for real scores (score > 0) to verify in the test.
    tx: UnboundedSender<EvmRiskScore>,
}

impl MockScreeningDb {
    fn new(tx: UnboundedSender<EvmRiskScore>) -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            tx,
        }
    }

    fn pending_addresses(&self) -> Vec<String> {
        self.pending.lock().unwrap().clone()
    }
}

#[async_trait]
impl EvmScreeningDb for MockScreeningDb {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()> {
        if score.risk_score == BigDecimal::from(0) {
            // Error sentinel: park for re-queuing, don't send to collector.
            self.pending.lock().unwrap().push(score.evm_address.clone());
        } else {
            let _ = self.tx.send(score.clone());
        }
        Ok(())
    }

    async fn is_fresh_in_db(&self, _evm: &str) -> bool {
        false
    }

    async fn load_pending_evms(&self) -> VecDeque<String> {
        self.pending.lock().unwrap().drain(..).collect()
    }
}

// ---------------------------------------------------------------------------
// Integration test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
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

    let lz = LzEnricher::new(Arc::new(MockLzDb), lz_scan_base);
    // Use relaxed limits and a short TTL in tests (60 s so the 3 test addresses
    // are never suppressed as duplicates within a single test run).
    let hn = HypernativeClient::new(client_id, client_secret, None, screener_url, 4, 10, 60);

    let (guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();
    let (score_tx, mut score_rx) = mpsc::unbounded_channel::<EvmRiskScore>();

    let db = Arc::new(MockScreeningDb::new(score_tx));

    // Tick interval 50 ms so items are dispatched quickly in the test.
    let loop_ = EnricherLoop::new(db, guid_rx, evm_rx, lz, hn, 50);
    tokio::spawn(loop_.run());

    // Send EVM_APPROVE before the loop connects to Hypernative.
    // MockScreeningDb will park it as a pending error score and re-queue it
    // once load_pending_evms() is called after the first successful ping.
    evm_tx.send(EVM_APPROVE.to_string()).unwrap();

    // Wait for the connection to be established (~1 s typical).
    let _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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

    // --- approve address: Approve / N/A → score < 1.0 -----------------------
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

    // --- deny address: Deny / High → score > 1.0 ----------------------------
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

    // --- GUID-resolved address: known sender → Approve / N/A → < 1.0 -------
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

// ===========================================================================
// Wiremock-based tests — no real credentials or DB required
// ===========================================================================

const TEST_EVM: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// The null address used by HypernativeClient::ping() to verify connectivity.
const PING_ADDR: &str = "0x0000000000000000000000000000000000000000";
/// MAX_RETRIES from evm_connection.rs.
const MAX_RETRIES: u32 = 5;

fn approve_body() -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "data": [{
            "address": TEST_EVM,
            "recommendation": "Approve",
            "severity": "N/A",
            "totalIncomingUsd": 0.0,
            "totalOutgoingUsd": 0.0,
            "policyId": "test-policy",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "flags": []
        }],
        "error": null
    })
}

fn deny_body() -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "data": [{
            "address": TEST_EVM,
            "recommendation": "Deny",
            "severity": "High",
            "totalIncomingUsd": 1000000.0,
            "totalOutgoingUsd": 2000000.0,
            "policyId": "test-policy",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "flags": []
        }],
        "error": null
    })
}

/// Valid JSON but missing the required "recommendation" and "severity" fields.
fn malformed_body() -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "data": [{
            "address": TEST_EVM,
            "totalIncomingUsd": 0.0,
            "flags": []
        }],
        "error": null
    })
}

fn make_client(url: String) -> HypernativeClient {
    HypernativeClient::new(
        "id".to_string(),
        "secret".to_string(),
        None,
        url,
        4,
        10,
        3600,
    )
}

/// Register a ping mock (matches the null-address body) that always returns 200.
/// Must be mounted FIRST so wiremock's FIFO matching routes ping requests here
/// before any screening mock can claim them.
async fn mount_ping_mock(server: &MockServer) {
    Mock::given(method("POST"))
        .and(body_string_contains(PING_ADDR))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"success":true,"data":[],"error":null})),
        )
        .mount(server)
        .await;
}

/// Count requests that are NOT pings (i.e. real screening calls).
async fn screening_request_count(mock: &MockServer) -> usize {
    mock.received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| !String::from_utf8_lossy(&r.body).contains(PING_ADDR))
        .count()
}

/// Build an EnricherLoop wired to a wiremock server with short delays for tests.
fn make_loop(
    db: Arc<dyn EvmScreeningDb>,
    guid_rx: UnboundedReceiver<String>,
    evm_rx: UnboundedReceiver<String>,
    hn: HypernativeClient,
) -> EnricherLoop {
    let lz = LzEnricher::new(Arc::new(MockLzDb), "http://x".to_string());
    EnricherLoop::new(db, guid_rx, evm_rx, lz, hn, 5)
        .with_retry_delay(Duration::from_millis(10))
        .with_reconnect_interval(Duration::from_millis(10))
}

// ---------------------------------------------------------------------------
// Test 1: HTTP 429 → backoff pause → resume with correct score
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_429_pauses_then_resumes() {
    let mock = MockServer::start().await;

    // Ping always succeeds (registered first = matched first by FIFO).
    mount_ping_mock(&mock).await;

    // First screening call: 429.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&mock)
        .await;

    // Fallback: approve after the 429 backoff expires.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(approve_body()))
        .mount(&mock)
        .await;

    let (score_tx, mut score_rx) = mpsc::unbounded_channel();
    let mock_db = Arc::new(MockScreeningDb::new(score_tx));
    let db = Arc::clone(&mock_db) as Arc<dyn EvmScreeningDb>;

    // Short backoff so the test completes in ~200 ms.
    let hn = make_client(mock.uri()).with_backoff_429(Duration::from_millis(50));
    let (_guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(make_loop(db, guid_rx, evm_rx, hn).run());
    evm_tx.send(TEST_EVM.to_string()).unwrap();

    // Allow time for: ping + 1st attempt (429 + 50ms backoff) + retry → approve.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let score = score_rx
        .try_recv()
        .expect("approve score should be saved after 429 recovery");
    assert_eq!(score.evm_address.to_lowercase(), TEST_EVM);
    assert_eq!(score.recommendation.to_lowercase(), "approve");
    assert!(
        mock_db.pending_addresses().is_empty(),
        "no error score expected on success"
    );

    // Exactly 2 screening attempts: 1 × 429 + 1 × 200.
    assert_eq!(
        screening_request_count(&mock).await,
        2,
        "expected 1 × 429 + 1 × 200 screening requests"
    );
}

// ---------------------------------------------------------------------------
// Test 2: 5xx every attempt → exhausted → address stored with score 0
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_5xx_exhausted_saves_error_score() {
    let mock = MockServer::start().await;

    mount_ping_mock(&mock).await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock)
        .await;

    let (score_tx, _score_rx) = mpsc::unbounded_channel();
    let mock_db = Arc::new(MockScreeningDb::new(score_tx));
    let db = Arc::clone(&mock_db) as Arc<dyn EvmScreeningDb>;

    let hn = make_client(mock.uri());
    let (_guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(make_loop(db, guid_rx, evm_rx, hn).run());
    evm_tx.send(TEST_EVM.to_string()).unwrap();

    // Allow time for: ping + 5 attempts × 10ms retry delay.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        mock_db.pending_addresses().contains(&TEST_EVM.to_string()),
        "error score (score=0) should be stored after all retries exhausted"
    );
    assert_eq!(
        screening_request_count(&mock).await,
        MAX_RETRIES as usize,
        "should have made exactly MAX_RETRIES screening attempts"
    );
}

// ---------------------------------------------------------------------------
// Test 3: 5xx on first 2 attempts, then success → correct score stored
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_5xx_recovers_saves_correct_score() {
    let mock = MockServer::start().await;

    mount_ping_mock(&mock).await;

    // First 2 screening calls: 500.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(2)
        .mount(&mock)
        .await;

    // Fallback: deny response after the 500s are exhausted.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(deny_body()))
        .mount(&mock)
        .await;

    let (score_tx, mut score_rx) = mpsc::unbounded_channel();
    let mock_db = Arc::new(MockScreeningDb::new(score_tx));
    let db = Arc::clone(&mock_db) as Arc<dyn EvmScreeningDb>;

    let hn = make_client(mock.uri());
    let (_guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(make_loop(db, guid_rx, evm_rx, hn).run());
    evm_tx.send(TEST_EVM.to_string()).unwrap();

    // Allow time for: ping + 2 × 500 (10ms each) + 1 × 200.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let score = score_rx
        .try_recv()
        .expect("deny score should be saved after recovery");
    assert_eq!(score.evm_address.to_lowercase(), TEST_EVM);
    assert_eq!(score.recommendation.to_lowercase(), "deny");
    assert_eq!(score.severity.to_lowercase(), "high");
    assert!(
        score.risk_score > BigDecimal::from(1),
        "deny+high score should be > 1, got {}",
        score.risk_score
    );
    assert!(
        mock_db.pending_addresses().is_empty(),
        "no error score should be stored on successful recovery"
    );
    assert_eq!(
        screening_request_count(&mock).await,
        3,
        "expected 2 × 500 + 1 × 200 = 3 screening requests"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Auth error (401) every attempt → exhausted → error score stored
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_auth_error_exhausted_saves_error_score() {
    let mock = MockServer::start().await;

    mount_ping_mock(&mock).await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&mock)
        .await;

    let (score_tx, _score_rx) = mpsc::unbounded_channel();
    let mock_db = Arc::new(MockScreeningDb::new(score_tx));
    let db = Arc::clone(&mock_db) as Arc<dyn EvmScreeningDb>;

    let hn = make_client(mock.uri());
    let (_guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(make_loop(db, guid_rx, evm_rx, hn).run());
    evm_tx.send(TEST_EVM.to_string()).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        mock_db.pending_addresses().contains(&TEST_EVM.to_string()),
        "error score should be stored after auth-error exhaustion"
    );
    assert_eq!(
        screening_request_count(&mock).await,
        MAX_RETRIES as usize,
        "should have made exactly MAX_RETRIES screening attempts"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Malformed response (missing required fields) → exhausted → error score
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_malformed_response_exhausted_saves_error_score() {
    let mock = MockServer::start().await;

    mount_ping_mock(&mock).await;

    // 200 OK but body is missing "recommendation" and "severity".
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(malformed_body()))
        .mount(&mock)
        .await;

    let (score_tx, _score_rx) = mpsc::unbounded_channel();
    let mock_db = Arc::new(MockScreeningDb::new(score_tx));
    let db = Arc::clone(&mock_db) as Arc<dyn EvmScreeningDb>;

    let hn = make_client(mock.uri());
    let (_guid_tx, guid_rx) = mpsc::unbounded_channel::<String>();
    let (evm_tx, evm_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(make_loop(db, guid_rx, evm_rx, hn).run());
    evm_tx.send(TEST_EVM.to_string()).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        mock_db.pending_addresses().contains(&TEST_EVM.to_string()),
        "error score should be stored after malformed-response exhaustion"
    );
    assert_eq!(
        screening_request_count(&mock).await,
        MAX_RETRIES as usize,
        "should have made exactly MAX_RETRIES screening attempts on malformed responses"
    );
}
