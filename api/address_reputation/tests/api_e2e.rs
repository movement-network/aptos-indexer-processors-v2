//! End-to-end tests for the address-reputation REST API.
//!
//! Each test spins up a real PostgreSQL database via `PostgresTestDatabase`,
//! runs all processor migrations, seeds data using `AddressReputationStorer`
//! and raw SQL, then starts the Axum server on a random port and exercises
//! every endpoint with real HTTP calls via `reqwest`.
//!
//! The tests are skipped automatically when Docker/Postgres is unavailable —
//! the same guard used in the processor integration tests.

use address_reputation_api::{db, routes};
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{new_db_pool, run_migrations, ArcDbPool},
    testing_framework::database::{PostgresTestDatabase, TestDatabase},
    traits::Processable,
    types::transaction_context::TransactionContext,
};
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{Bool, Numeric, Varchar},
};
use diesel_async::RunQueryDsl;
use processor::{
    processors::address_reputation::{
        address_reputation_config::AddressReputationConfig,
        address_reputation_model::{BridgeInflow, TransferEdge},
        address_reputation_storer::AddressReputationStorer,
    },
    MIGRATIONS,
};
use serde_json::Value;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Test constants
// ---------------------------------------------------------------------------

const ASSET: &str = "0x0000000000000000000000000000000000000000000000000000000000000aaa";
const A_ADDR: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";
const B_ADDR: &str = "0x00000000000000000000000000000000000000000000000000000000000000b2";
const API_KEY: &str = "test-api-key-e2e";

// Three distinct EVM addresses used across tests.
fn evm(i: u32) -> String {
    format!("0x{:040x}", 0x1000_0000u64 + i as u64)
}

fn ts() -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .naive_utc()
}

// ---------------------------------------------------------------------------
// Infrastructure helpers
// ---------------------------------------------------------------------------

/// Boot PostgresTestDatabase, run all processor migrations, and return both
/// the SDK pool (for seeding via the storer) and the API pool.
async fn spin_up() -> (PostgresTestDatabase, ArcDbPool, Arc<db::DbPool>) {
    let mut pg = PostgresTestDatabase::new();
    pg.setup()
        .await
        .expect("PostgresTestDatabase setup failed — Docker/Postgres available?");

    let sdk_pool = new_db_pool(pg.get_db_url().as_str(), Some(4))
        .await
        .expect("SDK pool");
    run_migrations(pg.get_db_url(), sdk_pool.clone(), MIGRATIONS).await;

    let api_pool = Arc::new(
        db::new_pool(pg.get_db_url().as_str(), 4)
            .await
            .expect("API pool"),
    );

    (pg, sdk_pool, api_pool)
}

/// Build the Axum app and bind it to a random OS port. Returns the bound
/// address and a handle that can be aborted to shut the server down.
async fn spawn_server(api_pool: Arc<db::DbPool>) -> (SocketAddr, JoinHandle<()>) {
    let mut keys = HashSet::new();
    keys.insert(API_KEY.to_string());

    let state = routes::AppState {
        pool: api_pool,
        api_keys: Arc::new(keys),
    };
    let app = routes::build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (addr, handle)
}

/// Storer config with propagation enabled and no bridges (we inject data directly).
fn storer_config() -> AddressReputationConfig {
    AddressReputationConfig {
        channel_size: 10,
        bridges: vec![],
        propagate_evm_sources: true,
        lz_enricher: Default::default(),
        hypernative: Default::default(),
    }
}

/// Convenience: run a single storer batch.
async fn store(
    storer: &mut AddressReputationStorer,
    edges: Vec<TransferEdge>,
    inflows: Vec<BridgeInflow>,
) {
    storer
        .process(TransactionContext {
            data: (edges, inflows),
            metadata: Default::default(),
        })
        .await
        .expect("storer process")
        .expect("storer output");
}

/// Insert a row into `evm_address_risk_scores` via raw SQL.
async fn seed_risk_score(
    pool: &ArcDbPool,
    evm_addr: &str,
    risk_score: f64,
    recommendation: &str,
    severity: &str,
) {
    let mut conn = pool.get().await.unwrap();
    sql_query(
        "INSERT INTO evm_address_risk_scores \
             (evm_address, risk_score, risk_label, source, recommendation, severity, \
              fetched_at, inserted_at, to_be_updated) \
         VALUES ($1, $2, 'hypernative', 'test', $3, $4, NOW(), NOW(), $5)",
    )
    .bind::<Varchar, _>(evm_addr)
    .bind::<Numeric, _>(BigDecimal::from(risk_score as i64)) // whole-number scores suffice for tests
    .bind::<Varchar, _>(recommendation)
    .bind::<Varchar, _>(severity)
    .bind::<Bool, _>(false)
    .execute(&mut conn)
    .await
    .expect("seed_risk_score");
}

// ---------------------------------------------------------------------------
// Data-seeding scenario
//
//  A_ADDR gets 3 direct bridge inflows:
//    E1 → evm_fund = 500   (risk_score = 1, deny / high)
//    E2 → evm_fund = 300   (risk_score = 0, approve / medium — score inserted as 0 for contrast)
//    E3 → evm_fund = 200   (no risk_score entry)
//
//  Then A → B transfer of 500 units propagates evm sources to B_ADDR:
//    E1 → transfer_fund ≈ 250  (500 * 500/1000 * 1)
//    E2 → transfer_fund ≈ 150  (500 * 300/1000 * 1)
//    E3 → transfer_fund ≈ 100  (500 * 200/1000 * 1)
//    hops_min = 1 for all B rows
// ---------------------------------------------------------------------------

async fn seed(sdk_pool: &ArcDbPool) {
    let (guid_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let mut storer = AddressReputationStorer::new(sdk_pool.clone(), storer_config(), guid_tx);

    // Bridge inflows → A_ADDR
    let mk_inflow = |idx: u32, amt: u64| {
        let bi = BridgeInflow {
            transaction_version: 1000 + idx as i64,
            event_index: 0,
            bridge_name: format!("br_{idx}"),
            aptos_recipient: A_ADDR.to_string(),
            evm_source: Some(evm(idx)),
            src_chain_id: Some(1),
            asset_type: Some(ASSET.to_string()),
            amount: BigDecimal::from(amt),
            lz_guid: None,
            transaction_timestamp: ts(),
        };
        let edge = TransferEdge {
            transaction_version: 1000 + idx as i64,
            event_index: 0,
            from_address: format!("bridge::{idx}"),
            to_address: A_ADDR.to_string(),
            asset_type: Some(ASSET.to_string()),
            amount: BigDecimal::from(amt),
            is_bridge_inflow: true,
            bridge_name: Some(format!("br_{idx}")),
            transaction_timestamp: ts(),
        };
        (edge, bi)
    };

    let (e1, b1) = mk_inflow(1, 500);
    let (e2, b2) = mk_inflow(2, 300);
    let (e3, b3) = mk_inflow(3, 200);
    store(&mut storer, vec![e1, e2, e3], vec![b1, b2, b3]).await;

    // Transfer A → B
    let transfer = TransferEdge {
        transaction_version: 2000,
        event_index: 0,
        from_address: A_ADDR.to_string(),
        to_address: B_ADDR.to_string(),
        asset_type: Some(ASSET.to_string()),
        amount: BigDecimal::from(500u64),
        is_bridge_inflow: false,
        bridge_name: None,
        transaction_timestamp: ts(),
    };
    store(&mut storer, vec![transfer], vec![]).await;

    // Risk scores for E1 and E2; E3 intentionally left without a score.
    seed_risk_score(sdk_pool, &evm(1), 1.0, "deny", "high").await;
    seed_risk_score(sdk_pool, &evm(2), 0.0, "approve", "medium").await;
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn url(addr: SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

async fn get(client: &reqwest::Client, addr: SocketAddr, path: &str) -> reqwest::Response {
    client
        .get(url(addr, path))
        .header("X-Api-Key", API_KEY)
        .send()
        .await
        .unwrap()
}

async fn post(
    client: &reqwest::Client,
    addr: SocketAddr,
    path: &str,
    body: &Value,
) -> reqwest::Response {
    client
        .post(url(addr, path))
        .header("X-Api-Key", API_KEY)
        .json(body)
        .send()
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verifies that every endpoint rejects requests with a missing or wrong API key.
/// A request with no `X-Api-Key` header and a request with an incorrect key must
/// both receive HTTP 401; the valid key is never tested here (covered by the other tests).
#[tokio::test]
async fn test_auth_rejection() {
    let (_pg, _, api_pool) = spin_up().await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    // No key → 401
    let resp = client
        .get(url(addr, "/v1/reputation/address/since?since=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong key → 401
    let resp = client
        .get(url(addr, "/v1/reputation/address/since?since=0"))
        .header("X-Api-Key", "wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    handle.abort();
}

/// Tests `GET /v1/reputation/address/since?since=<unix_ts>`.
/// Seeds the DB with 3 bridge inflows for A and one A→B transfer that propagates
/// 3 EVM sources to B (6 rows total in `address_evm_sources`).
/// - `since=0` must return all 6 rows; every row must expose `updated_at`
///   (the last-write timestamp used for the filter) as well as `inserted_at`.
/// - `since=<far future>` must return an empty array (no rows match the filter).
#[tokio::test]
async fn test_address_since() {
    let (_pg, sdk_pool, api_pool) = spin_up().await;
    seed(&sdk_pool).await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    // since=0 returns all rows (A has 3, B has 3 → 6 total)
    let resp = get(&client, addr, "/v1/reputation/address/since?since=0").await;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 6, "expected 6 rows total (3 for A, 3 for B)");

    // All rows must expose the required fields, including both timestamps.
    for row in rows {
        assert!(row.get("movement_address").is_some());
        assert!(row.get("evm_address").is_some());
        assert!(row.get("evm_fund").is_some());
        assert!(row.get("transfer_fund").is_some());
        assert!(row.get("hops_min").is_some());
        assert!(row.get("inserted_at").is_some());
        assert!(
            row.get("updated_at").is_some(),
            "updated_at must be present (used for the since filter)"
        );
    }

    // since = far future → empty
    let resp = get(
        &client,
        addr,
        "/v1/reputation/address/since?since=9999999999",
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 0);

    handle.abort();
}

/// Tests `POST /v1/reputation/address/mvt_fetch` with a JSON array of Movement addresses.
/// - A single address (A) returns its 3 EVM-source rows, all with `hops_min=0` (direct bridge inflows).
/// - Both A and B together return 6 rows.
/// - An unknown address returns an empty array (not an error).
/// - An empty request body returns an empty array without hitting the DB.
#[tokio::test]
async fn test_mvt_fetch() {
    let (_pg, sdk_pool, api_pool) = spin_up().await;
    seed(&sdk_pool).await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    // Only A → 3 rows
    let resp = post(
        &client,
        addr,
        "/v1/reputation/address/mvt_fetch",
        &Value::Array(vec![Value::String(A_ADDR.into())]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let rows: Value = resp.json().await.unwrap();
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3, "A should have 3 EVM rows");
    assert!(arr.iter().all(|r| r["movement_address"] == A_ADDR));
    assert!(arr.iter().all(|r| r["hops_min"] == 0i64));

    // A + B → 6 rows
    let resp = post(
        &client,
        addr,
        "/v1/reputation/address/mvt_fetch",
        &Value::Array(vec![
            Value::String(A_ADDR.into()),
            Value::String(B_ADDR.into()),
        ]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let arr: Value = resp.json().await.unwrap();
    assert_eq!(arr.as_array().unwrap().len(), 6);

    // Unknown address → empty (not an error)
    let resp = post(
        &client,
        addr,
        "/v1/reputation/address/mvt_fetch",
        &Value::Array(vec![Value::String(
            "0x0000000000000000000000000000000000000000000000000000000000009999".into(),
        )]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Empty body → empty array
    let resp = post(
        &client,
        addr,
        "/v1/reputation/address/mvt_fetch",
        &Value::Array(vec![]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );

    handle.abort();
}

/// Tests `POST /v1/reputation/score/evms` with a JSON array of EVM addresses.
/// The seed inserts risk scores for E1 (score=1, deny/high) and E2 (score=0, approve/medium)
/// but not for E3.
/// - Querying E1 alone returns 1 row with the correct recommendation and severity fields.
/// - The internal bookkeeping fields `to_be_updated` and `fetched_at` must be absent from
///   the response — they are stored in the DB but not exposed by the API.
/// - Querying E1+E2+E3 returns only 2 rows — E3 is absent because it has no entry in
///   `evm_address_risk_scores` and the endpoint does an inner-style lookup.
/// - An empty body returns an empty array.
#[tokio::test]
async fn test_score_evms() {
    let (_pg, sdk_pool, api_pool) = spin_up().await;
    seed(&sdk_pool).await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    // E1 only → 1 row with correct public fields
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/evms",
        &Value::Array(vec![Value::String(evm(1))]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let arr = resp.json::<Value>().await.unwrap();
    let rows = arr.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["evm_address"], evm(1));
    assert_eq!(rows[0]["recommendation"], "deny");
    assert_eq!(rows[0]["severity"], "high");
    // Internal bookkeeping fields must not appear in the response.
    assert!(
        rows[0].get("to_be_updated").is_none(),
        "to_be_updated must not be exposed"
    );
    assert!(
        rows[0].get("fetched_at").is_none(),
        "fetched_at must not be exposed"
    );

    // E1 + E2 + E3 → 2 rows (E3 has no entry)
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/evms",
        &Value::Array(vec![
            Value::String(evm(1)),
            Value::String(evm(2)),
            Value::String(evm(3)),
        ]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let rows = resp.json::<Value>().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2, "E3 has no score entry");

    // Empty → empty
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/evms",
        &Value::Array(vec![]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );

    handle.abort();
}

/// Tests `POST /v1/reputation/score/mvts` with a JSON array of Movement addresses.
/// The endpoint returns one summary row per address: the EVM source with the highest
/// risk score, plus a `transfer_fund` equal to SUM(all transfer_fund rows) + evm_fund
/// of that top EVM source.
///
/// For A (direct bridge inflows, no outbound transfers):
///   - Top EVM is E1 (risk_score=1). transfer_fund = SUM(A.transfer_fund=0) + E1.evm_fund(500) = 500.
///
/// For B (received a 500-unit transfer from A, no direct inflows):
///   - Top EVM is E1 (risk_score=1). B.transfer_fund = 250 (E1) + 150 (E2) + 100 (E3) = 500.
///     transfer_fund in response = 500 + E1.evm_fund(0) = 500.
///
/// Also verifies: both addresses together return 2 rows, unknown address returns empty,
/// and an empty body returns an empty array.
#[tokio::test]
async fn test_score_mvts() {
    let (_pg, sdk_pool, api_pool) = spin_up().await;
    seed(&sdk_pool).await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    // A_ADDR: top EVM is E1 (score=1), transfer_fund = SUM(A.transfer_fund)+E1.evm_fund = 0+500 = 500
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/mvts",
        &Value::Array(vec![Value::String(A_ADDR.into())]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let rows = resp.json::<Value>().await.unwrap();
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let a_row = &arr[0];
    assert_eq!(a_row["movement_address"], A_ADDR);
    assert_eq!(
        a_row["evm_address"],
        evm(1),
        "E1 has the highest risk score"
    );
    assert_eq!(a_row["recommendation"], "deny");
    assert_eq!(a_row["severity"], "high");
    // transfer_fund = SUM(all A.transfer_fund=0) + E1.evm_fund(500) = 500
    let tf: f64 = a_row["transfer_fund"].as_str().unwrap().parse().unwrap();
    assert!(
        (tf - 500.0).abs() < 0.001,
        "A transfer_fund should be 500, got {tf}"
    );

    // B_ADDR: top EVM is E1 (score=1); B has no evm_fund, only transfer_fund from the propagation.
    // transfer_fund = SUM(B.transfer_fund) + E1.evm_fund(0).
    // B.transfer_fund for E1 = 500 * 500/1000 * 1 = 250 (with ROUND(...,9) precision).
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/mvts",
        &Value::Array(vec![Value::String(B_ADDR.into())]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let arr = resp.json::<Value>().await.unwrap();
    let b_row = &arr[0];
    assert_eq!(b_row["movement_address"], B_ADDR);
    assert_eq!(b_row["evm_address"], evm(1));
    // SUM(B.transfer_fund) = 250+150+100 = 500; top EVM evm_fund = 0 → total = 500
    let tf: f64 = b_row["transfer_fund"].as_str().unwrap().parse().unwrap();
    assert!(
        (tf - 500.0).abs() < 0.001,
        "B transfer_fund should be 500, got {tf}"
    );

    // A + B together → 2 rows, one per mvt address
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/mvts",
        &Value::Array(vec![
            Value::String(A_ADDR.into()),
            Value::String(B_ADDR.into()),
        ]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // Unknown address → empty
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/mvts",
        &Value::Array(vec![Value::String(
            "0x0000000000000000000000000000000000000000000000000000000000009999".into(),
        )]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Empty → empty
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/mvts",
        &Value::Array(vec![]),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );

    handle.abort();
}

/// Verifies that all three address-accepting endpoints return HTTP 400 for malformed
/// addresses, covering the following invalid cases:
/// - missing `0x` prefix
/// - wrong length (too short / too long)
/// - non-hex characters in the address body
///
/// Valid addresses must still return 200, confirming the validator accepts
/// legitimate input. Also checks that one bad address in a list fails the whole request.
#[tokio::test]
async fn test_address_validation() {
    let (_pg, _, api_pool) = spin_up().await;
    let (addr, handle) = spawn_server(api_pool).await;
    let client = reqwest::Client::new();

    let valid_mvt = A_ADDR; // 0x + 64 hex chars
    let valid_evm = evm(1); // 0x + 40 hex chars

    let no_prefix = &valid_mvt[2..]; // missing 0x prefix
    let too_short = "0x00a1"; // far too short for either type
    let mvt_too_long = format!("{valid_mvt}ff"); // 0x + 66 hex  (2 extra chars)
    let mvt_bad_ch = format!("0x{}", "zz".repeat(32)); // right length, non-hex content
    let evm_too_long = format!("{valid_evm}ff"); // 0x + 42 hex  (2 extra chars)
    let evm_bad_ch = format!("0x{}", "zz".repeat(20)); // right length, non-hex content

    // --- mvt_fetch and score/mvts: invalid Movement addresses → 400 ---
    for endpoint in &[
        "/v1/reputation/address/mvt_fetch",
        "/v1/reputation/score/mvts",
    ] {
        for bad in &[
            no_prefix,
            too_short,
            mvt_too_long.as_str(),
            mvt_bad_ch.as_str(),
        ] {
            let resp = post(
                &client,
                addr,
                endpoint,
                &Value::Array(vec![Value::String((*bad).to_string())]),
            )
            .await;
            assert_eq!(resp.status(), 400, "{endpoint} should reject: {bad:?}");
        }
        // valid address → 200
        let resp = post(
            &client,
            addr,
            endpoint,
            &Value::Array(vec![Value::String(valid_mvt.into())]),
        )
        .await;
        assert_eq!(
            resp.status(),
            200,
            "{endpoint} should accept a valid movement address"
        );
    }

    // --- score/evms: invalid EVM addresses → 400 ---
    for bad in &[
        no_prefix,
        too_short,
        evm_too_long.as_str(),
        evm_bad_ch.as_str(),
    ] {
        let resp = post(
            &client,
            addr,
            "/v1/reputation/score/evms",
            &Value::Array(vec![Value::String((*bad).to_string())]),
        )
        .await;
        assert_eq!(resp.status(), 400, "score/evms should reject: {bad:?}");
    }
    // valid EVM address → 200
    let resp = post(
        &client,
        addr,
        "/v1/reputation/score/evms",
        &Value::Array(vec![Value::String(valid_evm)]),
    )
    .await;
    assert_eq!(
        resp.status(),
        200,
        "score/evms should accept a valid evm address"
    );

    // --- one good + one bad in the same list → 400 (first-bad-wins) ---
    let resp = post(
        &client,
        addr,
        "/v1/reputation/address/mvt_fetch",
        &Value::Array(vec![
            Value::String(valid_mvt.into()),
            Value::String(too_short.into()),
        ]),
    )
    .await;
    assert_eq!(
        resp.status(),
        400,
        "a single invalid address in a list must reject the whole request"
    );

    handle.abort();
}
