//! Integration test for the address_evm_sources rollup produced by
//! AddressReputationStorer. All propagation runs inside a single DB transaction
//! (per batch) and this test exercises that path against a real Postgres.
//!
//! Skipped automatically when no Postgres is reachable via testcontainers /
//! PostgresTestDatabase; the test framework the rest of the crate uses.
//!
//! Scenario 1 (M -> N across one batch):
//!   - Seed sender A with M direct bridge inflows (M distinct EVM addresses).
//!   - Emit one A->B transfer edge.
//!   - Assert B ends up with exactly M rows (one per EVM), evm_fund shares
//!     sum to the transferred amount, hops_min = 1.
//!
//! Scenario 2 (overlap merge):
//!   - Sender A carries {E1..E5}, receiver B already carries {E4..E8}.
//!   - After A->B, B has {E1..E8}: E4,E5 have merged (evm_fund summed,
//!     hops_min = LEAST of the two), E1..E3 are new at hop 1, E6..E8 stay at
//!     their prior hop with their prior evm_fund untouched.

use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    aptos_protos::util::timestamp::Timestamp,
    postgres::utils::database::{new_db_pool, run_migrations},
    testing_framework::database::{PostgresTestDatabase, TestDatabase},
    traits::Processable,
    types::transaction_context::TransactionContext,
};
use bigdecimal::BigDecimal;
use diesel::{sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use processor::{
    processors::address_reputation::{
        address_reputation_config::AddressReputationConfig,
        address_reputation_model::{BridgeInflow, TransferEdge},
        address_reputation_storer::AddressReputationStorer,
    },
    MIGRATIONS,
};
use std::str::FromStr;

const ASSET: &str = "0x0000000000000000000000000000000000000000000000000000000000000aaa";
const A_ADDR: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";
const B_ADDR: &str = "0x00000000000000000000000000000000000000000000000000000000000000b2";

fn evm(i: u32) -> String {
    format!("0x{:064x}", 0x1000_0000 + i)
}

fn ts() -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .naive_utc()
}

#[derive(diesel::QueryableByName, Clone, Debug)]
struct EvmRow {
    #[diesel(sql_type = Text)]
    evm_address: String,
    #[diesel(sql_type = diesel::sql_types::Numeric)]
    evm_fund: BigDecimal,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    hops_min: i32,
}

async fn read_rows(pool: &aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool, addr: &str) -> Vec<EvmRow> {
    let mut conn = pool.get().await.unwrap();
    sql_query("SELECT evm_address, evm_fund, hops_min FROM address_evm_sources WHERE movement_address = $1 ORDER BY evm_address")
        .bind::<Text, _>(addr)
        .get_results::<EvmRow>(&mut conn)
        .await
        .unwrap()
}

fn config() -> AddressReputationConfig {
    AddressReputationConfig {
        channel_size: 10,
        decay: 0.8,
        bridges: Vec::new(),
        propagate_evm_sources: true,
    }
}

/// Fabricate M bridge inflow edges + BridgeInflow rows, all delivering to A.
fn seed_bridge_batch(a: &str, m: u32, base_ord: i64, amounts: &[u64]) -> (Vec<TransferEdge>, Vec<BridgeInflow>) {
    assert_eq!(amounts.len() as u32, m);
    let mut edges = Vec::new();
    let mut inflows = Vec::new();
    for i in 0..m {
        let amt = BigDecimal::from(amounts[i as usize]);
        let e = TransferEdge {
            transaction_version: base_ord + i as i64,
            event_index: 0,
            from_address: format!("bridge::{i}"),
            to_address: a.to_string(),
            asset_type: Some(ASSET.to_string()),
            amount: amt.clone(),
            is_bridge_inflow: true,
            bridge_name: Some(format!("br_{i}")),
            transaction_timestamp: ts(),
        };
        let bi = BridgeInflow {
            transaction_version: base_ord + i as i64,
            event_index: 0,
            bridge_name: format!("br_{i}"),
            aptos_recipient: a.to_string(),
            evm_source: Some(evm(i)),
            src_chain_id: Some(1),
            asset_type: Some(ASSET.to_string()),
            amount: amt,
            transaction_timestamp: ts(),
        };
        edges.push(e);
        inflows.push(bi);
    }
    (edges, inflows)
}

fn transfer(from: &str, to: &str, amount: u64, version: i64, event_index: i64) -> TransferEdge {
    TransferEdge {
        transaction_version: version,
        event_index,
        from_address: from.to_string(),
        to_address: to.to_string(),
        asset_type: Some(ASSET.to_string()),
        amount: BigDecimal::from(amount),
        is_bridge_inflow: false,
        bridge_name: None,
        transaction_timestamp: ts(),
    }
}

async fn spin_up() -> (PostgresTestDatabase, aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool) {
    let mut db = PostgresTestDatabase::new();
    db.setup().await.expect("PostgresTestDatabase setup failed -- Docker/Postgres available?");
    let pool = new_db_pool(db.get_db_url().as_str(), Some(4))
        .await
        .expect("failed to create pool");
    run_migrations(db.get_db_url(), pool.clone(), MIGRATIONS).await;
    (db, pool)
}

// Silences unused-config warning; TransactionStreamConfig import isn't needed at runtime but
// keeps parity with the sibling smoke test's import block.
#[allow(dead_code)]
fn _keep_ts_type() -> Option<Timestamp> {
    None
}
#[allow(dead_code)]
fn _keep_stream_type() -> Option<TransactionStreamConfig> {
    None
}

#[tokio::test]
async fn propagates_m_evm_sources_from_a_to_b_in_one_batch() {
    let (_db, pool) = spin_up().await;
    let mut storer = AddressReputationStorer::new(pool.clone(), config());

    // 1) Seed A with M=5 bridge inflows in one batch. Each delivers `amounts[i]`.
    let m: u32 = 5;
    let amounts = vec![100u64, 200, 300, 400, 500];
    let seed_total: u64 = amounts.iter().sum();
    let (edges, inflows) = seed_bridge_batch(A_ADDR, m, 1000, &amounts);
    storer
        .process(TransactionContext { data: (edges, inflows), metadata: Default::default() })
        .await
        .expect("seed batch")
        .expect("seed output");

    // Sanity: A has M rows, all at hop 0.
    let a_rows = read_rows(&pool, A_ADDR).await;
    assert_eq!(a_rows.len(), m as usize, "A should hold M evm sources after seeding");
    assert!(a_rows.iter().all(|r| r.hops_min == 0));

    // 2) One A -> B transfer of `transfer_amt`. B should get M rows, each with
    // evm_fund = transfer_amt * (A's evm_fund for that E) / sum(A's evm_fund),
    // and hops_min = 1.
    let transfer_amt: u64 = 700;
    let edge = transfer(A_ADDR, B_ADDR, transfer_amt, 2000, 0);
    storer
        .process(TransactionContext { data: (vec![edge], vec![]), metadata: Default::default() })
        .await
        .expect("transfer batch")
        .expect("transfer output");

    let b_rows = read_rows(&pool, B_ADDR).await;
    assert_eq!(b_rows.len(), m as usize, "B should have M evm sources after A->B");
    for r in &b_rows {
        assert_eq!(r.hops_min, 1, "downstream hop should be exactly 1");
    }

    // Sum of B's evm_fund should equal transfer_amt (proportional distribution
    // preserves the total). Allow for BigDecimal exact arithmetic since
    // amounts and shares are integer-friendly here.
    let total_b: BigDecimal = b_rows
        .iter()
        .fold(BigDecimal::from(0), |acc, r| acc + &r.evm_fund);
    let expected = BigDecimal::from(transfer_amt);
    assert_eq!(total_b, expected, "sum(evm_fund on B) must equal transfer amount");

    // Spot-check one share: E0 seeded 100 out of 1500, so B[E0] = 700 * 100 / 1500.
    let e0 = evm(0);
    let e0_row = b_rows.iter().find(|r| r.evm_address == e0).expect("E0 present");
    // Postgres NUMERIC division may produce a specific scale; compare via
    // |e0 - 700*100/1500| < epsilon rather than exact string equality.
    let target = BigDecimal::from(transfer_amt) * BigDecimal::from(100) / BigDecimal::from(seed_total);
    let diff = (&e0_row.evm_fund - &target).abs();
    assert!(diff < BigDecimal::from_str("0.000001").unwrap(),
        "E0 share off: got {}, expected ~{}, diff={}",
        e0_row.evm_fund, target, diff);

    // A must be unchanged (no debit on outflow).
    let a_rows_after = read_rows(&pool, A_ADDR).await;
    assert_eq!(a_rows_after.len(), m as usize);
    for (before, after) in a_rows.iter().zip(a_rows_after.iter()) {
        assert_eq!(before.evm_fund, after.evm_fund, "sender's evm_fund must not change on outflow");
    }
}

#[tokio::test]
async fn merges_overlapping_evm_sources_on_a_to_b() {
    let (_db, pool) = spin_up().await;
    let mut storer = AddressReputationStorer::new(pool.clone(), config());

    // Seed A with E1..E5 (indices 1..=5), amount 100 each -> total 500.
    let a_amounts: Vec<u64> = (1..=5).map(|_| 100).collect();
    let (mut a_edges, mut a_inflows) = seed_bridge_batch(A_ADDR, 5, 1000, &a_amounts);
    // shift EVM indices to be 1..=5 not 0..=4
    for i in 0..a_edges.len() {
        a_inflows[i].evm_source = Some(evm(i as u32 + 1));
    }
    // (edges don't carry evm; storer looks them up from inflows) -- but avoid
    // unused warnings by touching a_edges
    let _ = &mut a_edges;
    storer
        .process(TransactionContext { data: (a_edges, a_inflows), metadata: Default::default() })
        .await
        .expect("A seed")
        .expect("A seed output");

    // Seed B with E4..=E8, amount 50 each. This gives us overlap (E4, E5) with A.
    let b_amounts: Vec<u64> = (4..=8).map(|_| 50).collect();
    let (mut b_edges, mut b_inflows) = seed_bridge_batch(B_ADDR, 5, 1200, &b_amounts);
    for i in 0..b_edges.len() {
        b_inflows[i].evm_source = Some(evm(i as u32 + 4));
    }
    let _ = &mut b_edges;
    storer
        .process(TransactionContext { data: (b_edges, b_inflows), metadata: Default::default() })
        .await
        .expect("B seed")
        .expect("B seed output");

    let b_before = read_rows(&pool, B_ADDR).await;
    assert_eq!(b_before.len(), 5);
    let get = |rows: &[EvmRow], e: &str| rows.iter().find(|r| r.evm_address == e).cloned().map(|r| (r.evm_fund, r.hops_min));

    // Now A -> B, amount 500 (== A's total funding). Then each of E1..E5 sends
    // its full share (100) into B.
    let edge = transfer(A_ADDR, B_ADDR, 500, 2000, 0);
    storer
        .process(TransactionContext { data: (vec![edge], vec![]), metadata: Default::default() })
        .await
        .expect("A->B")
        .expect("A->B output");

    let b_after = read_rows(&pool, B_ADDR).await;
    assert_eq!(b_after.len(), 8, "B should hold union of {{E1..E5}} + {{E4..E8}} = 8");

    // New sources on B: E1, E2, E3 -- fund = 100, hop = 1.
    for i in 1..=3 {
        let (fund, hop) = get(&b_after, &evm(i)).unwrap_or_else(|| panic!("missing E{i}"));
        assert_eq!(fund, BigDecimal::from(100), "new source E{i} evm_fund");
        assert_eq!(hop, 1, "new source E{i} hop");
    }
    // Overlapping sources E4, E5: fund = 50 (prior direct) + 100 (from A) = 150,
    // hop = LEAST(0, 1) = 0 (their direct-seed hop wins).
    for i in 4..=5 {
        let (fund, hop) = get(&b_after, &evm(i)).unwrap();
        assert_eq!(fund, BigDecimal::from(150), "overlap E{i} evm_fund");
        assert_eq!(hop, 0, "overlap E{i} keeps direct-seed hop");
    }
    // Non-overlapping prior sources E6..E8: unchanged.
    for i in 6..=8 {
        let (fund, hop) = get(&b_after, &evm(i)).unwrap();
        assert_eq!(fund, BigDecimal::from(50), "prior-only E{i} evm_fund unchanged");
        assert_eq!(hop, 0, "prior-only E{i} hop unchanged");
    }

    // A must remain unchanged.
    let a_after = read_rows(&pool, A_ADDR).await;
    assert_eq!(a_after.len(), 5);
    assert!(a_after.iter().all(|r| r.hops_min == 0 && r.evm_fund == BigDecimal::from(100)));
}
