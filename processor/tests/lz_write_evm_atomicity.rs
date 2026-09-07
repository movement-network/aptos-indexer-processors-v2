//! `DbLzStore::write_evm` must mark `bridge_inflows.evm_source` and seed
//! `address_evm_sources` in one transaction. A committed mark with a failed
//! seed is unrecoverable: `load_pending_guids` and the UPDATE both filter
//! `evm_source IS NULL`.
//!
//! Needs Docker / `PostgresTestDatabase` (same as `address_evm_sources_propagation`).

use aptos_indexer_processor_sdk::{
    postgres::utils::database::{new_db_pool, run_migrations},
    testing_framework::database::{PostgresTestDatabase, TestDatabase},
};
use bigdecimal::BigDecimal;
use diesel::{sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use processor::{
    processors::address_reputation::{
        address_reputation_model::BridgeInflow,
        evm_screening::lz_storer::{DbLzStore, LzDb},
    },
    schema, MIGRATIONS,
};

const ASSET: &str = "0x0000000000000000000000000000000000000000000000000000000000000aaa";
const RECIPIENT: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";
const GUID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const EVM: &str = "0x0000000000000000000000000000000000000000000000000000000000000e01";
const FAIL_EVM: &str = "0x000000000000000000000000000000000000000000000000000000000000dead";

fn ts() -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .naive_utc()
}

fn inflow(guid: &str, version: i64) -> BridgeInflow {
    BridgeInflow {
        transaction_version: version,
        event_index: 0,
        bridge_name: "lz".to_string(),
        aptos_recipient: RECIPIENT.to_string(),
        evm_source: None,
        src_chain_id: Some(1),
        asset_type: Some(ASSET.to_string()),
        amount: BigDecimal::from(100),
        lz_guid: Some(guid.to_string()),
        transaction_timestamp: ts(),
    }
}

async fn spin_up() -> (
    PostgresTestDatabase,
    aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool,
) {
    let mut db = PostgresTestDatabase::new();
    db.setup()
        .await
        .expect("PostgresTestDatabase setup failed -- Docker/Postgres available?");
    let pool = new_db_pool(db.get_db_url().as_str(), Some(4))
        .await
        .expect("failed to create pool");
    run_migrations(db.get_db_url(), pool.clone(), MIGRATIONS).await;
    (db, pool)
}

async fn insert_inflow(
    pool: &aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool,
    row: &BridgeInflow,
) {
    let mut conn = pool.get().await.unwrap();
    diesel::insert_into(schema::bridge_inflows::table)
        .values(row)
        .execute(&mut conn)
        .await
        .expect("insert bridge_inflows");
}

#[derive(diesel::QueryableByName)]
struct EvmSourceRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    evm_source: Option<String>,
}

#[derive(diesel::QueryableByName)]
struct FundRow {
    #[diesel(sql_type = diesel::sql_types::Numeric)]
    evm_fund: BigDecimal,
}

async fn read_evm_source(
    pool: &aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool,
    guid: &str,
) -> Option<String> {
    let mut conn = pool.get().await.unwrap();
    sql_query("SELECT evm_source FROM bridge_inflows WHERE lz_guid = $1")
        .bind::<Text, _>(guid)
        .get_result::<EvmSourceRow>(&mut conn)
        .await
        .ok()
        .and_then(|r| r.evm_source)
}

async fn read_aes_funds(
    pool: &aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool,
    evm: &str,
) -> Vec<BigDecimal> {
    let mut conn = pool.get().await.unwrap();
    sql_query(
        "SELECT evm_fund FROM address_evm_sources \
         WHERE movement_address = $1 AND evm_address = $2",
    )
    .bind::<Text, _>(RECIPIENT)
    .bind::<Text, _>(evm)
    .get_results::<FundRow>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.evm_fund)
    .collect()
}

#[tokio::test]
async fn write_evm_marks_and_seeds_together() {
    let (_db, pool) = spin_up().await;
    insert_inflow(&pool, &inflow(GUID, 1)).await;
    let store = DbLzStore::new(pool.clone(), true);

    store.write_evm(GUID, EVM).await.expect("write_evm");

    assert_eq!(read_evm_source(&pool, GUID).await.as_deref(), Some(EVM));
    let funds = read_aes_funds(&pool, EVM).await;
    assert_eq!(funds, vec![BigDecimal::from(100)]);
    assert!(
        store.load_pending_guids().await.is_empty(),
        "resolved GUID must leave the pending set"
    );

    // A second call matches 0 rows (already resolved) and must not add funds.
    store.write_evm(GUID, EVM).await.expect("replay write_evm");
    let funds_after_replay = read_aes_funds(&pool, EVM).await;
    assert_eq!(
        funds_after_replay,
        vec![BigDecimal::from(100)],
        "replay of an already-marked GUID must not add evm_fund"
    );
}

#[tokio::test]
async fn write_evm_seed_failure_rolls_back_mark() {
    let (_db, pool) = spin_up().await;
    insert_inflow(&pool, &inflow(GUID, 2)).await;

    {
        let mut conn = pool.get().await.unwrap();
        sql_query(format!(
            "ALTER TABLE address_evm_sources \
             ADD CONSTRAINT lz_test_reject_seed \
             CHECK (evm_address <> '{FAIL_EVM}')"
        ))
        .execute(&mut conn)
        .await
        .expect("install seed-failure CHECK");
    }

    let store = DbLzStore::new(pool.clone(), true);
    store
        .write_evm(GUID, FAIL_EVM)
        .await
        .expect_err("seed CHECK failure must fail write_evm");

    assert_eq!(
        read_evm_source(&pool, GUID).await,
        None,
        "evm_source must stay NULL so load_pending_guids can retry"
    );
    assert!(
        read_aes_funds(&pool, FAIL_EVM).await.is_empty(),
        "failed seed must not leave a partial address_evm_sources row"
    );
    let pending = store.load_pending_guids().await;
    assert!(
        pending.iter().any(|g| g == GUID),
        "rolled-back mark must keep the GUID pending; got {pending:?}"
    );

    {
        let mut conn = pool.get().await.unwrap();
        sql_query("ALTER TABLE address_evm_sources DROP CONSTRAINT lz_test_reject_seed")
            .execute(&mut conn)
            .await
            .expect("drop seed-failure CHECK");
    }

    store
        .write_evm(GUID, FAIL_EVM)
        .await
        .expect("retry after CHECK drop");
    assert_eq!(
        read_evm_source(&pool, GUID).await.as_deref(),
        Some(FAIL_EVM)
    );
    assert_eq!(read_aes_funds(&pool, FAIL_EVM).await, vec![
        BigDecimal::from(100)
    ]);
}
