// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::{
    sql_query,
    sql_types::{Array, Bool, Numeric, Text, Timestamp, Varchar},
};
use diesel_async::{
    pooled_connection::{bb8::Pool, AsyncDieselConnectionManager},
    AsyncPgConnection, RunQueryDsl,
};

// `Pool<AsyncPgConnection>` expands to
// `bb8::Pool<AsyncDieselConnectionManager<AsyncPgConnection>>` internally.
// Do NOT write Pool<AsyncDieselConnectionManager<AsyncPgConnection>> — that
// double-wraps the manager.
use serde::Serialize;

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

pub type DbPool = Pool<AsyncPgConnection>;

pub async fn new_pool(connection_string: &str, pool_size: u32) -> Result<DbPool> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(connection_string);
    Pool::builder()
        .max_size(pool_size)
        .build(manager)
        .await
        .map_err(|e| anyhow::anyhow!("DB pool build failed: {e}"))
}

// ---------------------------------------------------------------------------
// Row types — used as both Diesel results and JSON response bodies
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName, Serialize, Debug)]
pub struct AesRow {
    #[diesel(sql_type = Varchar)]
    pub movement_address: String,
    #[diesel(sql_type = Varchar)]
    pub asset_type: String,
    #[diesel(sql_type = Varchar)]
    pub evm_address: String,
    #[diesel(sql_type = Numeric)]
    pub evm_fund: BigDecimal,
    #[diesel(sql_type = Numeric)]
    pub transfer_fund: BigDecimal,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub hops_min: i32,
    #[diesel(sql_type = Timestamp)]
    pub inserted_at: NaiveDateTime,
}

#[derive(diesel::QueryableByName, Serialize, Debug)]
pub struct ErsRow {
    #[diesel(sql_type = Varchar)]
    pub evm_address: String,
    #[diesel(sql_type = Numeric)]
    pub risk_score: BigDecimal,
    #[diesel(sql_type = Varchar)]
    pub risk_label: String,
    #[diesel(sql_type = Varchar)]
    pub source: String,
    #[diesel(sql_type = Varchar)]
    pub recommendation: String,
    #[diesel(sql_type = Varchar)]
    pub severity: String,
    #[diesel(sql_type = Bool)]
    pub to_be_updated: bool,
    #[diesel(sql_type = Timestamp)]
    pub fetched_at: NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub inserted_at: NaiveDateTime,
}

#[derive(diesel::QueryableByName, Serialize, Debug)]
pub struct MvtScoreRow {
    #[diesel(sql_type = Varchar)]
    pub movement_address: String,
    #[diesel(sql_type = Varchar)]
    pub evm_address: String,
    #[diesel(sql_type = Numeric)]
    pub risk_score: BigDecimal,
    #[diesel(sql_type = Varchar)]
    pub recommendation: String,
    #[diesel(sql_type = Varchar)]
    pub severity: String,
    /// SUM(transfer_fund for this mvt address) + evm_fund of the highest-risk EVM source.
    #[diesel(sql_type = Numeric)]
    pub transfer_fund: BigDecimal,
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// Returns all `address_evm_sources` rows inserted on or after `since`.
///
/// Note: `inserted_at` records the row's first creation time; rows updated via
/// UPSERT retain their original `inserted_at`. A true last-modified filter
/// would require an `updated_at` column.
pub async fn query_since(pool: &DbPool, since: NaiveDateTime) -> Result<Vec<AesRow>> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "SELECT movement_address, asset_type, evm_address, \
                evm_fund, transfer_fund, hops_min, inserted_at \
           FROM address_evm_sources \
          WHERE inserted_at >= $1 \
          ORDER BY inserted_at, movement_address, evm_address",
    )
    .bind::<Timestamp, _>(since)
    .get_results(&mut conn)
    .await?)
}

/// Returns all `address_evm_sources` rows for the given Movement addresses.
pub async fn query_mvt_fetch(pool: &DbPool, addrs: Vec<String>) -> Result<Vec<AesRow>> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "SELECT movement_address, asset_type, evm_address, \
                evm_fund, transfer_fund, hops_min, inserted_at \
           FROM address_evm_sources \
          WHERE movement_address = ANY($1) \
          ORDER BY movement_address, evm_address",
    )
    .bind::<Array<Text>, _>(addrs)
    .get_results(&mut conn)
    .await?)
}

/// Returns `evm_address_risk_scores` rows for the given EVM addresses.
pub async fn query_evms(pool: &DbPool, addrs: Vec<String>) -> Result<Vec<ErsRow>> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "SELECT evm_address, risk_score, risk_label, source, \
                recommendation, severity, to_be_updated, fetched_at, inserted_at \
           FROM evm_address_risk_scores \
          WHERE evm_address = ANY($1) \
          ORDER BY evm_address",
    )
    .bind::<Array<Text>, _>(addrs)
    .get_results(&mut conn)
    .await?)
}

/// For each Movement address returns a single row with:
/// - the EVM source that has the highest risk score,
/// - and `transfer_fund` = SUM(all transfer_fund rows) + evm_fund of that top EVM source.
///
/// Movement addresses with no matching `address_evm_sources` row are omitted.
pub async fn query_mvt_scores(pool: &DbPool, addrs: Vec<String>) -> Result<Vec<MvtScoreRow>> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "WITH base AS ( \
             SELECT \
                 aes.movement_address, \
                 aes.evm_address, \
                 aes.evm_fund, \
                 aes.transfer_fund, \
                 COALESCE(ers.risk_score,      0)              AS risk_score, \
                 COALESCE(ers.recommendation, '')              AS recommendation, \
                 COALESCE(ers.severity,        '')             AS severity, \
                 SUM(aes.transfer_fund) OVER \
                     (PARTITION BY aes.movement_address)       AS total_transfer_fund \
               FROM address_evm_sources aes \
               LEFT JOIN evm_address_risk_scores ers \
                      ON aes.evm_address = ers.evm_address \
              WHERE aes.movement_address = ANY($1) \
         ), \
         ranked AS ( \
             SELECT *, \
                 ROW_NUMBER() OVER \
                     (PARTITION BY movement_address \
                          ORDER BY risk_score DESC, evm_address) AS rn \
               FROM base \
         ) \
         SELECT movement_address, \
                evm_address, \
                risk_score, \
                recommendation, \
                severity, \
                total_transfer_fund + evm_fund AS transfer_fund \
           FROM ranked \
          WHERE rn = 1 \
          ORDER BY movement_address",
    )
    .bind::<Array<Text>, _>(addrs)
    .get_results(&mut conn)
    .await?)
}
