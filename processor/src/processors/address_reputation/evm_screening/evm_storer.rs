use super::super::address_reputation_model::EvmRiskScore;
use super::lz_enricher::EVM_NULL_SENTINEL;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use diesel::{
    sql_query,
    sql_types::{Numeric, Timestamp, Varchar},
};
use diesel_async::RunQueryDsl;
use std::{collections::VecDeque, time::Duration};
use tracing::warn;

// ---------------------------------------------------------------------------
// DB abstraction — trait for dependency injection (enables mocking in tests)
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName)]
pub struct EvmRow {
    #[diesel(sql_type = Varchar)]
    pub evm_source: String,
}

/// Abstracts all DB operations needed by the EVM screening loop.
/// The production implementation hits PostgreSQL; tests provide an in-memory mock.
#[async_trait]
pub trait EvmScreeningDb: Send + Sync + 'static {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()>;
    async fn is_fresh_in_db(&self, evm: &str) -> bool;
    async fn load_pending_evms(&self) -> VecDeque<String>;
}

/// Production implementation backed by PostgreSQL.
pub struct DbScoreSaver {
    pool: Option<ArcDbPool>,
    ttl: Duration,
}

impl DbScoreSaver {
    pub fn new(pool: Option<ArcDbPool>, ttl: Duration) -> Self {
        Self { pool, ttl }
    }
}

#[async_trait]
impl EvmScreeningDb for DbScoreSaver {
    async fn save(&self, score: &EvmRiskScore) -> anyhow::Result<()> {
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => return Ok(()),
        };
        save_risk_score(pool, score).await
    }

    async fn is_fresh_in_db(&self, evm: &str) -> bool {
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => return false,
        };
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(err = ?e, "evm_fetch_loop: failed to get DB connection for TTL check");
                return false;
            },
        };
        let ttl_cutoff =
            (chrono::Utc::now() - chrono::Duration::seconds(self.ttl.as_secs() as i64)).naive_utc();
        sql_query(
            "SELECT evm_address AS evm_source \
             FROM evm_address_risk_scores \
             WHERE evm_address = $1 \
               AND risk_score > 0 \
               AND fetched_at > $2 \
             LIMIT 1",
        )
        .bind::<Varchar, _>(evm)
        .bind::<Timestamp, _>(ttl_cutoff)
        .get_results::<EvmRow>(&mut conn)
        .await
        .map(|rows| !rows.is_empty())
        .unwrap_or(false)
    }

    async fn load_pending_evms(&self) -> VecDeque<String> {
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => return VecDeque::new(),
        };
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(err = ?e, "evm_fetch_loop: failed to get DB connection for EVM seed");
                return VecDeque::new();
            },
        };
        let ttl_cutoff =
            (chrono::Utc::now() - chrono::Duration::seconds(self.ttl.as_secs() as i64)).naive_utc();
        match sql_query(
            "SELECT DISTINCT bi.evm_source \
               FROM bridge_inflows bi \
              WHERE bi.evm_source IS NOT NULL \
                AND bi.evm_source != $1 \
                AND bi.evm_source NOT IN (\
                  SELECT evm_address \
                    FROM evm_address_risk_scores \
                   WHERE risk_score > 0 \
                     AND fetched_at > $2\
                ) \
             UNION \
             -- Previously failed and TTL expired: worth retrying. \
             SELECT evm_address AS evm_source \
               FROM evm_address_risk_scores \
              WHERE risk_score = 0 \
                AND fetched_at <= $2",
        )
        .bind::<Varchar, _>(EVM_NULL_SENTINEL)
        .bind::<Timestamp, _>(ttl_cutoff)
        .get_results::<EvmRow>(&mut conn)
        .await
        {
            Ok(rows) => rows.into_iter().map(|r| r.evm_source).collect(),
            Err(e) => {
                tracing::error!(err = ?e, "evm_fetch_loop: failed to load pending EVMs; starting empty");
                VecDeque::new()
            },
        }
    }
}

// ---------------------------------------------------------------------------
// DB persistence
// ---------------------------------------------------------------------------

async fn save_risk_score(pool: &ArcDbPool, score: &EvmRiskScore) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    sql_query(
        "INSERT INTO evm_address_risk_scores \
            (evm_address, risk_score, risk_label, source, recommendation, severity, fetched_at, inserted_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) \
         ON CONFLICT (evm_address) DO UPDATE SET \
            risk_score     = EXCLUDED.risk_score, \
            risk_label     = EXCLUDED.risk_label, \
            source         = EXCLUDED.source, \
            recommendation = EXCLUDED.recommendation, \
            severity       = EXCLUDED.severity, \
            fetched_at     = EXCLUDED.fetched_at",
    )
    .bind::<Varchar, _>(&score.evm_address)
    .bind::<Numeric, _>(&score.risk_score)
    .bind::<Varchar, _>(&score.risk_label)
    .bind::<Varchar, _>(&score.source)
    .bind::<Varchar, _>(&score.recommendation)
    .bind::<Varchar, _>(&score.severity)
    .bind::<Timestamp, _>(score.fetched_at)
    .execute(&mut conn)
    .await?;
    Ok(())
}
