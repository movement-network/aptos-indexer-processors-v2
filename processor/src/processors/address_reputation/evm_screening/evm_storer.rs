use super::{super::address_reputation_model::EvmRiskScore, lz_enricher::EVM_NULL_SENTINEL};
use anyhow::Context;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use diesel::{
    sql_query,
    sql_types::{Bool, Numeric, Timestamp, Varchar},
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
    /// Return EVM addresses that still need screening (startup / reconnect seed).
    ///
    /// Must return `Err` on connection or query failure. Callers retry; they
    /// must not treat a failed load as an empty queue (that drops pending
    /// addresses until the next successful reconnect or process restart).
    async fn load_pending_evms(&self) -> anyhow::Result<VecDeque<String>>;
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
               AND to_be_updated = FALSE \
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

    async fn load_pending_evms(&self) -> anyhow::Result<VecDeque<String>> {
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => return Ok(VecDeque::new()),
        };
        let mut conn = pool
            .get()
            .await
            .context("evm_fetch_loop: failed to get DB connection for EVM seed")?;
        let rows = sql_query(
            "SELECT DISTINCT bi.evm_source \
               FROM bridge_inflows bi \
              WHERE bi.evm_source IS NOT NULL \
                AND bi.evm_source != $1 \
                AND bi.evm_source NOT IN (\
                  SELECT evm_address \
                    FROM evm_address_risk_scores \
                   WHERE to_be_updated = FALSE\
                ) \
             UNION \
             SELECT evm_address AS evm_source \
               FROM evm_address_risk_scores \
              WHERE to_be_updated = TRUE",
        )
        .bind::<Varchar, _>(EVM_NULL_SENTINEL)
        .get_results::<EvmRow>(&mut conn)
        .await
        .context("evm_fetch_loop: failed to load pending EVMs")?;
        Ok(rows.into_iter().map(|r| r.evm_source).collect())
    }
}

/// Retry `load_pending_evms` until the store answers. A failed query used to
/// return an empty queue, which dropped every pending EVM until the next
/// successful reconnect or process restart. An empty `Ok` is still valid
/// (nothing pending).
pub(crate) async fn load_pending_evms_retrying(
    db: &dyn EvmScreeningDb,
    retry_delay: Duration,
) -> VecDeque<String> {
    loop {
        match db.load_pending_evms().await {
            Ok(q) => return q,
            Err(e) => {
                tracing::error!(
                    err = %e,
                    retry_secs = retry_delay.as_secs_f64(),
                    "evm_fetch_loop: failed to load pending EVMs; retrying"
                );
                tokio::time::sleep(retry_delay).await;
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
            (evm_address, risk_score, risk_label, source, recommendation, severity, fetched_at, inserted_at, to_be_updated) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8) \
         ON CONFLICT (evm_address) DO UPDATE SET \
            risk_score     = EXCLUDED.risk_score, \
            risk_label     = EXCLUDED.risk_label, \
            source         = EXCLUDED.source, \
            recommendation = EXCLUDED.recommendation, \
            severity       = EXCLUDED.severity, \
            fetched_at     = EXCLUDED.fetched_at, \
            to_be_updated  = EXCLUDED.to_be_updated",
    )
    .bind::<Varchar, _>(&score.evm_address)
    .bind::<Numeric, _>(&score.risk_score)
    .bind::<Varchar, _>(&score.risk_label)
    .bind::<Varchar, _>(&score.source)
    .bind::<Varchar, _>(&score.recommendation)
    .bind::<Varchar, _>(&score.severity)
    .bind::<Timestamp, _>(score.fetched_at)
    .bind::<Bool, _>(score.to_be_updated)
    .execute(&mut conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    struct FailDb;

    #[async_trait]
    impl EvmScreeningDb for FailDb {
        async fn save(&self, _score: &EvmRiskScore) -> anyhow::Result<()> {
            Ok(())
        }

        async fn is_fresh_in_db(&self, _evm: &str) -> bool {
            false
        }

        async fn load_pending_evms(&self) -> anyhow::Result<VecDeque<String>> {
            Err(anyhow::anyhow!("db down"))
        }
    }

    struct FailThenOk {
        remaining_failures: AtomicU32,
        evms: Vec<String>,
    }

    #[async_trait]
    impl EvmScreeningDb for FailThenOk {
        async fn save(&self, _score: &EvmRiskScore) -> anyhow::Result<()> {
            Ok(())
        }

        async fn is_fresh_in_db(&self, _evm: &str) -> bool {
            false
        }

        async fn load_pending_evms(&self) -> anyhow::Result<VecDeque<String>> {
            let left = self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
            if left > 0 {
                Err(anyhow::anyhow!("transient db error"))
            } else {
                Ok(self.evms.iter().cloned().collect())
            }
        }
    }

    #[tokio::test]
    async fn load_pending_evms_error_is_not_empty_success() {
        let db: Arc<dyn EvmScreeningDb> = Arc::new(FailDb);
        assert!(
            db.load_pending_evms().await.is_err(),
            "a failed load must surface as Err, not an empty queue"
        );
    }

    #[tokio::test]
    async fn load_pending_evms_retries_until_success() {
        let pending = "0x5e87d7e75b272fb7150b4d1a05afb6bd71474950";
        let db = FailThenOk {
            remaining_failures: AtomicU32::new(2),
            evms: vec![pending.to_string()],
        };
        let queue = load_pending_evms_retrying(&db, Duration::from_millis(1)).await;
        assert_eq!(queue, VecDeque::from([pending.to_string()]));
    }

    #[tokio::test]
    async fn load_pending_evms_empty_ok_is_valid() {
        let db = FailThenOk {
            remaining_failures: AtomicU32::new(0),
            evms: vec![],
        };
        let queue = load_pending_evms_retrying(&db, Duration::from_millis(1)).await;
        assert!(queue.is_empty());
    }
}
