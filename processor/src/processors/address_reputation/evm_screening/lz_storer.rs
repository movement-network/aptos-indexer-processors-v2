// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use super::{
    super::address_reputation_storer::{seen_ord, upsert_bridge_seed},
    lz_enricher::EVM_NULL_SENTINEL,
};
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{BigInt, Nullable, Numeric, Varchar},
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncConnection, RunQueryDsl};
use std::collections::VecDeque;
use tracing::{error, warn};

// ---------------------------------------------------------------------------
// Diesel query result types (private to this module)
// ---------------------------------------------------------------------------

#[derive(diesel::QueryableByName)]
struct GuidRow {
    #[diesel(sql_type = Varchar)]
    lz_guid: String,
}

#[derive(diesel::QueryableByName)]
struct InflowRow {
    #[diesel(sql_type = Varchar)]
    aptos_recipient: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    asset_type: Option<String>,
    #[diesel(sql_type = Numeric)]
    amount: BigDecimal,
    #[diesel(sql_type = BigInt)]
    transaction_version: i64,
    #[diesel(sql_type = BigInt)]
    event_index: i64,
}

// ---------------------------------------------------------------------------
// Trait — abstracts DB operations for LzEnricher (enables mocking in tests)
// ---------------------------------------------------------------------------

#[async_trait]
pub trait LzDb: Send + Sync + 'static {
    /// Return all GUIDs that have no EVM source yet (startup seed).
    async fn load_pending_guids(&self) -> VecDeque<String>;
    /// Persist the resolved EVM address (or sentinel) for a GUID and
    /// optionally propagate it to `address_evm_sources`.
    ///
    /// Returns `Err` on a transient write failure so the caller can retry.
    /// Marking `bridge_inflows.evm_source` and seeding `address_evm_sources`
    /// share one transaction: a committed mark with a failed seed is
    /// unrecoverable (`load_pending_guids` and the UPDATE both filter
    /// `evm_source IS NULL`).
    async fn write_evm(&self, guid: &str, evm: &str) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Production implementation backed by PostgreSQL
// ---------------------------------------------------------------------------

pub struct DbLzStore {
    pool: ArcDbPool,
    propagate_evm: bool,
}

impl DbLzStore {
    pub fn new(pool: ArcDbPool, propagate_evm: bool) -> Self {
        Self {
            pool,
            propagate_evm,
        }
    }
}

/// Whether this `write_evm` call must also seed `address_evm_sources`.
///
/// When true, the seed and the `bridge_inflows.evm_source` UPDATE must share
/// one transaction. Seed-before-mark without a txn is also wrong:
/// `upsert_bridge_seed` ADDS `evm_fund`, so a retried mark would double-count.
pub fn write_evm_must_seed(propagate_evm: bool, evm: &str) -> bool {
    propagate_evm && evm != EVM_NULL_SENTINEL
}

#[async_trait]
impl LzDb for DbLzStore {
    async fn load_pending_guids(&self) -> VecDeque<String> {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(err = ?e, "lz_enricher: failed to get DB connection for startup seed");
                return VecDeque::new();
            },
        };
        match sql_query(
            "SELECT lz_guid FROM bridge_inflows \
              WHERE lz_guid IS NOT NULL AND evm_source IS NULL",
        )
        .get_results::<GuidRow>(&mut conn)
        .await
        {
            Ok(rows) => rows.into_iter().map(|r| r.lz_guid).collect(),
            Err(e) => {
                error!(err = ?e, "lz_enricher: failed to load pending GUIDs; starting empty");
                VecDeque::new()
            },
        }
    }

    async fn write_evm(&self, guid: &str, evm: &str) -> anyhow::Result<()> {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to get DB connection");
                return Err(anyhow::anyhow!(
                    "lz_enricher: failed to get DB connection: {e}"
                ));
            },
        };

        let propagate_evm = self.propagate_evm;
        let guid = guid.to_owned();
        let evm = evm.to_owned();
        // Keep a copy for the outer error path; the transaction closure moves
        // the owned strings in.
        let guid_for_err = guid.clone();

        type BoxErr = Box<dyn std::error::Error + Send + Sync>;
        conn.transaction::<_, BoxErr, _>(move |conn| {
            async move {
                let rows: Vec<InflowRow> = sql_query(
                    "UPDATE bridge_inflows \
                       SET evm_source = $1 \
                     WHERE lz_guid = $2 AND evm_source IS NULL \
                     RETURNING aptos_recipient, asset_type, amount, \
                               transaction_version, event_index",
                )
                .bind::<Varchar, _>(evm.as_str())
                .bind::<Varchar, _>(guid.as_str())
                .get_results(conn)
                .await?;

                if rows.is_empty() {
                    warn!(lz_guid = guid, evm_address = evm, "lz_enricher: UPDATE matched 0 rows — GUID not found in bridge_inflows or already resolved");
                    return Ok(());
                }
                if !write_evm_must_seed(propagate_evm, &evm) {
                    return Ok(());
                }

                for row in &rows {
                    let Some(ref asset) = row.asset_type else {
                        warn!(
                            lz_guid = guid,
                            aptos_recipient = %row.aptos_recipient,
                            transaction_version = row.transaction_version,
                            "lz_enricher: bridge_inflows row has NULL asset_type — address_evm_sources seed skipped"
                        );
                        continue;
                    };
                    let ord = seen_ord(row.transaction_version, row.event_index);
                    // `?` aborts the transaction so the evm_source UPDATE
                    // rolls back. Swallowing here (the old warn-only path)
                    // left evm_source set and skipped the seed forever.
                    upsert_bridge_seed(
                        conn,
                        &row.aptos_recipient,
                        asset,
                        &evm,
                        &row.amount,
                        ord,
                    )
                    .await?;
                }
                Ok(())
            }
            .scope_boxed()
        })
        .await
        .map_err(|e| {
            error!(
                lz_guid = %guid_for_err,
                err = ?e,
                "lz_enricher: write_evm transaction failed; evm_source left unresolved for retry"
            );
            anyhow::anyhow!("lz_enricher: write_evm transaction failed: {e}")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVM: &str = "0x0000000000000000000000000000000000000001";

    #[test]
    fn must_seed_resolved_evm_when_propagation_enabled() {
        assert!(
            write_evm_must_seed(true, EVM),
            "a real EVM with propagate_evm must seed address_evm_sources in the same txn as the mark"
        );
    }

    #[test]
    fn must_not_seed_null_sentinel() {
        assert!(
            !write_evm_must_seed(true, EVM_NULL_SENTINEL),
            "404 sentinel is a mark-only write; there is no depositor to seed"
        );
    }

    #[test]
    fn must_not_seed_when_propagation_disabled() {
        assert!(
            !write_evm_must_seed(false, EVM),
            "propagate_evm=false commits the mark alone"
        );
    }
}
