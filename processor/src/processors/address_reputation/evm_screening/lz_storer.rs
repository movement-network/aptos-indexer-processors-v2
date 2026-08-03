// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use super::super::address_reputation_storer::{seen_ord, upsert_bridge_seed};
use super::lz_enricher::EVM_NULL_SENTINEL;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{BigInt, Nullable, Numeric, Varchar},
};
use diesel_async::RunQueryDsl;
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
    async fn write_evm(&self, guid: &str, evm: &str);
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
        Self { pool, propagate_evm }
    }
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

    async fn write_evm(&self, guid: &str, evm: &str) {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to get DB connection");
                return;
            },
        };

        let rows: Vec<InflowRow> = match sql_query(
            "UPDATE bridge_inflows \
               SET evm_source = $1 \
             WHERE lz_guid = $2 AND evm_source IS NULL \
             RETURNING aptos_recipient, asset_type, amount, \
                       transaction_version, event_index",
        )
        .bind::<Varchar, _>(evm)
        .bind::<Varchar, _>(guid)
        .get_results(&mut conn)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(lz_guid = guid, err = ?e, "lz_enricher: failed to update bridge_inflows");
                return;
            },
        };

        if rows.is_empty() || !self.propagate_evm || evm == EVM_NULL_SENTINEL {
            return;
        }

        for row in &rows {
            let Some(ref asset) = row.asset_type else {
                continue;
            };
            let ord = seen_ord(row.transaction_version, row.event_index);
            if let Err(e) = upsert_bridge_seed(
                &mut conn,
                &row.aptos_recipient,
                asset,
                evm,
                &row.amount,
                ord,
            )
            .await
            {
                warn!(lz_guid = guid, err = ?e, "lz_enricher: failed to seed address_evm_sources");
            }
        }
    }
}
