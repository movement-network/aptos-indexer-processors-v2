// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::address_reputation::{
        address_reputation_config::AddressReputationConfig,
        address_reputation_model::{BridgeInflow, TransferEdge},
    },
    schema,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::ArcDbPool,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, FromPrimitive, Zero};
use diesel::{
    dsl::sql,
    sql_query,
    sql_types::{BigInt, Numeric, Text, Varchar},
    ExpressionMethods, OptionalExtension, QueryDsl,
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncConnection, RunQueryDsl};
use tracing::debug;

pub struct AddressReputationStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    config: AddressReputationConfig,
}

impl AddressReputationStorer {
    pub fn new(conn_pool: ArcDbPool, config: AddressReputationConfig) -> Self {
        Self { conn_pool, config }
    }
}

#[async_trait]
impl Processable for AddressReputationStorer {
    type Input = (Vec<TransferEdge>, Vec<BridgeInflow>);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<TransferEdge>, Vec<BridgeInflow>)>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (mut edges, inflows) = input.data;
        // Edges are propagated in (version, event_index) order so that a bridge
        // seed emitted earlier in a txn is visible to a subsequent A->B in the
        // same txn (read-your-writes within the enclosing DB transaction).
        edges.sort_by_key(|e| (e.transaction_version, e.event_index));

        let mut conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get conn: {e:?}"),
                query: None,
            })?;

        let decay = BigDecimal::from_f64(self.config.decay).unwrap_or_else(|| BigDecimal::from(0));
        let propagate_evm = self.config.propagate_evm_sources;
        let start_v = input.metadata.start_version;
        let end_v = input.metadata.end_version;
        let edges_len = edges.len();
        let inflows_len = inflows.len();

        // Wrap the whole batch -- inserts, reputation propagation, and EVM-source
        // rollup upserts -- in a single Postgres transaction. This gives us two
        // properties we rely on:
        //   1. Atomic: partial failure rolls back; the batch is retried cleanly.
        //   2. Read-your-writes within the batch: a bridge seed written for edge
        //      idx=5 is visible to the A->B upsert done for edge idx=7, so
        //      multi-hop cascades within a single txn/batch produce the same
        //      result as processing the batch one-edge-at-a-time.
        // Concurrency is left to the SDK: this processor runs single-threaded
        // per shard, so batches are strictly serialized end-to-end.
        type BoxErr = Box<dyn std::error::Error + Send + Sync>;
        conn.transaction::<_, BoxErr, _>(move |conn| {
            let edges = edges;
            let inflows = inflows;
            async move {
                if !edges.is_empty() {
                    diesel::insert_into(schema::address_transfer_edges::table)
                        .values(&edges)
                        .on_conflict((
                            schema::address_transfer_edges::transaction_version,
                            schema::address_transfer_edges::event_index,
                        ))
                        .do_nothing()
                        .execute(conn)
                        .await
                        .map_err(|e| ProcessorError::DBStoreError {
                            message: format!("Failed to insert transfer edges: {e:?}"),
                            query: None,
                        })?;
                }

                if !inflows.is_empty() {
                    diesel::insert_into(schema::bridge_inflows::table)
                        .values(&inflows)
                        .on_conflict((
                            schema::bridge_inflows::transaction_version,
                            schema::bridge_inflows::event_index,
                        ))
                        .do_nothing()
                        .execute(conn)
                        .await
                        .map_err(|e| ProcessorError::DBStoreError {
                            message: format!("Failed to insert bridge inflows: {e:?}"),
                            query: None,
                        })?;
                }

                for edge in &edges {
                    // Reputation score propagation (unchanged semantics).
                    let (contribution, highest_seed, hop) = if edge.is_bridge_inflow {
                        let seed = lookup_bridge_seed(conn, &inflows, edge).await;
                        (seed.clone(), seed, Some(0))
                    } else {
                        let sender = lookup_score(conn, &edge.from_address).await;
                        let s = &decay * &sender.score;
                        let hop = sender.nearest_seed_hop.map(|h| h + 1);
                        (s, sender.highest_seed, hop)
                    };
                    upsert_address_reputation(
                        conn,
                        &edge.to_address,
                        &contribution,
                        &highest_seed,
                        hop,
                        edge.transaction_version,
                        edge.transaction_timestamp,
                    )
                    .await?;

                    // EVM-source rollup propagation (address_evm_sources).
                    if !propagate_evm {
                        continue;
                    }
                    let Some(asset) = edge.asset_type.as_deref() else {
                        continue;
                    };
                    let ord = seen_ord(edge.transaction_version, edge.event_index);
                    if edge.is_bridge_inflow {
                        // Seed the recipient with the direct EVM source (if known).
                        let evm = inflows
                            .iter()
                            .find(|bi| {
                                bi.transaction_version == edge.transaction_version
                                    && bi.aptos_recipient == edge.to_address
                                    && bi.amount == edge.amount
                            })
                            .and_then(|bi| bi.evm_source.clone());
                        if let Some(evm) = evm {
                            upsert_bridge_seed(
                                conn,
                                &edge.to_address,
                                asset,
                                &evm,
                                &edge.amount,
                                ord,
                            )
                            .await?;
                        }
                    } else {
                        // Non-bridge transfer: distribute `amount` across the
                        // sender's existing EVM sources proportional to their
                        // current `evm_fund` share. Sender rows are not touched.
                        propagate_evm_sources(
                            conn,
                            &edge.from_address,
                            &edge.to_address,
                            asset,
                            &edge.amount,
                            ord,
                        )
                        .await?;
                    }
                }

                Ok::<(), BoxErr>(())
            }
            .scope_boxed()
        })
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("address_reputation batch txn failed: {e}"),
            query: None,
        })?;

        debug!(
            "address_reputation: stored {} edges, {} inflows for versions [{}, {}]",
            edges_len, inflows_len, start_v, end_v,
        );

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

/// Pack (version, event_index) into a single monotone i64 for
/// `first_seen_ord` / `last_seen_ord`. 24 bits of event_index is well above any
/// realistic per-txn event count (millions).
pub fn seen_ord(version: i64, event_index: i64) -> i64 {
    (version << 24) | (event_index & 0x00FF_FFFF)
}

impl AsyncStep for AddressReputationStorer {}

impl NamedStep for AddressReputationStorer {
    fn name(&self) -> String {
        "AddressReputationStorer".to_string()
    }
}

struct SenderState {
    score: BigDecimal,
    highest_seed: BigDecimal,
    nearest_seed_hop: Option<i32>,
}

async fn lookup_score(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    addr: &str,
) -> SenderState {
    use schema::address_reputation::dsl::*;
    let row: Option<(BigDecimal, BigDecimal, Option<i32>)> = address_reputation
        .filter(address.eq(addr))
        .select((score, highest_seed, nearest_seed_hop))
        .first::<(BigDecimal, BigDecimal, Option<i32>)>(conn)
        .await
        .optional()
        .unwrap_or(None);
    match row {
        Some((s, hs, hop)) => SenderState {
            score: s,
            highest_seed: hs,
            nearest_seed_hop: hop,
        },
        None => SenderState {
            score: BigDecimal::zero(),
            highest_seed: BigDecimal::zero(),
            nearest_seed_hop: None,
        },
    }
}

async fn lookup_bridge_seed(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    inflows_in_batch: &[BridgeInflow],
    edge: &TransferEdge,
) -> BigDecimal {
    // Find the bridge_inflow row this edge was tagged with.
    let evm_source = inflows_in_batch.iter().find(|bi| {
        bi.transaction_version == edge.transaction_version
            && bi.aptos_recipient == edge.to_address
            && bi.amount == edge.amount
    });
    let evm = match evm_source.and_then(|bi| bi.evm_source.clone()) {
        Some(e) => e,
        None => return BigDecimal::zero(),
    };

    use schema::evm_address_risk_scores::dsl::*;
    evm_address_risk_scores
        .filter(evm_address.eq(&evm))
        .select(risk_score)
        .first::<BigDecimal>(conn)
        .await
        .optional()
        .unwrap_or(None)
        .unwrap_or_else(BigDecimal::zero)
}

async fn upsert_address_reputation(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    address: &str,
    contribution: &BigDecimal,
    candidate_highest_seed: &BigDecimal,
    candidate_hop: Option<i32>,
    txn_version: i64,
    txn_ts: chrono::NaiveDateTime,
) -> Result<(), ProcessorError> {
    use schema::address_reputation::dsl as ar;
    diesel::insert_into(ar::address_reputation)
        .values((
            ar::address.eq(address),
            ar::score.eq(contribution),
            ar::highest_seed.eq(candidate_highest_seed),
            ar::nearest_seed_hop.eq(candidate_hop),
            ar::last_updated_version.eq(txn_version),
            ar::last_updated_timestamp.eq(txn_ts),
        ))
        .on_conflict(ar::address)
        .do_update()
        .set((
            ar::score.eq(sql::<Numeric>(
                "GREATEST(address_reputation.score, EXCLUDED.score)",
            )),
            ar::highest_seed.eq(sql::<Numeric>(
                "GREATEST(address_reputation.highest_seed, EXCLUDED.highest_seed)",
            )),
            ar::nearest_seed_hop.eq(sql::<diesel::sql_types::Nullable<diesel::sql_types::Int4>>(
                "LEAST(COALESCE(address_reputation.nearest_seed_hop, EXCLUDED.nearest_seed_hop), \
                 COALESCE(EXCLUDED.nearest_seed_hop, address_reputation.nearest_seed_hop))",
            )),
            ar::last_updated_version.eq(txn_version),
            ar::last_updated_timestamp.eq(txn_ts),
        ))
        .execute(conn)
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("Failed to upsert address_reputation for {address}: {e:?}"),
            query: None,
        })?;
    Ok(())
}

/// Insert / update a single (recipient, asset, evm) row for a direct bridge inflow.
/// `evm_fund` accumulates; `hops_min` is pinned to 0.
async fn upsert_bridge_seed(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    recipient: &str,
    asset: &str,
    evm: &str,
    amount: &BigDecimal,
    ord: i64,
) -> Result<(), ProcessorError> {
    let sql_str = "\
        INSERT INTO address_evm_sources \
            (movement_address, asset_type, evm_address, evm_fund, \
             first_seen_ord, last_seen_ord, hops_min) \
        VALUES ($1, $2, $3, $4, $5, $5, 0) \
        ON CONFLICT (movement_address, asset_type, evm_address) DO UPDATE SET \
            evm_fund      = address_evm_sources.evm_fund + EXCLUDED.evm_fund, \
            first_seen_ord = LEAST(address_evm_sources.first_seen_ord, EXCLUDED.first_seen_ord), \
            last_seen_ord = GREATEST(address_evm_sources.last_seen_ord, EXCLUDED.last_seen_ord), \
            hops_min      = 0";
    sql_query(sql_str)
        .bind::<Varchar, _>(recipient)
        .bind::<Varchar, _>(asset)
        .bind::<Varchar, _>(evm)
        .bind::<Numeric, _>(amount)
        .bind::<BigInt, _>(ord)
        .execute(conn)
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("Failed to upsert bridge evm seed: {e:?}"),
            query: None,
        })?;
    Ok(())
}

/// Distribute a transfer's `amount` across the sender's existing EVM sources.
/// Each source E gets `amount * (evm_fund_A[E] / SUM_E(evm_fund_A))` added to the
/// receiver's rollup. Sender rows are NOT modified. Runs inside the caller's
/// DB transaction so read-your-writes ordering is preserved within a batch.
async fn propagate_evm_sources(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    from_addr: &str,
    to_addr: &str,
    asset: &str,
    amount: &BigDecimal,
    ord: i64,
) -> Result<(), ProcessorError> {
    let sql_str = "\
        WITH src AS ( \
            SELECT evm_address, evm_fund, hops_min \
              FROM address_evm_sources \
             WHERE movement_address = $1 AND asset_type = $2 \
        ), \
        total AS (SELECT SUM(evm_fund) AS t FROM src) \
        INSERT INTO address_evm_sources \
            (movement_address, asset_type, evm_address, evm_fund, \
             first_seen_ord, last_seen_ord, hops_min) \
        SELECT $3, $2, s.evm_address, \
               (s.evm_fund * $4) / total.t, \
               $5, $5, s.hops_min + 1 \
          FROM src s CROSS JOIN total \
         WHERE total.t IS NOT NULL AND total.t > 0 \
        ON CONFLICT (movement_address, asset_type, evm_address) DO UPDATE SET \
            evm_fund      = address_evm_sources.evm_fund + EXCLUDED.evm_fund, \
            first_seen_ord = LEAST(address_evm_sources.first_seen_ord, EXCLUDED.first_seen_ord), \
            last_seen_ord = GREATEST(address_evm_sources.last_seen_ord, EXCLUDED.last_seen_ord), \
            hops_min      = LEAST(address_evm_sources.hops_min, EXCLUDED.hops_min)";
    sql_query(sql_str)
        .bind::<Text, _>(from_addr)
        .bind::<Varchar, _>(asset)
        .bind::<Varchar, _>(to_addr)
        .bind::<Numeric, _>(amount)
        .bind::<BigInt, _>(ord)
        .execute(conn)
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("Failed to propagate evm sources {from_addr} -> {to_addr}: {e:?}"),
            query: None,
        })?;
    Ok(())
}
