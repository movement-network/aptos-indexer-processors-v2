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
use diesel::{dsl::sql, sql_types::Numeric, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
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
        let (edges, inflows) = input.data;
        let mut conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get conn: {e:?}"),
                query: None,
            })?;

        // 1. Bulk-insert transfer edges.
        if !edges.is_empty() {
            diesel::insert_into(schema::address_transfer_edges::table)
                .values(&edges)
                .on_conflict((
                    schema::address_transfer_edges::transaction_version,
                    schema::address_transfer_edges::event_index,
                ))
                .do_nothing()
                .execute(&mut conn)
                .await
                .map_err(|e| ProcessorError::DBStoreError {
                    message: format!("Failed to insert transfer edges: {e:?}"),
                    query: None,
                })?;
        }

        // 2. Bulk-insert bridge inflows.
        if !inflows.is_empty() {
            diesel::insert_into(schema::bridge_inflows::table)
                .values(&inflows)
                .on_conflict((
                    schema::bridge_inflows::transaction_version,
                    schema::bridge_inflows::event_index,
                ))
                .do_nothing()
                .execute(&mut conn)
                .await
                .map_err(|e| ProcessorError::DBStoreError {
                    message: format!("Failed to insert bridge inflows: {e:?}"),
                    query: None,
                })?;
        }

        // 3. Propagate reputation along each edge (serial, P0).
        let decay = BigDecimal::from_f64(self.config.decay).unwrap_or_else(|| BigDecimal::from(0));
        for edge in &edges {
            let (contribution, highest_seed, hop) = if edge.is_bridge_inflow {
                let seed = lookup_bridge_seed(&mut conn, &inflows, edge).await;
                (seed.clone(), seed, Some(0))
            } else {
                let sender = lookup_score(&mut conn, &edge.from_address).await;
                let s = &decay * &sender.score;
                let hop = sender.nearest_seed_hop.map(|h| h + 1);
                (s, sender.highest_seed, hop)
            };

            upsert_address_reputation(
                &mut conn,
                &edge.to_address,
                &contribution,
                &highest_seed,
                hop,
                edge.transaction_version,
                edge.transaction_timestamp,
            )
            .await?;
        }

        debug!(
            "address_reputation: stored {} edges, {} inflows for versions [{}, {}]",
            edges.len(),
            inflows.len(),
            input.metadata.start_version,
            input.metadata.end_version,
        );

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
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
