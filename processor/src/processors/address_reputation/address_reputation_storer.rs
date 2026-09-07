// Copyright © MoveIndustries
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
use bigdecimal::BigDecimal;
use diesel::{
    sql_query,
    sql_types::{BigInt, Integer, Numeric, Text, Varchar},
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncConnection, RunQueryDsl};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info};

#[derive(diesel::QueryableByName)]
struct AesRow {
    #[diesel(sql_type = Varchar)]
    movement_address: String,
    #[diesel(sql_type = Varchar)]
    asset_type: String,
    #[diesel(sql_type = Varchar)]
    evm_address: String,
    #[diesel(sql_type = Numeric)]
    evm_fund: BigDecimal,
    #[diesel(sql_type = Numeric)]
    transfer_fund: BigDecimal,
    #[diesel(sql_type = Integer)]
    hops_min: i32,
}

pub struct AddressReputationStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    config: AddressReputationConfig,
    /// Receives LZ GUIDs after bridge_inflows is committed so the enricher never
    /// races against a row that doesn't exist yet.
    guid_sender: UnboundedSender<String>,
}

impl AddressReputationStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        config: AddressReputationConfig,
        guid_sender: UnboundedSender<String>,
    ) -> Self {
        Self {
            conn_pool,
            config,
            guid_sender,
        }
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

        let propagate_evm = self.config.propagate_evm_sources;
        let start_v = input.metadata.start_version;
        let end_v = input.metadata.end_version;
        let edges_len = edges.len();
        let inflows_len = inflows.len();
        // Collect GUIDs before moving inflows into the transaction closure.
        // They are sent to the enricher AFTER the transaction commits so the
        // bridge_inflows row is guaranteed to exist when write_evm runs.
        let pending_guids: Vec<String> =
            inflows.iter().filter_map(|bi| bi.lz_guid.clone()).collect();

        // Wrap edge/inflow inserts and the address_evm_sources rollup in one
        // Postgres transaction so the batch is atomic and read-your-writes
        // within the batch is preserved (a seed for edge idx=5 is visible when
        // edge idx=7 splits its sender's rollup).
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

                if !propagate_evm {
                    return Ok::<(), BoxErr>(());
                }

                for edge in &edges {
                    let Some(asset) = edge.asset_type.as_deref() else {
                        continue;
                    };
                    let ord = seen_ord(edge.transaction_version, edge.event_index);
                    if edge.is_bridge_inflow {
                        // Seed only the synthetic head (same event_index as the
                        // inflow). A paired Withdraw+Deposit can also be marked
                        // is_bridge_inflow so it skips hop propagation, but it
                        // must not add the same amount a second time.
                        let evm = inflow_for_bridge_seed(edge, &inflows)
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

        // Forward LZ GUIDs to the enricher now that the bridge_inflows rows exist.
        for guid in pending_guids {
            if let Err(e) = self.guid_sender.send(guid) {
                tracing::error!(err = ?e, "address_reputation: failed to forward lz_guid to enricher");
            }
        }

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

/// The synthetic head-injection edge uses the inflow's `event_index`. A paired
/// FA Withdraw+Deposit that delivered the same tokens is a different event and
/// must not match — `upsert_bridge_seed` adds `evm_fund`.
pub fn inflow_for_bridge_seed<'a>(
    edge: &TransferEdge,
    inflows: &'a [BridgeInflow],
) -> Option<&'a BridgeInflow> {
    inflows.iter().find(|bi| {
        bi.transaction_version == edge.transaction_version
            && bi.event_index == edge.event_index
            && bi.aptos_recipient == edge.to_address
            && bi.amount == edge.amount
    })
}

impl AsyncStep for AddressReputationStorer {}

impl NamedStep for AddressReputationStorer {
    fn name(&self) -> String {
        "AddressReputationStorer".to_string()
    }
}

/// Insert / update a single (recipient, asset, evm) row for a direct bridge inflow.
/// `evm_fund` accumulates; `hops_min` is pinned to 0.
pub async fn upsert_bridge_seed(
    conn: &mut aptos_indexer_processor_sdk::postgres::utils::database::DbPoolConnection<'_>,
    recipient: &str,
    asset: &str,
    evm: &str,
    amount: &BigDecimal,
    ord: i64,
) -> Result<(), ProcessorError> {
    let sql_str = "\
        INSERT INTO address_evm_sources \
            (movement_address, asset_type, evm_address, evm_fund, transfer_fund, \
             first_seen_ord, last_seen_ord, hops_min) \
        VALUES ($1, $2, $3, $4, 0, $5, $5, 0) \
        ON CONFLICT (movement_address, asset_type, evm_address) DO UPDATE SET \
            evm_fund       = address_evm_sources.evm_fund + EXCLUDED.evm_fund, \
            first_seen_ord = LEAST(address_evm_sources.first_seen_ord, EXCLUDED.first_seen_ord), \
            last_seen_ord  = GREATEST(address_evm_sources.last_seen_ord, EXCLUDED.last_seen_ord), \
            hops_min       = 0, \
            updated_at     = NOW() \
        RETURNING movement_address, asset_type, evm_address, evm_fund, transfer_fund, hops_min";
    let rows = sql_query(sql_str)
        .bind::<Varchar, _>(recipient)
        .bind::<Varchar, _>(asset)
        .bind::<Varchar, _>(evm)
        .bind::<Numeric, _>(amount)
        .bind::<BigInt, _>(ord)
        .get_results::<AesRow>(conn)
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("Failed to upsert bridge evm seed: {e:?}"),
            query: None,
        })?;
    for row in &rows {
        info!(
            movement_address = %row.movement_address,
            evm_address = %row.evm_address,
            asset_type = %row.asset_type,
            evm_fund = %row.evm_fund,
            transfer_fund = %row.transfer_fund,
            hops_min = row.hops_min,
            "aes: bridge seed upsert",
        );
    }
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
            (movement_address, asset_type, evm_address, evm_fund, transfer_fund, \
             first_seen_ord, last_seen_ord, hops_min) \
        SELECT $3, $2, s.evm_address, \
               0, \
               ROUND(($4 / total.t) * s.evm_fund * (1.0 / (s.hops_min + 1)), 9), \
               $5, $5, s.hops_min + 1 \
          FROM src s CROSS JOIN total \
         WHERE total.t IS NOT NULL AND total.t > 0 \
        ON CONFLICT (movement_address, asset_type, evm_address) DO UPDATE SET \
            transfer_fund  = address_evm_sources.transfer_fund + EXCLUDED.transfer_fund, \
            first_seen_ord = LEAST(address_evm_sources.first_seen_ord, EXCLUDED.first_seen_ord), \
            last_seen_ord  = GREATEST(address_evm_sources.last_seen_ord, EXCLUDED.last_seen_ord), \
            hops_min       = LEAST(address_evm_sources.hops_min, EXCLUDED.hops_min), \
            updated_at     = NOW() \
        RETURNING movement_address, asset_type, evm_address, evm_fund, transfer_fund, hops_min";
    let rows = sql_query(sql_str)
        .bind::<Text, _>(from_addr)
        .bind::<Varchar, _>(asset)
        .bind::<Varchar, _>(to_addr)
        .bind::<Numeric, _>(amount)
        .bind::<BigInt, _>(ord)
        .get_results::<AesRow>(conn)
        .await
        .map_err(|e| ProcessorError::DBStoreError {
            message: format!("Failed to propagate evm sources {from_addr} -> {to_addr}: {e:?}"),
            query: None,
        })?;
    for row in &rows {
        info!(
            movement_address = %row.movement_address,
            evm_address = %row.evm_address,
            asset_type = %row.asset_type,
            evm_fund = %row.evm_fund,
            transfer_fund = %row.transfer_fund,
            hops_min = row.hops_min,
            from_addr,
            "aes: propagate upsert",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::inflow_for_bridge_seed;
    use crate::processors::address_reputation::address_reputation_model::{
        BridgeInflow, TransferEdge,
    };
    use bigdecimal::BigDecimal;

    fn ts() -> chrono::NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    fn edge(event_index: i64, to: &str, amount: u64) -> TransferEdge {
        TransferEdge {
            transaction_version: 100,
            event_index,
            from_address: "0xfrom".to_string(),
            to_address: to.to_string(),
            asset_type: Some("0xasset".to_string()),
            amount: BigDecimal::from(amount),
            is_bridge_inflow: true,
            bridge_name: Some("circle_usdcx".to_string()),
            transaction_timestamp: ts(),
        }
    }

    fn inflow(event_index: i64, to: &str, amount: u64) -> BridgeInflow {
        BridgeInflow {
            transaction_version: 100,
            event_index,
            bridge_name: "circle_usdcx".to_string(),
            aptos_recipient: to.to_string(),
            evm_source: Some("0xevm".to_string()),
            src_chain_id: Some(1),
            asset_type: Some("0xasset".to_string()),
            amount: BigDecimal::from(amount),
            lz_guid: None,
            transaction_timestamp: ts(),
        }
    }

    #[test]
    fn synthetic_head_edge_matches_inflow() {
        let bi = inflow(2, "0xrecipient", 50);
        let synthetic = edge(2, "0xrecipient", 50);
        assert!(inflow_for_bridge_seed(&synthetic, &[bi]).is_some());
    }

    #[test]
    fn paired_withdraw_edge_does_not_match_inflow() {
        // Withdraw is event 0; the registered bridge event (and synthetic
        // head) is event 2. Same recipient and amount is not enough.
        let bi = inflow(2, "0xrecipient", 50);
        let paired = edge(0, "0xrecipient", 50);
        assert!(inflow_for_bridge_seed(&paired, &[bi]).is_none());
    }

    #[test]
    fn paired_plus_synthetic_seed_only_once() {
        let inflows = vec![inflow(2, "0xrecipient", 50)];
        let paired = edge(0, "0xrecipient", 50);
        let synthetic = edge(2, "0xrecipient", 50);
        let seeds: Vec<_> = [&paired, &synthetic]
            .into_iter()
            .filter_map(|e| inflow_for_bridge_seed(e, &inflows))
            .collect();
        assert_eq!(seeds.len(), 1);
        assert_eq!(seeds[0].event_index, 2);
    }
}
