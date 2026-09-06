// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod models;
pub mod stake_extractor;
pub mod stake_processor;
pub mod stake_storer;

use crate::processors::stake::models::{
    current_delegated_voter::CurrentDelegatedVoter,
    delegator_activities::DelegatedStakingActivity,
    delegator_balances::{CurrentDelegatorBalance, CurrentDelegatorBalanceMap, DelegatorBalance},
    delegator_pools::{
        CurrentDelegatorPoolBalance, DelegatorPool, DelegatorPoolBalance, DelegatorPoolMap,
    },
    proposal_votes::ProposalVote,
    stake_utils::DelegationVoteGovernanceRecordsResource,
    staking_pool_voter::{CurrentStakingPoolVoter, StakingPoolVoterMap},
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{write_set_change::Change, Transaction},
    postgres::utils::database::DbPoolConnection,
    utils::convert::standardize_address,
};

pub async fn parse_stake_data(
    transactions: &Vec<Transaction>,
    mut conn: Option<DbPoolConnection<'_>>,
    query_retries: u32,
    query_retry_delay_ms: u64,
) -> Result<
    (
        Vec<CurrentStakingPoolVoter>,
        Vec<ProposalVote>,
        Vec<DelegatedStakingActivity>,
        Vec<DelegatorBalance>,
        Vec<CurrentDelegatorBalance>,
        Vec<DelegatorPool>,
        Vec<DelegatorPoolBalance>,
        Vec<CurrentDelegatorPoolBalance>,
        Vec<CurrentDelegatedVoter>,
    ),
    anyhow::Error,
> {
    let mut all_current_stake_pool_voters: StakingPoolVoterMap = AHashMap::new();
    let mut all_proposal_votes = vec![];
    let mut all_delegator_activities = vec![];
    let mut all_delegator_balances = vec![];
    let mut all_current_delegator_balances: CurrentDelegatorBalanceMap = AHashMap::new();
    let mut all_delegator_pools: DelegatorPoolMap = AHashMap::new();
    let mut all_delegator_pool_balances = vec![];
    let mut all_current_delegator_pool_balances = AHashMap::new();

    let mut active_pool_to_staking_pool = AHashMap::new();
    // structs needed to get delegated voters
    let mut all_current_delegated_voter = AHashMap::new();
    let mut all_vote_delegation_handle_to_pool_address = AHashMap::new();

    for txn in transactions {
        let block_timestamp =
            parse_timestamp(txn.timestamp.as_ref().unwrap(), txn.version as i64).naive_utc();

        // Add votes data
        let current_stake_pool_voter = CurrentStakingPoolVoter::from_transaction(txn).unwrap();
        all_current_stake_pool_voters.extend(current_stake_pool_voter);
        let mut proposal_votes = ProposalVote::from_transaction(txn).unwrap();
        all_proposal_votes.append(&mut proposal_votes);

        // Add delegator activities
        let mut delegator_activities = DelegatedStakingActivity::from_transaction(txn).unwrap();
        all_delegator_activities.append(&mut delegator_activities);

        // Add delegator pools
        let (delegator_pools, mut delegator_pool_balances, current_delegator_pool_balances) =
            DelegatorPool::from_transaction(txn).unwrap();
        all_delegator_pools.extend(delegator_pools);
        all_delegator_pool_balances.append(&mut delegator_pool_balances);
        all_current_delegator_pool_balances.extend(current_delegator_pool_balances);

        // Moving the transaction code here is the new paradigm to avoid redoing a lot of the duplicate work
        // Currently only delegator voting follows this paradigm
        // TODO: refactor all the other staking code to follow this paradigm
        let txn_version = txn.version as i64;
        let txn_timestamp =
            parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        // adding some metadata for subsequent parsing
        for wsc in &transaction_info.changes {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some(DelegationVoteGovernanceRecordsResource::GovernanceRecords(inner)) =
                    DelegationVoteGovernanceRecordsResource::from_write_resource(
                        write_resource,
                        txn_version,
                        block_timestamp,
                    )?
                {
                    let delegation_pool_address =
                        standardize_address(&write_resource.address.to_string());
                    let vote_delegation_handle = inner.vote_delegation.buckets.inner.get_handle();

                    all_vote_delegation_handle_to_pool_address
                        .insert(vote_delegation_handle, delegation_pool_address.clone());
                }
                if let Some(map) = CurrentDelegatorBalance::get_active_pool_to_staking_pool_mapping(
                    write_resource,
                    txn_version,
                    block_timestamp,
                )
                .unwrap()
                {
                    active_pool_to_staking_pool.extend(map);
                }
            }
        }

        // Delegator balances: active shares (and inactive shares with in-batch
        // mappings) do not need a DB connection. Only inactive-share fallback
        // lookups use `conn`. ParquetStakeExtractor calls this with None, so
        // gating the whole block on Some(conn) would emit empty parquet tables
        // while still advancing the checkpoint.
        let (mut delegator_balances, current_delegator_balances) =
            CurrentDelegatorBalance::from_transaction(
                txn,
                &active_pool_to_staking_pool,
                conn.as_mut(),
                query_retries,
                query_retry_delay_ms,
            )
            .await
            .unwrap();
        all_delegator_balances.append(&mut delegator_balances);
        all_current_delegator_balances.extend(current_delegator_balances);

        if let Some(ref mut conn) = conn {
            // this write table item indexing is to get delegator address, table handle, and voter & pending voter
            for wsc in &transaction_info.changes {
                if let Change::WriteTableItem(write_table_item) = wsc.change.as_ref().unwrap() {
                    let voter_map = CurrentDelegatedVoter::from_write_table_item(
                        write_table_item,
                        txn_version,
                        txn_timestamp,
                        &all_vote_delegation_handle_to_pool_address,
                        conn,
                        query_retries,
                        query_retry_delay_ms,
                    )
                    .await
                    .unwrap();

                    all_current_delegated_voter.extend(voter_map);
                }
            }

            // we need one last loop to prefill delegators that got in before the delegated voting contract was deployed
            for wsc in &transaction_info.changes {
                if let Change::WriteTableItem(write_table_item) = wsc.change.as_ref().unwrap() {
                    if let Some(voter) =
                        CurrentDelegatedVoter::get_delegators_pre_contract_deployment(
                            write_table_item,
                            txn_version,
                            txn_timestamp,
                            &active_pool_to_staking_pool,
                            &all_current_delegated_voter,
                            conn,
                            query_retries,
                            query_retry_delay_ms,
                        )
                        .await
                        .unwrap()
                    {
                        all_current_delegated_voter.insert(voter.pk(), voter);
                    }
                }
            }
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut all_current_stake_pool_voters = all_current_stake_pool_voters
        .into_values()
        .collect::<Vec<CurrentStakingPoolVoter>>();
    let mut all_current_delegator_balances = all_current_delegator_balances
        .into_values()
        .collect::<Vec<CurrentDelegatorBalance>>();
    let mut all_delegator_pools = all_delegator_pools
        .into_values()
        .collect::<Vec<DelegatorPool>>();
    let mut all_current_delegator_pool_balances = all_current_delegator_pool_balances
        .into_values()
        .collect::<Vec<CurrentDelegatorPoolBalance>>();
    let mut all_current_delegated_voter = all_current_delegated_voter
        .into_values()
        .collect::<Vec<CurrentDelegatedVoter>>();

    // Sort by PK
    all_current_stake_pool_voters
        .sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegator_balances.sort_by(|a, b| {
        (&a.delegator_address, &a.pool_address, &a.pool_type).cmp(&(
            &b.delegator_address,
            &b.pool_address,
            &b.pool_type,
        ))
    });

    all_delegator_pools.sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegator_pool_balances
        .sort_by(|a, b| a.staking_pool_address.cmp(&b.staking_pool_address));
    all_current_delegated_voter.sort();

    Ok((
        all_current_stake_pool_voters,
        all_proposal_votes,
        all_delegator_activities,
        all_delegator_balances,
        all_current_delegator_balances,
        all_delegator_pools,
        all_delegator_pool_balances,
        all_current_delegator_pool_balances,
        all_current_delegated_voter,
    ))
}

#[cfg(test)]
mod tests {
    use super::parse_stake_data;
    use aptos_indexer_processor_sdk::aptos_protos::{
        transaction::v1::{
            transaction::TxnData, write_set_change::Change, MoveStructTag, Transaction,
            TransactionInfo, UserTransaction, WriteResource, WriteSetChange, WriteTableData,
            WriteTableItem,
        },
        util::timestamp::Timestamp,
    };

    const POOL_ADDRESS: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DELEGATOR_ADDRESS: &str =
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const ACTIVE_SHARE_HANDLE: &str =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const INACTIVE_PARENT_HANDLE: &str =
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const INACTIVE_SHARE_HANDLE: &str =
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn delegation_pool_write(active_handle: &str, inactive_handle: &str) -> WriteSetChange {
        let data = format!(
            r#"{{"active_shares":{{"shares":{{"inner":{{"handle":"{active_handle}"}}}},"total_coins":"1000","total_shares":"1000","scaling_factor":"1"}},"inactive_shares":{{"handle":"{inactive_handle}"}},"operator_commission_percentage":"0"}}"#
        );
        WriteSetChange {
            r#type: 0,
            change: Some(Change::WriteResource(WriteResource {
                address: POOL_ADDRESS.to_string(),
                state_key_hash: vec![],
                r#type: Some(MoveStructTag {
                    address: "0x1".to_string(),
                    module: "delegation_pool".to_string(),
                    name: "DelegationPool".to_string(),
                    generic_type_params: vec![],
                }),
                type_str: "0x1::delegation_pool::DelegationPool".to_string(),
                data,
            })),
        }
    }

    fn share_write(handle: &str, shares: &str) -> WriteSetChange {
        WriteSetChange {
            r#type: 0,
            change: Some(Change::WriteTableItem(WriteTableItem {
                state_key_hash: vec![],
                handle: handle.to_string(),
                key: DELEGATOR_ADDRESS.to_string(),
                data: Some(WriteTableData {
                    key: format!("\"{DELEGATOR_ADDRESS}\""),
                    key_type: "address".to_string(),
                    value: format!("\"{shares}\""),
                    value_type: "u128".to_string(),
                }),
            })),
        }
    }

    fn inactive_pool_write(parent_handle: &str, shares_handle: &str) -> WriteSetChange {
        let value = format!(
            r#"{{"shares":{{"inner":{{"handle":"{shares_handle}"}}}},"total_coins":"500","total_shares":"500","scaling_factor":"1"}}"#
        );
        WriteSetChange {
            r#type: 0,
            change: Some(Change::WriteTableItem(WriteTableItem {
                state_key_hash: vec![],
                handle: parent_handle.to_string(),
                key: "0x1".to_string(),
                data: Some(WriteTableData {
                    key: "\"0x1\"".to_string(),
                    key_type: "address".to_string(),
                    value,
                    value_type: "0x1::pool_u64_unbound::Pool".to_string(),
                }),
            })),
        }
    }

    fn stake_txn(changes: Vec<WriteSetChange>) -> Transaction {
        Transaction {
            timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            version: 42,
            info: Some(TransactionInfo {
                hash: vec![],
                state_change_hash: vec![],
                event_root_hash: vec![],
                state_checkpoint_hash: None,
                gas_used: 0,
                success: true,
                vm_status: String::new(),
                accumulator_root_hash: vec![],
                changes,
            }),
            epoch: 0,
            block_height: 0,
            r#type: 4,
            size_info: None,
            txn_data: Some(TxnData::User(UserTransaction::default())),
        }
    }

    #[tokio::test]
    async fn parse_stake_data_without_conn_emits_active_share_balances() {
        let txn = stake_txn(vec![
            delegation_pool_write(ACTIVE_SHARE_HANDLE, INACTIVE_PARENT_HANDLE),
            share_write(ACTIVE_SHARE_HANDLE, "1000000"),
        ]);

        let txns = vec![txn];
        let (_, _, _, delegator_balances, current_delegator_balances, _, _, _, _) =
            parse_stake_data(&txns, None, 0, 0)
                .await
                .expect("parse_stake_data with no conn should succeed");

        assert_eq!(
            delegator_balances.len(),
            1,
            "ParquetStakeExtractor passes None; active shares must still be parsed"
        );
        assert_eq!(delegator_balances[0].delegator_address, DELEGATOR_ADDRESS);
        assert_eq!(delegator_balances[0].pool_address, POOL_ADDRESS);
        assert_eq!(delegator_balances[0].pool_type, "active_shares");
        assert_eq!(delegator_balances[0].shares.to_string(), "1000000");
        assert_eq!(current_delegator_balances.len(), 1);
        assert_eq!(current_delegator_balances[0].pool_type, "active_shares");
        assert_eq!(current_delegator_balances[0].shares.to_string(), "1000000");
    }

    #[tokio::test]
    async fn parse_stake_data_without_conn_emits_in_batch_inactive_shares() {
        let txn = stake_txn(vec![
            delegation_pool_write(ACTIVE_SHARE_HANDLE, INACTIVE_PARENT_HANDLE),
            inactive_pool_write(INACTIVE_PARENT_HANDLE, INACTIVE_SHARE_HANDLE),
            share_write(INACTIVE_SHARE_HANDLE, "500"),
        ]);

        let txns = vec![txn];
        let (_, _, _, delegator_balances, current_delegator_balances, _, _, _, _) =
            parse_stake_data(&txns, None, 0, 0)
                .await
                .expect("in-batch inactive shares should not require a DB connection");

        assert_eq!(delegator_balances.len(), 1);
        assert_eq!(delegator_balances[0].pool_type, "inactive_shares");
        assert_eq!(delegator_balances[0].delegator_address, DELEGATOR_ADDRESS);
        assert_eq!(delegator_balances[0].pool_address, POOL_ADDRESS);
        assert_eq!(delegator_balances[0].shares.to_string(), "500");
        assert_eq!(
            delegator_balances[0].parent_table_handle,
            INACTIVE_PARENT_HANDLE
        );
        assert_eq!(current_delegator_balances.len(), 1);
        assert_eq!(current_delegator_balances[0].pool_type, "inactive_shares");
    }
}
