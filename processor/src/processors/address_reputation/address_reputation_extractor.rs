// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::resources::FromWriteResource,
    processors::{
        address_reputation::{
            address_reputation_model::{BridgeInflow, BridgeRegistryEntry, TransferEdge},
            intent_payload,
        },
        objects::v2_object_utils::ObjectWithMetadata,
    },
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{
        transaction::TxnData, transaction_payload::Payload as PayloadType,
        write_set_change::Change, Transaction,
    },
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{convert::standardize_address, errors::ProcessorError},
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use serde_json::Value;
use std::{str::FromStr, sync::Arc};

// P0 scope: only FA v2 events. Coin v1 (`0x1::coin::WithdrawEvent`/`DepositEvent`)
// is intentionally skipped because it includes gas-fee Withdraw/Deposit pairs that
// pollute the graph and we don't yet have a reliable filter for them.
const FA_WITHDRAW: &str = "0x1::fungible_asset::Withdraw";
const FA_DEPOSIT: &str = "0x1::fungible_asset::Deposit";

/// In-memory snapshot of the bridge_registry table loaded at processor startup.
pub type BridgeRegistry = Arc<Vec<BridgeRegistryEntry>>;

pub struct AddressReputationExtractor {
    pub bridge_registry: BridgeRegistry,
}

impl AddressReputationExtractor {
    pub fn new(bridge_registry: BridgeRegistry) -> Self {
        Self { bridge_registry }
    }
}

#[async_trait]
impl Processable for AddressReputationExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<TransferEdge>, Vec<BridgeInflow>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<(Vec<TransferEdge>, Vec<BridgeInflow>)>>, ProcessorError>
    {
        let mut edges: Vec<TransferEdge> = Vec::new();
        let mut inflows: Vec<BridgeInflow> = Vec::new();

        for txn in &input.data {
            let txn_version = txn.version as i64;
            let block_timestamp = match txn.timestamp.as_ref() {
                Some(ts) => parse_timestamp(ts, txn_version).naive_utc(),
                None => continue,
            };
            let txn_data = match txn.txn_data.as_ref() {
                Some(d) => d,
                None => continue,
            };
            let events = match txn_data {
                TxnData::User(inner) => &inner.events,
                _ => continue,
            };

            // Build two per-txn maps by scanning WriteResource changes:
            //   storage_id -> owner_address      (via 0x1::object::ObjectCore.owner)
            //   storage_id -> asset_type         (via 0x1::fungible_asset::FungibleStore.metadata.inner)
            // The owner map gives us a connected, owner-keyed graph. The asset_type map
            // lets each edge carry which FA was moved (USDCx, WETH.e, MOVE, etc.) so a
            // trace can scope to a specific asset and we don't pretend USDCx and WETH
            // share a value flow.
            let mut storage_to_owner: AHashMap<String, String> = AHashMap::new();
            let mut storage_to_asset: AHashMap<String, String> = AHashMap::new();
            if let Some(info) = txn.info.as_ref() {
                for wsc in info.changes.iter() {
                    if let Some(Change::WriteResource(wr)) = wsc.change.as_ref() {
                        if let Ok(Some(obj)) = ObjectWithMetadata::from_write_resource(wr) {
                            storage_to_owner.insert(
                                standardize_address(&wr.address),
                                obj.object_core.get_owner_address(),
                            );
                        }
                        if wr.type_str == "0x1::fungible_asset::FungibleStore" {
                            if let Ok(parsed) = serde_json::from_str::<Value>(&wr.data) {
                                if let Some(meta) = parsed
                                    .get("metadata")
                                    .and_then(|m| m.get("inner"))
                                    .and_then(|s| s.as_str())
                                {
                                    storage_to_asset.insert(
                                        standardize_address(&wr.address),
                                        standardize_address(meta),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // First pass: classify events into withdraws, deposits, bridge events.
            let mut withdraws: Vec<ParsedSide> = Vec::new();
            let mut deposits: Vec<ParsedSide> = Vec::new();
            let mut txn_inflows: Vec<BridgeInflow> = Vec::new();

            for (idx, event) in events.iter().enumerate() {
                let event_index = idx as i64;
                let type_str = event.type_str.as_str();

                // Bridge check (registry-driven, exact event-type match). We do this BEFORE
                // generic FA classification so a bridge's emitted FA Deposit (which fires
                // alongside the Mint) doesn't get double-counted as a user transfer.
                if let Some(entry) = self
                    .bridge_registry
                    .iter()
                    .find(|e| e.enabled && e.event_type == type_str)
                {
                    if let Some(mut inflow) = parse_bridge_event(
                        event.data.as_str(),
                        entry,
                        txn_version,
                        event_index,
                        block_timestamp,
                    ) {
                        // Fall back to the transaction's entry-function payload when
                        // the event itself doesn't carry the EVM depositor (Circle
                        // USDCx puts it in the IntentPayload arg, not the Mint event).
                        if inflow.evm_source.is_none() {
                            inflow.evm_source =
                                decode_payload_evm_source(txn, entry.payload_kind.as_deref());
                        }
                        txn_inflows.push(inflow);
                        continue;
                    }
                }

                // Withdraw / Deposit classification (FA v2 only in P0).
                let (is_withdraw, is_deposit) = match type_str {
                    FA_WITHDRAW => (true, false),
                    FA_DEPOSIT => (false, true),
                    _ => (false, false),
                };
                if !(is_withdraw || is_deposit) {
                    continue;
                }
                // FA module events emit with `event.key.account_address = 0x0`. The actual
                // participant is the FungibleStore object id, found inside `data.store`.
                // NOTE P0 limitation: this is a storage_id, not the owner. Owner resolution
                // requires joining current_fungible_asset_balances at query time.
                let v: Value = match serde_json::from_str(event.data.as_str()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let storage_id = match v.get("store").and_then(|x| x.as_str()) {
                    Some(s) => standardize_address(s),
                    None => continue,
                };
                // Resolve to owner; fall back to storage_id if ObjectCore wasn't written
                // in this txn (rare for FA transfers, common for deletions / edge cases).
                let owner = storage_to_owner
                    .get(&storage_id)
                    .cloned()
                    .unwrap_or_else(|| storage_id.clone());
                let amount = match v
                    .get("amount")
                    .and_then(|x| x.as_str())
                    .and_then(|s| BigDecimal::from_str(s).ok())
                {
                    Some(a) => a,
                    None => continue,
                };
                let side = ParsedSide {
                    event_index,
                    address: owner,
                    storage_id,
                    amount,
                };
                if is_withdraw {
                    withdraws.push(side);
                } else {
                    deposits.push(side);
                }
            }

            // Pair withdraws to deposits by equal amount within the txn.
            // P0 heuristic: greedy first-match; unmatched sides are skipped.
            let mut deposit_used = vec![false; deposits.len()];
            for w in &withdraws {
                let m = deposits.iter().enumerate().find(|(i, d)| {
                    !deposit_used[*i] && d.amount == w.amount && d.address != w.address
                });
                if let Some((i, d)) = m {
                    deposit_used[i] = true;
                    // Match this edge to a bridge inflow in the same txn if the recipient lines up.
                    let bridge_match = txn_inflows
                        .iter()
                        .find(|bi| bi.aptos_recipient == d.address && bi.amount == d.amount);
                    let (is_bridge_inflow, bridge_name) = match bridge_match {
                        Some(bi) => (true, Some(bi.bridge_name.clone())),
                        None => (false, None),
                    };
                    let asset = storage_to_asset
                        .get(&w.storage_id)
                        .or_else(|| storage_to_asset.get(&d.storage_id))
                        .cloned();
                    edges.push(TransferEdge {
                        transaction_version: txn_version,
                        event_index: w.event_index,
                        from_address: w.address.clone(),
                        to_address: d.address.clone(),
                        asset_type: asset,
                        amount: w.amount.clone(),
                        is_bridge_inflow,
                        bridge_name,
                        transaction_timestamp: block_timestamp,
                    });
                }
            }

            // Synthesize a head-injection edge per bridge inflow so the bridge module
            // is always a node in the graph, even when (as with USDCx Mint) there is
            // no paired Withdraw event. Resolve asset_type from the FA Deposit event
            // in the same txn that delivered tokens to this recipient.
            for bi in &mut txn_inflows {
                let from_addr = self
                    .bridge_registry
                    .iter()
                    .find(|e| e.bridge_name == bi.bridge_name)
                    .map(|e| e.module_address.clone())
                    .unwrap_or_else(|| format!("bridge::{}", bi.bridge_name));
                // Find the FA Deposit event for this recipient + amount and read the
                // FungibleStore.metadata.inner via storage_to_asset.
                let asset = deposits.iter().find_map(|d| {
                    if d.address == bi.aptos_recipient && d.amount == bi.amount {
                        storage_to_asset.get(&d.storage_id).cloned()
                    } else {
                        None
                    }
                });
                if asset.is_some() {
                    bi.asset_type = asset.clone();
                }
                edges.push(TransferEdge {
                    transaction_version: bi.transaction_version,
                    event_index: bi.event_index,
                    from_address: from_addr,
                    to_address: bi.aptos_recipient.clone(),
                    asset_type: asset,
                    amount: bi.amount.clone(),
                    is_bridge_inflow: true,
                    bridge_name: Some(bi.bridge_name.clone()),
                    transaction_timestamp: bi.transaction_timestamp,
                });
            }

            inflows.extend(txn_inflows);
        }

        Ok(Some(TransactionContext {
            data: (edges, inflows),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AddressReputationExtractor {}

impl NamedStep for AddressReputationExtractor {
    fn name(&self) -> String {
        "AddressReputationExtractor".to_string()
    }
}

struct ParsedSide {
    event_index: i64,
    address: String,
    storage_id: String,
    amount: BigDecimal,
}

/// Walk a dot-separated path into a JSON object and return the leaf as a string.
fn json_field(data: &Value, path: &str) -> Option<String> {
    let mut cur = data;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    match cur {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Dispatches to a named payload decoder to recover the EVM source address
/// from the transaction's entry-function arguments. Returns `None` when the
/// payload kind isn't recognized, the txn isn't an entry-function call, or the
/// decoder can't find a well-formed address.
fn decode_payload_evm_source(txn: &Transaction, kind: Option<&str>) -> Option<String> {
    let kind = kind?;
    let user = match txn.txn_data.as_ref()? {
        TxnData::User(u) => u,
        _ => return None,
    };
    let payload = user.request.as_ref()?.payload.as_ref()?.payload.as_ref()?;
    let entry_fn = match payload {
        PayloadType::EntryFunctionPayload(ef) => ef,
        _ => return None,
    };
    match kind {
        // Circle USDCx: `mint(&signer, intent, attestation, fee)` — intent
        // is arg[0], a big-endian IntentPayload whose `local_depositor` is the
        // EVM address that called `depositForBurn` on the source chain.
        "circle_intent" => {
            let arg = entry_fn.arguments.first()?;
            let bytes = intent_payload::parse_hex_arg(arg)?;
            intent_payload::decode_local_depositor(&bytes)
        },
        _ => None,
    }
}

fn parse_bridge_event(
    raw: &str,
    entry: &BridgeRegistryEntry,
    txn_version: i64,
    event_index: i64,
    block_timestamp: chrono::NaiveDateTime,
) -> Option<BridgeInflow> {
    let v: Value = serde_json::from_str(raw).ok()?;
    // EVM source is optional — some bridge events may not carry it; we still record the inflow
    // so it shows up as a "head" node in the graph.
    let evm_source = entry
        .evm_source_field_path
        .as_deref()
        .and_then(|p| json_field(&v, p));
    let recipient = json_field(&v, &entry.recipient_field_path)?;
    let amount = BigDecimal::from_str(&json_field(&v, &entry.amount_field_path)?).ok()?;
    let src_chain_id = entry
        .chain_id_field_path
        .as_deref()
        .and_then(|p| json_field(&v, p))
        .and_then(|s| s.parse::<i32>().ok());
    Some(BridgeInflow {
        transaction_version: txn_version,
        event_index,
        bridge_name: entry.bridge_name.clone(),
        aptos_recipient: standardize_address(&recipient),
        evm_source,
        src_chain_id,
        asset_type: None,
        amount,
        transaction_timestamp: block_timestamp,
    })
}
