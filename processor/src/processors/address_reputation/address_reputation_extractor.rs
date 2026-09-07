// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db::resources::FromWriteResource,
    processors::{
        address_reputation::{
            address_reputation_model::{
                standardize_evm_address, BridgeInflow, BridgeRegistryEntry, TransferEdge,
            },
            intent_payload, lz_payload,
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
use tokio::sync::mpsc::UnboundedSender;

// P0 scope: only FA v2 events. Coin v1 (`0x1::coin::WithdrawEvent`/`DepositEvent`)
// is intentionally skipped because it includes gas-fee Withdraw/Deposit pairs that
// pollute the graph and we don't yet have a reliable filter for them.
const FA_WITHDRAW: &str = "0x1::fungible_asset::Withdraw";
const FA_DEPOSIT: &str = "0x1::fungible_asset::Deposit";

/// In-memory snapshot of the bridge_registry table loaded at processor startup.
pub type BridgeRegistry = Arc<Vec<BridgeRegistryEntry>>;

pub struct AddressReputationExtractor {
    pub bridge_registry: BridgeRegistry,
    /// Sends directly-known EVM addresses (Circle USDCx, LZ compose) to the enricher
    /// loop for Hypernative screening without needing GUID resolution first.
    pub evm_sender: UnboundedSender<String>,
}

impl AddressReputationExtractor {
    pub fn new(bridge_registry: BridgeRegistry, evm_sender: UnboundedSender<String>) -> Self {
        Self {
            bridge_registry,
            evm_sender,
        }
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
                    match parse_bridge_event(
                        event.data.as_str(),
                        entry,
                        txn_version,
                        event_index,
                        block_timestamp,
                    ) {
                        None => {
                            tracing::warn!(
                                txn_version,
                                event_index,
                                bridge_name = %entry.bridge_name,
                                event_type = %type_str,
                                "extractor: parse_bridge_event returned None for registered bridge event"
                            );
                        },
                        Some(mut inflow) => {
                            // Fall back to the transaction's payload when the event
                            // itself doesn't carry EVM-side data.
                            let kind = entry.payload_kind.as_deref();
                            if inflow.evm_source.is_none() {
                                inflow.evm_source = decode_payload_evm_source(txn, kind);
                            }
                            inflow.evm_source =
                                inflow.evm_source.map(|s| standardize_evm_address(&s));
                            if inflow.lz_guid.is_none() {
                                inflow.lz_guid = extract_payload_guid(txn, kind);
                            }
                            // Forward Circle / LZ-compose EVM address to update its score in the evm fetch loop.
                            // NOTE: LZ GUIDs are NOT sent here — they are forwarded from the storer after
                            // the bridge_inflows row is committed, to avoid a race where the enricher
                            // resolves the GUID before the row exists.
                            if let Some(evm) = inflow.evm_source.as_ref() {
                                if let Err(e) = self.evm_sender.send(evm.clone()) {
                                    tracing::error!(err = ?e, "Send GUI channel to fetch evm address failed");
                                }
                            }
                            txn_inflows.push(inflow);
                            continue;
                        },
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
/// from the transaction's payload arguments. Returns `None` when the payload
/// kind isn't recognized, the txn has no user payload, or the decoder can't
/// find a well-formed address.
///
/// Both `EntryFunctionPayload` (Circle USDCx `mint`) and `ScriptPayload`
/// (LayerZero executor scripts) are supported — arguments are encoded
/// identically in both payload types.
fn decode_payload_evm_source(txn: &Transaction, kind: Option<&str>) -> Option<String> {
    let kind = kind?;
    let user = match txn.txn_data.as_ref()? {
        TxnData::User(u) => u,
        _ => return None,
    };
    let payload = user.request.as_ref()?.payload.as_ref()?.payload.as_ref()?;
    // Extract arguments from whichever payload type is present.
    let args: &[String] = match payload {
        PayloadType::EntryFunctionPayload(ef) => &ef.arguments,
        PayloadType::ScriptPayload(sp) => &sp.arguments,
        _ => return None,
    };
    match kind {
        // Circle USDCx: `mint(&signer, intent, attestation, fee)` — intent
        // is arg[0], a big-endian IntentPayload whose `local_depositor` is the
        // EVM address that called `depositForBurn` on the source chain.
        "circle_intent" => {
            let arg = match args.first() {
                Some(a) => a,
                None => {
                    tracing::warn!(
                        txn_version = txn.version,
                        "circle_intent: no args in payload"
                    );
                    return None;
                },
            };
            let bytes = match intent_payload::parse_hex_arg(arg) {
                Some(b) => b,
                None => {
                    tracing::warn!(
                        txn_version = txn.version,
                        arg0 = %arg,
                        "circle_intent: failed to parse arg[0] as hex"
                    );
                    return None;
                },
            };
            let result = intent_payload::decode_local_depositor(&bytes);
            if result.is_none() {
                tracing::warn!(
                    txn_version = txn.version,
                    arg0_len = bytes.len(),
                    arg0_prefix = %hex::encode(&bytes[..bytes.len().min(16)]),
                    "circle_intent: decode_local_depositor returned None"
                );
            }
            result
        },
        // LayerZero V2 OFT: when `sendParam.composeMsg` is non-empty on the
        // Ethereum side, OFTCore prepends `addressToBytes32(msg.sender)` to the
        // compose payload before encoding the OFT message. The resulting layout
        // at bytes 40–71 of the `message` executor argument is the EVM address
        // of the user who called `OFT.send()`. Returns None for standard
        // (compose-less) transfers where the 40-byte message carries no sender.
        // The LZ executor delivers packets via a Move script (ScriptPayload),
        // handled by the ScriptPayload arm above.
        "layerzero_oft" => {
            let result = lz_payload::scan_args_for_oft_sender(args);
            if result.is_none() {
                let arg_byte_lens: Vec<usize> = args
                    .iter()
                    .map(|a| a.strip_prefix("0x").unwrap_or(a).len() / 2)
                    .collect();
                tracing::debug!(
                    txn_version = txn.version,
                    args_count = args.len(),
                    arg_byte_lens = ?arg_byte_lens,
                    "layerzero_oft: no compose sender (standard transfer, evm_source=NULL)"
                );
            }
            result
        },
        _ => None,
    }
}

/// Extract the LayerZero GUID from the transaction payload for `layerzero_oft`
/// bridges. Returns `None` for all other payload kinds.
fn extract_payload_guid(txn: &Transaction, kind: Option<&str>) -> Option<String> {
    if kind? != "layerzero_oft" {
        return None;
    }
    let user = match txn.txn_data.as_ref()? {
        TxnData::User(u) => u,
        _ => return None,
    };
    let payload = user.request.as_ref()?.payload.as_ref()?.payload.as_ref()?;
    let args: &[String] = match payload {
        PayloadType::EntryFunctionPayload(ef) => &ef.arguments,
        PayloadType::ScriptPayload(sp) => &sp.arguments,
        _ => return None,
    };
    let result = lz_payload::extract_guid_from_args(args);
    if result.is_none() {
        let arg_byte_lens: Vec<usize> = args
            .iter()
            .map(|a| a.strip_prefix("0x").unwrap_or(a).len() / 2)
            .collect();
        tracing::warn!(
            txn_version = txn.version,
            args_count = args.len(),
            arg_byte_lens = ?arg_byte_lens,
            "layerzero_oft: no GUID found in payload args — inflow stored without lz_guid, enricher skipped"
        );
    }
    result
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
        .and_then(|p| json_field(&v, p))
        .map(|s| standardize_evm_address(&s));
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
        lz_guid: None, // filled by the caller from the tx payload
        transaction_timestamp: block_timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn ts() -> chrono::NaiveDateTime {
        DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn parse_bridge_event_lowercases_checksummed_evm_source() {
        let entry = BridgeRegistryEntry {
            bridge_name: "test".to_string(),
            module_address: "0x1".to_string(),
            event_type: "0x1::m::E".to_string(),
            evm_source_field_path: Some("from".to_string()),
            recipient_field_path: "to".to_string(),
            amount_field_path: "amount".to_string(),
            chain_id_field_path: None,
            payload_kind: None,
            enabled: true,
        };
        let raw = r#"{
            "from": "0x8F5633d77Eb1D6bf6c0D357148135A91B0e1F87F",
            "to": "0x1",
            "amount": "100"
        }"#;
        let inflow = parse_bridge_event(raw, &entry, 1, 0, ts()).unwrap();
        assert_eq!(
            inflow.evm_source.as_deref(),
            Some("0x8f5633d77eb1d6bf6c0d357148135a91b0e1f87f")
        );
    }
}
