// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::{address_evm_sources, address_transfer_edges, bridge_inflows};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = address_transfer_edges)]
pub struct TransferEdge {
    pub transaction_version: i64,
    pub event_index: i64,
    pub from_address: String,
    pub to_address: String,
    pub asset_type: Option<String>,
    pub amount: BigDecimal,
    pub is_bridge_inflow: bool,
    pub bridge_name: Option<String>,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Serialize)]
#[diesel(table_name = bridge_inflows)]
pub struct BridgeInflow {
    pub transaction_version: i64,
    pub event_index: i64,
    pub bridge_name: String,
    pub aptos_recipient: String,
    pub evm_source: Option<String>,
    pub src_chain_id: Option<i32>,
    pub asset_type: Option<String>,
    pub amount: BigDecimal,
    /// LayerZero message GUID (0x-prefixed 32-byte hash). Present only for LZ
    /// bridges; NULL for Circle USDCx and any non-LZ protocol. Joins to the
    /// Ethereum-side `OFTSent` event to recover the human EVM depositor address.
    pub lz_guid: Option<String>,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

/// One row per (movement_address, asset_type, evm_address).
///
/// - `evm_fund`: cumulative amount deposited directly via bridge inflows from
///   this EVM address to this Movement address. Updated only on bridge inflows;
///   never touched by transfers.
/// - `transfer_fund`: cumulative weighted attribution received through Movement
///   transfers. For a transfer A→B of amount C, each EVM source E of A
///   contributes `C / total_A_evm_fund * E_A.evm_fund * (1 / (E_A.hops_min + 1))`
///   to B's `transfer_fund` for E. Updated only on transfers; never touched by
///   bridge inflows.
#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Queryable, Serialize)]
#[diesel(table_name = address_evm_sources)]
pub struct AddressEvmSource {
    pub movement_address: String,
    pub asset_type: String,
    pub evm_address: String,
    pub evm_fund: BigDecimal,
    pub transfer_fund: BigDecimal,
    pub first_seen_ord: i64,
    pub last_seen_ord: i64,
    pub hops_min: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BridgeRegistryEntry {
    pub bridge_name: String,
    pub module_address: String,
    pub event_type: String,
    pub evm_source_field_path: Option<String>,
    pub recipient_field_path: String,
    pub amount_field_path: String,
    pub chain_id_field_path: Option<String>,
    /// Optional named decoder for the transaction's entry-function payload,
    /// used when the EVM source isn't present in the event data itself. The
    /// only value currently supported is `"circle_intent"`, which decodes
    /// `local_depositor` out of Circle's USDCx `IntentPayload` (see
    /// `intent_payload.rs`).
    pub payload_kind: Option<String>,
    pub enabled: bool,
}

/// Canonicalize an EVM address for DB keys and joins.
///
/// Circle intent / LZ payload decoders emit lowercase `0x` + 40 hex via
/// `hex::encode`. LZ Scan and Hypernative return EIP-55 checksum casing.
/// `address_evm_sources` and `evm_address_risk_scores` use a case-sensitive
/// Postgres PK / JOIN on `evm_address`, so mixed case splits one physical
/// address into two rows and drops risk-score joins.
pub fn standardize_evm_address(addr: &str) -> String {
    let hex = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .unwrap_or(addr);
    format!("0x{}", hex.to_ascii_lowercase())
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct EvmRiskScore {
    pub evm_address: String,
    pub risk_score: BigDecimal,
    pub risk_label: String,
    pub source: String,
    pub recommendation: String,
    pub severity: String,
    pub fetched_at: chrono::NaiveDateTime,
    /// `true` when this address still needs a valid screening result:
    /// set for error scores (risk_score=0) and cleared to `false` when a
    /// successful Hypernative result is saved.
    pub to_be_updated: bool,
}

#[cfg(test)]
mod tests {
    use super::standardize_evm_address;

    #[test]
    fn checksum_and_lowercase_collapse_to_one_key() {
        let checksummed = "0x8F5633d77Eb1D6bf6c0D357148135A91B0e1F87F";
        let lower = "0x8f5633d77eb1d6bf6c0d357148135a91b0e1f87f";
        let upper_prefix = "0X8F5633D77EB1D6BF6C0D357148135A91B0E1F87F";
        assert_eq!(standardize_evm_address(checksummed), lower);
        assert_eq!(standardize_evm_address(lower), lower);
        assert_eq!(standardize_evm_address(upper_prefix), lower);
        // Case-sensitive PK would treat these as distinct without normalization.
        assert_ne!(checksummed, lower);
        assert_eq!(
            standardize_evm_address(checksummed),
            standardize_evm_address(lower)
        );
    }

    #[test]
    fn sentinel_stays_canonical() {
        assert_eq!(
            standardize_evm_address("0x0000000000000000000000000000000000000000"),
            "0x0000000000000000000000000000000000000000"
        );
    }
}
