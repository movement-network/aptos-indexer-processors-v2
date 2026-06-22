// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::{address_reputation, address_transfer_edges, bridge_inflows};
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
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Insertable, Queryable, Serialize)]
#[diesel(table_name = address_reputation)]
pub struct AddressReputation {
    pub address: String,
    pub score: BigDecimal,
    pub highest_seed: BigDecimal,
    pub nearest_seed_hop: Option<i32>,
    pub last_updated_version: i64,
    pub last_updated_timestamp: chrono::NaiveDateTime,
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
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvmRiskScore {
    pub evm_address: String,
    pub risk_score: BigDecimal,
    pub risk_label: Option<String>,
    pub source: Option<String>,
    pub fetched_at: chrono::NaiveDateTime,
}
