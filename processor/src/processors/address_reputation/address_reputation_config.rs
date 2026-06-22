// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AddressReputationConfig {
    #[serde(default = "AddressReputationConfig::default_channel_size")]
    pub channel_size: usize,
    /// Multiplier applied to a sender's score when propagating to a receiver.
    /// 0.8 means each hop dilutes inherited reputation by 20%.
    #[serde(default = "AddressReputationConfig::default_decay")]
    pub decay: f64,
    /// Known bridge contracts whose deposit events should be recognized as head nodes.
    /// Lives in config (not in code or migrations) so mainnet/testnet/devnet each ship
    /// their own deployment addresses without touching SQL or Rust. To add a new bridge
    /// at runtime, append to the config and restart the processor.
    #[serde(default)]
    pub bridges: Vec<BridgeConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    pub name: String,
    pub module_address: String,
    pub event_type: String,
    /// JSON path into the deposit event's data field that yields the recipient Aptos
    /// address. e.g. `"recipient"` for Circle USDCx.
    pub recipient_field_path: String,
    /// Field path for the amount; defaults to `"amount"` if omitted.
    #[serde(default = "BridgeConfig::default_amount_path")]
    pub amount_field_path: String,
    /// Optional path to the source chain id (e.g. CCTP `remote_domain`, LZ `src_eid`).
    #[serde(default)]
    pub chain_id_field_path: Option<String>,
    /// Optional path to the EVM source address inside the event payload. NULL for
    /// bridges (like Circle USDCx) that don't put it in the event.
    #[serde(default)]
    pub evm_source_field_path: Option<String>,
    #[serde(default = "BridgeConfig::default_enabled")]
    pub enabled: bool,
}

impl BridgeConfig {
    fn default_amount_path() -> String {
        "amount".to_string()
    }
    const fn default_enabled() -> bool {
        true
    }
}

impl AddressReputationConfig {
    pub const fn default_channel_size() -> usize {
        10
    }
    pub const fn default_decay() -> f64 {
        0.8
    }
}

impl Default for AddressReputationConfig {
    fn default() -> Self {
        Self {
            channel_size: Self::default_channel_size(),
            decay: Self::default_decay(),
            bridges: Vec::new(),
        }
    }
}
