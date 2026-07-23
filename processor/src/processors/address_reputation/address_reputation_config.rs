// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AddressReputationConfig {
    #[serde(default = "AddressReputationConfig::default_channel_size")]
    pub channel_size: usize,
    /// Known bridge contracts whose deposit events should be recognized as head nodes.
    /// Lives in config (not in code or migrations) so mainnet/testnet/devnet each ship
    /// their own deployment addresses without touching SQL or Rust. To add a new bridge
    /// at runtime, append to the config and restart the processor.
    #[serde(default)]
    pub bridges: Vec<BridgeConfig>,
    /// If true, maintain the `address_evm_sources` rollup (per-address EVM funding
    /// provenance). Disable for lower write volume when only the score / edge log
    /// is needed.
    #[serde(default = "AddressReputationConfig::default_propagate_evm_sources")]
    pub propagate_evm_sources: bool,
    /// Background loop that resolves LayerZero GUIDs → EVM depositor addresses.
    #[serde(default)]
    pub lz_enricher: LzEnricherConfig,
}

/// Configuration for the background LZ GUID → EVM enrichment loop.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LzEnricherConfig {
    /// Enable or disable the enricher. Defaults to true.
    #[serde(default = "LzEnricherConfig::default_enabled")]
    pub enabled: bool,
    /// Milliseconds between consecutive LZ Scan API requests. Defaults to 500 (2 req/s).
    #[serde(default = "LzEnricherConfig::default_interval_ms")]
    pub interval_ms: u64,
    /// Number of retry attempts per GUID on transient failure. Defaults to 3.
    #[serde(default = "LzEnricherConfig::default_max_retries")]
    pub max_retries: u32,
}

impl LzEnricherConfig {
    const fn default_enabled() -> bool {
        true
    }

    const fn default_interval_ms() -> u64 {
        500
    }

    const fn default_max_retries() -> u32 {
        3
    }
}

impl Default for LzEnricherConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            interval_ms: Self::default_interval_ms(),
            max_retries: Self::default_max_retries(),
        }
    }
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
    /// Optional named decoder for the transaction's entry-function payload,
    /// used when `evm_source_field_path` isn't enough. Supported values:
    ///   - `"circle_intent"`: parses Circle's USDCx `IntentPayload` (arg[0])
    ///     and takes `local_depositor` as the EVM source. See
    ///     `usdc-bridge/smart_contracts/sources/usdcx.move` for the on-chain
    ///     layout this mirrors.
    #[serde(default)]
    pub payload_kind: Option<String>,
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

    pub const fn default_propagate_evm_sources() -> bool {
        true
    }
}

impl Default for AddressReputationConfig {
    fn default() -> Self {
        Self {
            channel_size: Self::default_channel_size(),
            bridges: Vec::new(),
            propagate_evm_sources: Self::default_propagate_evm_sources(),
            lz_enricher: LzEnricherConfig::default(),
        }
    }
}
