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
    /// Hypernative address screener, called for each resolved EVM address.
    #[serde(default)]
    pub hypernative: HypernativeConfig,
}

/// Configuration for the background LZ GUID → EVM enrichment loop.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LzEnricherConfig {
    /// Milliseconds between consecutive LZ Scan API requests. Defaults to 500 (2 req/s).
    #[serde(default = "LzEnricherConfig::default_interval_ms")]
    pub interval_ms: u64,
    /// Base URL for the LayerZero Scan API (GUID resolution endpoint).
    #[serde(default = "LzEnricherConfig::default_scan_api_base_url")]
    pub scan_api_base_url: String,
}

impl LzEnricherConfig {
    const fn default_interval_ms() -> u64 {
        500
    }

    fn default_scan_api_base_url() -> String {
        "https://scan.layerzero-api.com/v1/messages/guid".to_string()
    }
}

impl Default for LzEnricherConfig {
    fn default() -> Self {
        Self {
            interval_ms: Self::default_interval_ms(),
            scan_api_base_url: Self::default_scan_api_base_url(),
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
            hypernative: HypernativeConfig::default(),
        }
    }
}

/// Hypernative address-reputation screener configuration.
/// Requires API credentials (`client_id` + `client_secret`).
/// `enabled` defaults to false so the processor starts without credentials.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HypernativeConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Optional Screener Policy UUID. If omitted the Hypernative default policy is used.
    #[serde(default)]
    pub screener_policy_id: Option<String>,
    /// Hypernative screener API endpoint.
    #[serde(default = "HypernativeConfig::default_screener_url")]
    pub screener_url: String,
    /// Maximum number of concurrent in-flight HTTP requests to Hypernative.
    /// 0 means unlimited. Defaults to 4.
    #[serde(default = "HypernativeConfig::default_max_concurrent_requests")]
    pub max_concurrent_requests: u32,
    /// Maximum requests per second sent to Hypernative (sliding-window).
    /// 0 means unlimited. Defaults to 10.
    #[serde(default = "HypernativeConfig::default_max_rps")]
    pub max_rps: u32,
    /// How long a successfully-screened address is considered fresh, in seconds.
    /// Addresses screened within this window are skipped (both at startup via the
    /// DB query and at request time via the in-process in-flight cache).
    /// Defaults to 3600 (1 hour).
    #[serde(default = "HypernativeConfig::default_ttl_secs")]
    pub ttl_secs: u64,
}

impl HypernativeConfig {
    fn default_screener_url() -> String {
        "https://api.hypernative.xyz/screener/reputation".to_string()
    }

    const fn default_max_concurrent_requests() -> u32 {
        4
    }

    const fn default_max_rps() -> u32 {
        10
    }

    const fn default_ttl_secs() -> u64 {
        3600
    }
}

impl Default for HypernativeConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            screener_policy_id: None,
            screener_url: Self::default_screener_url(),
            max_concurrent_requests: Self::default_max_concurrent_requests(),
            max_rps: Self::default_max_rps(),
            ttl_secs: Self::default_ttl_secs(),
        }
    }
}
