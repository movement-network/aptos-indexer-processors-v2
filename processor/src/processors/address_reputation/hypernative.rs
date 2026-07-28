// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Hypernative address-reputation screener client.
//!
//! Calls `POST https://api.hypernative.xyz/screener/reputation` with a single
//! EVM address and returns the recommendation + severity from the response.

/// Fields extracted from a Hypernative screening response.
#[derive(Debug)]
pub struct HypernativeResult {
    pub address: String,
    pub recommendation: String,
    pub severity: String,
    pub total_incoming_usd: Option<f64>,
    pub total_outgoing_usd: Option<f64>,
    pub policy_id: Option<String>,
    pub screened_at: Option<String>,
}

pub struct HypernativeClient {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    screener_policy_id: Option<String>,
    screener_url: String,
}

impl HypernativeClient {
    pub fn screener_url(&self) -> &str {
        &self.screener_url
    }

    pub fn new(
        client_id: String,
        client_secret: String,
        screener_policy_id: Option<String>,
        screener_url: String,
    ) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("failed to build reqwest::Client for Hypernative"),
            client_id,
            client_secret,
            screener_policy_id,
            screener_url,
        }
    }

    /// Screen a single EVM address. Returns:
    /// - `Ok(Some(result))` — screened successfully
    /// - `Ok(None)`         — address absent in response data (shouldn't normally happen)
    /// - `Err(_)`           — network / HTTP / parse failure
    pub async fn fetch(&self, evm_address: &str) -> anyhow::Result<Option<HypernativeResult>> {
        let mut body = serde_json::json!({ "addresses": [evm_address] });
        if let Some(ref policy_id) = self.screener_policy_id {
            body["screenerPolicyId"] = serde_json::Value::String(policy_id.clone());
        }

        let resp = self
            .http
            .post(&self.screener_url)
            .header("Content-Type", "application/json")
            .header("x-client-id", &self.client_id)
            .header("x-client-secret", &self.client_secret)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("network: {e}"))?;

        if !resp.status().is_success() {
            anyhow::bail!("HTTP {}", resp.status());
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("json: {e}"))?;

        let entry = match json.get("data").and_then(|d| d.get(0)) {
            Some(e) => e,
            None => return Ok(None),
        };

        let get_str = |key: &str| entry.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        let get_f64 = |key: &str| entry.get(key).and_then(|v| v.as_f64());

        Ok(Some(HypernativeResult {
            address: get_str("address").unwrap_or_else(|| evm_address.to_owned()),
            recommendation: get_str("recommendation").unwrap_or_default(),
            severity: get_str("severity").unwrap_or_default(),
            total_incoming_usd: get_f64("totalIncomingUsd"),
            total_outgoing_usd: get_f64("totalOutgoingUsd"),
            policy_id: get_str("policyId"),
            screened_at: get_str("timestamp"),
        }))
    }
}
