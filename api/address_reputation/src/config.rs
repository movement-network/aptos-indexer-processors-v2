// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    /// List of accepted API keys. Requests must carry one in the `X-Api-Key` header.
    pub api_keys: Vec<String>,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_pool_size() -> u32 {
    5
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read config file {path}: {e}"))?;
        toml::from_str(&content).map_err(|e| anyhow::anyhow!("config parse error: {e}"))
    }
}
