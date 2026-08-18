// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use address_reputation_api::{config, db, routes};
use clap::Parser;
use std::{collections::HashSet, sync::Arc};
use tracing::info;

#[derive(Parser)]
#[command(
    name = "address-reputation-api",
    about = "REST API for address reputation data"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "address_reputation_api=info,tower_http=info".into()),
        )
        .json()
        .init();

    let cli = Cli::parse();
    let config = config::Config::from_file(&cli.config)?;

    info!(
        host = %config.server.host,
        port = config.server.port,
        pool_size = config.database.pool_size,
        api_key_count = config.auth.api_keys.len(),
        "address-reputation-api: starting"
    );

    let pool = Arc::new(
        db::new_pool(
            &config.database.connection_string,
            config.database.pool_size,
        )
        .await?,
    );

    let api_keys: HashSet<String> = config.auth.api_keys.into_iter().collect();
    let state = routes::AppState {
        pool,
        api_keys: Arc::new(api_keys),
    };

    let app = routes::build_router(state);
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!(addr = %bind_addr, "address-reputation-api: listening");

    axum::serve(listener, app).await?;
    Ok(())
}
