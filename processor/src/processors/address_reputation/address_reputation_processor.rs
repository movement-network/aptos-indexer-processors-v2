// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    processors::{
        address_reputation::{
            address_reputation_config::BridgeConfig,
            address_reputation_extractor::AddressReputationExtractor,
            address_reputation_model::BridgeRegistryEntry,
            address_reputation_storer::AddressReputationStorer,
            evm_fetch_loop::{DbScoreSaver, EnricherLoop},
            hypernative::HypernativeClient,
            lz_enricher::LzEnricher,
        },
        processor_status_saver::{
            get_end_version, get_starting_version, PostgresProcessorStatusSaver,
        },
    },
    MIGRATIONS,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    postgres::utils::{
        checkpoint::PostgresChainIdChecker,
        database::{new_db_pool, run_migrations, ArcDbPool},
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
    utils::chain_id_check::check_or_update_chain_id,
};
use std::sync::Arc;
use tracing::{debug, info};

pub struct AddressReputationProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl AddressReputationProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        match config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                let conn_pool = new_db_pool(
                    &postgres_config.connection_string,
                    Some(postgres_config.db_pool_size),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create connection pool for PostgresConfig: {:?}",
                        e
                    )
                })?;

                Ok(Self {
                    config,
                    db_pool: conn_pool,
                })
            },
            _ => Err(anyhow::anyhow!(
                "Invalid db config for AddressReputationProcessor {:?}",
                config.db_config
            )),
        }
    }

    fn load_bridge_registry_from_config(bridges: &[BridgeConfig]) -> Vec<BridgeRegistryEntry> {
        bridges
            .iter()
            .filter(|b| b.enabled)
            .map(|b| BridgeRegistryEntry {
                bridge_name: b.name.clone(),
                module_address: b.module_address.clone(),
                event_type: b.event_type.clone(),
                evm_source_field_path: b.evm_source_field_path.clone(),
                recipient_field_path: b.recipient_field_path.clone(),
                amount_field_path: b.amount_field_path.clone(),
                chain_id_field_path: b.chain_id_field_path.clone(),
                payload_kind: b.payload_kind.clone(),
                enabled: b.enabled,
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for AddressReputationProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
                MIGRATIONS,
            )
            .await;
        }

        let (starting_version, ending_version) = (
            get_starting_version(&self.config, self.db_pool.clone()).await?,
            get_end_version(&self.config, self.db_pool.clone()).await?,
        );

        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        let processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::AddressReputationProcessor(c) => c,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for AddressReputationProcessor: {:?}",
                    self.config.processor_config
                ))
            },
        };
        let channel_size = processor_config.channel_size;

        let registry = Arc::new(Self::load_bridge_registry_from_config(
            &processor_config.bridges,
        ));
        info!(
            entries = registry.len(),
            "address_reputation: loaded bridge registry from config"
        );

        // Spawn the background enricher loop when LZ enrichment is enabled.
        // Channels are unbounded so the extractor never blocks.
        let (guid_sender, guid_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (evm_sender, evm_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let lz = if processor_config.lz_enricher.enabled {
            info!("address_reputation: Layer Zero enricher enabled");
            Some(LzEnricher::new(
                Some(self.db_pool.clone()),
                processor_config.propagate_evm_sources,
                processor_config.lz_enricher.scan_api_base_url.clone(),
            ))
        } else {
            None
        };

        let hypernative = if processor_config.hypernative.enabled {
            info!("address_reputation: Hypernative screener enabled");
            Some(HypernativeClient::new(
                processor_config.hypernative.client_id.clone(),
                processor_config.hypernative.client_secret.clone(),
                processor_config.hypernative.screener_policy_id.clone(),
                processor_config.hypernative.screener_url.clone(),
            ))
        } else {
            None
        };
        let saver = std::sync::Arc::new(DbScoreSaver::new(self.db_pool.clone()));
        let loop_ = EnricherLoop::new(
            Some(self.db_pool.clone()),
            guid_rx,
            evm_rx,
            lz,
            hypernative,
            processor_config.lz_enricher.interval_ms,
            saver,
        );
        tokio::spawn(loop_.run());
        info!("address_reputation: enricher loop started");

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;
        let extractor = AddressReputationExtractor::new(registry, guid_sender, evm_sender);
        let storer = AddressReputationStorer::new(self.db_pool.clone(), processor_config);
        let version_tracker = VersionTrackerStep::new(
            PostgresProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(extractor.into_runnable_step(), channel_size)
        .connect_to(storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "address_reputation: finished versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                },
                Err(e) => {
                    info!("No more transactions in channel: {:?}", e);
                    break Ok(());
                },
            }
        }
    }
}
