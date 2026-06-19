use ahash::AHashMap;
use aptos_indexer_processor_sdk::testing_framework::sdk_test_context::SdkTestContext;
use processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
    processor_mode::{ProcessorMode, TestingConfig},
};
use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Pre-recorded synthetic transactions (one per CA event type)
// ---------------------------------------------------------------------------

pub const CA_TXN_REGISTER: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1001_ca_register.json");
pub const CA_TXN_DEPOSIT: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1002_ca_deposit.json");
pub const CA_TXN_WITHDRAW: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1003_ca_withdraw.json");
pub const CA_TXN_TRANSFER: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1004_ca_transfer.json");
pub const CA_TXN_ROLLOVER: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1005_ca_rollover.json");
pub const CA_TXN_NORMALIZE: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1006_ca_normalize.json");
pub const CA_TXN_KEY_ROTATE: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1007_ca_key_rotate.json");
pub const CA_TXN_ASSET_AUDITOR_CHANGED: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1008_ca_asset_auditor_changed.json");
pub const CA_TXN_TOKEN_ALLOW_CHANGED: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1009_ca_token_allow_changed.json");
pub const CA_TXN_ALLOW_LIST_CHANGED: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1010_ca_allow_list_changed.json");
pub const CA_TXN_CHAIN_AUDITOR_ADMIN_CHANGED: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1011_ca_chain_auditor_admin_changed.json");
pub const CA_TXN_CHAIN_AUDITOR_CHANGED: &[u8] =
    include_bytes!("test_transactions/confidential_asset/1012_ca_chain_auditor_changed.json");

// ---------------------------------------------------------------------------
// Processor config helper
// ---------------------------------------------------------------------------

pub fn setup_ca_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config();
    let postgres_config = PostgresConfig {
        connection_string: db_url.to_string(),
        db_pool_size: 100,
    };
    let db_config = DbConfig::PostgresConfig(postgres_config);
    let default_processor_config = DefaultProcessorConfig {
        per_table_chunk_sizes: AHashMap::new(),
        channel_size: 100,
        tables_to_write: HashSet::new(),
    };
    let processor_config = ProcessorConfig::ConfidentialAssetProcessor(default_processor_config);
    let processor_name = processor_config.name();
    (
        IndexerProcessorConfig {
            processor_config,
            transaction_stream_config: transaction_stream_config.clone(),
            db_config,
            processor_mode: ProcessorMode::Testing(TestingConfig {
                override_starting_version: transaction_stream_config.starting_version.unwrap(),
                ending_version: transaction_stream_config.request_ending_version,
            }),
        },
        processor_name,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[allow(clippy::needless_return)]
#[cfg(test)]
mod sdk_confidential_asset_processor_tests {
    use super::{
        setup_ca_processor_config, CA_TXN_ALLOW_LIST_CHANGED, CA_TXN_ASSET_AUDITOR_CHANGED,
        CA_TXN_CHAIN_AUDITOR_ADMIN_CHANGED, CA_TXN_CHAIN_AUDITOR_CHANGED, CA_TXN_DEPOSIT,
        CA_TXN_KEY_ROTATE, CA_TXN_NORMALIZE,
        CA_TXN_REGISTER, CA_TXN_ROLLOVER, CA_TXN_TOKEN_ALLOW_CHANGED, CA_TXN_TRANSFER,
        CA_TXN_WITHDRAW,
    };
    use crate::{
        diff_test_helper::confidential_asset_processor::load_data,
        sdk_tests::test_helpers::{
            run_processor_test, setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_processor_sdk::testing_framework::{
        cli_parser::get_test_config, database::TestDatabase,
    };
    use processor::processors::confidential_asset::confidential_asset_processor::ConfidentialAssetProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_register() {
        process_single_ca_txn(CA_TXN_REGISTER, Some("ca_register".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_deposit() {
        process_single_ca_txn(CA_TXN_DEPOSIT, Some("ca_deposit".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_withdraw() {
        process_single_ca_txn(CA_TXN_WITHDRAW, Some("ca_withdraw".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_transfer() {
        process_single_ca_txn(CA_TXN_TRANSFER, Some("ca_transfer".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_rollover() {
        process_single_ca_txn(CA_TXN_ROLLOVER, Some("ca_rollover".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_normalize() {
        process_single_ca_txn(CA_TXN_NORMALIZE, Some("ca_normalize".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_key_rotate() {
        process_single_ca_txn(CA_TXN_KEY_ROTATE, Some("ca_key_rotate".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_asset_auditor_changed() {
        process_single_ca_txn(
            CA_TXN_ASSET_AUDITOR_CHANGED,
            Some("ca_asset_auditor_changed".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_token_allow_changed() {
        process_single_ca_txn(
            CA_TXN_TOKEN_ALLOW_CHANGED,
            Some("ca_token_allow_changed".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_allow_list_changed() {
        process_single_ca_txn(
            CA_TXN_ALLOW_LIST_CHANGED,
            Some("ca_allow_list_changed".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_chain_auditor_changed() {
        process_single_ca_txn(
            CA_TXN_CHAIN_AUDITOR_CHANGED,
            Some("ca_chain_auditor_changed".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ca_chain_auditor_admin_changed() {
        process_single_ca_txn(
            CA_TXN_CHAIN_AUDITOR_ADMIN_CHANGED,
            Some("ca_chain_auditor_admin_changed".to_string()),
        )
        .await;
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    async fn process_single_ca_txn(txn: &[u8], test_case_name: Option<String>) {
        let (generate_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(&[txn]).await;
        let db_url = db.get_db_url();
        let (config, processor_name) = setup_ca_processor_config(&test_context, &db_url);

        let processor = ConfidentialAssetProcessor::new(config)
            .await
            .expect("Failed to create ConfidentialAssetProcessor");

        match run_processor_test(
            &mut test_context,
            processor,
            load_data,
            db_url,
            generate_flag,
            output_path.clone(),
            test_case_name.clone(),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    test_context.get_request_start_version(),
                    processor_name,
                    output_path,
                    test_case_name,
                );
            },
            Err(e) => panic!(
                "Test failed for versions {:?}: {e}",
                test_context.get_test_transaction_versions()
            ),
        }
    }
}
