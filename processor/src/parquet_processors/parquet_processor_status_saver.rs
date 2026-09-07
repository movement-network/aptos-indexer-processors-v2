use super::parquet_utils::{
    parquet_version_tracker_step::ParquetProcessorStatusSaverTrait, util::format_table_name,
};
use crate::{
    config::{
        indexer_processor_config::IndexerProcessorConfig,
        processor_mode::{BackfillConfig, BootStrapConfig, ProcessorMode, TestingConfig},
    },
    db::backfill_processor_status::{
        BackfillProcessorStatus, BackfillProcessorStatusQuery, BackfillStatus,
    },
    processors::processor_status_saver::{log_ascii_warning, save_processor_status},
    schema::backfill_processor_status,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::{
        models::processor_status::ProcessorStatusQuery,
        utils::database::{execute_with_better_error, ArcDbPool},
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};

/// A trait implementation of ProcessorStatusSaver for Parquet.
pub struct ParquetProcessorStatusSaver {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl ParquetProcessorStatusSaver {
    pub fn new(config: IndexerProcessorConfig, db_pool: ArcDbPool) -> Self {
        Self { config, db_pool }
    }
}

#[async_trait]
impl ParquetProcessorStatusSaverTrait for ParquetProcessorStatusSaver {
    async fn save_parquet_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
        table_name: &str,
    ) -> Result<(), ProcessorError> {
        let processor_id = format_table_name(self.config.processor_config.name(), table_name);
        save_processor_status(
            &processor_id,
            self.config.processor_mode.clone(),
            last_success_batch,
            self.db_pool.clone(),
        )
        .await
    }
}

/// Get the appropriate minimum last success version for the parquet processors.
///
/// This will return the minimum of the last success version of the processors in the list.
/// If no processor has a checkpoint, this will return the `starting_version` from the config, or 0 if not set.
pub async fn get_parquet_starting_version(
    config: &IndexerProcessorConfig,
    db_pool: ArcDbPool,
) -> Result<Option<u64>, ProcessorError> {
    let table_names = config
        .processor_config
        .get_processor_status_table_names()
        .map_err(|e| ProcessorError::ProcessError {
            message: format!("Failed to get processor status table names. {e:?}"),
        })?;

    match &config.processor_mode {
        ProcessorMode::Default(BootStrapConfig {
            initial_starting_version,
        }) => {
            let min_processed_version =
                get_min_processed_version_from_db(db_pool.clone(), table_names)
                    .await
                    .map_err(|e| ProcessorError::ProcessError {
                        message: format!(
                            "Failed to get minimum last success version from DB. {e:?}"
                        ),
                    })?;

            // If there's no last success version saved, start with the version from config
            Ok(Some(
                min_processed_version.map_or(*initial_starting_version, |version: u64| {
                    std::cmp::max(version, *initial_starting_version)
                }),
            ))
        },
        ProcessorMode::Backfill(BackfillConfig {
            backfill_id,
            initial_starting_version,
            ending_version,
            overwrite_checkpoint,
        }) => {
            let backfill_statuses = get_parquet_backfill_statuses(
                db_pool.clone(),
                table_names.clone(),
                backfill_id.clone(),
            )
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to query backfill_processor_status table. {e:?}"),
            })?;

            // Missing rows stay as None so a new table cannot be ignored.
            // If every table has no checkpoint, start from the config version.
            if backfill_statuses.iter().all(|status| status.is_none()) {
                return Ok(Some(*initial_starting_version));
            }

            // If every table has a Complete row and overwrite_checkpoint is false, end the backfill.
            // A missing row is not Complete — that table still needs versions.
            if all_backfill_tables_complete(
                backfill_statuses
                    .iter()
                    .map(|status| status.as_ref().map(|s| &s.backfill_status)),
            ) && !overwrite_checkpoint
            {
                return Ok(*ending_version);
            }

            // If overwrite_checkpoint is true, this is the start of a new backfill job.
            if *overwrite_checkpoint {
                // If the ending_version is provided, use it. If not, compute the ending_version from processor_status.last_success_version.
                let backfill_end_version = match *ending_version {
                    Some(e) => Some(e as i64),
                    None => get_min_processed_version_from_db(db_pool.clone(), table_names)
                        .await?
                        .map(|v| v as i64),
                };

                for backfill_status in backfill_statuses.into_iter().flatten() {
                    let backfill_alias = backfill_status.backfill_alias.clone();

                    let status = BackfillProcessorStatus {
                        backfill_alias,
                        backfill_status: BackfillStatus::InProgress,
                        last_success_version: 0,
                        last_transaction_timestamp: None,
                        backfill_start_version: *initial_starting_version as i64,
                        backfill_end_version,
                    };
                    execute_with_better_error(
                        db_pool.clone(),
                        diesel::insert_into(backfill_processor_status::table)
                            .values(&status)
                            .on_conflict(backfill_processor_status::backfill_alias)
                            .do_update()
                            .set((
                                backfill_processor_status::backfill_status
                                    .eq(excluded(backfill_processor_status::backfill_status)),
                                backfill_processor_status::last_success_version
                                    .eq(excluded(backfill_processor_status::last_success_version)),
                                backfill_processor_status::last_updated
                                    .eq(excluded(backfill_processor_status::last_updated)),
                                backfill_processor_status::last_transaction_timestamp.eq(excluded(
                                    backfill_processor_status::last_transaction_timestamp,
                                )),
                                backfill_processor_status::backfill_start_version.eq(excluded(
                                    backfill_processor_status::backfill_start_version,
                                )),
                                backfill_processor_status::backfill_end_version
                                    .eq(excluded(backfill_processor_status::backfill_end_version)),
                            )),
                    )
                    .await?;
                }
                return Ok(Some(*initial_starting_version));
            }

            // Present tables resume at last_success_version + 1.
            // A missing table uses initial_starting_version so it cannot skip
            // to another table's backfill watermark.
            let min_version = min_backfill_resume_version(
                backfill_statuses
                    .iter()
                    .map(|status| status.as_ref().map(|s| s.last_success_version as u64)),
                *initial_starting_version,
            );
            log_ascii_warning(min_version);
            Ok(Some(min_version))
        },
        ProcessorMode::Testing(TestingConfig {
            override_starting_version,
            ..
        }) => {
            // Always start from the override_starting_version.
            Ok(Some(*override_starting_version))
        },
    }
}

pub async fn get_parquet_end_version(
    config: &IndexerProcessorConfig,
    db_pool: ArcDbPool,
) -> Result<Option<u64>, ProcessorError> {
    let table_names = config
        .processor_config
        .get_processor_status_table_names()
        .map_err(|e| ProcessorError::ProcessError {
            message: format!("Failed to get processor status table names. {e:?}"),
        })?;

    match &config.processor_mode {
        ProcessorMode::Default(_) => Ok(None),
        ProcessorMode::Backfill(BackfillConfig { ending_version, .. }) => {
            match ending_version {
                Some(ending_version) => Ok(Some(*ending_version)),
                None => {
                    // If there is no ending version in the config, use last success version of the parquet processor.
                    let min_processed_version =
                        get_min_processed_version_from_db(db_pool.clone(), table_names)
                            .await
                            .map_err(|e| ProcessorError::ProcessError {
                                message: format!(
                                    "Failed to get minimum last success version from DB. {e:?}"
                                ),
                            })?;
                    Ok(min_processed_version)
                },
            }
        },
        ProcessorMode::Testing(TestingConfig {
            override_starting_version,
            ending_version,
        }) => {
            // If no ending version is provided, use the override_starting_version so testing mode only processes 1 transaction at a time.
            Ok(Some(ending_version.unwrap_or(*override_starting_version)))
        },
    }
}

/// Get the minimum last success version from the database for the given processors.
///
/// This should return the minimum of the last success version of the processors in the list.
/// If any of the tables handled by the parquet processor has no entry, it should use 0 as a default value.
/// To avoid skipping any versions, the minimum of the last success version should be used as the starting version.
async fn get_min_processed_version_from_db(
    db_pool: ArcDbPool,
    table_names: Vec<String>,
) -> Result<Option<u64>, ProcessorError> {
    let mut queries = Vec::new();

    // Spawn all queries concurrently with separate connections
    for processor_name in table_names {
        let db_pool = db_pool.clone();
        let processor_name = processor_name.clone();

        let query = async move {
            let mut conn = db_pool
                .get()
                .await
                .map_err(|err| ProcessorError::ProcessError {
                    message: format!("Failed to get database connection. {err:?}"),
                })?;
            ProcessorStatusQuery::get_by_processor(&processor_name, &mut conn)
                .await
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to query processor_status table. {e:?}"),
                })
        };

        queries.push(query);
    }

    let results = futures::future::join_all(queries).await;

    // Collect results and find the minimum processed version
    let min_processed_version = results
        .into_iter()
        .filter_map(|res| {
            match res {
                // If the result is `Ok`, proceed to check the status
                Ok(Some(status)) => {
                    // Return the version if the status contains a version
                    Some(status.last_success_version as u64)
                },
                // Handle specific cases where `Ok` contains `None` (no status found)
                Ok(None) => None,
                // TODO: If the result is an `Err`, what should we do?
                Err(e) => {
                    eprintln!("Error fetching processor status: {e:?}");
                    None
                },
            }
        })
        .min();

    Ok(min_processed_version)
}

async fn get_parquet_backfill_statuses(
    db_pool: ArcDbPool,
    table_names: Vec<String>,
    backfill_id: String,
) -> Result<Vec<Option<BackfillProcessorStatusQuery>>, ProcessorError> {
    let mut queries = Vec::new();

    // Spawn all queries concurrently with separate connections
    for processor_name in table_names {
        let db_pool = db_pool.clone();
        let processor_name = processor_name.clone();
        let backfill_id = backfill_id.clone();
        let query = async move {
            let mut conn = db_pool
                .get()
                .await
                .map_err(|err| ProcessorError::ProcessError {
                    message: format!("Failed to get database connection. {err:?}"),
                })?;
            BackfillProcessorStatusQuery::get_by_processor(&processor_name, &backfill_id, &mut conn)
                .await
                .map_err(|e| ProcessorError::ProcessError {
                    message: format!("Failed to query backfill_processor_status table. {e:?}"),
                })
        };

        queries.push(query);
    }

    let results = futures::future::join_all(queries).await;
    collect_backfill_status_query_results(results)
}

/// Collect per-table `backfill_processor_status` query results.
///
/// A missing row (`Ok(None)`) is retained so a new or lagging table cannot be
/// ignored. Query failures are propagated; silently dropping them would skip
/// versions the same way as dropping a missing row.
fn collect_backfill_status_query_results<T>(
    results: impl IntoIterator<Item = Result<Option<T>, ProcessorError>>,
) -> Result<Vec<Option<T>>, ProcessorError> {
    results.into_iter().collect()
}

/// Resume version for a parquet multi-table backfill.
///
/// Present tables resume at `last_success_version + 1`. A missing table (`None`)
/// uses `initial_starting_version` so it cannot skip to another table's watermark.
fn min_backfill_resume_version(
    last_success_versions: impl IntoIterator<Item = Option<u64>>,
    initial_starting_version: u64,
) -> u64 {
    last_success_versions
        .into_iter()
        .map(|version| version.map_or(initial_starting_version, |v| v + 1))
        .min()
        .unwrap_or(initial_starting_version)
}

/// True only when every table has a Complete backfill row.
/// Missing rows are not complete — a new table still needs backfill.
fn all_backfill_tables_complete<'a>(
    statuses: impl IntoIterator<Item = Option<&'a BackfillStatus>>,
) -> bool {
    let mut saw_table = false;
    for status in statuses {
        saw_table = true;
        if status != Some(&BackfillStatus::Complete) {
            return false;
        }
    }
    saw_table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            db_config::{DbConfig, ParquetConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::{ParquetDefaultProcessorConfig, ProcessorConfig},
        },
        db::backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
        MIGRATIONS,
    };
    use aptos_indexer_processor_sdk::{
        aptos_indexer_transaction_stream::{
            utils::additional_headers::AdditionalHeaders, TransactionStreamConfig,
        },
        postgres::{
            models::processor_status::ProcessorStatus,
            utils::database::{new_db_pool, run_migrations},
        },
        testing_framework::database::{PostgresTestDatabase, TestDatabase},
    };
    use diesel_async::RunQueryDsl;
    use url::Url;

    #[test]
    fn backfill_status_all_tables_present_are_retained() {
        let results = [Ok(Some(100u64)), Ok(Some(10)), Ok(Some(50))];
        assert_eq!(
            collect_backfill_status_query_results(results).unwrap(),
            vec![Some(100), Some(10), Some(50)]
        );
    }

    #[test]
    fn backfill_status_missing_row_is_retained() {
        // A new/lagging table has no backfill_processor_status row. Keeping
        // None prevents resume at another table's watermark (e.g. 100 + 1),
        // which would skip versions 0..=100 for the missing table forever.
        let results = [Ok(Some(100u64)), Ok(None), Ok(Some(50))];
        assert_eq!(
            collect_backfill_status_query_results(results).unwrap(),
            vec![Some(100), None, Some(50)]
        );
    }

    #[test]
    fn backfill_status_query_error_is_propagated() {
        let results = [
            Ok(Some(100u64)),
            Err(ProcessorError::ProcessError {
                message: "db down".to_string(),
            }),
            Ok(Some(50)),
        ];
        assert!(collect_backfill_status_query_results(results).is_err());
    }

    #[test]
    fn backfill_status_empty_results_is_empty() {
        let results: [Result<Option<u64>, ProcessorError>; 0] = [];
        assert_eq!(
            collect_backfill_status_query_results(results)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn min_backfill_resume_all_tables_present_uses_min_plus_one() {
        let versions = [Some(100u64), Some(10), Some(50)];
        assert_eq!(min_backfill_resume_version(versions, 0), 11);
    }

    #[test]
    fn min_backfill_resume_missing_table_uses_initial_start() {
        // Tables A/B checkpointed at 100; table C has no row. Resume must be
        // the config start (0), not 101. Otherwise C never sees 0..=100.
        let versions = [Some(100u64), None, Some(50)];
        assert_eq!(min_backfill_resume_version(versions, 0), 0);
    }

    #[test]
    fn min_backfill_resume_missing_table_respects_nonzero_initial() {
        let versions = [Some(100u64), None];
        assert_eq!(min_backfill_resume_version(versions, 7), 7);
    }

    #[test]
    fn min_backfill_resume_all_missing_uses_initial_start() {
        let versions = [None, None];
        assert_eq!(min_backfill_resume_version(versions, 3), 3);
    }

    #[test]
    fn all_backfill_tables_complete_requires_every_row() {
        assert!(all_backfill_tables_complete([
            Some(&BackfillStatus::Complete),
            Some(&BackfillStatus::Complete),
        ]));
        assert!(!all_backfill_tables_complete([
            Some(&BackfillStatus::Complete),
            None,
        ]));
        assert!(!all_backfill_tables_complete([
            Some(&BackfillStatus::Complete),
            Some(&BackfillStatus::InProgress),
        ]));
        assert!(!all_backfill_tables_complete([None, None]));
        let no_tables: [Option<&BackfillStatus>; 0] = [];
        assert!(!all_backfill_tables_complete(no_tables));
    }

    fn create_indexer_config(
        db_url: String,
        processor_mode: ProcessorMode,
    ) -> IndexerProcessorConfig {
        let processor_config =
            ProcessorConfig::ParquetDefaultProcessor(ParquetDefaultProcessorConfig::default());
        let postgres_config = ParquetConfig {
            connection_string: db_url.to_string(),
            db_pool_size: 100,
            google_application_credentials: None,
            bucket_name: "test".to_string(),
            bucket_root: "test".to_string(),
        };
        let db_config = DbConfig::ParquetConfig(postgres_config);
        IndexerProcessorConfig {
            processor_config,
            db_config,
            processor_mode,
            transaction_stream_config: TransactionStreamConfig {
                indexer_grpc_data_service_address: Url::parse("https://test.com").unwrap(),
                starting_version: None,
                request_ending_version: None,
                auth_token: "test".to_string(),
                request_name_header: "test".to_string(),
                indexer_grpc_http2_ping_interval_secs: 1,
                indexer_grpc_http2_ping_timeout_secs: 1,
                indexer_grpc_reconnection_timeout_secs: 1,
                indexer_grpc_response_item_timeout_secs: 1,
                indexer_grpc_reconnection_max_retries: 1,
                additional_headers: AdditionalHeaders::default(),
                transaction_filter: None,
            },
        }
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_bootstrap_no_checkpoint_in_db() {
        let initial_starting_version = 0;
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version,
            }),
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );
        assert_eq!(starting_version, Some(initial_starting_version));
        assert_eq!(end_version, None);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_bootstrap_with_checkpoint_in_db() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();

        let last_success_version = 10;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version: 0,
            }),
        );

        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        let table_names = indexer_processor_config
            .processor_config
            .get_processor_status_table_names()
            .unwrap();

        for (i, table_name) in table_names.into_iter().enumerate() {
            diesel::insert_into(
                aptos_indexer_processor_sdk::postgres::processor_metadata_schema::processor_metadata::processor_status::table,
            )
            .values(ProcessorStatus {
                processor: table_name,
                last_success_version: last_success_version + i as i64,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");
        }

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(last_success_version as u64));
        assert_eq!(end_version, None);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_no_backfill_in_db() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_with_checkpoint_in_db_overwrite_false() {
        let last_success_version = 10;
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );

        let table_names = indexer_processor_config
            .processor_config
            .get_processor_status_table_names()
            .unwrap();

        for table_name in table_names.into_iter() {
            diesel::insert_into(crate::schema::backfill_processor_status::table)
                .values(BackfillProcessorStatus {
                    backfill_alias: format!("{table_name}_{backfill_id}"),
                    backfill_status: BackfillStatus::InProgress,
                    last_success_version: 10,
                    last_transaction_timestamp: None,
                    backfill_start_version: 0,
                    backfill_end_version: Some(10),
                })
                .execute(&mut conn_pool.clone().get().await.unwrap())
                .await
                .expect("Failed to insert backfill processor status");
        }

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(last_success_version as u64 + 1));
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_missing_table_does_not_skip_to_other_table_watermark() {
        // Only one of N parquet tables has a backfill_processor_status row.
        // The missing tables must pull the starting version back to the config
        // initial (0), not the checkpointed table's last_success + 1.
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: false,
            }),
        );
        let table_names = indexer_processor_config
            .processor_config
            .get_processor_status_table_names()
            .unwrap();
        assert!(
            table_names.len() > 1,
            "test requires a multi-table parquet processor"
        );

        diesel::insert_into(crate::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: format!("{}_{backfill_id}", table_names[0]),
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 100,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: Some(20),
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert backfill processor status");

        let starting_version =
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap();

        assert_eq!(starting_version, Some(0));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_with_checkpoint_in_db_overwrite_true() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: Some(20),
                overwrite_checkpoint: true,
            }),
        );
        let table_names = indexer_processor_config
            .processor_config
            .get_processor_status_table_names()
            .unwrap();

        for table_name in table_names.into_iter() {
            diesel::insert_into(crate::schema::backfill_processor_status::table)
                .values(BackfillProcessorStatus {
                    backfill_alias: format!("{table_name}_{backfill_id}"),
                    backfill_status: BackfillStatus::InProgress,
                    last_success_version: 10,
                    last_transaction_timestamp: None,
                    backfill_start_version: 0,
                    backfill_end_version: Some(10),
                })
                .execute(&mut conn_pool.clone().get().await.unwrap())
                .await
                .expect("Failed to insert backfill processor status");
        }

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_no_backfill_in_db_ending_version_none() {
        let backfill_id = "backfill_id".to_string();
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let head_processor_last_success_version = 10;
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: None,
                overwrite_checkpoint: false,
            }),
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;
        let table_names = indexer_processor_config
            .processor_config
            .get_processor_status_table_names()
            .unwrap();

        for (i, table_name) in table_names.into_iter().enumerate() {
            diesel::insert_into(
                aptos_indexer_processor_sdk::postgres::processor_metadata_schema::processor_metadata::processor_status::table,
            )
            .values(ProcessorStatus {
                processor: table_name,
                last_success_version: head_processor_last_success_version + i as i64,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");
        }

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(
            end_version,
            Some(head_processor_last_success_version as u64)
        );
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_testing_mode_with_ending_version() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Testing(TestingConfig {
                override_starting_version: 0,
                ending_version: Some(20),
            }),
        );

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(20));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_testing_mode_without_ending_version() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone(), MIGRATIONS).await;

        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            ProcessorMode::Testing(TestingConfig {
                override_starting_version: 0,
                ending_version: None,
            }),
        );

        let (starting_version, end_version) = (
            get_parquet_starting_version(&indexer_processor_config, conn_pool.clone())
                .await
                .unwrap(),
            get_parquet_end_version(&indexer_processor_config, conn_pool)
                .await
                .unwrap(),
        );

        assert_eq!(starting_version, Some(0));
        assert_eq!(end_version, Some(0));
    }
}
