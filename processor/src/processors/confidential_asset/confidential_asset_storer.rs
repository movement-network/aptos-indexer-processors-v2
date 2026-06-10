use crate::{
    config::processor_config::DefaultProcessorConfig,
    processors::confidential_asset::{
        confidential_asset_extractor::ConfidentialAssetEvents,
        models::confidential_asset_events::ConfidentialAssetActivity,
    },
    schema,
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use tracing::debug;

pub struct ConfidentialAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl ConfidentialAssetStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            per_table_chunk_sizes: processor_config.per_table_chunk_sizes,
        }
    }
}

#[async_trait]
impl Processable for ConfidentialAssetStorer {
    type Input = ConfidentialAssetEvents;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        events: TransactionContext<ConfidentialAssetEvents>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let activities_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_activities_query,
            &events.data.activities,
            get_config_table_chunk_size::<ConfidentialAssetActivity>(
                "confidential_asset_activities",
                &self.per_table_chunk_sizes,
            ),
        )
        .await;

        if let Err(e) = activities_res {
            return Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store confidential_asset_activities for versions {} to {}: {:?}",
                    events.metadata.start_version, events.metadata.end_version, e,
                ),
                query: None,
            });
        }

        debug!(
            "ConfidentialAsset versions [{}, {}] stored: {} activities",
            events.metadata.start_version,
            events.metadata.end_version,
            events.data.activities.len(),
        );

        Ok(Some(TransactionContext {
            data: (),
            metadata: events.metadata,
        }))
    }
}

impl AsyncStep for ConfidentialAssetStorer {}

impl NamedStep for ConfidentialAssetStorer {
    fn name(&self) -> String {
        "ConfidentialAssetStorer".to_string()
    }
}

// ---------------------------------------------------------------------------
// Query builder
// ---------------------------------------------------------------------------

fn insert_activities_query(
    items: Vec<ConfidentialAssetActivity>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::confidential_asset_activities::dsl::*;

    diesel::insert_into(schema::confidential_asset_activities::table)
        .values(items)
        .on_conflict((transaction_version, event_index))
        .do_update()
        .set((
            event_type.eq(excluded(event_type)),
            account_address.eq(excluded(account_address)),
            asset_type.eq(excluded(asset_type)),
            from_address.eq(excluded(from_address)),
            to_address.eq(excluded(to_address)),
            event_data.eq(excluded(event_data)),
            block_height.eq(excluded(block_height)),
            transaction_timestamp.eq(excluded(transaction_timestamp)),
            inserted_at.eq(excluded(inserted_at)),
        ))
}
