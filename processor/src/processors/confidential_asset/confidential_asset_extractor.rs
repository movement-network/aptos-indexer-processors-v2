use crate::processors::confidential_asset::models::confidential_asset_events::{
    parse_ca_events, ConfidentialAssetActivity,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use rayon::prelude::*;

/// Aggregated output for one batch of transactions.
pub struct ConfidentialAssetEvents {
    pub activities: Vec<ConfidentialAssetActivity>,
}

pub struct ConfidentialAssetExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for ConfidentialAssetExtractor {
    type Input = Vec<Transaction>;
    type Output = ConfidentialAssetEvents;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<ConfidentialAssetEvents>>, ProcessorError> {
        // Parse all transactions in parallel; collect per-tx results then flatten.
        let activities: Vec<ConfidentialAssetActivity> = item
            .data
            .par_iter()
            .map(parse_ca_events)
            .flat_map(|p| p.activities)
            .collect();

        Ok(Some(TransactionContext {
            data: ConfidentialAssetEvents { activities },
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for ConfidentialAssetExtractor {}

impl NamedStep for ConfidentialAssetExtractor {
    fn name(&self) -> String {
        "ConfidentialAssetExtractor".to_string()
    }
}
