use diesel::{Identifiable, Queryable};
use processor::schema::confidential_asset_activities;
use serde::{Deserialize, Serialize};

/// Queryable mirror of `confidential_asset_activities`.
/// Columns must be listed in the same order as the table definition so that
/// Diesel's `Queryable` derive maps them correctly.
#[derive(Clone, Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = confidential_asset_activities)]
pub struct ConfidentialAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_type: String,
    pub asset_type: Option<String>,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub event_data: serde_json::Value,
    pub block_height: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
}
