use crate::models::confidential_asset_models::ConfidentialAssetActivity;
use anyhow::Result;
use diesel::{pg::PgConnection, query_dsl::methods::ThenOrderDsl, ExpressionMethods, RunQueryDsl};
use processor::schema::confidential_asset_activities::dsl::*;
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(conn: &mut PgConnection) -> Result<HashMap<String, Value>> {
    let rows = confidential_asset_activities
        .then_order_by(transaction_version.asc())
        .then_order_by(event_index.asc())
        .load::<ConfidentialAssetActivity>(conn)?;

    let json = serde_json::to_string_pretty(&rows)?;
    let mut result = HashMap::new();
    result.insert(
        "confidential_asset_activities".to_string(),
        serde_json::from_str(&json)?,
    );
    Ok(result)
}
