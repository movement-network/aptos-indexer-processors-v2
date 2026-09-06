// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::resources::FromWriteResource,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::{
            token_models::token_utils::TokenWriteSet,
            token_v2_models::v2_token_utils::{
                TokenStandard, TokenV2, TokenV2Burned, DEFAULT_NONE,
            },
        },
    },
    schema::current_token_datas_v2,
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteResource, WriteResource, WriteTableItem},
    utils::convert::standardize_address,
};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TokenDataV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    // Here for consistency but we don't need to actually fill it
    pub is_deleted_v2: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
}

impl TokenDataV2 {
    // TODO: remove the useless_asref lint when new clippy nighly is released.
    #[allow(clippy::useless_asref)]
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentTokenDataV2)>> {
        if let Some(inner) = &TokenV2::from_write_resource(write_resource)? {
            let token_data_id = standardize_address(&write_resource.address.to_string());
            let mut token_name = inner.get_name_trunc();
            let is_fungible_v2;
            // Get token properties from 0x4::property_map::PropertyMap
            let mut token_properties = serde_json::Value::Null;
            // TokenV2 was identified. Missing ObjectCore is not "this write
            // resource is not a token" — that is `TokenV2::from_write_resource`
            // returning `Ok(None)`. The old path mapped the hole to `Ok(None)`
            // so `parse_v2_token` skipped token_datas_v2 / current_token_datas_v2
            // while TokenV2Extractor still succeeded and VersionTrackerStep
            // advanced the checkpoint.
            let object_metadata = require_object_core_for_token_data(
                object_metadatas.get(&token_data_id),
                txn_version,
                &token_data_id,
            )?;
            let fungible_asset_metadata = object_metadata.fungible_asset_metadata.as_ref();
            if fungible_asset_metadata.is_some() {
                is_fungible_v2 = Some(true);
            } else {
                is_fungible_v2 = Some(false);
            }
            token_properties = object_metadata
                .property_map
                .as_ref()
                .map(|m| m.inner.clone())
                .unwrap_or(token_properties);
            // In aggregator V2 name is now derived from a separate struct
            if let Some(token_identifier) = object_metadata.token_identifier.as_ref() {
                token_name = token_identifier.get_name_trunc();
            }

            let collection_id = inner.get_collection_address();
            let token_uri = inner.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    token_data_id: token_data_id.clone(),
                    collection_id: collection_id.clone(),
                    token_name: token_name.clone(),
                    maximum: None,
                    supply: None,
                    largest_property_version_v1: None,
                    token_uri: token_uri.clone(),
                    token_properties: token_properties.clone(),
                    description: inner.description.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2,
                    transaction_timestamp: txn_timestamp,
                    decimals: None,
                    is_deleted_v2: None,
                },
                CurrentTokenDataV2 {
                    token_data_id,
                    collection_id,
                    token_name,
                    maximum: None,
                    supply: None,
                    largest_property_version_v1: None,
                    token_uri,
                    token_properties,
                    description: inner.description.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2,
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    decimals: None,
                    is_deleted_v2: Some(false),
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where token is burned but objectCore is still there
    pub async fn get_burned_nft_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        tokens_burned: &TokenV2Burned,
    ) -> anyhow::Result<Option<CurrentTokenDataV2>> {
        let token_data_id = standardize_address(&write_resource.address.to_string());
        // reminder that v1 events won't get to this codepath
        if let Some(burn_event_v2) = tokens_burned.get(&standardize_address(&token_data_id)) {
            Ok(Some(CurrentTokenDataV2 {
                token_data_id,
                collection_id: burn_event_v2.get_collection_address(),
                token_name: "".to_string(),
                maximum: None,
                supply: None,
                largest_property_version_v1: None,
                token_uri: "".to_string(),
                token_properties: serde_json::Value::Null,
                description: "".to_string(),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: Some(false),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                decimals: None,
                is_deleted_v2: Some(true),
            }))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where token is burned and objectCore is deleted
    pub async fn get_burned_nft_v2_from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        tokens_burned: &TokenV2Burned,
    ) -> anyhow::Result<Option<CurrentTokenDataV2>> {
        let token_data_id = standardize_address(&delete_resource.address.to_string());
        // reminder that v1 events won't get to this codepath
        if let Some(burn_event_v2) = tokens_burned.get(&standardize_address(&token_data_id)) {
            Ok(Some(CurrentTokenDataV2 {
                token_data_id,
                collection_id: burn_event_v2.get_collection_address(),
                token_name: "".to_string(),
                maximum: None,
                supply: None,
                largest_property_version_v1: None,
                token_uri: "".to_string(),
                token_properties: serde_json::Value::Null,
                description: "".to_string(),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: Some(false),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                decimals: None,
                is_deleted_v2: Some(true),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentTokenDataV2)>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_token_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenData(inner)) => Some(inner),
            _ => None,
        };

        if let Some(token_data) = maybe_token_data {
            let maybe_token_data_id = match TokenWriteSet::from_table_item_type(
                table_item_data.key_type.as_str(),
                &table_item_data.key,
                txn_version,
            )? {
                Some(TokenWriteSet::TokenDataId(inner)) => Some(inner),
                _ => None,
            };
            if let Some(token_data_id_struct) = maybe_token_data_id {
                let collection_id = token_data_id_struct.get_collection_id();
                let token_data_id = token_data_id_struct.to_id();
                let token_name = token_data_id_struct.get_name_trunc();
                let token_uri = token_data.get_uri_trunc();

                return Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        token_data_id: token_data_id.clone(),
                        collection_id: collection_id.clone(),
                        token_name: token_name.clone(),
                        maximum: Some(token_data.maximum.clone()),
                        supply: Some(token_data.supply.clone()),
                        largest_property_version_v1: Some(
                            token_data.largest_property_version.clone(),
                        ),
                        token_uri: token_uri.clone(),
                        token_properties: token_data.default_properties.clone(),
                        description: token_data.description.clone(),
                        token_standard: TokenStandard::V1.to_string(),
                        is_fungible_v2: None,
                        transaction_timestamp: txn_timestamp,
                        decimals: None,
                        is_deleted_v2: None,
                    },
                    CurrentTokenDataV2 {
                        token_data_id,
                        collection_id,
                        token_name,
                        maximum: Some(token_data.maximum),
                        supply: Some(token_data.supply),
                        largest_property_version_v1: Some(token_data.largest_property_version),
                        token_uri,
                        token_properties: token_data.default_properties,
                        description: token_data.description,
                        token_standard: TokenStandard::V1.to_string(),
                        is_fungible_v2: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        decimals: None,
                        is_deleted_v2: None,
                    },
                )));
            } else {
                tracing::warn!(
                    transaction_version = txn_version,
                    key_type = table_item_data.key_type,
                    key = table_item_data.key,
                    "Expecting token_data_id as key for value = token_data"
                );
            }
        }
        Ok(None)
    }
}

/// After a TokenV2 write resource is identified, ObjectCore must be present
/// in `object_metadatas` (same ObjectGroup).
///
/// * `Ok(metadata)` — persist token_datas_v2 / current_token_datas_v2
/// * `Err(_)` — propagate so `parse_v2_token` fails (`.unwrap()` on main;
///   `?` once #36/#37 land) and TokenV2Extractor does not succeed. The
///   checkpoint does not advance.
///
/// The old write path mapped a missing ObjectCore onto `Ok(None)`, which is
/// the same success signal as "this write resource is not TokenV2".
pub(crate) fn require_object_core_for_token_data<T>(
    object_metadata: Option<T>,
    txn_version: i64,
    token_data_id: &str,
) -> anyhow::Result<T> {
    object_metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "ObjectCore missing for TokenV2 token_data_id {token_data_id}, txn version {txn_version}"
        )
    })
}

/// This is a parquet version of TokenDataV2

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetTokenDataV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub largest_property_version_v1: Option<String>, // String format of BigDecimal
    pub token_uri: String,
    pub token_properties: String,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub is_deleted_v2: Option<bool>,
}

impl NamedTable for ParquetTokenDataV2 {
    const TABLE_NAME: &'static str = "token_datas_v2";
}

impl HasVersion for ParquetTokenDataV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<TokenDataV2> for ParquetTokenDataV2 {
    fn from(raw_item: TokenDataV2) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            largest_property_version_v1: raw_item
                .largest_property_version_v1
                .map(|v| v.to_string()),
            token_uri: raw_item.token_uri,
            token_properties: canonical_json::to_string(&raw_item.token_properties.clone())
                .context("Failed to serialize token properties")
                .unwrap(),
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            block_timestamp: raw_item.transaction_timestamp,
            is_deleted_v2: raw_item.is_deleted_v2,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetCurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<String>,                     // BigDecimal
    pub supply: Option<String>,                      // BigDecimal
    pub largest_property_version_v1: Option<String>, // String format of BigDecimal
    pub token_uri: String,
    pub token_properties: String, // serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
}

impl NamedTable for ParquetCurrentTokenDataV2 {
    const TABLE_NAME: &'static str = "current_token_datas_v2";
}

impl HasVersion for ParquetCurrentTokenDataV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentTokenDataV2> for ParquetCurrentTokenDataV2 {
    fn from(raw_item: CurrentTokenDataV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            maximum: raw_item.maximum.map(|v| v.to_string()),
            supply: raw_item.supply.map(|v| v.to_string()),
            largest_property_version_v1: raw_item
                .largest_property_version_v1
                .map(|v| v.to_string()),
            token_uri: raw_item.token_uri,
            token_properties: canonical_json::to_string(&raw_item.token_properties).unwrap_or_else(
                |_| {
                    error!(
                        "Failed to serialize token_properties to JSON: {:?}",
                        raw_item.token_properties
                    );
                    DEFAULT_NONE.to_string()
                },
            ),
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            decimals: raw_item.decimals,
            is_deleted_v2: raw_item.is_deleted_v2,
        }
    }
}

// PK of current_token_datas_v2, i.e. token_data_id
pub type CurrentTokenDataV2PK = String;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id))]
#[diesel(table_name = current_token_datas_v2)]
pub struct PostgresCurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
}

impl From<CurrentTokenDataV2> for PostgresCurrentTokenDataV2 {
    fn from(raw_item: CurrentTokenDataV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            maximum: raw_item.maximum,
            supply: raw_item.supply,
            largest_property_version_v1: raw_item.largest_property_version_v1,
            token_uri: raw_item.token_uri,
            token_properties: raw_item.token_properties,
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            decimals: raw_item.decimals,
            is_deleted_v2: raw_item.is_deleted_v2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{require_object_core_for_token_data, TokenDataV2};
    use crate::processors::objects::v2_object_utils::ObjectAggregatedData;
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::{
        aptos_protos::transaction::v1::{MoveStructTag, WriteResource},
        utils::convert::standardize_address,
    };

    fn token_v2_write_resource(address: &str) -> WriteResource {
        WriteResource {
            address: address.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x4".to_string(),
                module: "token".to_string(),
                name: "Token".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x4::token::Token".to_string(),
            data: r#"{"collection":{"inner":"0xcol"},"description":"d","name":"n","uri":"u"}"#
                .to_string(),
        }
    }

    fn non_token_write_resource(address: &str) -> WriteResource {
        WriteResource {
            address: address.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "object".to_string(),
                name: "ObjectCore".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::object::ObjectCore".to_string(),
            data: r#"{"allow_ungated_transfer":true,"guid_creation_num":"0","owner":"0xabc"}"#
                .to_string(),
        }
    }

    #[test]
    fn missing_object_core_is_not_ok_none_skip() {
        let result = require_object_core_for_token_data(None::<()>, 42, "0xtoken");
        assert!(
            result.is_err(),
            "missing ObjectCore after TokenV2 is identified must fail the write, not skip"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for TokenV2"),
            "error should name the TokenV2 ObjectCore lookup, got: {message}"
        );
        assert!(
            message.contains("0xtoken"),
            "error should include token_data_id, got: {message}"
        );
    }

    /// `get_v2_from_write_resource` used to map a missing ObjectCore to
    /// `Ok(None)`. That is the same success signal as "this write resource
    /// is not TokenV2", so `parse_v2_token` still returned the batch and
    /// `TokenV2Extractor` let `VersionTrackerStep` advance the checkpoint
    /// while `token_datas_v2` / `current_token_datas_v2` never received
    /// the confirmed TokenV2 write.
    ///
    /// The write path now uses `require_object_core_for_token_data`: only
    /// `TokenV2::from_write_resource` returning `Ok(None)` skips; a hole
    /// after TokenV2 is identified is `Err`.
    #[test]
    fn write_path_does_not_map_missing_object_core_to_ok_none() {
        let write_resource = token_v2_write_resource(
            "0x00000000000000000000000000000000000000000000000000000000000000aa",
        );
        let empty_object_metadatas = AHashMap::new();
        let write_result = TokenDataV2::get_v2_from_write_resource(
            &write_resource,
            99,
            0,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        );
        assert!(
            write_result.is_err(),
            "missing ObjectCore must fail the write, not skip the token data row: {write_result:?}"
        );
        let message = write_result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for TokenV2"),
            "write-path error should name the ObjectCore hole, got: {message}"
        );
    }

    #[test]
    fn non_token_write_is_intentional_ok_none() {
        let write_resource = non_token_write_resource("0x1");
        let empty_object_metadatas = AHashMap::new();
        let result = TokenDataV2::get_v2_from_write_resource(
            &write_resource,
            1,
            0,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        )
        .unwrap();
        assert!(
            result.is_none(),
            "a non-Token write resource is intentional absence"
        );
    }

    #[test]
    fn resolved_object_core_is_kept() {
        let metadata =
            require_object_core_for_token_data(Some("object-core"), 1, "0xtoken").unwrap();
        assert_eq!(metadata, "object-core");

        let address = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let write_resource = token_v2_write_resource(address);
        let mut object_metadatas = AHashMap::new();
        object_metadatas.insert(
            standardize_address(address),
            ObjectAggregatedData::default(),
        );
        let (token_data, current) = TokenDataV2::get_v2_from_write_resource(
            &write_resource,
            7,
            1,
            chrono::NaiveDateTime::default(),
            &object_metadatas,
        )
        .unwrap()
        .expect("TokenV2 plus ObjectCore must persist token data");
        assert_eq!(token_data.token_name, "n");
        assert_eq!(current.last_transaction_version, 7);
        assert_eq!(current.is_deleted_v2, Some(false));
    }
}
