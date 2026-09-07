// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::resources::FromWriteResource,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        fungible_asset::{
            coin_models::coin_utils::{CoinInfoType, CoinResource},
            fungible_asset_models::v2_fungible_asset_utils::FungibleAssetMetadata,
        },
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_metadata,
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteResource, WriteResource},
    utils::convert::standardize_address,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// This is the asset type
pub type FungibleAssetMetadataPK = String;
pub type FungibleAssetMetadataMapping =
    AHashMap<FungibleAssetMetadataPK, FungibleAssetMetadataModel>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<BigDecimal>,
    pub maximum_v2: Option<BigDecimal>,
}

impl FungibleAssetMetadataModel {
    /// Fungible asset is part of an object and we need to get the object first to get owner address
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) = &FungibleAssetMetadata::from_write_resource(write_resource)? {
            // the new coin type
            let asset_type = standardize_address(&write_resource.address.to_string());
            // Metadata was identified. Missing ObjectCore is not "this write
            // resource is not FA metadata" — that is
            // `FungibleAssetMetadata::from_write_resource` returning `Ok(None)`.
            // The old path mapped the hole to `Ok(None)` so `parse_v2_coin`
            // skipped fungible_asset_metadata while FungibleAssetExtractor
            // still succeeded and VersionTrackerStep advanced the checkpoint.
            let object_metadata = require_object_core_for_fa_metadata(
                object_metadatas.get(&asset_type),
                txn_version,
                &asset_type,
            )?;
            let object = &object_metadata.object.object_core;
            let (maximum_v2, supply_v2) = if let Some(fungible_asset_supply) =
                object_metadata.fungible_asset_supply.as_ref()
            {
                (
                    fungible_asset_supply.get_maximum(),
                    Some(fungible_asset_supply.current.clone()),
                )
            } else if let Some(concurrent_fungible_asset_supply) =
                object_metadata.concurrent_fungible_asset_supply.as_ref()
            {
                (
                    Some(concurrent_fungible_asset_supply.current.max_value.clone()),
                    Some(concurrent_fungible_asset_supply.current.value.clone()),
                )
            } else {
                (None, None)
            };

            return Ok(Some(Self {
                asset_type: asset_type.clone(),
                creator_address: object.get_owner_address(),
                name: inner.get_name(),
                symbol: inner.get_symbol(),
                decimals: inner.decimals,
                icon_uri: Some(inner.get_icon_uri()),
                project_uri: Some(inner.get_project_uri()),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                supply_aggregator_table_handle_v1: None,
                supply_aggregator_table_key_v1: None,
                token_standard: TokenStandard::V2.to_string(),
                is_token_v2: None,
                supply_v2,
                maximum_v2,
            }));
        }
        Ok(None)
    }

    /// We can find v1 coin info from resources
    pub fn get_v1_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_write_resource(write_resource, txn_version, txn_timestamp)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    write_resource.type_str.as_ref(),
                    txn_version,
                    write_set_change_index,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));
                // If asset type is too long, just ignore
                if let Some(asset_type) = coin_info_type.get_coin_type_below_max() {
                    Ok(Some(Self {
                        asset_type,
                        creator_address: coin_info_type.get_creator_address(),
                        name: inner.get_name_trunc(),
                        symbol: inner.get_symbol_trunc(),
                        decimals: inner.decimals,
                        icon_uri: None,
                        project_uri: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        supply_aggregator_table_handle_v1: supply_aggregator_table_handle,
                        supply_aggregator_table_key_v1: supply_aggregator_table_key,
                        token_standard: TokenStandard::V1.to_string(),
                        is_token_v2: None,
                        supply_v2: None,
                        maximum_v2: None,
                    }))
                } else {
                    Ok(None)
                }
            },
            _ => Ok(None),
        }
    }

    pub fn get_v1_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_delete_resource(delete_resource, txn_version)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &delete_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    delete_resource.type_str.as_ref(),
                    txn_version,
                    write_set_change_index,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));
                // If asset type is too long, just ignore
                if let Some(asset_type) = coin_info_type.get_coin_type_below_max() {
                    Ok(Some(Self {
                        asset_type,
                        creator_address: coin_info_type.get_creator_address(),
                        name: inner.get_name_trunc(),
                        symbol: inner.get_symbol_trunc(),
                        decimals: inner.decimals,
                        icon_uri: None,
                        project_uri: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        supply_aggregator_table_handle_v1: supply_aggregator_table_handle,
                        supply_aggregator_table_key_v1: supply_aggregator_table_key,
                        token_standard: TokenStandard::V1.to_string(),
                        is_token_v2: None,
                        supply_v2: None,
                        maximum_v2: None,
                    }))
                } else {
                    Ok(None)
                }
            },
            _ => Ok(None),
        }
    }
}

/// After a `0x1::fungible_asset::Metadata` write resource is identified,
/// ObjectCore must be present in `object_metadatas` (same ObjectGroup).
///
/// * `Ok(metadata)` — persist fungible_asset_metadata
/// * `Err(_)` — propagate so `parse_v2_coin` fails (`unwrap_or_else` + panic
///   on main) and FungibleAssetExtractor does not succeed. The checkpoint
///   does not advance.
///
/// The old write path mapped a missing ObjectCore onto `Ok(None)`, which is
/// the same success signal as "this write resource is not FA metadata".
pub(crate) fn require_object_core_for_fa_metadata<T>(
    object_metadata: Option<T>,
    txn_version: i64,
    asset_type: &str,
) -> anyhow::Result<T> {
    object_metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "ObjectCore missing for FA metadata asset_type {asset_type}, txn version {txn_version}"
        )
    })
}

// Parquet version of FungibleAssetMetadataModel
#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetFungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<String>, // it is a string representation of the u128
    pub maximum_v2: Option<String>, // it is a string representation of the u128
}

impl NamedTable for ParquetFungibleAssetMetadataModel {
    const TABLE_NAME: &'static str = "fungible_asset_metadata";
}

impl HasVersion for ParquetFungibleAssetMetadataModel {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<FungibleAssetMetadataModel> for ParquetFungibleAssetMetadataModel {
    fn from(raw: FungibleAssetMetadataModel) -> Self {
        Self {
            asset_type: raw.asset_type,
            creator_address: raw.creator_address,
            name: raw.name,
            symbol: raw.symbol,
            decimals: raw.decimals,
            icon_uri: raw.icon_uri,
            project_uri: raw.project_uri,
            last_transaction_version: raw.last_transaction_version,
            last_transaction_timestamp: raw.last_transaction_timestamp,
            supply_aggregator_table_handle_v1: raw.supply_aggregator_table_handle_v1,
            supply_aggregator_table_key_v1: raw.supply_aggregator_table_key_v1,
            token_standard: raw.token_standard,
            is_token_v2: raw.is_token_v2,
            supply_v2: raw.supply_v2.map(|x| x.to_string()),
            maximum_v2: raw.maximum_v2.map(|x| x.to_string()),
        }
    }
}

// Postgres Model

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(asset_type))]
#[diesel(table_name = fungible_asset_metadata)]
pub struct PostgresFungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<BigDecimal>,
    pub maximum_v2: Option<BigDecimal>,
}

impl From<FungibleAssetMetadataModel> for PostgresFungibleAssetMetadataModel {
    fn from(raw: FungibleAssetMetadataModel) -> Self {
        Self {
            asset_type: raw.asset_type,
            creator_address: raw.creator_address,
            name: raw.name,
            symbol: raw.symbol,
            decimals: raw.decimals,
            icon_uri: raw.icon_uri,
            project_uri: raw.project_uri,
            last_transaction_version: raw.last_transaction_version,
            last_transaction_timestamp: raw.last_transaction_timestamp,
            supply_aggregator_table_handle_v1: raw.supply_aggregator_table_handle_v1,
            supply_aggregator_table_key_v1: raw.supply_aggregator_table_key_v1,
            token_standard: raw.token_standard,
            is_token_v2: raw.is_token_v2,
            supply_v2: raw.supply_v2,
            maximum_v2: raw.maximum_v2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{require_object_core_for_fa_metadata, FungibleAssetMetadataModel};
    use crate::processors::objects::v2_object_utils::ObjectAggregatedData;
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::{
        aptos_protos::transaction::v1::{MoveStructTag, WriteResource},
        utils::convert::standardize_address,
    };

    fn fa_metadata_write_resource(address: &str) -> WriteResource {
        WriteResource {
            address: address.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "fungible_asset".to_string(),
                name: "Metadata".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::fungible_asset::Metadata".to_string(),
            data: r#"{"name":"USD Coin","symbol":"USDC","decimals":6,"icon_uri":"https://icon","project_uri":"https://proj"}"#
                .to_string(),
        }
    }

    fn non_metadata_write_resource(address: &str) -> WriteResource {
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
        let result = require_object_core_for_fa_metadata(None::<()>, 42, "0xasset");
        assert!(
            result.is_err(),
            "missing ObjectCore after FA Metadata is identified must fail the write, not skip"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for FA metadata"),
            "error should name the FA metadata ObjectCore lookup, got: {message}"
        );
        assert!(
            message.contains("0xasset"),
            "error should include asset_type, got: {message}"
        );
    }

    /// `get_v2_from_write_resource` used to map a missing ObjectCore to
    /// `Ok(None)`. That is the same success signal as "this write resource
    /// is not FA metadata", so `parse_v2_coin` still returned the batch and
    /// `FungibleAssetExtractor` let `VersionTrackerStep` advance the
    /// checkpoint while `fungible_asset_metadata` never received the
    /// confirmed Metadata write.
    ///
    /// The write path now uses `require_object_core_for_fa_metadata`: only
    /// `FungibleAssetMetadata::from_write_resource` returning `Ok(None)`
    /// skips; a hole after Metadata is identified is `Err`.
    #[test]
    fn write_path_does_not_map_missing_object_core_to_ok_none() {
        let write_resource = fa_metadata_write_resource(
            "0x00000000000000000000000000000000000000000000000000000000000000aa",
        );
        let empty_object_metadatas = AHashMap::new();
        let write_result = FungibleAssetMetadataModel::get_v2_from_write_resource(
            &write_resource,
            99,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        );
        assert!(
            write_result.is_err(),
            "missing ObjectCore must fail the write, not skip the metadata row: {write_result:?}"
        );
        let message = write_result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for FA metadata"),
            "write-path error should name the ObjectCore hole, got: {message}"
        );
    }

    #[test]
    fn non_metadata_write_is_intentional_ok_none() {
        let write_resource = non_metadata_write_resource("0x1");
        let empty_object_metadatas = AHashMap::new();
        let result = FungibleAssetMetadataModel::get_v2_from_write_resource(
            &write_resource,
            1,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        )
        .unwrap();
        assert!(
            result.is_none(),
            "a non-Metadata write resource is intentional absence"
        );
    }

    #[test]
    fn resolved_object_core_is_kept() {
        let metadata =
            require_object_core_for_fa_metadata(Some("object-core"), 1, "0xasset").unwrap();
        assert_eq!(metadata, "object-core");

        let address = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let write_resource = fa_metadata_write_resource(address);
        let mut object_metadatas = AHashMap::new();
        object_metadatas.insert(
            standardize_address(address),
            ObjectAggregatedData::default(),
        );
        let fa_metadata = FungibleAssetMetadataModel::get_v2_from_write_resource(
            &write_resource,
            7,
            chrono::NaiveDateTime::default(),
            &object_metadatas,
        )
        .unwrap()
        .expect("FA Metadata plus ObjectCore must persist metadata");
        assert_eq!(fa_metadata.name, "USD Coin");
        assert_eq!(fa_metadata.symbol, "USDC");
        assert_eq!(fa_metadata.decimals, 6);
        assert_eq!(fa_metadata.last_transaction_version, 7);
        assert_eq!(fa_metadata.token_standard, "v2");
    }
}
