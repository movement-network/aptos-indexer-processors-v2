// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_fungible_asset_activities::{
        OwnerAddressToCoinType, StoreAddressToDeletedFungibleAssetStoreEvent,
    },
    v2_fungible_asset_to_coin_mappings::{FungibleAssetToCoinMapping, FungibleAssetToCoinMappings},
};
use crate::{
    db::resources::FromWriteResource,
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        default::models::move_resources::MoveResource,
        fungible_asset::{
            coin_models::coin_utils::{CoinInfoType, CoinResource},
            fungible_asset_models::{
                v2_fungible_asset_activities::EventToCoinType,
                v2_fungible_asset_utils::FungibleAssetStore,
            },
        },
        objects::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2::token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::{
        current_fungible_asset_balances, current_fungible_asset_balances_legacy,
        fungible_asset_balances,
    },
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteResource, WriteResource},
    utils::{
        constants::{APTOS_COIN_TYPE_STR, APT_METADATA_ADDRESS_HEX, APT_METADATA_ADDRESS_RAW},
        convert::{hex_to_raw_bytes, sha3_256, standardize_address},
    },
};
use bigdecimal::{BigDecimal, Zero};
use field_count::FieldCount;
use lazy_static::lazy_static;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

lazy_static! {
    pub static ref DEFAULT_AMOUNT_VALUE: String = "0".to_string();
}

// Storage id
pub type CurrentUnifiedFungibleAssetMapping = AHashMap<String, CurrentUnifiedFungibleAssetBalance>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FungibleAssetBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct CurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

pub fn get_paired_metadata_address(coin_type_name: &str) -> String {
    if coin_type_name == APTOS_COIN_TYPE_STR {
        APT_METADATA_ADDRESS_HEX.clone()
    } else {
        let mut preimage = APT_METADATA_ADDRESS_RAW.to_vec();
        preimage.extend(coin_type_name.as_bytes());
        preimage.push(0xFE);
        format!("0x{}", hex::encode(sha3_256(&preimage)))
    }
}

pub fn get_primary_fungible_store_address(
    owner_address: &str,
    metadata_address: &str,
) -> anyhow::Result<String> {
    let mut preimage = hex_to_raw_bytes(owner_address)?;
    preimage.append(&mut hex_to_raw_bytes(metadata_address)?);
    preimage.push(0xFC);
    Ok(standardize_address(&hex::encode(sha3_256(&preimage))))
}

impl CurrentUnifiedFungibleAssetBalance {
    pub fn from_fungible_asset_balances(
        fungible_asset_balances: &[FungibleAssetBalance],
        fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
    ) -> (
        CurrentUnifiedFungibleAssetMapping,
        CurrentUnifiedFungibleAssetMapping,
    ) {
        // Dedupe fungible asset balances by storage_id, keeping latest version
        let mut v1_balances: CurrentUnifiedFungibleAssetMapping = AHashMap::new();
        let mut v2_balances: CurrentUnifiedFungibleAssetMapping = AHashMap::new();

        for balance in fungible_asset_balances.iter() {
            let unified_balance = Self::from_balance(balance, fa_to_coin_mapping);
            match TokenStandard::from_str(&balance.token_standard).expect("Invalid token standard")
            {
                TokenStandard::V1 => {
                    v1_balances.insert(unified_balance.storage_id.clone(), unified_balance);
                },
                TokenStandard::V2 => {
                    v2_balances.insert(unified_balance.storage_id.clone(), unified_balance);
                },
            }
        }
        (v1_balances, v2_balances)
    }

    pub fn from_balance(
        fab: &FungibleAssetBalance,
        fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
    ) -> Self {
        // Determine if this is a V2 token standard
        let is_v2 = matches!(
            TokenStandard::from_str(&fab.token_standard).expect("Invalid token standard"),
            TokenStandard::V2
        );
        // For V2 tokens, asset_type_v2 is the original asset type
        // For V1 tokens, asset_type_v2 is None
        let asset_type_v2 = is_v2.then(|| fab.asset_type.clone());

        // For V2 tokens, look up V1 equivalent in mapping
        // For V1 tokens, use original asset type
        let asset_type_v1 = if is_v2 {
            FungibleAssetToCoinMapping::get_asset_type_v1(&fab.asset_type, fa_to_coin_mapping)
        } else {
            Some(fab.asset_type.clone())
        };

        // V1 tokens are always primary, V2 tokens use the stored value
        let is_primary = if is_v2 { fab.is_primary } else { true };

        // Amount and transaction details are stored in v1 or v2 fields based on token standard
        let (amount_v1, amount_v2, version_v1, version_v2, timestamp_v1, timestamp_v2) = if is_v2 {
            (
                None,
                Some(fab.amount.clone()),
                None,
                Some(fab.transaction_version),
                None,
                Some(fab.transaction_timestamp),
            )
        } else {
            (
                Some(fab.amount.clone()),
                None,
                Some(fab.transaction_version),
                None,
                Some(fab.transaction_timestamp),
                None,
            )
        };

        Self {
            storage_id: fab.storage_id.clone(),
            owner_address: fab.owner_address.clone(),
            asset_type_v1,
            asset_type_v2,
            is_primary,
            is_frozen: fab.is_frozen,
            amount_v1,
            amount_v2,
            last_transaction_version_v1: version_v1,
            last_transaction_version_v2: version_v2,
            last_transaction_timestamp_v1: timestamp_v1,
            last_transaction_timestamp_v2: timestamp_v2,
        }
    }
}

impl FungibleAssetBalance {
    /// Basically just need to index FA Store, but we'll need to look up FA metadata
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) = &FungibleAssetStore::from_write_resource(write_resource)? {
            let storage_id = standardize_address(write_resource.address.as_str());
            // FungibleStore was identified. Missing ObjectCore is not "this write
            // resource is not an FA balance" — that is
            // `FungibleAssetStore::from_write_resource` returning `Ok(None)`.
            // The old path mapped the hole to `Ok(None)` so `parse_v2_coin`
            // skipped fungible_asset_balances while FungibleAssetExtractor
            // still succeeded and VersionTrackerStep advanced the checkpoint.
            let object_data = require_object_core_for_fa_balance(
                object_metadatas.get(&storage_id),
                txn_version,
                &storage_id,
            )?;
            let object = &object_data.object.object_core;
            let owner_address = object.get_owner_address();
            let asset_type = inner.metadata.get_reference_address();
            let is_primary = Self::is_primary(&owner_address, &asset_type, &storage_id);

            #[allow(clippy::useless_asref)]
            let concurrent_balance = object_data.concurrent_fungible_asset_balance.as_ref().map(
                |concurrent_fungible_asset_balance| {
                    concurrent_fungible_asset_balance.balance.value.clone()
                },
            );

            let coin_balance = Self {
                transaction_version: txn_version,
                write_set_change_index,
                storage_id: storage_id.clone(),
                owner_address: owner_address.clone(),
                asset_type: asset_type.clone(),
                is_primary,
                is_frozen: inner.frozen,
                amount: concurrent_balance
                    .clone()
                    .unwrap_or_else(|| inner.balance.clone()),
                transaction_timestamp: txn_timestamp,
                token_standard: TokenStandard::V2.to_string(),
            };
            return Ok(Some(coin_balance));
        }

        Ok(None)
    }

    pub fn get_v2_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        store_address_to_deleted_fa_store_events: &StoreAddressToDeletedFungibleAssetStoreEvent,
    ) -> anyhow::Result<Option<Self>> {
        if delete_resource.type_str == "0x1::object::ObjectGroup" {
            let resource = match MoveResource::from_delete_resource(
                delete_resource,
                0, // Placeholder, this isn't used anyway
                txn_version,
                0, // Placeholder, this isn't used anyway
                txn_timestamp,
            ) {
                Ok(Some(resource)) => resource,
                Ok(None) => {
                    tracing::error!("No resource found for transaction version {}", txn_version);
                    return Ok(None);
                },
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Error getting resource from delete resource: {}",
                        e
                    ))
                },
            };

            if let Some(deleted_fa_store_event) =
                store_address_to_deleted_fa_store_events.get(&resource.resource_address)
            {
                let owner_address = standardize_address(deleted_fa_store_event.owner.as_str());
                let asset_type = standardize_address(deleted_fa_store_event.metadata.as_str());

                return Ok(Some(Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: resource.resource_address.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: asset_type.clone(),
                    is_primary: false, // Deleted stores can only be secondary
                    is_frozen: false,
                    amount: BigDecimal::zero(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V2.to_string(),
                }));
            }
        }
        Ok(None)
    }

    pub fn get_v1_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, OwnerAddressToCoinType)>> {
        if let Some(CoinResource::CoinStoreDeletion) =
            &CoinResource::from_delete_resource(delete_resource, txn_version)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &delete_resource.r#type.as_ref().unwrap().generic_type_params[0],
                delete_resource.type_str.as_ref(),
                txn_version,
                write_set_change_index,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(delete_resource.address.as_str());
                // Storage id should be derived (for the FA migration)
                let metadata_addr = get_paired_metadata_address(&coin_type);
                let storage_id = get_primary_fungible_store_address(&owner_address, &metadata_addr)
                    .expect("calculate primary fungible store failed");
                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: false,
                    amount: BigDecimal::zero(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                // Create address to coin type mapping
                let mut address_to_coin_type = AHashMap::new();
                address_to_coin_type.extend([(owner_address.clone(), coin_type.clone())]);
                return Ok(Some((coin_balance, address_to_coin_type)));
            }
        }
        Ok(None)
    }

    /// Getting coin balances from resources for v1
    /// If the fully qualified coin type is too long (currently 1000 length), we exclude from indexing
    pub fn get_v1_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, EventToCoinType)>> {
        if let Some(CoinResource::CoinStoreResource(inner)) =
            &CoinResource::from_write_resource(write_resource, txn_version, txn_timestamp)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                write_resource.type_str.as_ref(),
                txn_version,
                write_set_change_index,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(write_resource.address.as_str());
                // Storage id should be derived (for the FA migration)
                let metadata_addr = get_paired_metadata_address(&coin_type);
                let storage_id = get_primary_fungible_store_address(&owner_address, &metadata_addr)
                    .expect("calculate primary fungible store failed");
                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: inner.frozen,
                    amount: inner.coin.value.clone(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                let event_to_coin_mapping: EventToCoinType = AHashMap::from([
                    (
                        inner.withdraw_events.guid.id.get_standardized(),
                        coin_type.clone(),
                    ),
                    (inner.deposit_events.guid.id.get_standardized(), coin_type),
                ]);
                return Ok(Some((coin_balance, event_to_coin_mapping)));
            }
        }
        Ok(None)
    }

    /// Primary store address are derived from the owner address and object address in this format: sha3_256([source | object addr | 0xFC]).
    /// This function expects the addresses to have length 66
    pub fn is_primary(
        owner_address: &str,
        metadata_address: &str,
        fungible_store_address: &str,
    ) -> bool {
        fungible_store_address
            == get_primary_fungible_store_address(owner_address, metadata_address).unwrap()
    }
}

/// After a `0x1::fungible_asset::FungibleStore` write resource is identified,
/// ObjectCore must be present in `object_metadatas` (same ObjectGroup).
///
/// * `Ok(metadata)` — persist fungible_asset_balances
/// * `Err(_)` — propagate so `parse_v2_coin` fails (`unwrap_or_else` + panic
///   on main) and FungibleAssetExtractor does not succeed. The checkpoint
///   does not advance.
///
/// The old write path mapped a missing ObjectCore onto `Ok(None)`, which is
/// the same success signal as "this write resource is not an FA balance".
pub(crate) fn require_object_core_for_fa_balance<T>(
    object_metadata: Option<T>,
    txn_version: i64,
    storage_id: &str,
) -> anyhow::Result<T> {
    object_metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "ObjectCore missing for FA balance storage_id {storage_id}, txn version {txn_version}"
        )
    })
}

// Parquet Models
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetFungibleAssetBalance {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: String, // it is a string representation of the u128
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl NamedTable for ParquetFungibleAssetBalance {
    const TABLE_NAME: &'static str = "fungible_asset_balances";
}

impl HasVersion for ParquetFungibleAssetBalance {
    fn version(&self) -> i64 {
        self.txn_version
    }
}
impl From<FungibleAssetBalance> for ParquetFungibleAssetBalance {
    fn from(raw: FungibleAssetBalance) -> Self {
        Self {
            txn_version: raw.transaction_version,
            write_set_change_index: raw.write_set_change_index,
            storage_id: raw.storage_id,
            owner_address: raw.owner_address,
            asset_type: raw.asset_type,
            is_primary: raw.is_primary,
            is_frozen: raw.is_frozen,
            amount: raw.amount.to_string(),
            block_timestamp: raw.transaction_timestamp,
            token_standard: raw.token_standard,
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCurrentFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: String, // it is a string representation of the u128
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl NamedTable for ParquetCurrentFungibleAssetBalance {
    const TABLE_NAME: &'static str = "current_fungible_asset_balances_legacy";
}

impl HasVersion for ParquetCurrentFungibleAssetBalance {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}
/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<String>, // it is a string representation of the u128
    pub amount_v2: Option<String>, // it is a string representation of the u128
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    #[allocative(skip)]
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    #[allocative(skip)]
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

impl NamedTable for ParquetCurrentUnifiedFungibleAssetBalance {
    const TABLE_NAME: &'static str = "current_fungible_asset_balances";
}

/// This will be deprecated.
impl HasVersion for ParquetCurrentUnifiedFungibleAssetBalance {
    fn version(&self) -> i64 {
        -1
    }
}

impl From<CurrentUnifiedFungibleAssetBalance> for ParquetCurrentUnifiedFungibleAssetBalance {
    fn from(raw: CurrentUnifiedFungibleAssetBalance) -> Self {
        Self {
            storage_id: raw.storage_id,
            owner_address: raw.owner_address,
            asset_type_v1: raw.asset_type_v1,
            asset_type_v2: raw.asset_type_v2,
            is_primary: raw.is_primary,
            is_frozen: raw.is_frozen,
            amount_v1: raw.amount_v1.map(|x| x.to_string()),
            amount_v2: raw.amount_v2.map(|x| x.to_string()),
            last_transaction_version_v1: raw.last_transaction_version_v1,
            last_transaction_version_v2: raw.last_transaction_version_v2,
            last_transaction_timestamp_v1: raw.last_transaction_timestamp_v1,
            last_transaction_timestamp_v2: raw.last_transaction_timestamp_v2,
        }
    }
}

// Postgres Models

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = fungible_asset_balances)]
pub struct PostgresFungibleAssetBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl From<FungibleAssetBalance> for PostgresFungibleAssetBalance {
    fn from(raw: FungibleAssetBalance) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            write_set_change_index: raw.write_set_change_index,
            storage_id: raw.storage_id,
            owner_address: raw.owner_address,
            asset_type: raw.asset_type,
            is_primary: raw.is_primary,
            is_frozen: raw.is_frozen,
            amount: raw.amount,
            transaction_timestamp: raw.transaction_timestamp,
            token_standard: raw.token_standard,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances_legacy)]
pub struct PostgresCurrentFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Default)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances)]
pub struct PostgresCurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

impl From<CurrentUnifiedFungibleAssetBalance> for PostgresCurrentUnifiedFungibleAssetBalance {
    fn from(raw: CurrentUnifiedFungibleAssetBalance) -> Self {
        Self {
            storage_id: raw.storage_id,
            owner_address: raw.owner_address,
            asset_type_v1: raw.asset_type_v1,
            asset_type_v2: raw.asset_type_v2,
            is_primary: raw.is_primary,
            is_frozen: raw.is_frozen,
            amount_v1: raw.amount_v1,
            amount_v2: raw.amount_v2,
            last_transaction_version_v1: raw.last_transaction_version_v1,
            last_transaction_version_v2: raw.last_transaction_version_v2,
            last_transaction_timestamp_v1: raw.last_transaction_timestamp_v1,
            last_transaction_timestamp_v2: raw.last_transaction_timestamp_v2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processors::objects::v2_object_utils::ObjectAggregatedData;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
        MoveStructTag, WriteResource,
    };

    #[test]
    fn test_is_primary() {
        let owner_address = "0xfd2984f201abdbf30ccd0ec5c2f2357789222c0bbd3c68999acfebe188fdc09d";
        let metadata_address = "0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309";
        let fungible_store_address =
            "0x5d2c93f23a3964409e8755a179417c4ef842166f6cc41e1416e2c705a02861a6";

        assert!(FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_is_not_primary() {
        let owner_address = "0xfd2984f201abdbf30ccd0ec5c2f2357789222c0bbd3c68999acfebe188fdc09d";
        let metadata_address = "0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309";
        let fungible_store_address = "something random";

        assert!(!FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_zero_prefix() {
        let owner_address = "0x049cad43b33c9f907ff80c5f0897ac6bfe6034feea0c9070e37814d1f9efd090";
        let metadata_address = "0x03b0e839106b65826e54fa4c160ca653594b723a5e481a5121c333849bc46f6c";
        let fungible_store_address =
            "0xd4af0c43c6228357d7a09da77bf244cd4a1b97a0eb8ef3df43823ff4a807d0b9";

        assert!(FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_paired_metadata_address() {
        assert_eq!(
            get_paired_metadata_address("0x1::aptos_coin::AptosCoin"),
            *APT_METADATA_ADDRESS_HEX
        );
        assert_eq!(get_paired_metadata_address("0x66c34778730acbb120cefa57a3d98fd21e0c8b3a51e9baee530088b2e444e94c::moon_coin::MoonCoin"), "0xf772c28c069aa7e4417d85d771957eb3c5c11b5bf90b1965cda23b899ebc0384");
    }

    fn fa_store_write_resource(address: &str) -> WriteResource {
        WriteResource {
            address: address.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "fungible_asset".to_string(),
                name: "FungibleStore".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::fungible_asset::FungibleStore".to_string(),
            data: r#"{"metadata":{"inner":"0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309"},"balance":"100","frozen":false}"#
                .to_string(),
        }
    }

    fn non_store_write_resource(address: &str) -> WriteResource {
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

    fn object_data_with_owner(owner: &str) -> ObjectAggregatedData {
        let mut object_data = ObjectAggregatedData::default();
        object_data.object.object_core = serde_json::from_str(&format!(
            r#"{{"allow_ungated_transfer":true,"guid_creation_num":"0","owner":"{owner}"}}"#
        ))
        .expect("valid ObjectCore JSON");
        object_data
    }

    /// parse_v2_coin Loop 5: `Ok(None)` skips the balance row and the
    /// extractor still returns `Ok`, so VersionTrackerStep advances.
    /// `Err` hits `unwrap_or_else` + panic and the checkpoint does not.
    fn parse_v2_coin_would_advance_checkpoint(
        write_result: &anyhow::Result<Option<FungibleAssetBalance>>,
    ) -> bool {
        write_result.is_ok()
    }

    #[test]
    fn missing_object_core_is_not_ok_none_skip() {
        let result = require_object_core_for_fa_balance(None::<()>, 42, "0xstore");
        assert!(
            result.is_err(),
            "missing ObjectCore after FungibleStore is identified must fail the write, not skip"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for FA balance"),
            "error should name the FA balance ObjectCore lookup, got: {message}"
        );
        assert!(
            message.contains("0xstore"),
            "error should include storage_id, got: {message}"
        );
    }

    /// `get_v2_from_write_resource` used to map a missing ObjectCore to
    /// `Ok(None)`. That is the same success signal as "this write resource
    /// is not an FA balance", so `parse_v2_coin` still returned the batch and
    /// `FungibleAssetExtractor` let `VersionTrackerStep` advance the
    /// checkpoint while `fungible_asset_balances` never received the
    /// confirmed FungibleStore write.
    ///
    /// The write path now uses `require_object_core_for_fa_balance`: only
    /// `FungibleAssetStore::from_write_resource` returning `Ok(None)`
    /// skips; a hole after FungibleStore is identified is `Err`.
    #[test]
    fn write_path_does_not_map_missing_object_core_to_ok_none() {
        let write_resource = fa_store_write_resource(
            "0x00000000000000000000000000000000000000000000000000000000000000aa",
        );
        let empty_object_metadatas = AHashMap::new();
        let write_result = FungibleAssetBalance::get_v2_from_write_resource(
            &write_resource,
            0,
            99,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        );
        assert!(
            write_result.is_err(),
            "missing ObjectCore must fail the write, not skip the balance row: {write_result:?}"
        );
        let message = write_result.as_ref().unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for FA balance"),
            "write-path error should name the ObjectCore hole, got: {message}"
        );
        assert!(
            !parse_v2_coin_would_advance_checkpoint(&write_result),
            "Err from a confirmed FungibleStore write must not advance the checkpoint"
        );
    }

    #[test]
    fn non_store_write_is_intentional_ok_none() {
        let write_resource = non_store_write_resource("0x1");
        let empty_object_metadatas = AHashMap::new();
        let result = FungibleAssetBalance::get_v2_from_write_resource(
            &write_resource,
            0,
            1,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        )
        .unwrap();
        assert!(
            result.is_none(),
            "a non-FungibleStore write resource is intentional absence"
        );
        assert!(
            parse_v2_coin_would_advance_checkpoint(&Ok(None)),
            "intentional absence remains Ok(None) and the checkpoint still advances"
        );
    }

    #[test]
    fn resolved_object_core_is_kept() {
        let metadata =
            require_object_core_for_fa_balance(Some("object-core"), 1, "0xstore").unwrap();
        assert_eq!(metadata, "object-core");

        let address = "0x5d2c93f23a3964409e8755a179417c4ef842166f6cc41e1416e2c705a02861a6";
        let owner = "0xfd2984f201abdbf30ccd0ec5c2f2357789222c0bbd3c68999acfebe188fdc09d";
        let write_resource = fa_store_write_resource(address);
        let mut object_metadatas = AHashMap::new();
        object_metadatas.insert(standardize_address(address), object_data_with_owner(owner));
        let balance = FungibleAssetBalance::get_v2_from_write_resource(
            &write_resource,
            1,
            7,
            chrono::NaiveDateTime::default(),
            &object_metadatas,
        )
        .unwrap()
        .expect("FungibleStore plus ObjectCore must persist the balance");
        assert_eq!(balance.owner_address, owner);
        assert_eq!(
            balance.asset_type,
            "0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309"
        );
        assert_eq!(balance.amount, BigDecimal::from(100));
        assert!(!balance.is_frozen);
        assert!(balance.is_primary);
        assert_eq!(balance.transaction_version, 7);
        assert_eq!(balance.token_standard, "v2");
        assert!(
            parse_v2_coin_would_advance_checkpoint(&Ok(Some(balance))),
            "a persisted FA balance row still advances the checkpoint"
        );
    }
}
