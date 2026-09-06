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
            token_models::{
                token_utils::{CollectionDataIdType, TokenWriteSet},
                tokens::TableHandleToOwner,
            },
            token_v2_models::v2_token_utils::{Collection, TokenStandard},
        },
    },
    schema::{collections_v2, current_collections_v2},
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{WriteResource, WriteTableItem},
    postgres::utils::database::{DbContext, DbPoolConnection},
    utils::convert::standardize_address,
};
use bigdecimal::{BigDecimal, Zero};
use diesel::{prelude::*, sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// PK of current_collections_v2, i.e. collection_id
pub type CurrentCollectionV2PK = String;

pub const DEFAULT_CREATOR_ADDRESS: &str = "unknown";

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = collections_v2)]
pub struct CollectionV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub collection_properties: Option<serde_json::Value>,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(collection_id))]
#[diesel(table_name = current_collections_v2)]
pub struct CurrentCollectionV2 {
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub token_standard: String,
    pub collection_properties: Option<serde_json::Value>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, QueryableByName)]
pub struct CreatorFromCollectionTableV1 {
    #[diesel(sql_type = Text)]
    pub creator_address: String,
}

impl CollectionV2 {
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        if let Some(inner) = Collection::from_write_resource(write_resource)? {
            let (mut current_supply, mut max_supply, mut total_minted_v2) =
                (BigDecimal::zero(), None, None);
            let (mut mutable_description, mut mutable_uri) = (None, None);
            let mut collection_properties = serde_json::Value::Null;
            let address = standardize_address(&write_resource.address);
            // Collection was identified. Missing ObjectCore is not "this write
            // resource is not a collection" — that is `Collection::from_write_resource`
            // returning `Ok(None)`. The old path mapped the hole to `Ok(None)`
            // so `parse_v2_token` skipped collections_v2 / current_collections_v2
            // while TokenV2Extractor still succeeded and VersionTrackerStep
            // advanced the checkpoint.
            let object_data = require_object_core_for_collection(
                object_metadatas.get(&address),
                txn_version,
                &address,
            )?;
            // Getting supply data (prefer fixed supply over unlimited supply although they should never appear at the same time anyway)
            let fixed_supply = object_data.fixed_supply.as_ref();
            let unlimited_supply = object_data.unlimited_supply.as_ref();
            if let Some(supply) = unlimited_supply {
                (current_supply, max_supply, total_minted_v2) = (
                    supply.current_supply.clone(),
                    None,
                    Some(supply.total_minted.clone()),
                );
            }
            if let Some(supply) = fixed_supply {
                (current_supply, max_supply, total_minted_v2) = (
                    supply.current_supply.clone(),
                    Some(supply.max_supply.clone()),
                    Some(supply.total_minted.clone()),
                );
            }

            // Aggregator V2 enables a separate struct for supply
            let concurrent_supply = object_data.concurrent_supply.as_ref();
            if let Some(supply) = concurrent_supply {
                (current_supply, max_supply, total_minted_v2) = (
                    supply.current_supply.value.clone(),
                    if supply.current_supply.max_value == u64::MAX.into() {
                        None
                    } else {
                        Some(supply.current_supply.max_value.clone())
                    },
                    Some(supply.total_minted.value.clone()),
                );
            }

            // Getting collection mutability config from AptosCollection
            let collection = object_data.aptos_collection.as_ref();
            if let Some(collection) = collection {
                mutable_description = Some(collection.mutable_description);
                mutable_uri = Some(collection.mutable_uri);
            }

            collection_properties = object_data
                .property_map
                .as_ref()
                .map(|m| m.inner.clone())
                .unwrap_or(collection_properties);

            let collection_id = address;
            let creator_address = inner.get_creator_address();
            let collection_name = inner.get_name_trunc();
            let description = inner.description.clone();
            let uri = inner.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: creator_address.clone(),
                    collection_name: collection_name.clone(),
                    description: description.clone(),
                    uri: uri.clone(),
                    current_supply: current_supply.clone(),
                    max_supply: max_supply.clone(),
                    total_minted_v2: total_minted_v2.clone(),
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    collection_properties: Some(collection_properties.clone()),
                    token_standard: TokenStandard::V2.to_string(),
                    transaction_timestamp: txn_timestamp,
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address,
                    collection_name,
                    description,
                    uri,
                    current_supply,
                    max_supply,
                    total_minted_v2,
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                    collection_properties: Some(collection_properties),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        db_context: &mut Option<DbContext<'_>>,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_collection_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::CollectionData(inner)) => Some(inner),
            _ => None,
        };
        if let Some(collection_data) = maybe_collection_data {
            let table_handle = table_item.handle.to_string();
            let maybe_creator_address = table_handle_to_owner
                .get(&standardize_address(&table_handle))
                .map(|table_metadata| table_metadata.get_owner_address());
            let mut creator_address = match maybe_creator_address {
                Some(ca) => ca,
                None => match db_context {
                    None => {
                        tracing::debug!(
                            transaction_version = txn_version,
                            lookup_key = &table_handle,
                            "Avoiding db lookup for Parquet."
                        );
                        DEFAULT_CREATOR_ADDRESS.to_string()
                    },
                    Some(db_context) => {
                        match Self::get_collection_creator_for_v1(
                            &mut db_context.conn,
                            &table_handle,
                            db_context.query_retries,
                            db_context.query_retry_delay_ms,
                        )
                        .await
                        {
                            Ok(ca) => ca,
                            Err(_) => {
                                tracing::warn!(
                                        transaction_version = txn_version,
                                        lookup_key = &table_handle,
                                        "Failed to get collection creator for table handle {table_handle}, txn version {txn_version}. You probably should backfill db."
                                    );
                                return Ok(None);
                            },
                        }
                    },
                },
            };
            creator_address = standardize_address(&creator_address);
            let collection_id_struct =
                CollectionDataIdType::new(creator_address, collection_data.get_name().to_string());
            let collection_id = collection_id_struct.to_id();
            let collection_name = collection_data.get_name_trunc();
            let uri = collection_data.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: collection_id_struct.creator.clone(),
                    collection_name: collection_name.clone(),
                    description: collection_data.description.clone(),
                    uri: uri.clone(),
                    current_supply: collection_data.supply.clone(),
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle.clone()),
                    token_standard: TokenStandard::V1.to_string(),
                    transaction_timestamp: txn_timestamp,
                    collection_properties: Some(serde_json::Value::Null),
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address: collection_id_struct.creator,
                    collection_name,
                    description: collection_data.description,
                    uri,
                    current_supply: collection_data.supply,
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle),
                    token_standard: TokenStandard::V1.to_string(),
                    collection_properties: Some(serde_json::Value::Null),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// If collection data is not in resources of the same transaction, then try looking for it in the database. Since collection owner
    /// cannot change, we can just look in the current_collection_datas table.
    /// Retrying a few times since this collection could've been written in a separate thread.
    async fn get_collection_creator_for_v1(
        conn: &mut DbPoolConnection<'_>,
        table_handle: &str,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<String> {
        let mut tried = 0;
        while tried < query_retries {
            tried += 1;
            match Self::get_by_table_handle(conn, table_handle).await {
                Ok(creator) => return Ok(creator),
                Err(_) => {
                    if tried < query_retries {
                        tokio::time::sleep(std::time::Duration::from_millis(query_retry_delay_ms))
                            .await;
                    }
                },
            }
        }
        Err(anyhow::anyhow!("Failed to get collection creator"))
    }

    /// TODO: Change this to a KV store
    async fn get_by_table_handle(
        conn: &mut DbPoolConnection<'_>,
        table_handle: &str,
    ) -> anyhow::Result<String> {
        let mut res: Vec<Option<CreatorFromCollectionTableV1>> = sql_query(
            "SELECT creator_address FROM current_collections_v2 WHERE table_handle_v1 = $1",
        )
        .bind::<Text, _>(table_handle)
        .get_results(conn)
        .await?;
        Ok(res
            .pop()
            .context("collection result empty")?
            .context("collection result null")?
            .creator_address)
    }
}

/// After a `0x4::collection::Collection` write resource is identified,
/// ObjectCore must be present in `object_metadatas` (same ObjectGroup).
///
/// * `Ok(metadata)` — persist collections_v2 / current_collections_v2
/// * `Err(_)` — propagate so `parse_v2_token` fails (`.unwrap()` on main;
///   `?` once #36/#37 land) and TokenV2Extractor does not succeed. The
///   checkpoint does not advance.
///
/// The old write path mapped a missing ObjectCore onto `Ok(None)`, which is
/// the same success signal as "this write resource is not CollectionV2".
pub(crate) fn require_object_core_for_collection<T>(
    object_metadata: Option<T>,
    txn_version: i64,
    collection_id: &str,
) -> anyhow::Result<T> {
    object_metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "ObjectCore missing for CollectionV2 collection_id {collection_id}, txn version {txn_version}"
        )
    })
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCollectionV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: String,          // BigDecimal
    pub max_supply: Option<String>,      // BigDecimal
    pub total_minted_v2: Option<String>, // BigDecimal
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub collection_properties: Option<String>, // json
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetCollectionV2 {
    const TABLE_NAME: &'static str = "collections_v2";
}

impl HasVersion for ParquetCollectionV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}
impl From<CollectionV2> for ParquetCollectionV2 {
    fn from(collection: CollectionV2) -> Self {
        ParquetCollectionV2 {
            txn_version: collection.transaction_version,
            write_set_change_index: collection.write_set_change_index,
            collection_id: collection.collection_id,
            creator_address: collection.creator_address,
            collection_name: collection.collection_name,
            description: collection.description,
            uri: collection.uri,
            current_supply: collection.current_supply.to_string(),
            max_supply: collection.max_supply.map(|v| v.to_string()),
            total_minted_v2: collection.total_minted_v2.map(|v| v.to_string()),
            mutable_description: collection.mutable_description,
            mutable_uri: collection.mutable_uri,
            table_handle_v1: collection.table_handle_v1,
            collection_properties: collection
                .collection_properties
                .map(|v| serde_json::to_string(&v).unwrap()),
            token_standard: collection.token_standard,
            block_timestamp: collection.transaction_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{require_object_core_for_collection, CollectionV2};
    use crate::processors::objects::v2_object_utils::ObjectAggregatedData;
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::{
        aptos_protos::transaction::v1::{MoveStructTag, WriteResource},
        utils::convert::standardize_address,
    };

    fn collection_write_resource(address: &str) -> WriteResource {
        WriteResource {
            address: address.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x4".to_string(),
                module: "collection".to_string(),
                name: "Collection".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x4::collection::Collection".to_string(),
            data: r#"{"creator":"0xabc","description":"d","name":"n","uri":"u"}"#.to_string(),
        }
    }

    fn non_collection_write_resource(address: &str) -> WriteResource {
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
        let result = require_object_core_for_collection(None::<()>, 42, "0xcol");
        assert!(
            result.is_err(),
            "missing ObjectCore after Collection is identified must fail the write, not skip"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for CollectionV2"),
            "error should name the CollectionV2 ObjectCore lookup, got: {message}"
        );
        assert!(
            message.contains("0xcol"),
            "error should include collection_id, got: {message}"
        );
    }

    /// `get_v2_from_write_resource` used to map a missing ObjectCore to
    /// `Ok(None)`. That is the same success signal as "this write resource
    /// is not CollectionV2", so `parse_v2_token` still returned the batch and
    /// `TokenV2Extractor` let `VersionTrackerStep` advance the checkpoint
    /// while `collections_v2` / `current_collections_v2` never received
    /// the confirmed Collection write.
    ///
    /// The write path now uses `require_object_core_for_collection`: only
    /// `Collection::from_write_resource` returning `Ok(None)` skips; a hole
    /// after Collection is identified is `Err`.
    #[test]
    fn write_path_does_not_map_missing_object_core_to_ok_none() {
        let write_resource = collection_write_resource(
            "0x00000000000000000000000000000000000000000000000000000000000000aa",
        );
        let empty_object_metadatas = AHashMap::new();
        let write_result = CollectionV2::get_v2_from_write_resource(
            &write_resource,
            99,
            0,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        );
        assert!(
            write_result.is_err(),
            "missing ObjectCore must fail the write, not skip the collection row: {write_result:?}"
        );
        let message = write_result.unwrap_err().to_string();
        assert!(
            message.contains("ObjectCore missing for CollectionV2"),
            "write-path error should name the ObjectCore hole, got: {message}"
        );
    }

    #[test]
    fn non_collection_write_is_intentional_ok_none() {
        let write_resource = non_collection_write_resource("0x1");
        let empty_object_metadatas = AHashMap::new();
        let result = CollectionV2::get_v2_from_write_resource(
            &write_resource,
            1,
            0,
            chrono::NaiveDateTime::default(),
            &empty_object_metadatas,
        )
        .unwrap();
        assert!(
            result.is_none(),
            "a non-Collection write resource is intentional absence"
        );
    }

    #[test]
    fn resolved_object_core_is_kept() {
        let metadata = require_object_core_for_collection(Some("object-core"), 1, "0xcol").unwrap();
        assert_eq!(metadata, "object-core");

        let address = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let write_resource = collection_write_resource(address);
        let mut object_metadatas = AHashMap::new();
        object_metadatas.insert(
            standardize_address(address),
            ObjectAggregatedData::default(),
        );
        let (collection, current) = CollectionV2::get_v2_from_write_resource(
            &write_resource,
            7,
            1,
            chrono::NaiveDateTime::default(),
            &object_metadatas,
        )
        .unwrap()
        .expect("Collection plus ObjectCore must persist collection");
        assert_eq!(collection.collection_name, "n");
        assert_eq!(collection.creator_address, standardize_address("0xabc"));
        assert_eq!(current.last_transaction_version, 7);
        assert_eq!(current.token_standard, "v2");
    }
}
