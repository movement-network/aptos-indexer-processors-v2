// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_object_utils::{CurrentObjectPK, ObjectAggregatedDataMapping};
use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::default::models::move_resources::MoveResource,
    schema::{current_objects, objects},
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteResource, WriteResource},
    postgres::utils::database::{DbContext, DbPoolConnection},
    utils::convert::standardize_address,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::error;

fn is_current_object_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<diesel::result::Error>() == Some(&diesel::result::Error::NotFound)
}

const DELETED_RESOURCE_OWNER_ADDRESS: &str = "Unknown";

#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct Object {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}
#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct CurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct CurrentObjectQuery {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
    pub untransferrable: bool,
}

impl Object {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_metadata_mapping: &ObjectAggregatedDataMapping,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        let address = standardize_address(&write_resource.address.to_string());
        if let Some(object_aggregated_metadata) = object_metadata_mapping.get(&address) {
            // do something
            let object_with_metadata = object_aggregated_metadata.object.clone();
            let object_core = object_with_metadata.object_core;

            let untransferrable = if object_aggregated_metadata.untransferable.as_ref().is_some() {
                true
            } else {
                !object_core.allow_ungated_transfer
            };
            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    object_address: address.clone(),
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash.clone(),
                    guid_creation_num: object_core.guid_creation_num.clone(),
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    is_deleted: false,
                    untransferrable,
                    block_timestamp,
                },
                CurrentObject {
                    object_address: address,
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash,
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    last_guid_creation_num: object_core.guid_creation_num.clone(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    untransferrable,
                    block_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where the entire object is deleted
    /// TODO: We need to detect if an object is only partially deleted
    /// using KV store
    pub async fn from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_mapping: &AHashMap<CurrentObjectPK, CurrentObject>,
        db_context: &mut Option<DbContext<'_>>,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        if delete_resource.type_str != "0x1::object::ObjectGroup" {
            return Ok(None);
        }

        let resource = match MoveResource::from_delete_resource(
            delete_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
            block_timestamp,
        ) {
            Ok(Some(resource)) => resource,
            Ok(None) => {
                error!("No resource found for transaction version {}", txn_version);
                return Ok(None);
            },
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error getting resource from delete resource: {}",
                    e
                ))
            },
        };

        // ObjectGroup DeleteResource is authoritative: persist is_deleted=true even
        // when current_objects is missing (backfill hole). Transient DB errors are
        // returned so the extractor fails and the checkpoint does not advance.
        let previous_object = Self::lookup_previous_object(
            &resource.resource_address,
            object_mapping,
            db_context,
            txn_version,
        )
        .await?;

        Ok(Some(Self::deleted_object_pair(
            &resource,
            previous_object.as_ref(),
            txn_version,
            write_set_change_index,
            block_timestamp,
        )))
    }

    /// In-batch map first, then current_objects. `None` means "use fallback owner"
    /// (parquet / missing row), not "drop the delete".
    async fn lookup_previous_object(
        object_address: &str,
        object_mapping: &AHashMap<CurrentObjectPK, CurrentObject>,
        db_context: &mut Option<DbContext<'_>>,
        txn_version: i64,
    ) -> anyhow::Result<Option<CurrentObject>> {
        if let Some(object) = object_mapping.get(object_address) {
            return Ok(Some(object.clone()));
        }
        let Some(db_context) = db_context else {
            return Ok(None);
        };
        Self::previous_object_from_lookup_result(
            Self::get_current_object(
                &mut db_context.conn,
                object_address,
                db_context.query_retries,
                db_context.query_retry_delay_ms,
            )
            .await,
            object_address,
            txn_version,
        )
    }

    fn previous_object_from_lookup_result(
        result: anyhow::Result<CurrentObject>,
        object_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<CurrentObject>> {
        match result {
            Ok(object) => Ok(Some(object)),
            Err(e) if is_current_object_not_found(&e) => {
                tracing::error!(
                    transaction_version = txn_version,
                    lookup_key = object_address,
                    "Missing current_object for object_address: {object_address}. Persisting ObjectGroup delete with fallback owner so the delete is not dropped.",
                );
                Ok(None)
            },
            Err(e) => Err(anyhow::anyhow!(
                "Failed to look up current_object for {object_address} at version {txn_version}: {e:#}"
            )),
        }
    }

    fn deleted_object_pair(
        resource: &MoveResource,
        previous: Option<&CurrentObject>,
        txn_version: i64,
        write_set_change_index: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentObject) {
        let (owner_address, guid_creation_num, allow_ungated_transfer, untransferrable) =
            match previous {
                Some(prev) => (
                    prev.owner_address.clone(),
                    prev.last_guid_creation_num.clone(),
                    prev.allow_ungated_transfer,
                    prev.untransferrable,
                ),
                None => (
                    DELETED_RESOURCE_OWNER_ADDRESS.to_string(),
                    BigDecimal::default(),
                    false,
                    false,
                ),
            };

        (
            Self {
                transaction_version: txn_version,
                write_set_change_index,
                object_address: resource.resource_address.clone(),
                owner_address: owner_address.clone(),
                state_key_hash: resource.state_key_hash.clone(),
                guid_creation_num: guid_creation_num.clone(),
                allow_ungated_transfer,
                is_deleted: true,
                untransferrable,
                block_timestamp,
            },
            CurrentObject {
                object_address: resource.resource_address.clone(),
                owner_address,
                state_key_hash: resource.state_key_hash.clone(),
                last_guid_creation_num: guid_creation_num,
                allow_ungated_transfer,
                last_transaction_version: txn_version,
                is_deleted: true,
                untransferrable,
                block_timestamp,
            },
        )
    }

    /// This is actually not great because object owner can change. The best we can do now though.
    pub async fn get_current_object(
        conn: &mut DbPoolConnection<'_>,
        object_address: &str,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<CurrentObject> {
        let mut last_error = None;
        let mut tried = 0;
        while tried < query_retries {
            tried += 1;
            match CurrentObjectQuery::get_by_address(object_address, conn).await {
                Ok(res) => {
                    return Ok(CurrentObject {
                        object_address: res.object_address,
                        owner_address: res.owner_address,
                        state_key_hash: res.state_key_hash,
                        allow_ungated_transfer: res.allow_ungated_transfer,
                        last_guid_creation_num: res.last_guid_creation_num,
                        last_transaction_version: res.last_transaction_version,
                        is_deleted: res.is_deleted,
                        untransferrable: res.untransferrable,
                        block_timestamp: chrono::NaiveDateTime::default(), // this won't be used
                    });
                },
                Err(e) => {
                    last_error = Some(e);
                    if tried < query_retries {
                        tokio::time::sleep(std::time::Duration::from_millis(query_retry_delay_ms))
                            .await;
                    }
                },
            }
        }
        Err(last_error
            .map(Into::into)
            .unwrap_or_else(|| anyhow::anyhow!("Failed to get object owner")))
    }
}

impl CurrentObjectQuery {
    /// TODO: Change this to a KV store
    pub async fn get_by_address(
        object_address: &str,
        conn: &mut DbPoolConnection<'_>,
    ) -> diesel::QueryResult<Self> {
        current_objects::table
            .filter(current_objects::object_address.eq(object_address))
            .first::<Self>(conn)
            .await
    }
}

/// Parquet
///
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetObject {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: u64, //  BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetObject {
    const TABLE_NAME: &'static str = "objects";
}

impl HasVersion for ParquetObject {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<Object> for ParquetObject {
    fn from(base_item: Object) -> Self {
        Self {
            txn_version: base_item.transaction_version,
            write_set_change_index: base_item.write_set_change_index,
            object_address: base_item.object_address,
            owner_address: base_item.owner_address,
            state_key_hash: base_item.state_key_hash,
            guid_creation_num: base_item.guid_creation_num.to_u64().unwrap(),
            allow_ungated_transfer: base_item.allow_ungated_transfer,
            is_deleted: base_item.is_deleted,
            untransferrable: base_item.untransferrable,
            block_timestamp: base_item.block_timestamp,
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: u64, //  BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetCurrentObject {
    const TABLE_NAME: &'static str = "objects";
}

impl HasVersion for ParquetCurrentObject {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentObject> for ParquetCurrentObject {
    fn from(base_item: CurrentObject) -> Self {
        Self {
            object_address: base_item.object_address,
            owner_address: base_item.owner_address,
            state_key_hash: base_item.state_key_hash,
            allow_ungated_transfer: base_item.allow_ungated_transfer,
            last_guid_creation_num: base_item.last_guid_creation_num.to_u64().unwrap(),
            last_transaction_version: base_item.last_transaction_version,
            is_deleted: base_item.is_deleted,
            untransferrable: base_item.untransferrable,
            block_timestamp: base_item.block_timestamp,
        }
    }
}

/// Postgres models
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = objects)]
pub struct PostgresObject {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
}

impl From<Object> for PostgresObject {
    fn from(base_item: Object) -> Self {
        Self {
            transaction_version: base_item.transaction_version,
            write_set_change_index: base_item.write_set_change_index,
            object_address: base_item.object_address,
            owner_address: base_item.owner_address,
            state_key_hash: base_item.state_key_hash,
            guid_creation_num: base_item.guid_creation_num,
            allow_ungated_transfer: base_item.allow_ungated_transfer,
            is_deleted: base_item.is_deleted,
            untransferrable: base_item.untransferrable,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct PostgresCurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
}

impl From<CurrentObject> for PostgresCurrentObject {
    fn from(raw: CurrentObject) -> Self {
        Self {
            object_address: raw.object_address,
            owner_address: raw.owner_address,
            state_key_hash: raw.state_key_hash,
            allow_ungated_transfer: raw.allow_ungated_transfer,
            last_guid_creation_num: raw.last_guid_creation_num,
            last_transaction_version: raw.last_transaction_version,
            is_deleted: raw.is_deleted,
            untransferrable: raw.untransferrable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::MoveStructTag;
    use chrono::NaiveDateTime;

    fn object_group_delete(address: &str) -> DeleteResource {
        DeleteResource {
            address: address.to_string(),
            state_key_hash: hex::decode(
                "d03f63d32c154a084e982a5fe910ef8e5d692d4c9e9380eb96192335adf32b83",
            )
            .unwrap(),
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "object".to_string(),
                name: "ObjectGroup".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::object::ObjectGroup".to_string(),
        }
    }

    fn timestamp() -> NaiveDateTime {
        NaiveDateTime::default()
    }

    fn sample_current_object(address: &str, owner: &str) -> CurrentObject {
        CurrentObject {
            object_address: address.to_string(),
            owner_address: owner.to_string(),
            state_key_hash: "0xabc".to_string(),
            allow_ungated_transfer: true,
            last_guid_creation_num: BigDecimal::from(7),
            last_transaction_version: 1,
            is_deleted: false,
            untransferrable: true,
            block_timestamp: timestamp(),
        }
    }

    #[tokio::test]
    async fn object_group_delete_without_prior_row_is_persisted() {
        let delete = object_group_delete(
            "0x1ac3cb52493947623cd727e2db9e4cfd828d5f9cd264920d253828276a5e314e",
        );
        let result =
            Object::from_delete_resource(&delete, 42, 7, &AHashMap::new(), &mut None, timestamp())
                .await
                .unwrap();

        let (object, current) = result.expect("ObjectGroup delete must not be skipped");
        assert!(object.is_deleted);
        assert!(current.is_deleted);
        assert_eq!(object.owner_address, DELETED_RESOURCE_OWNER_ADDRESS);
        assert_eq!(current.owner_address, DELETED_RESOURCE_OWNER_ADDRESS);
        assert_eq!(object.transaction_version, 42);
        assert_eq!(current.last_transaction_version, 42);
        assert_eq!(
            object.object_address,
            "0x1ac3cb52493947623cd727e2db9e4cfd828d5f9cd264920d253828276a5e314e"
        );
    }

    #[tokio::test]
    async fn object_group_delete_uses_in_batch_previous_object() {
        let address = "0x1ac3cb52493947623cd727e2db9e4cfd828d5f9cd264920d253828276a5e314e";
        let owner = "0x3fda2b751a0d209e17069ae72ccde256efeaad39e5403ea0e65ef5dcebdf6763";
        let delete = object_group_delete(address);
        let mut mapping = AHashMap::new();
        mapping.insert(address.to_string(), sample_current_object(address, owner));

        let (object, current) =
            Object::from_delete_resource(&delete, 99, 1, &mapping, &mut None, timestamp())
                .await
                .unwrap()
                .expect("mapped ObjectGroup delete must be persisted");

        assert!(object.is_deleted);
        assert!(current.is_deleted);
        assert_eq!(object.owner_address, owner);
        assert_eq!(current.owner_address, owner);
        assert_eq!(object.guid_creation_num, BigDecimal::from(7));
        assert!(object.allow_ungated_transfer);
        assert!(object.untransferrable);
    }

    #[tokio::test]
    async fn non_object_group_delete_is_ignored() {
        let mut delete = object_group_delete(
            "0x1ac3cb52493947623cd727e2db9e4cfd828d5f9cd264920d253828276a5e314e",
        );
        delete.type_str = "0x1::coin::CoinStore".to_string();

        let result =
            Object::from_delete_resource(&delete, 1, 0, &AHashMap::new(), &mut None, timestamp())
                .await
                .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn missing_current_object_row_does_not_drop_delete() {
        let err = anyhow::Error::from(diesel::result::Error::NotFound);
        let previous = Object::previous_object_from_lookup_result(Err(err), "0xabc", 10).unwrap();
        assert!(
            previous.is_none(),
            "NotFound must yield fallback previous object, not skip"
        );
    }

    #[test]
    fn transient_lookup_error_fails_the_batch() {
        let err = anyhow::anyhow!("connection reset");
        let result = Object::previous_object_from_lookup_result(Err(err), "0xabc", 10);
        assert!(
            result.is_err(),
            "transient DB errors must fail the batch so the checkpoint does not advance"
        );
    }

    #[test]
    fn deleted_object_pair_marks_deleted_with_fallback_owner() {
        let resource = MoveResource {
            txn_version: 5,
            write_set_change_index: 0,
            block_height: 0,
            fun: "ObjectGroup".to_string(),
            resource_type: "0x1::object::ObjectGroup".to_string(),
            resource_address: "0xabc".to_string(),
            module: "object".to_string(),
            generic_type_params: None,
            data: None,
            is_deleted: true,
            state_key_hash: "0xdef".to_string(),
            block_timestamp: timestamp(),
        };
        let (object, current) = Object::deleted_object_pair(&resource, None, 5, 2, timestamp());
        assert!(object.is_deleted);
        assert!(current.is_deleted);
        assert_eq!(object.owner_address, DELETED_RESOURCE_OWNER_ADDRESS);
        assert_eq!(current.last_transaction_version, 5);
    }
}
