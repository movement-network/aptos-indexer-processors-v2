// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::token_v2::{
        token_models::{
            token_utils::TokenWriteSet,
            tokens::{TableHandleToOwner, TokenV1WithdrawModuleEvents},
        },
        token_v2_models::v2_token_activities::TokenActivityHelperV1,
    },
    schema::current_token_pending_claims,
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem},
    utils::convert::standardize_address,
};
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// Map to keep track of the metadata of token offers that were claimed. The key is the token data id of the offer.
pub type TokenV1Claimed = AHashMap<String, TokenActivityHelperV1>;

// Map to keep track of the metadata of token offers that were canceled. The key is the token data id of the offer.
pub type TokenV1Canceled = AHashMap<String, TokenActivityHelperV1>;

// Map to keep track of TokenOffer / Offer events so a later WriteTableItem can recover
// the offerer when PendingClaims is not rewritten in the same transaction.
pub type TokenV1Offered = AHashMap<String, TokenActivityHelperV1>;

/// Resolve the offerer (`from_address`) for a pending-claim table item.
///
/// PendingClaims is a table inside the offerer's account. Adding another offer
/// writes a table item but does **not** rewrite the parent resource, so
/// `table_handle_to_owner` is empty for every offer after the first. Without a
/// fallback the write is dropped and the processor still checkpoints.
pub(crate) fn resolve_pending_claim_from_address(
    table_handle: &str,
    token_data_id: &str,
    table_handle_to_owner: &TableHandleToOwner,
    tokens_offered: &TokenV1Offered,
    tokens_withdrawn: &TokenV1WithdrawModuleEvents,
) -> Option<String> {
    if let Some(table_metadata) = table_handle_to_owner.get(table_handle) {
        return Some(table_metadata.get_owner_address());
    }
    if let Some(from) = tokens_offered
        .get(token_data_id)
        .and_then(|offered| offered.from_address.clone())
    {
        return Some(from);
    }
    tokens_withdrawn
        .get(token_data_id)
        .and_then(|withdrawn| withdrawn.from_address.clone())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: BigDecimal,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl Ord for CurrentTokenPendingClaim {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id_hash
            .cmp(&other.token_data_id_hash)
            .then(self.property_version.cmp(&other.property_version))
            .then(self.from_address.cmp(&other.from_address))
            .then(self.to_address.cmp(&other.to_address))
    }
}

impl PartialOrd for CurrentTokenPendingClaim {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentTokenPendingClaim {
    /// Token claim is stored in a table in the offerer's account. The key is token_offer_id (token_id + to address)
    /// and value is token (token_id + amount)
    pub fn from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        tokens_offered: &TokenV1Offered,
        tokens_withdrawn: &TokenV1WithdrawModuleEvents,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_offer = match TokenWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.key,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenOfferId(inner)) => Some(inner),
            _ => None,
        };
        if let Some(offer) = &maybe_offer {
            let maybe_token = match TokenWriteSet::from_table_item_type(
                table_item_data.value_type.as_str(),
                &table_item_data.value,
                txn_version,
            )? {
                Some(TokenWriteSet::Token(inner)) => Some(inner),
                _ => None,
            };
            if let Some(token) = &maybe_token {
                let table_handle = standardize_address(&table_item.handle.to_string());
                let token_id = offer.token_id.clone();
                let token_data_id_struct = token_id.token_data_id;
                let token_data_id = token_data_id_struct.to_id();

                // PendingClaims is not rewritten on subsequent offers, so the
                // in-batch table-handle map is often empty. Fall back to the
                // offer / withdraw events from this transaction.
                let maybe_owner_address = resolve_pending_claim_from_address(
                    &table_handle,
                    &token_data_id,
                    table_handle_to_owner,
                    tokens_offered,
                    tokens_withdrawn,
                );

                if let Some(from_address) = maybe_owner_address {
                    let collection_data_id_hash =
                        token_data_id_struct.get_collection_data_id_hash();
                    let token_data_id_hash = token_data_id_struct.to_hash();
                    // Basically adding 0x prefix to the previous 2 lines. This is to be consistent with Token V2
                    let collection_id = token_data_id_struct.get_collection_id();
                    let collection_name = token_data_id_struct.get_collection_trunc();
                    let name = token_data_id_struct.get_name_trunc();

                    return Ok(Some(Self {
                        token_data_id_hash,
                        property_version: token_id.property_version,
                        from_address,
                        to_address: offer.get_to_address(),
                        collection_data_id_hash,
                        creator_address: token_data_id_struct.get_creator_address(),
                        collection_name,
                        name,
                        amount: token.amount.clone(),
                        table_handle,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        token_data_id,
                        collection_id,
                    }));
                } else {
                    tracing::warn!(
                        transaction_version = txn_version,
                        table_handle = table_handle,
                        token_data_id = token_data_id,
                        "Missing table handle metadata and offer/withdraw event for TokenClaim. {:?}",
                        table_handle_to_owner
                    );
                }
            } else {
                tracing::warn!(
                    transaction_version = txn_version,
                    value_type = table_item_data.value_type,
                    value = table_item_data.value,
                    "Expecting token as value for key = token_offer_id",
                );
            }
        }
        Ok(None)
    }

    pub fn from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        tokens_claimed: &TokenV1Claimed,
        tokens_canceled: &TokenV1Canceled,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_offer = match TokenWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.key,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenOfferId(inner)) => Some(inner),
            _ => None,
        };
        if let Some(offer) = &maybe_offer {
            let table_handle = standardize_address(&table_item.handle.to_string());
            let token_data_id = offer.token_id.token_data_id.to_id();

            // Try to find owner from write resources
            let mut maybe_owner_address = table_handle_to_owner
                .get(&table_handle)
                .map(|table_metadata| table_metadata.get_owner_address());

            // If table handle isn't in TableHandleToOwner, try to find owner from token v1 claim events
            if maybe_owner_address.is_none() {
                if let Some(token_claimed) = tokens_claimed.get(&token_data_id) {
                    maybe_owner_address = token_claimed.from_address.clone();
                }
                if let Some(token_canceled) = tokens_canceled.get(&token_data_id) {
                    maybe_owner_address = token_canceled.from_address.clone();
                }
            }

            let owner_address = maybe_owner_address.unwrap_or_else(|| {
                panic!(
                    "Missing table handle metadata for claim. \
                        Version: {txn_version}, table handle for PendingClaims: {table_handle}, all metadata: {table_handle_to_owner:?} \
                        Missing token data id in token claim event. \
                        token_data_id: {token_data_id}, all token claim events: {tokens_claimed:?}, all token cancel events: {tokens_canceled:?}"
                )
            });

            let token_id = offer.token_id.clone();
            let token_data_id_struct = token_id.token_data_id;
            let collection_data_id_hash = token_data_id_struct.get_collection_data_id_hash();
            let token_data_id_hash = token_data_id_struct.to_hash();
            // Basically adding 0x prefix to the previous 2 lines. This is to be consistent with Token V2
            let collection_id = token_data_id_struct.get_collection_id();
            let token_data_id = token_data_id_struct.to_id();
            let collection_name = token_data_id_struct.get_collection_trunc();
            let name = token_data_id_struct.get_name_trunc();

            return Ok(Some(Self {
                token_data_id_hash,
                property_version: token_id.property_version,
                from_address: owner_address,
                to_address: offer.get_to_address(),
                collection_data_id_hash,
                creator_address: token_data_id_struct.get_creator_address(),
                collection_name,
                name,
                amount: BigDecimal::zero(),
                table_handle,
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                token_data_id,
                collection_id,
            }));
        }
        Ok(None)
    }
}

/// This is a parquet version of CurrentTokenPendingClaim
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetCurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: u64,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: String, // String format of BigDecimal
    pub table_handle: String,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl NamedTable for ParquetCurrentTokenPendingClaim {
    const TABLE_NAME: &'static str = "current_token_pending_claims";
}

impl HasVersion for ParquetCurrentTokenPendingClaim {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentTokenPendingClaim> for ParquetCurrentTokenPendingClaim {
    fn from(raw_item: CurrentTokenPendingClaim) -> Self {
        Self {
            token_data_id_hash: raw_item.token_data_id_hash,
            property_version: raw_item
                .property_version
                .to_u64()
                .expect("Failed to convert property_version to u64"),
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            collection_data_id_hash: raw_item.collection_data_id_hash,
            creator_address: raw_item.creator_address,
            collection_name: raw_item.collection_name,
            name: raw_item.name,
            amount: raw_item.amount.to_string(), // (assuming amount is non-critical)
            table_handle: raw_item.table_handle,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
        }
    }
}

/// This is a postgres version of CurrentTokenPendingClaim
#[derive(
    Clone, Debug, Deserialize, Eq, FieldCount, Identifiable, Insertable, PartialEq, Serialize,
)]
#[diesel(primary_key(token_data_id_hash, property_version, from_address, to_address))]
#[diesel(table_name = current_token_pending_claims)]
pub struct PostgresCurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: BigDecimal,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: BigDecimal,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl Ord for PostgresCurrentTokenPendingClaim {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id_hash
            .cmp(&other.token_data_id_hash)
            .then(self.property_version.cmp(&other.property_version))
            .then(self.from_address.cmp(&other.from_address))
            .then(self.to_address.cmp(&other.to_address))
    }
}

impl PartialOrd for PostgresCurrentTokenPendingClaim {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<CurrentTokenPendingClaim> for PostgresCurrentTokenPendingClaim {
    fn from(raw_item: CurrentTokenPendingClaim) -> Self {
        Self {
            token_data_id_hash: raw_item.token_data_id_hash,
            property_version: raw_item.property_version,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            collection_data_id_hash: raw_item.collection_data_id_hash,
            creator_address: raw_item.creator_address,
            collection_name: raw_item.collection_name,
            name: raw_item.name,
            amount: raw_item.amount,
            table_handle: raw_item.table_handle,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processors::token_v2::token_models::token_utils::TokenDataIdType;
    use ahash::AHashMap;
    use bigdecimal::BigDecimal;

    fn token_data_id() -> TokenDataIdType {
        serde_json::from_value(serde_json::json!({
            "creator": "0x1",
            "collection": "col",
            "name": "tok",
        }))
        .unwrap()
    }

    fn activity(from: &str) -> TokenActivityHelperV1 {
        TokenActivityHelperV1 {
            token_data_id_struct: token_data_id(),
            property_version: BigDecimal::from(0),
            from_address: Some(from.to_string()),
            to_address: Some(
                "0x00000000000000000000000000000000000000000000000000000000000000bb".to_string(),
            ),
            token_amount: BigDecimal::from(1),
        }
    }

    #[test]
    fn missing_table_handle_without_events_skips() {
        let id = token_data_id().to_id();
        let owner = resolve_pending_claim_from_address(
            "0xabc",
            &id,
            &AHashMap::new(),
            &AHashMap::new(),
            &AHashMap::new(),
        );
        assert_eq!(owner, None);
    }

    #[test]
    fn offer_event_recovers_offerer_when_pending_claims_not_rewritten() {
        let id = token_data_id().to_id();
        let offerer = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let mut offered = AHashMap::new();
        offered.insert(id.clone(), activity(offerer));

        let owner = resolve_pending_claim_from_address(
            "0xabc",
            &id,
            &AHashMap::new(),
            &offered,
            &AHashMap::new(),
        );
        assert_eq!(owner.as_deref(), Some(offerer));
    }

    #[test]
    fn withdraw_event_recovers_offerer_when_offer_event_missing() {
        let id = token_data_id().to_id();
        let offerer = "0x00000000000000000000000000000000000000000000000000000000000000cc";
        let mut withdrawn = AHashMap::new();
        withdrawn.insert(id.clone(), activity(offerer));

        let owner = resolve_pending_claim_from_address(
            "0xabc",
            &id,
            &AHashMap::new(),
            &AHashMap::new(),
            &withdrawn,
        );
        assert_eq!(owner.as_deref(), Some(offerer));
    }

    #[test]
    fn offer_event_is_preferred_over_withdraw() {
        let id = token_data_id().to_id();
        let offerer = "0x00000000000000000000000000000000000000000000000000000000000000dd";
        let other = "0x00000000000000000000000000000000000000000000000000000000000000ee";
        let mut offered = AHashMap::new();
        offered.insert(id.clone(), activity(offerer));
        let mut withdrawn = AHashMap::new();
        withdrawn.insert(id.clone(), activity(other));

        let owner = resolve_pending_claim_from_address(
            "0xabc",
            &id,
            &AHashMap::new(),
            &offered,
            &withdrawn,
        );
        assert_eq!(owner.as_deref(), Some(offerer));
    }
}
