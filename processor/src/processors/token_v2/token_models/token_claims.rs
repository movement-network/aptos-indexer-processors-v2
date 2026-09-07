// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::token_v2::{
        token_models::{token_utils::TokenWriteSet, tokens::TableHandleToOwner},
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

/// (token_data_id, property_version). Token V1 offers of the same named token
/// with different property versions are distinct `current_token_pending_claims` rows.
pub type TokenV1OfferEventKey = (String, BigDecimal);

// Map to keep track of the metadata of token offers that were claimed.
pub type TokenV1Claimed = AHashMap<TokenV1OfferEventKey, TokenActivityHelperV1>;

// Map to keep track of the metadata of token offers that were canceled.
pub type TokenV1Canceled = AHashMap<TokenV1OfferEventKey, TokenActivityHelperV1>;

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

                let maybe_table_metadata = table_handle_to_owner.get(&table_handle);

                if let Some(table_metadata) = maybe_table_metadata {
                    let token_id = offer.token_id.clone();
                    let token_data_id_struct = token_id.token_data_id;
                    let collection_data_id_hash =
                        token_data_id_struct.get_collection_data_id_hash();
                    let token_data_id_hash = token_data_id_struct.to_hash();
                    // Basically adding 0x prefix to the previous 2 lines. This is to be consistent with Token V2
                    let collection_id = token_data_id_struct.get_collection_id();
                    let token_data_id = token_data_id_struct.to_id();
                    let collection_name = token_data_id_struct.get_collection_trunc();
                    let name = token_data_id_struct.get_name_trunc();

                    return Ok(Some(Self {
                        token_data_id_hash,
                        property_version: token_id.property_version,
                        from_address: table_metadata.get_owner_address(),
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
                        "Missing table handle metadata for TokenClaim. {:?}",
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
            let offer_key = (
                token_data_id.clone(),
                offer.token_id.property_version.clone(),
            );

            // Try to find owner from write resources
            let mut maybe_owner_address = table_handle_to_owner
                .get(&table_handle)
                .map(|table_metadata| table_metadata.get_owner_address());

            // If table handle isn't in TableHandleToOwner, try to find owner from token v1 claim events
            if maybe_owner_address.is_none() {
                if let Some(token_claimed) = tokens_claimed.get(&offer_key) {
                    maybe_owner_address = token_claimed.from_address.clone();
                }
                if let Some(token_canceled) = tokens_canceled.get(&offer_key) {
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
    use crate::processors::token_v2::token_v2_models::v2_token_activities::TokenActivityV2;
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
        DeleteTableData, DeleteTableItem, Event, EventKey,
    };
    use bigdecimal::BigDecimal;

    const ALICE: &str = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    const BOB: &str = "0x00000000000000000000000000000000000000000000000000000000000000bb";
    const CAROL: &str = "0x00000000000000000000000000000000000000000000000000000000000000cc";
    const ALICE_CLAIMS: &str = "0x0000000000000000000000000000000000000000000000000000000000000ca1";
    const BOB_CLAIMS: &str = "0x0000000000000000000000000000000000000000000000000000000000000cb2";

    fn ts() -> chrono::NaiveDateTime {
        chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc()
    }

    fn token_id_json(property_version: u32) -> String {
        format!(
            r#"{{"token_data_id":{{"creator":"0x1","collection":"col","name":"tok"}},"property_version":"{property_version}"}}"#
        )
    }

    fn claim_event(offerer: &str, to: &str, property_version: u32) -> Event {
        Event {
            key: Some(EventKey {
                creation_number: 0,
                account_address: offerer.to_string(),
            }),
            sequence_number: 0,
            r#type: None,
            type_str: "0x3::token_transfers::Claim".to_string(),
            data: format!(
                r#"{{"amount":"1","account":"{offerer}","to_address":"{to}","token_id":{}}}"#,
                token_id_json(property_version)
            ),
        }
    }

    fn cancel_event(offerer: &str, to: &str, property_version: u32) -> Event {
        Event {
            key: Some(EventKey {
                creation_number: 0,
                account_address: offerer.to_string(),
            }),
            sequence_number: 0,
            r#type: None,
            type_str: "0x3::token_transfers::CancelOffer".to_string(),
            data: format!(
                r#"{{"amount":"1","account":"{offerer}","to_address":"{to}","token_id":{}}}"#,
                token_id_json(property_version)
            ),
        }
    }

    fn delete_offer(handle: &str, to: &str, property_version: u32) -> DeleteTableItem {
        let key = format!(
            r#"{{"to_addr":"{to}","token_id":{}}}"#,
            token_id_json(property_version)
        );
        DeleteTableItem {
            state_key_hash: vec![],
            handle: handle.to_string(),
            key: key.clone(),
            data: Some(DeleteTableData {
                key,
                key_type: "0x3::token_transfers::TokenOfferId".to_string(),
            }),
        }
    }

    fn parse_claim_events(events: &[Event]) -> (TokenV1Claimed, TokenV1Canceled) {
        let mut tokens_claimed = AHashMap::new();
        let mut tokens_canceled = AHashMap::new();
        let mut withdrawn = AHashMap::new();
        let mut deposited = AHashMap::new();
        for (i, event) in events.iter().enumerate() {
            TokenActivityV2::get_v1_from_parsed_event(
                event,
                1,
                ts(),
                i as i64,
                &None,
                &mut tokens_claimed,
                &mut tokens_canceled,
                &mut withdrawn,
                &mut deposited,
            )
            .unwrap();
        }
        (tokens_claimed, tokens_canceled)
    }

    /// Two holders of the same named token (pv0 vs a mutated pv1) offer to the
    /// same recipient. The recipient claims both in one txn. PendingClaims is
    /// not rewritten, so from_address comes from the claim-event map.
    ///
    /// Keying that map only by token_data_id keeps the last event and writes
    /// Alice's delete against Bob's PK (wrong-row update / silent skip of
    /// Alice's pending-claim row).
    #[test]
    fn claim_fallback_does_not_reuse_offerer_across_property_versions() {
        let (tokens_claimed, tokens_canceled) =
            parse_claim_events(&[claim_event(ALICE, CAROL, 0), claim_event(BOB, CAROL, 1)]);

        assert_eq!(
            tokens_claimed.len(),
            2,
            "both property versions must be kept"
        );

        let alice_row = CurrentTokenPendingClaim::from_delete_table_item(
            &delete_offer(ALICE_CLAIMS, CAROL, 0),
            1,
            ts(),
            &AHashMap::new(),
            &tokens_claimed,
            &tokens_canceled,
        )
        .unwrap()
        .expect("alice pv0 claim must resolve");
        let bob_row = CurrentTokenPendingClaim::from_delete_table_item(
            &delete_offer(BOB_CLAIMS, CAROL, 1),
            1,
            ts(),
            &AHashMap::new(),
            &tokens_claimed,
            &tokens_canceled,
        )
        .unwrap()
        .expect("bob pv1 claim must resolve");

        assert_eq!(alice_row.from_address, ALICE);
        assert_eq!(alice_row.to_address, CAROL);
        assert_eq!(alice_row.property_version, BigDecimal::from(0));
        assert_eq!(alice_row.amount, BigDecimal::zero());

        assert_eq!(bob_row.from_address, BOB);
        assert_eq!(bob_row.to_address, CAROL);
        assert_eq!(bob_row.property_version, BigDecimal::from(1));
        assert_eq!(bob_row.amount, BigDecimal::zero());

        assert_ne!(
            (
                &alice_row.token_data_id_hash,
                &alice_row.property_version,
                &alice_row.from_address,
                &alice_row.to_address
            ),
            (
                &bob_row.token_data_id_hash,
                &bob_row.property_version,
                &bob_row.from_address,
                &bob_row.to_address
            ),
            "pending-claim PKs must stay distinct"
        );
    }

    /// Cancel of pv1 must not steal the offerer of a same-token_data_id pv0 claim.
    #[test]
    fn cancel_fallback_does_not_overwrite_other_property_version_claim() {
        let (tokens_claimed, tokens_canceled) =
            parse_claim_events(&[claim_event(ALICE, CAROL, 0), cancel_event(BOB, CAROL, 1)]);

        let alice_row = CurrentTokenPendingClaim::from_delete_table_item(
            &delete_offer(ALICE_CLAIMS, CAROL, 0),
            1,
            ts(),
            &AHashMap::new(),
            &tokens_claimed,
            &tokens_canceled,
        )
        .unwrap()
        .expect("alice pv0 claim must resolve");
        let bob_row = CurrentTokenPendingClaim::from_delete_table_item(
            &delete_offer(BOB_CLAIMS, CAROL, 1),
            1,
            ts(),
            &AHashMap::new(),
            &tokens_claimed,
            &tokens_canceled,
        )
        .unwrap()
        .expect("bob pv1 cancel must resolve");

        assert_eq!(alice_row.from_address, ALICE);
        assert_eq!(alice_row.property_version, BigDecimal::from(0));
        assert_eq!(bob_row.from_address, BOB);
        assert_eq!(bob_row.property_version, BigDecimal::from(1));
    }
}
