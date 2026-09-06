// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    parquet_processors::parquet_utils::util::{HasVersion, NamedTable},
    processors::{
        ans::models::{
            ans_lookup::{AnsLookup, CurrentAnsLookup},
            ans_utils::{get_token_name, NameRecordV2, SubdomainExtV2},
        },
        token_v2::token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::{ans_lookup_v2, current_ans_lookup_v2},
};
use ahash::AHashMap;
use allocative::Allocative;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::WriteResource, utils::convert::standardize_address,
};
use diesel::prelude::*;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

type Domain = String;
type Subdomain = String;
pub type TokenStandardType = String;
// PK of current_ans_lookup_v2
type CurrentAnsLookupV2PK = (Domain, Subdomain, TokenStandardType);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AnsLookupV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
    pub block_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl Ord for CurrentAnsLookupV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Must match current_ans_lookup_v2 PK (domain, subdomain, token_standard).
        // Omitting token_standard treats v1 and v2 rows for the same name as Equal
        // despite PartialEq, so .sort() leaves their relative order undefined and
        // parallel upserts can deadlock on distinct PK tuples.
        self.domain
            .cmp(&other.domain)
            .then(self.subdomain.cmp(&other.subdomain))
            .then(self.token_standard.cmp(&other.token_standard))
    }
}

impl PartialOrd for CurrentAnsLookupV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetAnsLookupV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    #[allocative(skip)]
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetAnsLookupV2 {
    const TABLE_NAME: &'static str = "ans_lookup_v2";
}

impl HasVersion for ParquetAnsLookupV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl From<AnsLookupV2> for ParquetAnsLookupV2 {
    fn from(raw_item: AnsLookupV2) -> Self {
        ParquetAnsLookupV2 {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
            block_timestamp: raw_item.block_timestamp,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct ParquetCurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl NamedTable for ParquetCurrentAnsLookupV2 {
    const TABLE_NAME: &'static str = "current_ans_lookup_v2";
}

impl HasVersion for ParquetCurrentAnsLookupV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl From<CurrentAnsLookupV2> for ParquetCurrentAnsLookupV2 {
    fn from(raw_item: CurrentAnsLookupV2) -> Self {
        ParquetCurrentAnsLookupV2 {
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            last_transaction_version: raw_item.last_transaction_version,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct PostgresAnsLookupV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl From<AnsLookupV2> for PostgresAnsLookupV2 {
    fn from(raw_item: AnsLookupV2) -> Self {
        PostgresAnsLookupV2 {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
        }
    }
}

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
)]
#[diesel(primary_key(domain, subdomain, token_standard))]
#[diesel(table_name = current_ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct PostgresCurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl From<CurrentAnsLookupV2> for PostgresCurrentAnsLookupV2 {
    fn from(raw_item: CurrentAnsLookupV2) -> Self {
        PostgresCurrentAnsLookupV2 {
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            last_transaction_version: raw_item.last_transaction_version,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
        }
    }
}

impl CurrentAnsLookupV2 {
    pub fn pk(&self) -> CurrentAnsLookupV2PK {
        (
            self.domain.clone(),
            self.subdomain.clone(),
            self.token_standard.clone(),
        )
    }

    pub fn get_v2_from_v1(
        v1_current_ans_lookup: CurrentAnsLookup,
        v1_ans_lookup: AnsLookup,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, AnsLookupV2) {
        (
            Self {
                domain: v1_current_ans_lookup.domain,
                subdomain: v1_current_ans_lookup.subdomain,
                token_standard: TokenStandard::V1.to_string(),
                registered_address: v1_current_ans_lookup.registered_address,
                last_transaction_version: v1_current_ans_lookup.last_transaction_version,
                expiration_timestamp: v1_current_ans_lookup.expiration_timestamp,
                token_name: v1_current_ans_lookup.token_name,
                is_deleted: v1_current_ans_lookup.is_deleted,
                subdomain_expiration_policy: None,
            },
            AnsLookupV2 {
                transaction_version: v1_ans_lookup.transaction_version,
                write_set_change_index: v1_ans_lookup.write_set_change_index,
                domain: v1_ans_lookup.domain,
                subdomain: v1_ans_lookup.subdomain,
                token_standard: TokenStandard::V1.to_string(),
                registered_address: v1_ans_lookup.registered_address,
                expiration_timestamp: v1_ans_lookup.expiration_timestamp,
                token_name: v1_ans_lookup.token_name,
                is_deleted: v1_ans_lookup.is_deleted,
                subdomain_expiration_policy: None,
                block_timestamp,
            },
        )
    }

    pub fn parse_name_record_from_write_resource_v2(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
        write_set_change_index: i64,
        address_to_subdomain_ext: &AHashMap<String, SubdomainExtV2>,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, AnsLookupV2)>> {
        if let Some(inner) =
            NameRecordV2::from_write_resource(write_resource, ans_v2_contract_address, txn_version)
                .unwrap()
        {
            // If this resource account has a SubdomainExt, then it's a subdomain
            let (subdomain_name, subdomain_expiration_policy) = match address_to_subdomain_ext
                .get(&standardize_address(write_resource.address.as_str()))
            {
                Some(s) => (s.get_subdomain_trunc(), Some(s.subdomain_expiration_policy)),
                None => ("".to_string(), None),
            };

            let token_name = get_token_name(
                inner.get_domain_trunc().as_str(),
                subdomain_name.clone().as_str(),
            );

            return Ok(Some((
                Self {
                    domain: inner.get_domain_trunc(),
                    subdomain: subdomain_name.clone().to_string(),
                    token_standard: TokenStandard::V2.to_string(),
                    registered_address: inner.get_target_address(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name: token_name.clone(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    subdomain_expiration_policy,
                },
                AnsLookupV2 {
                    transaction_version: txn_version,
                    write_set_change_index,
                    domain: inner.get_domain_trunc().clone(),
                    subdomain: subdomain_name.clone().to_string(),
                    token_standard: TokenStandard::V2.to_string(),
                    registered_address: inner.get_target_address().clone(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name,
                    is_deleted: false,
                    subdomain_expiration_policy,
                    block_timestamp,
                },
            )));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(domain: &str, subdomain: &str, token_standard: &str) -> CurrentAnsLookupV2 {
        CurrentAnsLookupV2 {
            domain: domain.to_string(),
            subdomain: subdomain.to_string(),
            token_standard: token_standard.to_string(),
            registered_address: None,
            last_transaction_version: 1,
            expiration_timestamp: chrono::DateTime::from_timestamp(0, 0)
                .unwrap()
                .naive_utc(),
            token_name: String::new(),
            is_deleted: false,
            subdomain_expiration_policy: None,
        }
    }

    #[test]
    fn ord_includes_token_standard_in_pk() {
        let v1 = lookup("example", "www", "v1");
        let v2 = lookup("example", "www", "v2");
        assert_ne!(v1, v2);
        assert_ne!(v1.cmp(&v2), std::cmp::Ordering::Equal);
        assert_eq!(v1.cmp(&v2), std::cmp::Ordering::Less);

        let mut rows = vec![v2.clone(), v1.clone()];
        rows.sort();
        assert_eq!(rows[0].token_standard, "v1");
        assert_eq!(rows[1].token_standard, "v2");
    }
}
