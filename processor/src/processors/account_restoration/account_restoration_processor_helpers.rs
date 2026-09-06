// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_restoration_models::public_key_auth_keys::PublicKeyAuthKeyHelper;
use crate::{
    db::resources::V2TokenResource,
    processors::account_restoration::account_restoration_models::{
        account_restoration_utils::KeyRotationToPublicKeyEvent,
        auth_key_account_addresses::AuthKeyAccountAddress, public_key_auth_keys::PublicKeyAuthKey,
    },
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        transaction::TxnData, write_set_change::Change, Transaction, WriteResource,
    },
    utils::{convert::standardize_address, extract::get_entry_function_from_user_request},
};
use lazy_static::lazy_static;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;

lazy_static! {
    pub static ref ROTATE_AUTH_KEY_ENTRY_FUNCTIONS: Vec<&'static str> = vec![
        "0x1::account::rotate_authentication_key",
        "0x1::account::rotate_authentication_key_with_rotation_capability",
        "0x1::account::upsert_ed25519_backup_key_on_keyless_account",
    ];
}

lazy_static! {
    pub static ref ROTATE_AUTH_KEY_UNVERIFIED_ENTRY_FUNCTIONS: Vec<&'static str> = vec![
        "0x1::account::rotate_authentication_key_call",
        "0x1::account::rotate_authentication_key_from_public_key",
    ];
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Account {
    authentication_key: String,
}

impl TryFrom<&WriteResource> for Account {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

pub fn parse_account_restoration_models(
    transactions: &Vec<Transaction>,
) -> (Vec<AuthKeyAccountAddress>, Vec<PublicKeyAuthKey>) {
    let mut all_auth_key_account_addresses = AHashMap::new();
    let mut all_public_key_auth_keys: Vec<PublicKeyAuthKey> = Vec::new();

    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut auth_key_account_addresses = AHashMap::new();
            let mut public_key_auth_keys: Vec<PublicKeyAuthKey> = Vec::new();

            let txn_version = txn.version as i64;
            let (entry_function_id_str, signature, sender) = match &txn.txn_data {
                Some(TxnData::User(inner)) => {
                    let user_request = inner
                        .request
                        .as_ref()
                        .expect("Sends is not present in user txn");
                    (
                        get_entry_function_from_user_request(user_request),
                        user_request.signature.clone(),
                        Some(standardize_address(&user_request.sender)),
                    )
                },
                _ => (None, None, None),
            };

            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            if !transaction_info.success {
                return (auth_key_account_addresses, public_key_auth_keys);
            }

            // At the end of this loop we'll get all account addresses and their corresponding auth keys
            // with the following conditions:
            // 1. Key rotation transaction
            // 2. Auth key is different from account address
            // 3. Multi-key transaction

            let key_rotation_event = KeyRotationToPublicKeyEvent::from_transaction(txn);
            let mut multi_key_helper = signature.as_ref().and_then(|sig| {
                PublicKeyAuthKeyHelper::get_multi_key_from_signature(sig, txn_version)
            });
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(V2TokenResource::Account(account)) =
                        V2TokenResource::from_write_resource(wr).unwrap()
                    {
                        let auth_key = standardize_address(&account.authentication_key);
                        let account_address = standardize_address(&wr.address);
                        // If the this isn't a change on the sender account (i.e. it is a change of a recipient
                        // account's token resource), we skip.
                        if sender.as_ref() != Some(&account_address) {
                            continue;
                        }

                        // If the transaction is an unverified key rotation transaction, we need to insert the auth key account address
                        // with is_auth_key_used set to false.  This allows us to filter out accounts that are not actually owned by the
                        // owner of the auth key.
                        if ROTATE_AUTH_KEY_UNVERIFIED_ENTRY_FUNCTIONS
                            .contains(&entry_function_id_str.as_deref().unwrap_or(""))
                        {
                            auth_key_account_addresses.insert(
                                account_address.clone(),
                                AuthKeyAccountAddress {
                                    auth_key: auth_key.clone(),
                                    account_address,
                                    last_transaction_version: txn_version,
                                    is_auth_key_used: false,
                                },
                            );
                        }
                        // In all other cases
                        // - If the transaction is a verified key rotation transaction
                        // - If the transaction is a multi-key transaction
                        // - If the transaction is on a rotated account
                        // we need to insert the auth key account address with is_auth_key_used set to true.
                        else if ROTATE_AUTH_KEY_ENTRY_FUNCTIONS
                            .contains(&entry_function_id_str.as_deref().unwrap_or(""))
                            || auth_key != account_address
                            || multi_key_helper.is_some()
                            || key_rotation_event.is_some()
                        {
                            auth_key_account_addresses.insert(
                                account_address.clone(),
                                AuthKeyAccountAddress {
                                    auth_key: auth_key.clone(),
                                    account_address,
                                    last_transaction_version: txn_version,
                                    is_auth_key_used: true,
                                },
                            );
                        }
                    }
                }
            }

            // If there is a KeyRotationToPublicKeyEvent event, use the PublicKeyAuthKeyHelper constructed from it instead.
            // In the case of a single key, there is no helper to construct.
            if let Some(key_rotation_event) = key_rotation_event {
                multi_key_helper = PublicKeyAuthKeyHelper::create_helper_from_key_rotation_event(
                    &key_rotation_event,
                    txn_version,
                );
            }

            if let Some(helper) = &multi_key_helper {
                if let Some(sender) = sender {
                    if let Some(auth_key_account_address) = auth_key_account_addresses.get(&sender)
                    {
                        public_key_auth_keys.extend(
                            PublicKeyAuthKeyHelper::get_public_key_auth_keys(
                                helper,
                                &auth_key_account_address.auth_key,
                                txn_version,
                            ),
                        );
                    }
                }
            }

            (auth_key_account_addresses, public_key_auth_keys)
        })
        .collect();
    for (auth_key_account_addresses, public_key_auth_keys) in data {
        all_auth_key_account_addresses.extend(auth_key_account_addresses);
        all_public_key_auth_keys.extend(public_key_auth_keys);
    }

    let all_auth_key_account_addresses = all_auth_key_account_addresses
        .into_values()
        .collect::<Vec<AuthKeyAccountAddress>>();

    // Deduplicate both tables so batch ON CONFLICT inserts cannot contain the same
    // primary key twice (Postgres rejects "ON CONFLICT DO UPDATE command cannot
    // affect row a second time").
    (
        deduplicate_auth_key_account_addresses(all_auth_key_account_addresses),
        deduplicate_public_key_auth_keys(all_public_key_auth_keys),
    )
}

/// Deduplicate `public_key_auth_keys` on the diesel PK
/// `(public_key, public_key_type, auth_key)`.
///
/// The previous sort+dedup only collapsed rows that shared the same
/// `last_transaction_version`. A multi-key account that sends two transactions
/// in one batch therefore produced two rows with the same PK and different
/// versions, which crashed the storer.
///
/// Keep the latest version and OR `is_public_key_used`: once a key has signed
/// for an auth key it cannot become unused.
fn deduplicate_public_key_auth_keys(
    public_key_auth_keys: Vec<PublicKeyAuthKey>,
) -> Vec<PublicKeyAuthKey> {
    let mut deduped: AHashMap<(String, String, String), PublicKeyAuthKey> = AHashMap::new();
    for key in public_key_auth_keys {
        let pk = (
            key.public_key.clone(),
            key.public_key_type.clone(),
            key.auth_key.clone(),
        );
        if let Some(existing) = deduped.get_mut(&pk) {
            let take_metadata = key.last_transaction_version >= existing.last_transaction_version;
            existing.is_public_key_used = existing.is_public_key_used || key.is_public_key_used;
            existing.last_transaction_version = max(
                existing.last_transaction_version,
                key.last_transaction_version,
            );
            if take_metadata {
                existing.account_public_key = key.account_public_key;
                existing.signature_type = key.signature_type;
            }
        } else {
            deduped.insert(pk, key);
        }
    }
    let mut out: Vec<PublicKeyAuthKey> = deduped.into_values().collect();
    // Stable PK order so chunked upserts cannot deadlock across workers.
    out.sort_by(|a, b| {
        a.auth_key
            .cmp(&b.auth_key)
            .then_with(|| a.public_key.cmp(&b.public_key))
            .then_with(|| a.public_key_type.cmp(&b.public_key_type))
    });
    out
}

/// Deduplicate `auth_key_account_addresses` on `account_address`.
/// Keep the latest version as-is: `is_auth_key_used` can go true→false on an
/// unverified rotation, so do not OR that flag.
fn deduplicate_auth_key_account_addresses(
    mut auth_key_account_addresses: Vec<AuthKeyAccountAddress>,
) -> Vec<AuthKeyAccountAddress> {
    auth_key_account_addresses.sort_by(|a, b| {
        a.account_address
            .cmp(&b.account_address)
            .then_with(|| b.last_transaction_version.cmp(&a.last_transaction_version))
    });
    auth_key_account_addresses.dedup_by(|a, b| a.account_address == b.account_address);
    auth_key_account_addresses
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk_row(
        public_key: &str,
        auth_key: &str,
        version: i64,
        is_public_key_used: bool,
        account_public_key: &str,
    ) -> PublicKeyAuthKey {
        PublicKeyAuthKey {
            public_key: public_key.to_string(),
            public_key_type: "ed25519".to_string(),
            auth_key: auth_key.to_string(),
            account_public_key: account_public_key.to_string(),
            is_public_key_used,
            last_transaction_version: version,
            signature_type: "multi_ed25519_signature".to_string(),
        }
    }

    fn auth_row(
        account_address: &str,
        auth_key: &str,
        version: i64,
        is_auth_key_used: bool,
    ) -> AuthKeyAccountAddress {
        AuthKeyAccountAddress {
            auth_key: auth_key.to_string(),
            account_address: account_address.to_string(),
            last_transaction_version: version,
            is_auth_key_used,
        }
    }

    #[test]
    fn public_key_rows_at_different_versions_collapse_to_one_pk() {
        // Same multi-ed25519 account sending two txs in one batch used to emit
        // two rows with PK (auth_key, public_key, public_key_type) and crash
        // diesel ON CONFLICT upsert.
        let rows = vec![
            pk_row("0xaaa", "0xauth", 100, true, "0xacct-v100"),
            pk_row("0xaaa", "0xauth", 101, false, "0xacct-v101"),
            pk_row("0xbbb", "0xauth", 101, false, "0xacct-v101"),
        ];

        let deduped = deduplicate_public_key_auth_keys(rows);
        assert_eq!(deduped.len(), 2);

        let aaa = deduped
            .iter()
            .find(|r| r.public_key == "0xaaa")
            .expect("collapsed aaa row");
        assert_eq!(aaa.last_transaction_version, 101);
        assert!(
            aaa.is_public_key_used,
            "used=true from v100 must survive v101 unused"
        );
        assert_eq!(aaa.account_public_key, "0xacct-v101");

        let bbb = deduped
            .iter()
            .find(|r| r.public_key == "0xbbb")
            .expect("distinct key kept");
        assert!(!bbb.is_public_key_used);
        assert_eq!(bbb.last_transaction_version, 101);
    }

    #[test]
    fn public_key_same_version_prefers_used_true() {
        let rows = vec![
            pk_row("0xaaa", "0xauth", 50, false, "0xacct"),
            pk_row("0xaaa", "0xauth", 50, true, "0xacct"),
        ];
        let deduped = deduplicate_public_key_auth_keys(rows);
        assert_eq!(deduped.len(), 1);
        assert!(deduped[0].is_public_key_used);
        assert_eq!(deduped[0].last_transaction_version, 50);
    }

    #[test]
    fn auth_key_address_keeps_latest_version_without_or_ing_used() {
        // Unverified rotation can set is_auth_key_used back to false; latest wins.
        let rows = vec![
            auth_row("0xacct", "0xold", 10, true),
            auth_row("0xacct", "0xnew", 20, false),
            auth_row("0xother", "0xother-auth", 15, true),
        ];
        let deduped = deduplicate_auth_key_account_addresses(rows);
        assert_eq!(deduped.len(), 2);

        let acct = deduped
            .iter()
            .find(|r| r.account_address == "0xacct")
            .expect("acct row");
        assert_eq!(acct.last_transaction_version, 20);
        assert_eq!(acct.auth_key, "0xnew");
        assert!(
            !acct.is_auth_key_used,
            "latest unverified rotation must not inherit used=true"
        );
    }
}
