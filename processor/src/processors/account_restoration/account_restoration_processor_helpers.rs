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

lazy_static! {
    /// Entry functions that rotate the *sender's* authentication key.
    ///
    /// `rotate_authentication_key_with_rotation_capability` is intentionally
    /// excluded: the delegate sender does not rotate their own key. That
    /// function writes the *offerer's* `Account` resource instead.
    pub static ref ROTATE_AUTH_KEY_SELF_ENTRY_FUNCTIONS: Vec<&'static str> = vec![
        "0x1::account::rotate_authentication_key",
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
            // 1. Key rotation transaction (including capability rotation of a non-sender)
            // 2. Auth key is different from account address
            // 3. Multi-key transaction (sender only)

            let key_rotation_event = KeyRotationToPublicKeyEvent::from_transaction(txn);
            let event_new_auth_key = key_rotation_event
                .as_ref()
                .map(|event| standardize_address(&hex::encode(&event.new_auth_key)));
            let mut helper_from_rotation_event = false;
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
                        let is_sender = sender.as_ref() == Some(&account_address);
                        if let Some(is_auth_key_used) = index_decision_for_account_write(
                            is_sender,
                            &auth_key,
                            &account_address,
                            entry_function_id_str.as_deref(),
                            multi_key_helper.is_some(),
                        ) {
                            auth_key_account_addresses.insert(
                                account_address.clone(),
                                AuthKeyAccountAddress {
                                    auth_key: auth_key.clone(),
                                    account_address,
                                    last_transaction_version: txn_version,
                                    is_auth_key_used,
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
                helper_from_rotation_event = multi_key_helper.is_some();
            }

            if let Some(helper) = &multi_key_helper {
                for auth_key in public_key_mapping_auth_keys(
                    helper_from_rotation_event,
                    event_new_auth_key.as_deref(),
                    sender.as_deref(),
                    &auth_key_account_addresses,
                ) {
                    public_key_auth_keys.extend(PublicKeyAuthKeyHelper::get_public_key_auth_keys(
                        helper,
                        &auth_key,
                        txn_version,
                    ));
                }
            }

            (auth_key_account_addresses, public_key_auth_keys)
        })
        .collect();
    for (auth_key_account_addresses, public_key_auth_keys) in data {
        all_auth_key_account_addresses.extend(auth_key_account_addresses);
        all_public_key_auth_keys.extend(public_key_auth_keys);
    }

    let mut all_auth_key_account_addresses = all_auth_key_account_addresses
        .into_values()
        .collect::<Vec<AuthKeyAccountAddress>>();

    // Below we do sorting and deduplication. This is for a couple of reasons:
    // 1. It makes the processor more efficient as there is less data I/O
    // 2. Makes processing more consistent and easier to reason about
    // 3. Handles cases where within the same version if there are multiple entries for the same public key.
    //    In this case, if among any of the duplicatesis_public_key_used is true, we want to keep that entry.

    // Sort first to ensure consistent deduplication
    all_public_key_auth_keys.sort_by(|a, b| {
        a.public_key
            .cmp(&b.public_key)
            .then_with(|| a.public_key_type.cmp(&b.public_key_type))
            .then_with(|| a.auth_key.cmp(&b.auth_key))
            .then_with(|| a.last_transaction_version.cmp(&b.last_transaction_version))
            .then_with(|| b.is_public_key_used.cmp(&a.is_public_key_used)) // true comes before false
    });

    // Deduplicate keys based on public_key, public_key_type, auth_key, and last_transaction_version.
    // Since we sorted by public_key, public_key_type, auth_key, last_transaction_version, and is_public_key_used,
    // if any duplicates exist, the ones with is_public_key_used set to true will be the first ones.
    all_public_key_auth_keys.dedup_by(|a, b| {
        a.public_key == b.public_key
            && a.public_key_type == b.public_key_type
            && a.auth_key == b.auth_key
            && a.last_transaction_version == b.last_transaction_version
    });

    // Here we only want the latest entry for each account address.
    all_auth_key_account_addresses.sort_by(|a, b| {
        a.account_address
            .cmp(&b.account_address)
            .then_with(|| b.last_transaction_version.cmp(&a.last_transaction_version))
    });

    // Deduplicate auth key account addresses based on account_address. Since we sorted by account_address and last_transaction_version,
    // the latest entry will be the first one.
    all_auth_key_account_addresses.dedup_by(|a, b| a.account_address == b.account_address);
    (all_auth_key_account_addresses, all_public_key_auth_keys)
}

/// Decide whether an `Account` write should produce an `auth_key_account_addresses`
/// row, and whether `is_auth_key_used` should be true.
///
/// `rotate_authentication_key_with_rotation_capability` writes the *offerer's*
/// Account, not the delegate sender's. Treating that entry function as a
/// sender-only rotation both:
///   * drops the offerer's new auth-key mapping, and
///   * falsely marks the delegate as having rotated.
///
/// Non-sender Account writes are kept only when `auth_key != account_address`
/// (the offerer after a capability rotation, or any already-rotated account
/// that was actually written). Incidental writes such as account creation
/// inside a multi-key transaction still have `auth_key == account_address`
/// and are skipped.
fn index_decision_for_account_write(
    is_sender: bool,
    auth_key: &str,
    account_address: &str,
    entry_function_id_str: Option<&str>,
    has_multi_key_helper: bool,
) -> Option<bool> {
    let entry_fn = entry_function_id_str.unwrap_or("");
    let is_rotated_account = auth_key != account_address;

    if !is_sender {
        return is_rotated_account.then_some(true);
    }

    if ROTATE_AUTH_KEY_UNVERIFIED_ENTRY_FUNCTIONS.contains(&entry_fn) {
        return Some(false);
    }

    if ROTATE_AUTH_KEY_SELF_ENTRY_FUNCTIONS.contains(&entry_fn)
        || is_rotated_account
        || has_multi_key_helper
    {
        return Some(true);
    }

    None
}

/// Auth keys to attach `public_key_auth_keys` rows to.
///
/// When the helper comes from `KeyRotationToPublicKey`, bind it to the
/// account whose auth key matches the event (the offerer in a capability
/// rotation), not the transaction sender.
fn public_key_mapping_auth_keys(
    helper_from_rotation_event: bool,
    event_new_auth_key: Option<&str>,
    sender: Option<&str>,
    auth_key_account_addresses: &AHashMap<String, AuthKeyAccountAddress>,
) -> Vec<String> {
    if helper_from_rotation_event {
        if let Some(new_auth_key) = event_new_auth_key {
            return auth_key_account_addresses
                .values()
                .filter(|acct| acct.auth_key == new_auth_key)
                .map(|acct| acct.auth_key.clone())
                .collect();
        }
    }
    if let Some(sender) = sender {
        if let Some(acct) = auth_key_account_addresses.get(sender) {
            return vec![acct.auth_key.clone()];
        }
    }
    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROTATE_AUTH_KEY_WITH_CAPABILITY: &str =
        "0x1::account::rotate_authentication_key_with_rotation_capability";

    fn acct(auth_key: &str, account_address: &str) -> AuthKeyAccountAddress {
        AuthKeyAccountAddress {
            auth_key: auth_key.to_string(),
            account_address: account_address.to_string(),
            last_transaction_version: 1,
            is_auth_key_used: true,
        }
    }

    #[test]
    fn capability_rotation_indexes_offerer_not_delegate() {
        let offerer = "0xbb";
        let offerer_new_auth_key = "0xcc";
        let delegate = "0xaa";

        // Offerer Account write: new auth key, not the sender.
        assert_eq!(
            index_decision_for_account_write(
                false,
                offerer_new_auth_key,
                offerer,
                Some(ROTATE_AUTH_KEY_WITH_CAPABILITY),
                false,
            ),
            Some(true)
        );
        // Delegate sender's sequence-number Account write is not a rotation.
        assert_eq!(
            index_decision_for_account_write(
                true,
                delegate,
                delegate,
                Some(ROTATE_AUTH_KEY_WITH_CAPABILITY),
                false,
            ),
            None
        );
    }

    #[test]
    fn self_rotation_still_indexes_sender() {
        assert_eq!(
            index_decision_for_account_write(
                true,
                "0xcc",
                "0xaa",
                Some("0x1::account::rotate_authentication_key"),
                false,
            ),
            Some(true)
        );
    }

    #[test]
    fn multi_key_txn_does_not_index_newly_created_recipient() {
        assert_eq!(
            index_decision_for_account_write(false, "0xdd", "0xdd", None, true),
            None
        );
        assert_eq!(
            index_decision_for_account_write(true, "0xaa", "0xaa", None, true),
            Some(true)
        );
    }

    #[test]
    fn unverified_rotation_marks_sender_unused() {
        assert_eq!(
            index_decision_for_account_write(
                true,
                "0xcc",
                "0xaa",
                Some("0x1::account::rotate_authentication_key_call"),
                false,
            ),
            Some(false)
        );
    }

    #[test]
    fn rotation_event_public_keys_bind_to_offerer_auth_key() {
        let offerer = "0xbb";
        let offerer_new_auth_key = "0xcc";
        let delegate = "0xaa";
        let mut accounts = AHashMap::new();
        accounts.insert(offerer.to_string(), acct(offerer_new_auth_key, offerer));

        let keys = public_key_mapping_auth_keys(
            true,
            Some(offerer_new_auth_key),
            Some(delegate),
            &accounts,
        );
        assert_eq!(keys, vec![offerer_new_auth_key.to_string()]);
    }

    #[test]
    fn signature_helper_public_keys_still_bind_to_sender() {
        let sender = "0xaa";
        let auth_key = "0xcc";
        let mut accounts = AHashMap::new();
        accounts.insert(sender.to_string(), acct(auth_key, sender));

        let keys = public_key_mapping_auth_keys(false, None, Some(sender), &accounts);
        assert_eq!(keys, vec![auth_key.to_string()]);
    }
}
