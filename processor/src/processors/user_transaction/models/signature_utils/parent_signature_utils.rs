// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::account_signature_utils::{
    from_account_signature, get_account_signature_type_from_enum,
};
use crate::processors::user_transaction::models::signatures::Signature;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{
        account_signature::Type as AccountSignatureTypeEnum,
        signature::{Signature as SignatureEnum, Type as SignatureTypeEnum},
        Ed25519Signature, FeePayerSignature, MultiAgentSignature, MultiEd25519Signature,
        Signature as SignaturePb, SingleSender,
    },
    utils::convert::standardize_address,
};
use tracing::warn;

/// Signatures has multiple layers in the proto. This is the top layer. It's only used in user_transactions table
pub fn get_parent_signature_type(t: &SignaturePb) -> String {
    get_parent_signature_type_from_enum(&t.r#type())
}

fn get_parent_signature_type_from_enum(t: &SignatureTypeEnum) -> String {
    match t {
        SignatureTypeEnum::Ed25519 => "ed25519_signature".to_string(),
        SignatureTypeEnum::MultiEd25519 => "multi_ed25519_signature".to_string(),
        SignatureTypeEnum::MultiAgent => "multi_agent_signature".to_string(),
        SignatureTypeEnum::FeePayer => "fee_payer_signature".to_string(),
        SignatureTypeEnum::SingleSender => "single_sender_signature".to_string(),
        SignatureTypeEnum::Unspecified => {
            warn!("Unspecified signature type encountered");
            "unknown".to_string()
        },
    }
}

pub fn from_parent_signature(
    s: &SignaturePb,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
    block_timestamp: chrono::NaiveDateTime,
) -> Vec<Signature> {
    match s.signature.as_ref().unwrap() {
        SignatureEnum::Ed25519(sig) => vec![parse_ed25519_signature(
            sig,
            &get_account_signature_type_from_enum(&AccountSignatureTypeEnum::Ed25519),
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
            block_timestamp,
        )],
        SignatureEnum::MultiEd25519(sig) => parse_multi_ed25519_signature(
            sig,
            &get_account_signature_type_from_enum(&AccountSignatureTypeEnum::MultiEd25519),
            sender,
            transaction_version,
            transaction_block_height,
            is_sender_primary,
            multi_agent_index,
            override_address,
            block_timestamp,
        ),
        SignatureEnum::MultiAgent(sig) => parse_multi_agent_signature(
            sig,
            sender,
            transaction_version,
            transaction_block_height,
            block_timestamp,
        ),
        SignatureEnum::FeePayer(sig) => parse_fee_payer_signature(
            sig,
            sender,
            transaction_version,
            transaction_block_height,
            block_timestamp,
        ),
        SignatureEnum::SingleSender(s) => parse_single_sender(
            s,
            sender,
            transaction_version,
            transaction_block_height,
            block_timestamp,
        ),
    }
}

pub fn parse_ed25519_signature(
    s: &Ed25519Signature,
    account_signature_type: &str,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
    block_timestamp: chrono::NaiveDateTime,
) -> Signature {
    let signer = standardize_address(override_address.unwrap_or(sender));
    Signature {
        transaction_version,
        transaction_block_height,
        block_timestamp,
        signer,
        is_sender_primary,
        account_signature_type: account_signature_type.to_string(),
        any_signature_type: None,
        public_key_type: None,
        public_key: format!("0x{}", hex::encode(s.public_key.as_slice())),
        threshold: 1,
        public_key_indices: serde_json::Value::Array(vec![]),
        signature: format!("0x{}", hex::encode(s.signature.as_slice())),
        multi_agent_index,
        multi_sig_index: 0,
    }
}

pub fn parse_multi_ed25519_signature(
    s: &MultiEd25519Signature,
    account_signature_type: &str,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    is_sender_primary: bool,
    multi_agent_index: i64,
    override_address: Option<&String>,
    block_timestamp: chrono::NaiveDateTime,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    let signer = standardize_address(override_address.unwrap_or(sender));

    let public_key_indices = get_public_key_indices_from_multi_ed25519_signature(s);
    for (index, signature) in s.signatures.iter().enumerate() {
        let public_key = s
            .public_keys
            .get(public_key_indices.clone()[index])
            .unwrap()
            .clone();
        signatures.push(Signature {
            transaction_version,
            transaction_block_height,
            signer: signer.clone(),
            is_sender_primary,
            account_signature_type: account_signature_type.to_string(),
            any_signature_type: None,
            public_key_type: None,
            public_key: format!("0x{}", hex::encode(public_key.as_slice())),
            threshold: s.threshold as i64,
            signature: format!("0x{}", hex::encode(signature.as_slice())),
            public_key_indices: serde_json::Value::Array(
                public_key_indices
                    .iter()
                    .map(|index| serde_json::Value::Number(serde_json::Number::from(*index as i64)))
                    .collect(),
            ),
            multi_agent_index,
            multi_sig_index: index as i64,
            block_timestamp,
        });
    }
    signatures
}

pub fn get_public_key_indices_from_multi_ed25519_signature(
    s: &MultiEd25519Signature,
) -> Vec<usize> {
    s.public_key_indices
        .iter()
        .map(|index| *index as usize)
        .collect()
}

pub fn parse_multi_agent_signature(
    s: &MultiAgentSignature,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    block_timestamp: chrono::NaiveDateTime,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    // process sender signature
    signatures.append(&mut from_account_signature(
        s.sender.as_ref().unwrap(),
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
        block_timestamp,
    ));
    for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
        let secondary_sig = match s.secondary_signers.get(index) {
            Some(sig) => sig,
            None => {
                tracing::error!(
                    transaction_version = transaction_version,
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
                panic!("Failed to parse index {index} for multi agent secondary signers");
            },
        };
        signatures.append(&mut from_account_signature(
            secondary_sig,
            sender,
            transaction_version,
            transaction_block_height,
            false,
            index as i64,
            Some(&address.to_string()),
            block_timestamp,
        ));
    }
    signatures
}

pub fn parse_fee_payer_signature(
    s: &FeePayerSignature,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    block_timestamp: chrono::NaiveDateTime,
) -> Vec<Signature> {
    let mut signatures = Vec::default();
    // process sender signature
    signatures.append(&mut from_account_signature(
        s.sender.as_ref().unwrap(),
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
        block_timestamp,
    ));
    for (index, address) in s.secondary_signer_addresses.iter().enumerate() {
        let secondary_sig = match s.secondary_signers.get(index) {
            Some(sig) => sig,
            None => {
                tracing::error!(
                    transaction_version = transaction_version,
                    "Failed to parse index {} for multi agent secondary signers",
                    index
                );
                panic!("Failed to parse index {index} for multi agent secondary signers");
            },
        };
        signatures.append(&mut from_account_signature(
            secondary_sig,
            sender,
            transaction_version,
            transaction_block_height,
            false,
            index as i64,
            Some(&address.to_string()),
            block_timestamp,
        ));
    }
    // Fee payer is a required signer. Omitting it drops the payer from
    // `signatures`, undercounts `num_signatures`, and hides the payer from
    // `account_transactions` (which derives signers from this list).
    if let Some(fee_payer_signer) = s.fee_payer_signer.as_ref() {
        signatures.append(&mut from_account_signature(
            fee_payer_signer,
            sender,
            transaction_version,
            transaction_block_height,
            false,
            s.secondary_signer_addresses.len() as i64,
            Some(&s.fee_payer_address),
            block_timestamp,
        ));
    }
    signatures
}

pub fn get_fee_payer_address(t: &SignaturePb, transaction_version: i64) -> Option<String> {
    let sig = t.signature.as_ref().unwrap_or_else(|| {
        tracing::error!(
            transaction_version = transaction_version,
            "Transaction signature is missing"
        );
        panic!("Transaction signature is missing");
    });
    if let SignatureEnum::FeePayer(sig) = sig {
        Some(standardize_address(&sig.fee_payer_address))
    } else {
        None
    }
}

pub fn parse_single_sender(
    s: &SingleSender,
    sender: &String,
    transaction_version: i64,
    transaction_block_height: i64,
    block_timestamp: chrono::NaiveDateTime,
) -> Vec<Signature> {
    let signature = s.sender.as_ref().unwrap();
    if signature.signature.is_none() {
        warn!(
            transaction_version = transaction_version,
            "Transaction signature is unknown"
        );
        return vec![];
    }
    from_account_signature(
        signature,
        sender,
        transaction_version,
        transaction_block_height,
        true,
        0,
        None,
        block_timestamp,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
        account_signature::{Signature as AccountSignatureEnum, Type as AccountSignatureTypeEnum},
        AccountSignature,
    };
    use chrono::DateTime;

    fn ed25519_account_signature(seed: u8) -> AccountSignature {
        AccountSignature {
            r#type: AccountSignatureTypeEnum::Ed25519 as i32,
            signature: Some(AccountSignatureEnum::Ed25519(Ed25519Signature {
                public_key: vec![seed; 32],
                signature: vec![seed.wrapping_add(1); 64],
            })),
        }
    }

    fn fee_payer_signature(fee_payer: &str) -> FeePayerSignature {
        FeePayerSignature {
            sender: Some(ed25519_account_signature(1)),
            secondary_signer_addresses: vec![],
            secondary_signers: vec![],
            fee_payer_address: fee_payer.to_string(),
            fee_payer_signer: Some(ed25519_account_signature(2)),
        }
    }

    #[test]
    fn parse_fee_payer_signature_includes_fee_payer_signer() {
        let sender = "0x1".to_string();
        let fee_payer = "0x2".to_string();
        let parsed = parse_fee_payer_signature(
            &fee_payer_signature(&fee_payer),
            &sender,
            42,
            7,
            DateTime::from_timestamp(1, 0).unwrap().naive_utc(),
        );

        let signers: Vec<String> = parsed.iter().map(|s| s.signer.clone()).collect();
        assert!(
            signers.iter().any(|s| s == &standardize_address(&sender)),
            "sender missing from fee-payer signatures: {signers:?}"
        );
        assert!(
            signers
                .iter()
                .any(|s| s == &standardize_address(&fee_payer)),
            "fee payer missing from fee-payer signatures: {signers:?}"
        );
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].is_sender_primary);
        assert!(!parsed[1].is_sender_primary);
        assert_eq!(parsed[1].multi_agent_index, 0);
        assert_eq!(
            parsed[1].public_key,
            format!("0x{}", hex::encode(vec![2u8; 32]))
        );
    }

    #[test]
    fn parse_fee_payer_signature_uses_unique_index_after_secondary_signers() {
        let sender = "0x1".to_string();
        let secondary = "0x3".to_string();
        let fee_payer = "0x2".to_string();
        let s = FeePayerSignature {
            sender: Some(ed25519_account_signature(1)),
            secondary_signer_addresses: vec![secondary.clone()],
            secondary_signers: vec![ed25519_account_signature(3)],
            fee_payer_address: fee_payer.clone(),
            fee_payer_signer: Some(ed25519_account_signature(2)),
        };
        let parsed = parse_fee_payer_signature(
            &s,
            &sender,
            42,
            7,
            DateTime::from_timestamp(1, 0).unwrap().naive_utc(),
        );

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[1].signer, standardize_address(&secondary));
        assert_eq!(parsed[1].multi_agent_index, 0);
        assert_eq!(parsed[2].signer, standardize_address(&fee_payer));
        assert_eq!(parsed[2].multi_agent_index, 1);
        assert!(!parsed[2].is_sender_primary);
    }
}
