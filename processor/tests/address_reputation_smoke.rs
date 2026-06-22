//! Smoke test for the address_reputation extractor against two real Movement testnet
//! transactions captured from https://indexer.testnet.movementnetwork.xyz/v1/graphql:
//!
//!   - txn 158025629: a USDCx Mint (bridge deposit). Events: FA Deposit + Mint.
//!   - txn 163802127: a primary_fungible_store::transfer between two users.
//!
//! What we want to prove without standing up Postgres:
//!   1. The bridge Mint produces a synthesized head-edge with `is_bridge_inflow=true`,
//!      pointing at the *owner* address from the Mint event's `recipient` field.
//!   2. The user-to-user transfer resolves `data.store` -> owner via the `ObjectCore`
//!      WriteResource, so the graph nodes are owner addresses, not raw storage_ids.
//!   3. The two edges therefore share the *address space* (owners), which is what
//!      makes multi-hop tracing reach back to the bridge.

use aptos_indexer_processor_sdk::{
    aptos_protos::{
        transaction::v1::{
            transaction::TxnData, write_set_change::Change, Event, EventKey, MoveStructTag,
            Transaction, TransactionInfo, UserTransaction, WriteResource, WriteSetChange,
        },
        util::timestamp::Timestamp,
    },
    traits::Processable,
    types::transaction_context::TransactionContext,
};
use processor::processors::address_reputation::{
    address_reputation_extractor::AddressReputationExtractor,
    address_reputation_model::BridgeRegistryEntry,
};
use std::sync::Arc;

const USDCX_MODULE: &str = "0x989577931ff5ec0575071a8bc9084c1c010981169e08cc29e1f82563ed03cafc";
const MINT_EVENT_TYPE: &str =
    "0x989577931ff5ec0575071a8bc9084c1c010981169e08cc29e1f82563ed03cafc::usdcx::Mint";

// Mint txn 158025629
const MINT_RECIPIENT_OWNER: &str =
    "0xfac658cc4b6b56c5b91e56d5cfd720104b1a3bb71f25899c45d6cca00fdd3e8a";
const MINT_RECIPIENT_STORE: &str =
    "0x7733d8c88ffaed0a6d236082b0970850283fd4c59aaf5cb27a12344ac33dd22a";
const MINT_AMOUNT: &str = "4206062";
// USDCx token metadata address (same as the bridge module_address by Move design).
const USDCX_METADATA: &str = "0x989577931ff5ec0575071a8bc9084c1c010981169e08cc29e1f82563ed03cafc";

// Transfer txn 163802127
const SENDER_OWNER: &str = "0xbb45ce1d3dfd1b8520c637e8968f9333022ec35d43cd5a03c053af98c0d2914f";
const SENDER_STORE: &str = "0x454d2b9ef09e5de2418d08d8008b1a3e2115e4eb4b9791d2c5f50cc7ec02f8d4";
const RECIPIENT_OWNER: &str = "0xf41d5b141e6201a76d8a8c6a27efc6bede8bbc7f6879d3f25d8038fb726bdaec";
const RECIPIENT_STORE: &str = "0xeabe6cd49883c961538c96cb123ff6268ca7c908c1161a6856564b47b5e38462";
const TRANSFER_AMOUNT: &str = "1000000000";
// Real on-chain FungibleStore.metadata.inner from the captured transfer txn 163802127.
const TRANSFER_ASSET: &str = "0x45142fb00dde90b950183d8ac2815597892f665c254c3f42b5768bc6ae4c8489";

fn fa_event(type_str: &str, data: String) -> Event {
    Event {
        // FA module events have account_address = 0x0; we mirror that exactly so the
        // extractor's reliance on data.store (rather than event.key.account_address)
        // is genuinely exercised.
        key: Some(EventKey {
            creation_number: 0,
            account_address: "0x0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        }),
        sequence_number: 0,
        r#type: None,
        type_str: type_str.to_string(),
        data,
    }
}

fn object_core_write(addr: &str, owner: &str) -> WriteSetChange {
    WriteSetChange {
        r#type: 0,
        change: Some(Change::WriteResource(WriteResource {
            address: addr.to_string(),
            state_key_hash: vec![],
            // Required: the FromWriteResource pipeline unwraps `r#type` while parsing.
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "object".to_string(),
                name: "ObjectCore".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::object::ObjectCore".to_string(),
            data: format!(
                r#"{{"allow_ungated_transfer":true,"guid_creation_num":"0","owner":"{owner}"}}"#
            ),
        })),
    }
}

fn fungible_store_write(addr: &str, metadata: &str) -> WriteSetChange {
    WriteSetChange {
        r#type: 0,
        change: Some(Change::WriteResource(WriteResource {
            address: addr.to_string(),
            state_key_hash: vec![],
            r#type: Some(MoveStructTag {
                address: "0x1".to_string(),
                module: "fungible_asset".to_string(),
                name: "FungibleStore".to_string(),
                generic_type_params: vec![],
            }),
            type_str: "0x1::fungible_asset::FungibleStore".to_string(),
            data: format!(
                r#"{{"balance":"0","frozen":false,"metadata":{{"inner":"{metadata}"}}}}"#
            ),
        })),
    }
}

fn make_mint_txn() -> Transaction {
    let fa_deposit_data =
        format!(r#"{{"store":"{MINT_RECIPIENT_STORE}","amount":"{MINT_AMOUNT}"}}"#);
    let mint_data = format!(
        r#"{{"amount":"{MINT_AMOUNT}","relayer":"0xdb8069db67708d47796f837e9862ca9aae6ed5a522bc0b9b7bc25584e77577bb","recipient":"{MINT_RECIPIENT_OWNER}","fee_amount":"0","remote_token":"0x63f169ba69623ba6ccf34620857644feb46d0f87e1d7bbcf8c071d30c3d94bd6","remote_domain":10005}}"#
    );
    Transaction {
        timestamp: Some(Timestamp { seconds: 1_700_000_000, nanos: 0 }),
        version: 158_025_629,
        info: Some(TransactionInfo {
            hash: vec![],
            state_change_hash: vec![],
            event_root_hash: vec![],
            state_checkpoint_hash: None,
            gas_used: 0,
            success: true,
            vm_status: String::new(),
            accumulator_root_hash: vec![],
            changes: vec![
                object_core_write(MINT_RECIPIENT_STORE, MINT_RECIPIENT_OWNER),
                fungible_store_write(MINT_RECIPIENT_STORE, USDCX_METADATA),
            ],
        }),
        epoch: 0,
        block_height: 0,
        r#type: 4, // User
        size_info: None,
        txn_data: Some(TxnData::User(UserTransaction {
            request: None,
            events: vec![
                fa_event("0x1::fungible_asset::Deposit", fa_deposit_data),
                fa_event(MINT_EVENT_TYPE, mint_data),
            ],
        })),
    }
}

fn make_transfer_txn() -> Transaction {
    let withdraw_data =
        format!(r#"{{"store":"{SENDER_STORE}","amount":"{TRANSFER_AMOUNT}"}}"#);
    let deposit_data =
        format!(r#"{{"store":"{RECIPIENT_STORE}","amount":"{TRANSFER_AMOUNT}"}}"#);
    Transaction {
        timestamp: Some(Timestamp { seconds: 1_700_000_500, nanos: 0 }),
        version: 163_802_127,
        info: Some(TransactionInfo {
            hash: vec![],
            state_change_hash: vec![],
            event_root_hash: vec![],
            state_checkpoint_hash: None,
            gas_used: 0,
            success: true,
            vm_status: String::new(),
            accumulator_root_hash: vec![],
            changes: vec![
                object_core_write(SENDER_STORE, SENDER_OWNER),
                object_core_write(RECIPIENT_STORE, RECIPIENT_OWNER),
                fungible_store_write(SENDER_STORE, TRANSFER_ASSET),
                fungible_store_write(RECIPIENT_STORE, TRANSFER_ASSET),
            ],
        }),
        epoch: 0,
        block_height: 0,
        r#type: 4,
        size_info: None,
        txn_data: Some(TxnData::User(UserTransaction {
            request: None,
            events: vec![
                fa_event("0x1::fungible_asset::Withdraw", withdraw_data),
                fa_event("0x1::fungible_asset::Deposit", deposit_data),
            ],
        })),
    }
}

fn registry() -> Arc<Vec<BridgeRegistryEntry>> {
    Arc::new(vec![BridgeRegistryEntry {
        bridge_name: "circle_usdcx".to_string(),
        module_address: USDCX_MODULE.to_string(),
        event_type: MINT_EVENT_TYPE.to_string(),
        evm_source_field_path: None,
        recipient_field_path: "recipient".to_string(),
        amount_field_path: "amount".to_string(),
        chain_id_field_path: Some("remote_domain".to_string()),
        enabled: true,
    }])
}

#[tokio::test]
async fn extractor_emits_bridge_head_and_owner_keyed_transfer() {
    let mut extractor = AddressReputationExtractor::new(registry());
    let input = TransactionContext {
        data: vec![make_mint_txn(), make_transfer_txn()],
        metadata: Default::default(),
    };

    let out = extractor
        .process(input)
        .await
        .expect("extractor errored")
        .expect("no output");
    let (edges, inflows) = out.data;

    println!("== bridge_inflows ({}) ==", inflows.len());
    for i in &inflows {
        println!(
            "  v={} idx={} bridge={} recipient={} evm={:?} chain={:?} amt={}",
            i.transaction_version,
            i.event_index,
            i.bridge_name,
            i.aptos_recipient,
            i.evm_source,
            i.src_chain_id,
            i.amount
        );
    }
    println!("== transfer_edges ({}) ==", edges.len());
    for e in &edges {
        println!(
            "  v={} idx={} {} -> {}  amt={} asset={:?} bridge_inflow={} bridge={:?}",
            e.transaction_version,
            e.event_index,
            e.from_address,
            e.to_address,
            e.amount,
            e.asset_type,
            e.is_bridge_inflow,
            e.bridge_name
        );
    }

    // --- assertions ---
    assert_eq!(inflows.len(), 1, "expected one bridge inflow for the mint txn");
    let bi = &inflows[0];
    assert_eq!(bi.aptos_recipient, MINT_RECIPIENT_OWNER);
    assert_eq!(bi.bridge_name, "circle_usdcx");
    assert_eq!(bi.src_chain_id, Some(10005));
    assert!(bi.evm_source.is_none(), "Mint event carries no EVM source field");

    assert_eq!(edges.len(), 2, "expected one synthetic bridge edge + one user transfer edge");

    let bridge_edge = edges
        .iter()
        .find(|e| e.is_bridge_inflow)
        .expect("missing synthesized bridge edge");
    assert_eq!(bridge_edge.from_address, USDCX_MODULE);
    assert_eq!(bridge_edge.to_address, MINT_RECIPIENT_OWNER);
    assert_eq!(bridge_edge.bridge_name.as_deref(), Some("circle_usdcx"));
    assert_eq!(
        bridge_edge.asset_type.as_deref(),
        Some(USDCX_METADATA),
        "bridge edge must carry USDCx asset_type"
    );

    let user_edge = edges
        .iter()
        .find(|e| !e.is_bridge_inflow)
        .expect("missing user transfer edge");
    // The whole point of the ObjectCore-resolution pass: these must be OWNER
    // addresses, not raw storage_ids.
    assert_eq!(user_edge.from_address, SENDER_OWNER);
    assert_eq!(user_edge.to_address, RECIPIENT_OWNER);
    assert_ne!(user_edge.from_address, SENDER_STORE);
    assert_ne!(user_edge.to_address, RECIPIENT_STORE);
    assert_eq!(
        user_edge.asset_type.as_deref(),
        Some(TRANSFER_ASSET),
        "user transfer edge must carry the FA metadata address from FungibleStore"
    );
}
