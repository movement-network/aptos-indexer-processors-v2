use crate::schema::confidential_asset_activities;
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    aptos_protos::transaction::v1::{transaction::TxnData, Event, Transaction},
    utils::convert::standardize_address,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use tracing::warn;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Substring present in every event type emitted by the confidential_asset module.
/// Matches both `0x7::confidential_asset::*` and `0x1::confidential_asset::*`.
const CA_MODULE_MARKER: &str = "::confidential_asset::";

// ---------------------------------------------------------------------------
// Opaque crypto type aliases
// ---------------------------------------------------------------------------

/// Compressed Twisted-ElGamal ciphertext (four-chunk balance or single-chunk amount).
/// Stored as a JSONB blob — the indexer does not decrypt or validate its contents.
pub type CompressedBalance = serde_json::Value;

/// Compressed Twisted-ElGamal public key.
/// Stored as a JSONB blob.
pub type CompressedPubkey = serde_json::Value;

// ---------------------------------------------------------------------------
// Per-event wire-format structs
// ---------------------------------------------------------------------------

/// `confidential_asset::Registered`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegisteredEvent {
    #[serde(skip_serializing)]
    pub addr: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// Encryption key registered for this account/asset pair.
    pub ek: CompressedPubkey,
}

/// `confidential_asset::Deposited`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DepositedEvent {
    #[serde(skip_serializing)]
    pub from: String,
    #[serde(skip_serializing)]
    pub to: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// Plaintext amount brought into the protocol (serialized as a string by the node).
    pub amount: serde_json::Value,
    /// Recipient's new pending balance ciphertext after the deposit.
    pub new_pending_balance: CompressedBalance,
}

/// `confidential_asset::Withdrawn`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WithdrawnEvent {
    #[serde(skip_serializing)]
    pub from: String,
    #[serde(skip_serializing)]
    pub to: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// Plaintext amount taken out of the protocol (serialized as a string by the node).
    pub amount: serde_json::Value,
    /// Sender's new available balance ciphertext after the withdrawal.
    pub new_available_balance: CompressedBalance,
}

/// `confidential_asset::Transferred`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransferredEvent {
    #[serde(skip_serializing)]
    pub from: String,
    #[serde(skip_serializing)]
    pub to: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// Encrypted transfer amount under the recipient key (four-chunk pending-balance layout).
    pub amount: CompressedBalance,
    /// Flattened transfer sigma `x7s` commitments: `128 × n` bytes, hex-encoded.
    pub ek_volun_auds: String,
    /// Opaque sender-supplied auditor hint bytes, hex-encoded.
    pub sender_auditor_hint: String,
    /// Sender's new available balance ciphertext after the debit.
    pub new_sender_available_balance: CompressedBalance,
    /// Recipient's new pending balance ciphertext after the credit.
    pub new_recip_pending_balance: CompressedBalance,
    /// Reserved memo payload (currently always empty), hex-encoded.
    pub memo: String,
}

/// `confidential_asset::Normalized`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NormalizedEvent {
    #[serde(skip_serializing)]
    pub addr: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// New available balance ciphertext after re-encryption to normalize chunk bounds.
    pub new_available_balance: CompressedBalance,
}

/// `confidential_asset::RolledOver`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RolledOverEvent {
    #[serde(skip_serializing)]
    pub addr: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// New available balance ciphertext after the pending balance was rolled in.
    pub new_available_balance: CompressedBalance,
}

/// `confidential_asset::KeyRotated`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeyRotatedEvent {
    #[serde(skip_serializing)]
    pub addr: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// The new encryption key after rotation.
    pub new_ek: CompressedPubkey,
    /// Available balance re-encrypted under the new key.
    pub new_available_balance: CompressedBalance,
}

/// `confidential_asset::FreezeChanged`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FreezeChangedEvent {
    #[serde(skip_serializing)]
    pub addr: String,
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// `true` = incoming transfers paused; `false` = resumed.
    pub frozen: bool,
}

/// `confidential_asset::AllowListChanged`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AllowListChangedEvent {
    /// `true` = global allow list enabled; `false` = disabled.
    pub enabled: bool,
}

/// `confidential_asset::TokenAllowChanged`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenAllowChangedEvent {
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// `true` = confidential transfers permitted for this token; `false` = disabled.
    pub allowed: bool,
}

/// `confidential_asset::AuditorChanged`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuditorChangedEvent {
    #[serde(skip_serializing)]
    pub asset_type: String,
    /// New asset-specific auditor key, or `None` (as `{"vec":[]}`) when cleared.
    pub new_auditor_ek: serde_json::Value,
}

// ---------------------------------------------------------------------------
// CaEvent enum
// ---------------------------------------------------------------------------

/// Typed representation of every event emitted by the `confidential_asset` module.
///
/// Serialized with an adjacently-tagged layout so the stored JSONB is self-describing:
/// `{"type":"Transferred","data":{...}}`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum CaEvent {
    Registered(RegisteredEvent),
    Deposited(DepositedEvent),
    Withdrawn(WithdrawnEvent),
    Transferred(TransferredEvent),
    Normalized(NormalizedEvent),
    RolledOver(RolledOverEvent),
    KeyRotated(KeyRotatedEvent),
    FreezeChanged(FreezeChangedEvent),
    AllowListChanged(AllowListChangedEvent),
    TokenAllowChanged(TokenAllowChangedEvent),
    AuditorChanged(AuditorChangedEvent),
}

impl CaEvent {
    /// Attempts to parse a raw event into a typed `CaEvent`.
    ///
    /// Returns `Ok(None)` for non-CA events and for unknown CA variant names
    /// (forward-compatibility). Returns an error only when the event type matches
    /// a known CA variant but the payload cannot be deserialized.
    pub fn from_event(type_str: &str, data: &str, txn_version: i64) -> Result<Option<Self>> {
        if !type_str.contains(CA_MODULE_MARKER) {
            return Ok(None);
        }
        let short = short_event_type(type_str);
        let result = match short {
            "Registered" => serde_json::from_str(data).map(|e| Some(Self::Registered(e))),
            "Deposited" => serde_json::from_str(data).map(|e| Some(Self::Deposited(e))),
            "Withdrawn" => serde_json::from_str(data).map(|e| Some(Self::Withdrawn(e))),
            "Transferred" => serde_json::from_str(data).map(|e| Some(Self::Transferred(e))),
            "Normalized" => serde_json::from_str(data).map(|e| Some(Self::Normalized(e))),
            "RolledOver" => serde_json::from_str(data).map(|e| Some(Self::RolledOver(e))),
            "KeyRotated" => serde_json::from_str(data).map(|e| Some(Self::KeyRotated(e))),
            "FreezeChanged" => serde_json::from_str(data).map(|e| Some(Self::FreezeChanged(e))),
            "AllowListChanged" => {
                serde_json::from_str(data).map(|e| Some(Self::AllowListChanged(e)))
            },
            "TokenAllowChanged" => {
                serde_json::from_str(data).map(|e| Some(Self::TokenAllowChanged(e)))
            },
            "AuditorChanged" => serde_json::from_str(data).map(|e| Some(Self::AuditorChanged(e))),
            _ => return Ok(None),
        };
        result.context(format!(
            "version {txn_version}: failed to parse {type_str}: {data}"
        ))
    }

    /// The fungible-asset metadata object address, if present for this event type.
    pub fn asset_type(&self) -> Option<String> {
        match self {
            Self::Registered(e) => Some(standardize_address(&e.asset_type)),
            Self::Deposited(e) => Some(standardize_address(&e.asset_type)),
            Self::Withdrawn(e) => Some(standardize_address(&e.asset_type)),
            Self::Transferred(e) => Some(standardize_address(&e.asset_type)),
            Self::Normalized(e) => Some(standardize_address(&e.asset_type)),
            Self::RolledOver(e) => Some(standardize_address(&e.asset_type)),
            Self::KeyRotated(e) => Some(standardize_address(&e.asset_type)),
            Self::FreezeChanged(e) => Some(standardize_address(&e.asset_type)),
            Self::TokenAllowChanged(e) => Some(standardize_address(&e.asset_type)),
            Self::AuditorChanged(e) => Some(standardize_address(&e.asset_type)),
            Self::AllowListChanged(_) => None,
        }
    }

    /// The "from" / primary actor address, if present for this event type.
    pub fn from_address(&self) -> Option<String> {
        match self {
            Self::Deposited(e) => Some(standardize_address(&e.from)),
            Self::Withdrawn(e) => Some(standardize_address(&e.from)),
            Self::Transferred(e) => Some(standardize_address(&e.from)),
            Self::Registered(e) => Some(standardize_address(&e.addr)),
            Self::Normalized(e) => Some(standardize_address(&e.addr)),
            Self::RolledOver(e) => Some(standardize_address(&e.addr)),
            Self::KeyRotated(e) => Some(standardize_address(&e.addr)),
            Self::FreezeChanged(e) => Some(standardize_address(&e.addr)),
            _ => None,
        }
    }

    /// The "to" / recipient address, if present for this event type.
    pub fn to_address(&self) -> Option<String> {
        match self {
            Self::Deposited(e) => Some(standardize_address(&e.to)),
            Self::Withdrawn(e) => Some(standardize_address(&e.to)),
            Self::Transferred(e) => Some(standardize_address(&e.to)),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Database model
// ---------------------------------------------------------------------------

/// One row in `confidential_asset_activities` — one per CA event of any type.
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = confidential_asset_activities)]
pub struct ConfidentialAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    /// Short event variant name: "Transferred", "Registered", etc.
    pub event_type: String,
    /// Fungible-asset metadata object address; `None` for `AllowListChanged`.
    pub asset_type: Option<String>,
    /// Sender / primary actor address; `None` for governance events.
    pub from_address: Option<String>,
    /// Recipient address; populated only for `Deposited`, `Withdrawn`, `Transferred`.
    pub to_address: Option<String>,
    /// Full typed event payload serialized as JSONB: `{"type":"…","data":{…}}`.
    pub event_data: serde_json::Value,
    pub block_height: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Output of parsing one transaction.
pub struct ParsedCaEvents {
    pub activities: Vec<ConfidentialAssetActivity>,
}

/// Parses all confidential-asset events from a single transaction.
pub fn parse_ca_events(txn: &Transaction) -> ParsedCaEvents {
    let txn_version = txn.version as i64;
    let block_height = txn.block_height as i64;
    let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version).naive_utc();
    let now = chrono::Utc::now().naive_utc();

    let raw_events: &Vec<Event> = match txn.txn_data.as_ref() {
        Some(TxnData::User(u)) => &u.events,
        Some(TxnData::BlockMetadata(b)) => &b.events,
        Some(TxnData::Genesis(g)) => &g.events,
        Some(TxnData::Validator(v)) => &v.events,
        _ => return ParsedCaEvents { activities: vec![] },
    };

    let mut activities: Vec<ConfidentialAssetActivity> = Vec::new();

    for (index, event) in raw_events.iter().enumerate() {
        let type_str = event.type_str.as_str();
        let event_index = index as i64;

        let ca_event = match CaEvent::from_event(type_str, &event.data, txn_version) {
            Ok(Some(e)) => e,
            Ok(None) => continue,
            Err(e) => {
                warn!(
                    transaction_version = txn_version,
                    event_index, type_str, "Failed to parse CA event: {e:#}"
                );
                continue;
            },
        };

        let short_type = short_event_type(type_str).to_string();
        let asset_type = ca_event.asset_type();
        let from_address = ca_event.from_address();
        let to_address = ca_event.to_address();
        let event_data = serde_json::to_value(&ca_event).unwrap_or(serde_json::Value::Null);

        activities.push(ConfidentialAssetActivity {
            transaction_version: txn_version,
            event_index,
            event_type: short_type,
            asset_type,
            from_address,
            to_address,
            event_data,
            block_height,
            transaction_timestamp: block_timestamp,
            inserted_at: now,
        });
    }

    ParsedCaEvents { activities }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts the short variant name from a fully-qualified type string.
/// `"0x1::confidential_asset::Transferred"` → `"Transferred"`
fn short_event_type(type_str: &str) -> &str {
    type_str
        .rfind("::")
        .map(|pos| &type_str[pos + 2..])
        .unwrap_or(type_str)
}
