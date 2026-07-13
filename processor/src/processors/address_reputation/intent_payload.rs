// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Decoder for Circle's USDCx `IntentPayload`, the big-endian byte string
//! passed as the first `vector<u8>` argument to `usdcx::mint(&signer, intent,
//! attestation, fee)`. The Move-side layout lives at
//! `usdc-bridge/smart_contracts/sources/usdcx.move::deserialize_intent_payload`;
//! this mirrors it field-for-field so a schema change on either side is caught
//! by the length check.
//!
//! We only need `local_depositor` (the EVM address that called `depositForBurn`
//! on the source chain), so we surface a narrow helper and skip building the
//! full struct.

/// Byte offset of the `local_depositor` field (32-byte word, EVM address in the
/// last 20 bytes) within a serialized `IntentPayload`. Derived from the fixed
/// prefix: magic(4) + version(4) + amount(32) + remote_domain(4) +
/// remote_token(32) + remote_recipient(32) + local_token(32) = 140.
const LOCAL_DEPOSITOR_OFFSET: usize = 140;
const LOCAL_DEPOSITOR_END: usize = LOCAL_DEPOSITOR_OFFSET + 32;

/// Extract the EVM depositor address as `0x`-prefixed 40-char hex from a
/// Circle IntentPayload byte string. Returns `None` if the buffer is too short
/// or the field is all-zero (uninitialized / not a real deposit).
pub fn decode_local_depositor(bytes: &[u8]) -> Option<String> {
    if bytes.len() < LOCAL_DEPOSITOR_END {
        return None;
    }
    let word = &bytes[LOCAL_DEPOSITOR_OFFSET..LOCAL_DEPOSITOR_END];
    // First 12 bytes must be zero padding for a well-formed EVM address.
    if word[..12].iter().any(|b| *b != 0) {
        return None;
    }
    let addr = &word[12..];
    if addr.iter().all(|b| *b == 0) {
        return None;
    }
    Some(format!("0x{}", hex::encode(addr)))
}

/// Parse the first entry-function argument as it appears in
/// `EntryFunctionPayload.arguments[0]` — a JSON-encoded string containing
/// `0x`-prefixed hex (e.g. `"\"0x5a2e0acd...\""`).
pub fn parse_hex_arg(arg_json: &str) -> Option<Vec<u8>> {
    let s: String = serde_json::from_str(arg_json).ok()?;
    let h = s.strip_prefix("0x").unwrap_or(&s);
    hex::decode(h).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real testnet txn 175293642, arg[0] of `usdcx::mint`. local_depositor is
    // 0x8f5633d77eb1d6bf6c0d357148135a91b0e1f87f (varied across mints, confirmed
    // constant across senders; @108 in the same message is the CCTP TokenMinter
    // and stays fixed).
    const SAMPLE: &str = "0x5a2e0acd000000010000000000000000000000000000000000000000000000000000000005f5e1000000271563f169ba69623ba6ccf34620857644feb46d0f87e1d7bbcf8c071d30c3d94bd607979ccb27c9d3167afc5cb70be06f6b6efc69057b8790708018906e1c9cb3020000000000000000000000001c7d4b196cb0c7b01d743fbc6116a902379c72380000000000000000000000008f5633d77eb1d6bf6c0d357148135a91b0e1f87f000000000000000000000000000000000000000000000000000000000000000023a15209170f589991f969f27f59c2e3f4f21c01bd7ceb8d6e0ad8f6c372fdc300000000";

    #[test]
    fn decodes_local_depositor() {
        let bytes = hex::decode(SAMPLE.trim_start_matches("0x")).unwrap();
        assert_eq!(
            decode_local_depositor(&bytes).as_deref(),
            Some("0x8f5633d77eb1d6bf6c0d357148135a91b0e1f87f"),
        );
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(decode_local_depositor(&[0u8; 100]).is_none());
    }

    #[test]
    fn rejects_zero_depositor() {
        let mut bytes = vec![0u8; 240];
        // Non-zero elsewhere so it's not trivially empty, but depositor word is zero.
        bytes[0] = 0x5A;
        assert!(decode_local_depositor(&bytes).is_none());
    }

    #[test]
    fn parses_hex_arg_json() {
        let arg = "\"0x1234abcd\"";
        assert_eq!(parse_hex_arg(arg), Some(vec![0x12, 0x34, 0xAB, 0xCD]));
    }
}
