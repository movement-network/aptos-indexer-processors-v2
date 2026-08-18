// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

//! Decoder for the LayerZero V2 OFT message received on Movement.
//!
//! When a user on Ethereum calls `OFT.send()` with a non-empty `composeMsg`,
//! `OFTCore._buildMsgAndOptions()` prepends `addressToBytes32(msg.sender)` to
//! the compose payload before passing it to `OFTMsgCodec.encode()`. The resulting
//! on-wire message layout (as it appears in the executor's `lz_receive` entry-
//! function argument on Movement) is:
//!
//!   bytes  0–31: `to`       (bytes32, Movement recipient, zero-left-padded)
//!   bytes 32–39: `amountSD` (uint64, big-endian)
//!   bytes 40–71: `bytes32(msg.sender)` — EVM address of the user who called
//!                `OFT.send()` on Ethereum; first 12 bytes are zero-padding,
//!                last 20 bytes are the 20-byte EVM address
//!   bytes 72+:   user-supplied `composeMsg` payload (arbitrary)
//!
//! This field is **only present** when `sendParam.composeMsg` is non-empty on
//! the Ethereum side. Standard transfers (no compose) produce a 40-byte message
//! with no EVM sender; `decode_oft_sender` returns `None` for those.
//!
//! The typical executor entry function on Movement is something like:
//!   `lz_receive(executor, src_eid: u32, sender: vector<u8>, nonce: u64,
//!               guid: vector<u8>, message: vector<u8>, extra_data: vector<u8>)`
//! We scan all arguments for one that has a valid EVM address at the compose
//! prefix position rather than hard-coding an argument index.

/// Byte offset of the compose-sender word within the OFT message.
/// Derived from: to (32) + amountSD (8) = 40.
const COMPOSE_SENDER_OFFSET: usize = 40;
const COMPOSE_SENDER_END: usize = COMPOSE_SENDER_OFFSET + 32; // 72

/// Extract the EVM address of the user who initiated the `OFT.send()` call on
/// Ethereum, from a LayerZero V2 OFT message that was encoded with compose data.
///
/// Returns `None` for:
/// - Standard (compose-less) OFT messages (< 72 bytes).
/// - Messages where the compose-sender word is not a valid EVM address (non-zero
///   in the upper 12 bytes, or all-zero in the lower 20 bytes).
fn decode_oft_sender(bytes: &[u8]) -> Option<String> {
    if bytes.len() < COMPOSE_SENDER_END {
        return None;
    }
    let word = &bytes[COMPOSE_SENDER_OFFSET..COMPOSE_SENDER_END];
    // A properly ABI-encoded EVM address has the 20-byte address right-aligned
    // in a 32-byte word; the leading 12 bytes must all be zero.
    if word[..12].iter().any(|b| *b != 0) {
        return None;
    }
    let addr = &word[12..];
    if addr.iter().all(|b| *b == 0) {
        return None;
    }
    Some(format!("0x{}", hex::encode(addr)))
}

/// Parse a JSON-quoted hex argument as it appears in `EntryFunctionPayload.arguments`
/// (e.g. `"\"0x5a2e...\""`), returning the raw bytes.
fn parse_hex_arg(arg_json: &str) -> Option<Vec<u8>> {
    let s: String = serde_json::from_str(arg_json).ok()?;
    let h = s.strip_prefix("0x").unwrap_or(&s);
    hex::decode(h).ok()
}

/// Scan all entry-function arguments for a LayerZero OFT message that contains
/// compose data with a valid EVM sender address. Returns the first match.
///
/// We scan rather than index into a fixed position because the argument order of
/// the `lz_receive` entry function can vary across OFT module versions. The
/// detection criteria (>= 72 bytes, valid EVM word at offset 40) are specific
/// enough to avoid false positives: the other hex arguments in a typical
/// `lz_receive` call are either 32-byte fixed-size fields (guid, sender) or
/// short/empty vectors (extra_data), none of which satisfy both checks.
pub fn scan_args_for_oft_sender(args: &[String]) -> Option<String> {
    for arg in args {
        if let Some(bytes) = parse_hex_arg(arg) {
            if let Some(evm) = decode_oft_sender(&bytes) {
                return Some(evm);
            }
        }
    }
    None
}

/// Extract the LayerZero message GUID from a transaction's payload arguments.
///
/// In the LZ V2 executor script the GUID is always a 32-byte keccak hash passed
/// as one of the `vector<u8>` arguments (arg[3] in the typical 8-arg script).
/// We identify it by two criteria that hold for all LZ V2 packets:
///   - exactly 32 bytes long, AND
///   - upper 12 bytes are NOT all zero (distinguishing it from the `sender`
///     arg which is a zero-padded EVM address word, upper 12 = 0x00).
///
/// Returns the GUID as a `0x`-prefixed 64-hex-char string, or `None` if no
/// matching argument is found (e.g. Circle USDCx entry-function calls have no GUID).
pub fn extract_guid_from_args(args: &[String]) -> Option<String> {
    for arg in args {
        let Some(bytes) = parse_hex_arg(arg) else {
            continue;
        };
        if bytes.len() == 32 && bytes[..12].iter().any(|b| *b != 0) {
            return Some(format!("0x{}", hex::encode(&bytes)));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic OFT message with compose sender at offset 40.
    /// Layout: to (32) + amountSD (8) + bytes32(evm_sender) (32) + user_compose (4)
    fn make_oft_msg_with_compose(evm_sender_hex: &str) -> Vec<u8> {
        let mut msg = vec![0u8; 76]; // 32 + 8 + 32 + 4
                                     // to: zero-padded Movement address (bytes 0-31, leave as 0x00)
                                     // amountSD: 1000 in big-endian uint64 (bytes 32-39)
        msg[39] = 232; // 1000 = 0x03E8
        msg[38] = 3;
        // compose sender as bytes32 at bytes 40-71
        let raw = hex::decode(evm_sender_hex.trim_start_matches("0x")).unwrap();
        assert_eq!(raw.len(), 20);
        // right-align in the 32-byte word (bytes 52..72)
        msg[52..72].copy_from_slice(&raw);
        // user composeMsg: 4 bytes at 72..76 (leave as 0)
        msg
    }

    #[test]
    fn decodes_compose_sender() {
        let evm = "0x8f5633d77eb1d6bf6c0d357148135a91b0e1f87f";
        let msg = make_oft_msg_with_compose(evm);
        assert_eq!(decode_oft_sender(&msg).as_deref(), Some(evm));
    }

    #[test]
    fn rejects_standard_40_byte_message() {
        let msg = vec![0u8; 40];
        assert!(decode_oft_sender(&msg).is_none());
    }

    #[test]
    fn rejects_non_evm_word_at_compose_offset() {
        // Non-zero upper bytes — not a valid EVM address word.
        let mut msg = vec![0u8; 76];
        msg[40] = 0xDE; // upper byte of the 32-byte word is non-zero
        assert!(decode_oft_sender(&msg).is_none());
    }

    #[test]
    fn rejects_all_zero_evm_address() {
        let msg = vec![0u8; 76]; // all zeros → address is zero, rejected
        assert!(decode_oft_sender(&msg).is_none());
    }

    #[test]
    fn scan_args_finds_oft_sender_in_correct_arg() {
        let evm = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        let msg_bytes = make_oft_msg_with_compose(evm);
        let msg_hex = format!("\"0x{}\"", hex::encode(&msg_bytes));

        // Simulate typical lz_receive arguments:
        //   arg[0] src_eid as string, arg[1] 32-byte sender (OFT contract),
        //   arg[2] nonce string, arg[3] 32-byte guid, arg[4] OFT message, arg[5] extra
        let args = vec![
            "\"30101\"".to_string(),
            "\"0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\"".to_string(),
            "\"42\"".to_string(),
            "\"0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd\"".to_string(),
            msg_hex,
            "\"0x\"".to_string(),
        ];
        assert_eq!(scan_args_for_oft_sender(&args).as_deref(), Some(evm));
    }

    #[test]
    fn scan_args_returns_none_for_standard_transfer() {
        // Standard 40-byte OFT message — no compose, no EVM sender
        let msg_40 = vec![0u8; 40];
        let msg_hex = format!("\"0x{}\"", hex::encode(&msg_40));
        let args = vec![
            "\"30101\"".to_string(),
            "\"0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\"".to_string(),
            msg_hex,
        ];
        assert!(scan_args_for_oft_sender(&args).is_none());
    }

    #[test]
    fn parse_hex_arg_roundtrip() {
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let arg_json = "\"0xdeadbeef\"";
        assert_eq!(parse_hex_arg(arg_json), Some(bytes));
    }
}
