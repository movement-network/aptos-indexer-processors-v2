# address_reputation processor

Builds an owner-keyed transfer graph on Movement and annotates bridge deposits
as terminal "head" nodes, so any address's funds can be backtraced to the bridge
they came from.

**Scope: stables + FA v2 only.** Tracks USDCx (Circle) and USDC.e / USDT.e
(LayerZero). All in-scope tokens are FA v2 — coin v1 and non-stable assets are
intentionally out of scope for P0.

## Tables

- `address_transfer_edges` — one row per FA transfer; `from`/`to` are owners
- `bridge_inflows` — head nodes (bridge module → recipient, with `src_chain_id`)
- `evm_address_risk_scores` — external input, NULL `evm_source` allowed
- `address_reputation` — materialized score, `max(current, decay * sender)` upsert

See `db/queries/trace.sql` for the recursive CTE that walks the graph backward,
short-circuiting on `is_bridge_inflow`.

## Bridge config (YAML, not SQL)

Bridges live in `AddressReputationConfig.bridges` — mainnet/testnet differ by
config file, not migration. Restart picks up changes.

Testnet seed (`example-configs/address_reputation.testnet.yaml`), each verified
against captured on-chain events:

| name | module | event |
|---|---|---|
| `circle_usdcx` | `0x989577...` | `::usdcx::Mint` |
| `layerzero_weth_e` | `0x2fa1f2...` | `::oft_core::OftReceived` |
| `layerzero_usdt_e` | `0x9cda67...` | `::oft_core::OftReceived` |
| `layerzero_usdc_e` | `0x339873...` | `::oft_core::OftReceived` |
| `layerzero_usdc_e_alt` | `0xdbfc7b...` | dormant, disabled |
| `layerzero_send_oft_demo` | `0xe02484...` | `::oft_core::OftReceived` |

## Correctness mechanics

- **Owner resolution**: per-txn scan of `WriteResource` for `0x1::object::ObjectCore`
  builds `storage_id → owner`. Mirrors `fungible_asset_processor_helpers::parse_v2_coin`
  Loop 1. Without this, bridge mints (owner-keyed) wouldn't connect to subsequent
  user transfers (storage_id-keyed).
- **Asset resolution**: per-txn scan of `0x1::fungible_asset::FungibleStore` builds
  `storage_id → metadata` so every edge carries `asset_type`.
- **Synthetic bridge edge**: bridge mints emit only a `Deposit` (no paired
  `Withdraw`); extractor synthesizes a `bridge_module → recipient` edge so the
  head always lands in the graph.

## P0 limitations

- Coin v1 events skipped. Every in-scope token is FA v2 — USDCx, the LayerZero
  `.e` family, and native MOVE all live as fungible assets, not legacy `Coin<T>`.
  Coin v1 on Movement is dominated by gas fees and legacy test tokens, none of
  which we want in the graph. Revisit if a future bridge starts minting into
  `Coin<T>`.
- `evm_source` is always NULL for now — neither USDCx `Mint` nor LayerZero
  `OftReceived` carries it in the event; it's in entry-function args / LZ packet.
  P1.
- Reputation propagation is naive `max(current, decay * sender_score)` — cycle-
  safe and idempotent, but not amount-weighted.

## Test

```
cargo test -p processor --test address_reputation_smoke -- --nocapture
```

Builds minimal protos from real testnet txns 158025629 (USDCx mint) and
163802127 (user FA transfer); asserts bridge head edges, owner-keyed user
edges, and `asset_type` resolution.

## Run on testnet

```
diesel migration run --database-url postgres://...
cargo run -p processor --release -- \
  --config processor/example-configs/address_reputation.testnet.yaml
```
