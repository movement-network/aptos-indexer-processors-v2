-- Address reputation processor (P0): transfer graph + bridge inflows + per-address score.

-- One row per FA / coin transfer between two distinct Aptos addresses.
CREATE TABLE IF NOT EXISTS address_transfer_edges (
    transaction_version BIGINT NOT NULL,
    event_index BIGINT NOT NULL,
    from_address VARCHAR(66) NOT NULL,
    to_address VARCHAR(66) NOT NULL,
    asset_type VARCHAR(1100),
    amount NUMERIC NOT NULL,
    is_bridge_inflow BOOLEAN NOT NULL DEFAULT FALSE,
    bridge_name VARCHAR(64),
    transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX IF NOT EXISTS idx_ate_to_ts ON address_transfer_edges (to_address, transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ate_from_ts ON address_transfer_edges (from_address, transaction_timestamp);
CREATE INDEX IF NOT EXISTS idx_ate_bridge ON address_transfer_edges (bridge_name) WHERE is_bridge_inflow;

-- (Bridge configuration lives in the processor's YAML config, not in the database.
-- This keeps mainnet/testnet/devnet deploys swappable without a SQL migration. See
-- AddressReputationConfig.bridges in processor/src/processors/address_reputation/address_reputation_config.rs.)

-- Bridge deposit events on Aptos. The cross-chain EVM source is the "head" of any trace.
CREATE TABLE IF NOT EXISTS bridge_inflows (
    transaction_version BIGINT NOT NULL,
    event_index BIGINT NOT NULL,
    bridge_name VARCHAR(64) NOT NULL,
    aptos_recipient VARCHAR(66) NOT NULL,
    evm_source VARCHAR(66),
    src_chain_id INTEGER,
    asset_type VARCHAR(1100),
    amount NUMERIC NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX IF NOT EXISTS idx_bi_recipient ON bridge_inflows (aptos_recipient);
CREATE INDEX IF NOT EXISTS idx_bi_evm_source ON bridge_inflows (evm_source);

-- Risk scores for EVM addresses, populated by an external screening pipeline.
CREATE TABLE IF NOT EXISTS evm_address_risk_scores (
    evm_address VARCHAR(66) NOT NULL PRIMARY KEY,
    risk_score NUMERIC(5,4) NOT NULL,
    risk_label VARCHAR(32),
    source VARCHAR(64),
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Per-address rollup of upstream EVM funding sources.
-- One row per (movement_address, asset_type, evm_address).
--
-- `evm_fund` is the cumulative amount of `asset_type` that has flowed into
-- `movement_address` attributable to `evm_address`. Monotone non-decreasing --
-- outflows are NOT debited. Interpret at read time via
-- `evm_fund / current_balance(movement_address, asset_type)`.
--
-- Propagation (see AddressReputationStorer):
--   * Bridge inflow with evm_source E delivering X to B:
--       upsert (B, asset, E, evm_fund += X, hops_min = 0).
--   * Non-bridge transfer A -> B of X (asset), for each row (A, asset, E, w):
--       upsert (B, asset, E,
--               evm_fund += X * w / SUM_E(w over A,asset),
--               hops_min = LEAST(hops_min, A's hops_min + 1)).
--     The sender's rows are NOT touched.
--
-- `last_seen_ord` is a monotone i64 = (txn_version << 24) | event_index, so
-- "most recent X evm sources for this address" is an indexed range scan.
CREATE TABLE IF NOT EXISTS address_evm_sources (
    movement_address VARCHAR(66)   NOT NULL,
    asset_type       VARCHAR(1100) NOT NULL,
    evm_address      VARCHAR(66)   NOT NULL,
    evm_fund         NUMERIC       NOT NULL,
    first_seen_ord   BIGINT        NOT NULL,
    last_seen_ord    BIGINT        NOT NULL,
    hops_min         INTEGER       NOT NULL,
    inserted_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (movement_address, asset_type, evm_address)
);
CREATE INDEX IF NOT EXISTS idx_aes_recent
    ON address_evm_sources (movement_address, asset_type, last_seen_ord DESC);
CREATE INDEX IF NOT EXISTS idx_aes_evm ON address_evm_sources (evm_address);

-- Seed: Circle USDCx bridge (Movement's bridged USDC, ../usdc-bridge/smart_contracts/sources/usdcx.move).
-- The Mint event is emitted when funds arrive from a remote chain (USDC -> USDCx mint).
-- Fields available in the event:
--   recipient (address, Movement-side owner), amount (u64), fee_amount, relayer,
--   remote_domain (u32, source chain id), remote_token (address, source-chain USDC), nonce.
-- The EVM depositor address is NOT in the event payload -- it is carried inside the
-- `intent_payload` entry function argument and would need BCS-decoding to recover.
-- For P0 we record the inflow with NULL evm_source; downstream can backfill it.
--
