-- Confidential Asset processor table
--
-- One row per confidential_asset module event (all event types).
-- Encrypted balance fields are opaque Twisted-ElGamal ciphertexts stored as JSONB.
-- The event_data column stores the fully typed event payload as self-describing JSONB:
--   {"type":"Transferred","data":{...}}
CREATE TABLE IF NOT EXISTS confidential_asset_activities (
    transaction_version   BIGINT        NOT NULL,
    event_index           BIGINT        NOT NULL,
    -- Short event type: "Transferred", "Registered", "FreezeChanged", etc.
    event_type            VARCHAR(50)   NOT NULL,
    -- Fungible-asset metadata object address; NULL for AllowListChanged.
    asset_type            VARCHAR(66),
    -- Sender / primary actor address; NULL for governance events.
    from_address          VARCHAR(66),
    -- Recipient address; populated only for Deposited, Withdrawn, Transferred.
    to_address            VARCHAR(66),
    -- Full typed event payload: {"type":"…","data":{…}}.
    event_data            JSONB         NOT NULL,
    block_height          BIGINT        NOT NULL,
    transaction_timestamp TIMESTAMP     NOT NULL,
    inserted_at           TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);

CREATE INDEX IF NOT EXISTS caa_asset_idx   ON confidential_asset_activities (asset_type);
CREATE INDEX IF NOT EXISTS caa_from_idx    ON confidential_asset_activities (from_address);
CREATE INDEX IF NOT EXISTS caa_to_idx      ON confidential_asset_activities (to_address);
CREATE INDEX IF NOT EXISTS caa_type_idx    ON confidential_asset_activities (event_type);
CREATE INDEX IF NOT EXISTS caa_insat_idx   ON confidential_asset_activities (inserted_at);
