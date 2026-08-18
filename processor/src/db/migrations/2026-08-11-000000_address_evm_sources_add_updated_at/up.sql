ALTER TABLE address_evm_sources
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

-- Backfill existing rows: the first update time is the insert time.
UPDATE address_evm_sources SET updated_at = inserted_at;

CREATE INDEX IF NOT EXISTS idx_aes_updated_at ON address_evm_sources (updated_at);
