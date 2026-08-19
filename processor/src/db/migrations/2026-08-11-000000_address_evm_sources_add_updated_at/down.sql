DROP INDEX IF EXISTS idx_aes_updated_at;
ALTER TABLE address_evm_sources DROP COLUMN IF EXISTS updated_at;
