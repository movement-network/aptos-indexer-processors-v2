DROP INDEX IF EXISTS idx_bi_lz_guid;
ALTER TABLE bridge_inflows DROP COLUMN IF EXISTS lz_guid;
