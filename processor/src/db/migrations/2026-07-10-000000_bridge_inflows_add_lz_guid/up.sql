-- Add LayerZero message GUID to bridge_inflows.
-- The GUID is a 32-byte hash (0x-prefixed, 66 chars) emitted by the LZ executor
-- script as arg[3]. It is NULL for non-LZ bridges (e.g. Circle USDCx).
-- Used to join with Ethereum-side OFTSent events to recover the EVM sender.
ALTER TABLE bridge_inflows ADD COLUMN IF NOT EXISTS lz_guid VARCHAR(66);
CREATE INDEX IF NOT EXISTS idx_bi_lz_guid ON bridge_inflows (lz_guid) WHERE lz_guid IS NOT NULL;
