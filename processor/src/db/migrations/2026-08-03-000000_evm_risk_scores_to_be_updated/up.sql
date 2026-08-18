ALTER TABLE evm_address_risk_scores
    ADD COLUMN IF NOT EXISTS to_be_updated BOOLEAN NOT NULL DEFAULT TRUE;

-- Addresses that already have a valid score do not need re-screening.
UPDATE evm_address_risk_scores SET to_be_updated = FALSE WHERE risk_score > 0;

-- Partial index: only indexes the small set of rows still awaiting a valid
-- screening result, keeping the index tiny and writes cheap.
CREATE INDEX IF NOT EXISTS idx_evm_risk_scores_pending
    ON evm_address_risk_scores (evm_address)
    WHERE to_be_updated = TRUE;
