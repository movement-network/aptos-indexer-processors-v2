DROP INDEX IF EXISTS idx_evm_risk_scores_pending;

ALTER TABLE evm_address_risk_scores
    DROP COLUMN IF EXISTS to_be_updated;
