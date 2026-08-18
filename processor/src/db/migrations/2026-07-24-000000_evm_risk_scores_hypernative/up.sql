-- Backfill existing nulls before making columns NOT NULL.
UPDATE evm_address_risk_scores SET risk_label = 'default' WHERE risk_label IS NULL;
UPDATE evm_address_risk_scores SET source     = ''            WHERE source     IS NULL;

ALTER TABLE evm_address_risk_scores
    ALTER COLUMN risk_label SET NOT NULL,
    ALTER COLUMN risk_label SET DEFAULT 'default',
    ALTER COLUMN source     SET NOT NULL,
    ALTER COLUMN source     SET DEFAULT '',
    ADD COLUMN IF NOT EXISTS recommendation VARCHAR(64) NOT NULL DEFAULT 'not available',
    ADD COLUMN IF NOT EXISTS severity       VARCHAR(32) NOT NULL DEFAULT 'not available';
