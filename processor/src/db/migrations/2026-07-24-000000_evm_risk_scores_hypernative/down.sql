ALTER TABLE evm_address_risk_scores
    DROP COLUMN IF EXISTS recommendation,
    DROP COLUMN IF EXISTS severity,
    ALTER COLUMN risk_label DROP NOT NULL,
    ALTER COLUMN risk_label DROP DEFAULT,
    ALTER COLUMN source     DROP NOT NULL,
    ALTER COLUMN source     DROP DEFAULT;
