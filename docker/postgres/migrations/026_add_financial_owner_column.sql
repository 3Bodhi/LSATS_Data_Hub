-- Migration: Add financial_owner_uniqname column to silver.computers
-- Date: 2025-01-23
-- Purpose: Separate operational owner (owner_uniqname) from financial owner
--
-- Semantic distinction:
--   - owner_uniqname: TDX Owning Customer (operational/day-to-day owner)
--   - financial_owner_uniqname: TDX Financial Owner (financial responsibility)

BEGIN;

-- Add financial_owner_uniqname column
ALTER TABLE silver.computers
ADD COLUMN financial_owner_uniqname VARCHAR(8);

-- Add foreign key constraint to silver.users
ALTER TABLE silver.computers
ADD CONSTRAINT fk_computers_financial_owner
FOREIGN KEY (financial_owner_uniqname)
REFERENCES silver.users(uniqname)
ON DELETE SET NULL;

-- Create index for fast lookups
CREATE INDEX idx_computers_financial_owner
ON silver.computers(financial_owner_uniqname);

-- Add comment explaining the distinction
COMMENT ON COLUMN silver.computers.owner_uniqname IS
'Operational owner - who uses/manages the computer (TDX Owning Customer → KC Owner → AD Managed By)';

COMMENT ON COLUMN silver.computers.financial_owner_uniqname IS
'Financial owner - who is financially responsible for the computer (TDX Financial Owner only, no fallback)';

COMMIT;
