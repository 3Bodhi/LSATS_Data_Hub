-- Migration: Add department fields to silver.tdx_labs
-- Date: 2024-11-24
-- Purpose: Enable department matching from TDX department_name field

BEGIN;

-- Add department fields
ALTER TABLE silver.tdx_labs
  ADD COLUMN IF NOT EXISTS department_id VARCHAR(50),
  ADD COLUMN IF NOT EXISTS department_match_method VARCHAR(50),
  ADD COLUMN IF NOT EXISTS department_match_confidence DECIMAL(3,2);

-- Add comments for documentation
COMMENT ON COLUMN silver.tdx_labs.department_id IS 
  'FK to silver.departments(dept_id), matched from department_name field using fuzzy matching or exact code extraction';

COMMENT ON COLUMN silver.tdx_labs.department_match_method IS
  'How the department was matched: exact_code (dept code extracted from name), fuzzy_match (similarity matching), pi_inherit (from PI user record)';

COMMENT ON COLUMN silver.tdx_labs.department_match_confidence IS
  'Confidence score 0.00-1.00 for the department match. 1.0 for exact matches, 0.65-0.99 for fuzzy matches';

-- Create index for department lookup
CREATE INDEX IF NOT EXISTS idx_tdx_labs_department_id ON silver.tdx_labs(department_id);

COMMIT;
