-- Migration: Add department fields to silver.ad_labs
-- Date: 2024-11-24
-- Purpose: Enable department extraction from AD OU hierarchy

BEGIN;

-- Add department fields
ALTER TABLE silver.ad_labs
  ADD COLUMN IF NOT EXISTS department_name VARCHAR(255),
  ADD COLUMN IF NOT EXISTS department_id VARCHAR(50),
  ADD COLUMN IF NOT EXISTS department_match_method VARCHAR(50),
  ADD COLUMN IF NOT EXISTS department_match_confidence DECIMAL(3,2);

-- Add comments for documentation
COMMENT ON COLUMN silver.ad_labs.department_name IS 
  'Department name extracted from AD OU hierarchy (typically 2nd level OU)';

COMMENT ON COLUMN silver.ad_labs.department_id IS
  'FK to silver.departments(dept_id), matched from extracted department_name using fuzzy matching';

COMMENT ON COLUMN silver.ad_labs.department_match_method IS
  'How the department was matched: fuzzy_match (similarity matching), exact_code (if dept code found in OU)';

COMMENT ON COLUMN silver.ad_labs.department_match_confidence IS
  'Confidence score 0.00-1.00 for the department match';

-- Create index for department lookup
CREATE INDEX IF NOT EXISTS idx_ad_labs_department_id ON silver.ad_labs(department_id);

COMMIT;
