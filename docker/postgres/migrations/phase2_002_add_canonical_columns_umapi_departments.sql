-- Migration: Add canonical column name to UMAPI Departments
-- Description: Add department_name alongside existing dept_description
-- Date: 2026-01-12
-- Phase: 2 - Departments Entity
-- Status: Step 1 - Add new column (keep old for rollback)

BEGIN;

-- Add new canonical column
ALTER TABLE silver.umapi_departments
  ADD COLUMN IF NOT EXISTS department_name VARCHAR(255);

-- Copy data from old column to new canonical column
UPDATE silver.umapi_departments SET
  department_name = dept_description;

-- Create index on new column
CREATE INDEX IF NOT EXISTS idx_umapi_departments_department_name ON silver.umapi_departments(department_name);

-- Validation: Check data copied correctly
DO $$
DECLARE
    old_desc_count INTEGER;
    new_name_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_desc_count FROM silver.umapi_departments WHERE dept_description IS NOT NULL;
    SELECT COUNT(*) INTO new_name_count FROM silver.umapi_departments WHERE department_name IS NOT NULL;

    IF old_desc_count != new_name_count THEN
        RAISE EXCEPTION 'Data mismatch: dept_description count (%) != department_name count (%)', old_desc_count, new_name_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % department_name records', new_name_count;
END $$;

COMMIT;

-- Verification query (run manually)
-- SELECT dept_id, dept_description, department_name FROM silver.umapi_departments LIMIT 10;
