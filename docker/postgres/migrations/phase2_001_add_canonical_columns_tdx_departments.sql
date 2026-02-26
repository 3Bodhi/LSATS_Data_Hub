-- Migration: Add canonical column names to TDX Departments
-- Description: Add dept_id and department_name alongside existing dept_code and dept_name
-- Date: 2026-01-12
-- Phase: 2 - Departments Entity
-- Status: Step 1 - Add new columns (keep old for rollback)

BEGIN;

-- Add new canonical columns
ALTER TABLE silver.tdx_departments
  ADD COLUMN IF NOT EXISTS dept_id VARCHAR(50),
  ADD COLUMN IF NOT EXISTS department_name VARCHAR(255);

-- Copy data from old columns to new canonical columns
UPDATE silver.tdx_departments SET
  dept_id = dept_code,
  department_name = dept_name;

-- Create indexes on new columns
CREATE INDEX IF NOT EXISTS idx_tdx_departments_dept_id ON silver.tdx_departments(dept_id);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_department_name ON silver.tdx_departments(department_name);

-- Validation: Check data copied correctly
DO $$
DECLARE
    old_code_count INTEGER;
    new_id_count INTEGER;
    old_name_count INTEGER;
    new_name_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_code_count FROM silver.tdx_departments WHERE dept_code IS NOT NULL;
    SELECT COUNT(*) INTO new_id_count FROM silver.tdx_departments WHERE dept_id IS NOT NULL;
    SELECT COUNT(*) INTO old_name_count FROM silver.tdx_departments WHERE dept_name IS NOT NULL;
    SELECT COUNT(*) INTO new_name_count FROM silver.tdx_departments WHERE department_name IS NOT NULL;

    IF old_code_count != new_id_count THEN
        RAISE EXCEPTION 'Data mismatch: dept_code count (%) != dept_id count (%)', old_code_count, new_id_count;
    END IF;

    IF old_name_count != new_name_count THEN
        RAISE EXCEPTION 'Data mismatch: dept_name count (%) != department_name count (%)', old_name_count, new_name_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % dept_id records, % department_name records', new_id_count, new_name_count;
END $$;

COMMIT;

-- Verification query (run manually)
-- SELECT dept_code, dept_id, dept_name, department_name FROM silver.tdx_departments LIMIT 10;
