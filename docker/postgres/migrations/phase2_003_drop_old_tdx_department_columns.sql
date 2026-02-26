-- Migration: Drop old column names from TDX Departments
-- Description: Remove dept_code and dept_name after successful migration to dept_id and department_name
-- Date: 2026-01-12
-- Phase: 2 - Departments Entity
-- Status: Cleanup - Drop old columns

BEGIN;

-- Validation: Check that new columns have data
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

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_code_count;
END $$;

-- Drop old column indexes first
DROP INDEX IF EXISTS silver.idx_tdx_departments_dept_code;

-- Drop old columns
ALTER TABLE silver.tdx_departments
  DROP COLUMN IF EXISTS dept_code,
  DROP COLUMN IF EXISTS dept_name;

-- Verify columns dropped
DO $$
DECLARE
    column_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name = 'tdx_departments'
      AND column_name IN ('dept_code', 'dept_name');

    IF column_count > 0 THEN
        RAISE EXCEPTION 'Failed to drop old columns: % columns still exist', column_count;
    END IF;

    RAISE NOTICE '✅ Successfully dropped 2 old columns from tdx_departments';
END $$;

COMMIT;
