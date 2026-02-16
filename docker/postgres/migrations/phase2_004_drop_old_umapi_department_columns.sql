-- Migration: Drop old column name from UMAPI Departments
-- Description: Remove dept_description after successful migration to department_name
-- Date: 2026-01-12
-- Phase: 2 - Departments Entity
-- Status: Cleanup - Drop old column

BEGIN;

-- Validation: Check that new column has data
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

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_desc_count;
END $$;

-- Drop old column
ALTER TABLE silver.umapi_departments
  DROP COLUMN IF EXISTS dept_description;

-- Verify column dropped
DO $$
DECLARE
    column_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'silver'
          AND table_name = 'umapi_departments'
          AND column_name = 'dept_description'
    ) INTO column_exists;

    IF column_exists THEN
        RAISE EXCEPTION 'Failed to drop dept_description column';
    END IF;

    RAISE NOTICE '✅ Successfully dropped dept_description from umapi_departments';
END $$;

COMMIT;
