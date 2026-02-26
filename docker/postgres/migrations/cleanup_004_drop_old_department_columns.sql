-- Migration: Drop old dept_name from consolidated departments table
-- Description: Remove redundant dept_name after standardization to department_name
-- Date: 2026-01-12
-- Status: Phase 1 Cleanup
-- Dependencies: All queries now use department_name

BEGIN;

-- Validation: Check that new column has data
DO $$
DECLARE
    old_name_count INTEGER;
    new_name_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_name_count FROM silver.departments WHERE dept_name IS NOT NULL;
    SELECT COUNT(*) INTO new_name_count FROM silver.departments WHERE department_name IS NOT NULL;

    IF old_name_count != new_name_count THEN
        RAISE EXCEPTION 'Data mismatch: dept_name count (%) != department_name count (%)', old_name_count, new_name_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_name_count;
END $$;

-- Drop old column
ALTER TABLE silver.departments
  DROP COLUMN IF EXISTS dept_name;

-- Verify column dropped
DO $$
DECLARE
    column_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'silver'
          AND table_name = 'departments'
          AND column_name = 'dept_name'
    ) INTO column_exists;

    IF column_exists THEN
        RAISE EXCEPTION 'Failed to drop dept_name column';
    END IF;

    RAISE NOTICE '✅ Successfully dropped dept_name from departments table';
END $$;

COMMIT;
