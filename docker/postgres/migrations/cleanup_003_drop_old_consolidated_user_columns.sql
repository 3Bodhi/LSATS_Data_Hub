-- Migration: Drop old display_name from consolidated users table
-- Description: Remove display_name after migration to preferred_name
-- Date: 2026-01-12
-- Status: Phase 1 Cleanup
-- Dependencies: Transformation scripts now use preferred_name

BEGIN;

-- Validation: Check that new column has data
DO $$
DECLARE
    old_display_count INTEGER;
    new_preferred_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_display_count FROM silver.users WHERE display_name IS NOT NULL;
    SELECT COUNT(*) INTO new_preferred_count FROM silver.users WHERE preferred_name IS NOT NULL;

    IF old_display_count != new_preferred_count THEN
        RAISE EXCEPTION 'Data mismatch: display_name count (%) != preferred_name count (%)', old_display_count, new_preferred_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_display_count;
END $$;

-- Drop old column index first
DROP INDEX IF EXISTS silver.idx_users_display_name;

-- Drop old column
ALTER TABLE silver.users
  DROP COLUMN IF EXISTS display_name;

-- Verify column dropped
DO $$
DECLARE
    column_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'silver'
          AND table_name = 'users'
          AND column_name = 'display_name'
    ) INTO column_exists;

    IF column_exists THEN
        RAISE EXCEPTION 'Failed to drop display_name column';
    END IF;

    RAISE NOTICE '✅ Successfully dropped display_name from users table';
END $$;

COMMIT;
