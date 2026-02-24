-- Migration: Drop old LDAP column names from MCommunity Users
-- Description: Remove uid, given_name, sn, display_name, mail, telephone_number after successful migration
-- Date: 2026-01-12
-- Status: Phase 1 Cleanup
-- Dependencies: Transformation scripts now use canonical names (uniqname, first_name, etc.)

BEGIN;

-- Validation: Check that new columns have data
DO $$
DECLARE
    old_uid_count INTEGER;
    new_uniqname_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_uid_count FROM silver.mcommunity_users WHERE uid IS NOT NULL;
    SELECT COUNT(*) INTO new_uniqname_count FROM silver.mcommunity_users WHERE uniqname IS NOT NULL;

    IF old_uid_count != new_uniqname_count THEN
        RAISE EXCEPTION 'Data mismatch: uid count (%) != uniqname count (%)', old_uid_count, new_uniqname_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_uid_count;
END $$;

-- Drop old column indexes first
DROP INDEX IF EXISTS silver.idx_mcommunity_users_uid;
DROP INDEX IF EXISTS silver.idx_mcommunity_users_given_name;
DROP INDEX IF EXISTS silver.idx_mcommunity_users_mail;

-- Drop old columns
ALTER TABLE silver.mcommunity_users
  DROP COLUMN IF EXISTS uid,
  DROP COLUMN IF EXISTS given_name,
  DROP COLUMN IF EXISTS sn,
  DROP COLUMN IF EXISTS display_name,
  DROP COLUMN IF EXISTS mail,
  DROP COLUMN IF EXISTS telephone_number;

-- Verify columns dropped
DO $$
DECLARE
    column_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name = 'mcommunity_users'
      AND column_name IN ('uid', 'given_name', 'sn', 'display_name', 'mail', 'telephone_number');

    IF column_count > 0 THEN
        RAISE EXCEPTION 'Failed to drop old columns: % columns still exist', column_count;
    END IF;

    RAISE NOTICE '✅ Successfully dropped 6 old columns from mcommunity_users';
END $$;

COMMIT;
