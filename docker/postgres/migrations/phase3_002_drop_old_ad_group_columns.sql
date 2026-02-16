-- Migration: Drop old LDAP column names from AD Groups
-- Description: Remove name and mail after successful migration to group_name and group_email
-- Date: 2026-01-12
-- Phase: 3 - Groups Entity (Final Phase!)
-- Status: Cleanup - Drop old columns

BEGIN;

-- Validation: Check that new columns have data
DO $$
DECLARE
    old_name_count INTEGER;
    new_name_count INTEGER;
    old_mail_count INTEGER;
    new_email_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_name_count FROM silver.ad_groups WHERE name IS NOT NULL;
    SELECT COUNT(*) INTO new_name_count FROM silver.ad_groups WHERE group_name IS NOT NULL;
    SELECT COUNT(*) INTO old_mail_count FROM silver.ad_groups WHERE mail IS NOT NULL;
    SELECT COUNT(*) INTO new_email_count FROM silver.ad_groups WHERE group_email IS NOT NULL;

    IF old_name_count != new_name_count THEN
        RAISE EXCEPTION 'Data mismatch: name count (%) != group_name count (%)', old_name_count, new_name_count;
    END IF;

    IF old_mail_count != new_email_count THEN
        RAISE EXCEPTION 'Data mismatch: mail count (%) != group_email count (%)', old_mail_count, new_email_count;
    END IF;

    RAISE NOTICE '✅ Validation passed: % records have both old and new columns', old_name_count;
END $$;

-- Drop old column indexes first
DROP INDEX IF EXISTS silver.idx_ad_groups_name;
DROP INDEX IF EXISTS silver.idx_ad_groups_mail;

-- Drop old columns
ALTER TABLE silver.ad_groups
  DROP COLUMN IF EXISTS name,
  DROP COLUMN IF EXISTS mail;

-- Verify columns dropped
DO $$
DECLARE
    column_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name = 'ad_groups'
      AND column_name IN ('name', 'mail');

    IF column_count > 0 THEN
        RAISE EXCEPTION 'Failed to drop old columns: % columns still exist', column_count;
    END IF;

    RAISE NOTICE '✅ Successfully dropped 2 old columns from ad_groups';
END $$;

COMMIT;
