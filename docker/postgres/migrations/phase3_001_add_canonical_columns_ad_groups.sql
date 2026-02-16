-- Migration: Add canonical column names to AD Groups
-- Description: Add group_name and group_email alongside existing name and mail
-- Date: 2026-01-12
-- Phase: 3 - Groups Entity (Final Phase!)
-- Status: Step 1 - Add new columns (keep old for rollback)

BEGIN;

-- Add new canonical columns
ALTER TABLE silver.ad_groups
  ADD COLUMN IF NOT EXISTS group_name VARCHAR(255),
  ADD COLUMN IF NOT EXISTS group_email VARCHAR(255);

-- Copy data from old columns to new canonical columns
UPDATE silver.ad_groups SET
  group_name = name,
  group_email = mail;

-- Create indexes on new columns
CREATE INDEX IF NOT EXISTS idx_ad_groups_group_name ON silver.ad_groups(group_name);
CREATE INDEX IF NOT EXISTS idx_ad_groups_group_email ON silver.ad_groups(group_email);

-- Validation: Check data copied correctly
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

    RAISE NOTICE '✅ Validation passed: % group_name records, % group_email records', new_name_count, new_email_count;
END $$;

COMMIT;

-- Verification query (run manually)
-- SELECT name, group_name, mail, group_email FROM silver.ad_groups LIMIT 10;
