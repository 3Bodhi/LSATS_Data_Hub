-- Migration: 024a_rename_legacy_users_to_users_legacy.sql
-- Purpose: Rename existing silver.users table to silver.users_legacy
-- Date: 2025-11-23
-- 
-- Strategy: Branching migration pattern - preserve existing table with new name
-- before creating consolidated table. This maintains foreign key dependencies
-- and allows for data migration validation.
--
-- Foreign key dependencies to preserve:
--   - silver.computers.owner_uniqname → silver.users(uniqname)
--   - silver.lab_managers.manager_uniqname → silver.users(uniqname)

-- Step 1: Drop FK constraints temporarily
ALTER TABLE silver.computers DROP CONSTRAINT IF EXISTS fk_computers_owner_user;
ALTER TABLE silver.lab_managers DROP CONSTRAINT IF EXISTS lab_managers_manager_uniqname_fkey;

-- Step 2: Rename existing table
ALTER TABLE silver.users RENAME TO users_legacy;

-- Step 3: Rename indexes to match new table name
ALTER INDEX IF EXISTS users_pkey RENAME TO users_legacy_pkey;
ALTER INDEX IF EXISTS users_silver_id_key RENAME TO users_legacy_silver_id_key;
ALTER INDEX IF EXISTS idx_silver_users_active RENAME TO idx_silver_users_legacy_active;
ALTER INDEX IF EXISTS idx_silver_users_ad_groups_gin RENAME TO idx_silver_users_legacy_ad_groups_gin;
ALTER INDEX IF EXISTS idx_silver_users_department RENAME TO idx_silver_users_legacy_department;
ALTER INDEX IF EXISTS idx_silver_users_dept_ids_gin RENAME TO idx_silver_users_legacy_dept_ids_gin;
ALTER INDEX IF EXISTS idx_silver_users_email RENAME TO idx_silver_users_legacy_email;
ALTER INDEX IF EXISTS idx_silver_users_entity_hash RENAME TO idx_silver_users_legacy_entity_hash;
ALTER INDEX IF EXISTS idx_silver_users_job_title RENAME TO idx_silver_users_legacy_job_title;
ALTER INDEX IF EXISTS idx_silver_users_ou_affiliations_gin RENAME TO idx_silver_users_legacy_ou_affiliations_gin;
ALTER INDEX IF EXISTS idx_silver_users_ou_dept_ids_gin RENAME TO idx_silver_users_legacy_ou_dept_ids_gin;
ALTER INDEX IF EXISTS idx_silver_users_quality RENAME TO idx_silver_users_legacy_quality;
ALTER INDEX IF EXISTS idx_silver_users_silver_id RENAME TO idx_silver_users_legacy_silver_id;
ALTER INDEX IF EXISTS idx_silver_users_source RENAME TO idx_silver_users_legacy_source;
ALTER INDEX IF EXISTS idx_silver_users_supervisor_ids_gin RENAME TO idx_silver_users_legacy_supervisor_ids_gin;
ALTER INDEX IF EXISTS idx_silver_users_tdx_user_uid RENAME TO idx_silver_users_legacy_tdx_user_uid;
ALTER INDEX IF EXISTS idx_silver_users_umich_empl_id RENAME TO idx_silver_users_legacy_umich_empl_id;

-- Step 4: Rename trigger
DROP TRIGGER IF EXISTS update_silver_users_updated_at ON silver.users_legacy;
CREATE TRIGGER update_silver_users_legacy_updated_at
    BEFORE UPDATE ON silver.users_legacy
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Step 5: Re-establish FK constraints pointing to legacy table temporarily
ALTER TABLE silver.computers 
    ADD CONSTRAINT fk_computers_owner_user_legacy 
    FOREIGN KEY (owner_uniqname) 
    REFERENCES silver.users_legacy(uniqname) 
    ON DELETE SET NULL;

ALTER TABLE silver.lab_managers 
    ADD CONSTRAINT lab_managers_manager_uniqname_fkey_legacy 
    FOREIGN KEY (manager_uniqname) 
    REFERENCES silver.users_legacy(uniqname) 
    ON DELETE CASCADE;

-- Add comment to legacy table
COMMENT ON TABLE silver.users_legacy IS 
'Legacy users table (renamed 2025-11-23). Replaced by consolidated silver.users which merges TDX, AD, UMAPI, and MCommunity sources. Preserved for data migration validation. Safe to drop after verification.';
