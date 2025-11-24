-- Migration: 024c_restore_foreign_keys_to_users.sql
-- Purpose: Re-establish foreign key constraints from other tables to silver.users
-- Date: 2025-11-23
--
-- After populating silver.users, point FK constraints from computers and lab_managers
-- to the new consolidated table instead of users_legacy.

-- Step 1: Drop temporary FK constraints to legacy table
ALTER TABLE silver.computers DROP CONSTRAINT IF EXISTS fk_computers_owner_user_legacy;
ALTER TABLE silver.lab_managers DROP CONSTRAINT IF EXISTS lab_managers_manager_uniqname_fkey_legacy;

-- Step 2: Re-establish FK constraints to new consolidated silver.users
ALTER TABLE silver.computers 
    ADD CONSTRAINT fk_computers_owner_user 
    FOREIGN KEY (owner_uniqname) 
    REFERENCES silver.users(uniqname) 
    ON DELETE SET NULL;

ALTER TABLE silver.lab_managers 
    ADD CONSTRAINT lab_managers_manager_uniqname_fkey 
    FOREIGN KEY (manager_uniqname) 
    REFERENCES silver.users(uniqname) 
    ON DELETE CASCADE;

-- Note: Apply this migration AFTER populating silver.users table
-- via 012_transform_users.py transformation script
