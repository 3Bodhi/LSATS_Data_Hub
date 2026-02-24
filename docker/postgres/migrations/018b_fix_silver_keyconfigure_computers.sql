-- Migration: Fix silver.keyconfigure_computers constraints
-- Date: 2025-11-22
-- Purpose: Fix issues discovered during initial transformation
--          1. Remove UNIQUE constraint on computer_name (200 duplicates exist)
--          2. Make last_user and last_ip_address nullable (some records have NULL)

-- Drop the unique index on computer_name
DROP INDEX IF EXISTS silver.idx_keyconfigure_computers_name;

-- Recreate as non-unique index
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_name
    ON silver.keyconfigure_computers (computer_name);

-- Make last_user nullable (some computers don't have last user)
ALTER TABLE silver.keyconfigure_computers 
    ALTER COLUMN last_user DROP NOT NULL;

-- Make last_ip_address nullable (some computers don't have last IP)
ALTER TABLE silver.keyconfigure_computers 
    ALTER COLUMN last_ip_address DROP NOT NULL;

-- Update comments to reflect changes
COMMENT ON COLUMN silver.keyconfigure_computers.computer_name IS
    'Computer hostname. 7,551 unique names (200 duplicates exist - e.g., shared computer  names). Used for joining with AD computers by name.';

COMMENT ON COLUMN silver.keyconfigure_computers.last_user IS
    'Last logged-in user. Most records populated but some computers may not have user data.';

COMMENT ON COLUMN silver.keyconfigure_computers.last_ip_address IS
    'Last known IP address. Most records populated but some computers may not have IP address data.';

DO $$
BEGIN
    RAISE NOTICE '✅ Fixed silver.keyconfigure_computers constraints';
    RAISE NOTICE '🔧 Removed UNIQUE constraint on computer_name (200 duplicates exist)';
    RAISE NOTICE '🔧 Made last_user and last_ip_address nullable';
END $$;
