-- Migration: Add supervisor_ids field to silver.users table
-- This migration adds the supervisor_ids JSONB array field to track SupervisorID from multiple UMAPI employment records

-- Add the supervisor_ids column to silver.users
ALTER TABLE silver.users
ADD COLUMN IF NOT EXISTS supervisor_ids JSONB DEFAULT '[]'::jsonb;

-- Add comment for documentation
COMMENT ON COLUMN silver.users.supervisor_ids IS 'Array of SupervisorID from UMAPI (multiple employment records)';

-- Optional: Create a GIN index for efficient JSONB queries on supervisor_ids
CREATE INDEX IF NOT EXISTS idx_silver_users_supervisor_ids_gin
ON silver.users USING gin (supervisor_ids);

-- Log the migration
DO $$
BEGIN
    RAISE NOTICE 'Migration 004: Added supervisor_ids field to silver.users table';
END $$;
