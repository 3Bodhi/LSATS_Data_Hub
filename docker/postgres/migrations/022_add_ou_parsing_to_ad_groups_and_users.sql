-- Migration: 022_add_ou_parsing_to_ad_groups_and_users.sql
-- Purpose: Add parsed OU hierarchy columns to AD groups and users tables
-- Date: 2025-11-23

-- Add columns to silver.ad_groups
ALTER TABLE silver.ad_groups
    ADD COLUMN IF NOT EXISTS ou_root VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_organization_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_organization VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_category VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_status VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_division VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_department VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_subdepartment VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_immediate_parent VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_full_path JSONB,
    ADD COLUMN IF NOT EXISTS ou_depth INTEGER,
    ADD COLUMN IF NOT EXISTS parent_ou_dn VARCHAR(500);

-- Add columns to silver.ad_users
ALTER TABLE silver.ad_users
    ADD COLUMN IF NOT EXISTS ou_root VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_organization_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_organization VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_category VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_status VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_division VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_department VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_subdepartment VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_immediate_parent VARCHAR(100),
    ADD COLUMN IF NOT EXISTS ou_full_path JSONB,
    ADD COLUMN IF NOT EXISTS ou_depth INTEGER,
    ADD COLUMN IF NOT EXISTS parent_ou_dn VARCHAR(500);

-- Add indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ad_groups_ou_organization ON silver.ad_groups(ou_organization);
CREATE INDEX IF NOT EXISTS idx_ad_groups_ou_department ON silver.ad_groups(ou_department);
CREATE INDEX IF NOT EXISTS idx_ad_users_ou_organization ON silver.ad_users(ou_organization);
CREATE INDEX IF NOT EXISTS idx_ad_users_ou_department ON silver.ad_users(ou_department);

-- Record migration
INSERT INTO meta.schema_migrations (version, description, applied_at)
VALUES ('022', 'Add OU parsing columns to AD groups and users', CURRENT_TIMESTAMP)
ON CONFLICT (version) DO NOTHING;
