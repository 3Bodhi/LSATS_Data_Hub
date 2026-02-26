-- Migration: 023_rename_legacy_groups_to_groups_legacy.sql  
-- Purpose: Rename existing silver.groups to silver.groups_legacy
-- Date: 2025-11-23
-- Dependencies: silver.groups (old schema)

-- ===========================================================================
-- RENAME LEGACY GROUPS TABLE
-- ===========================================================================
-- The existing silver.groups table was created before the source-specific
-- architecture was implemented. We're renaming it to groups_legacy and will
-- create a new consolidated groups table that merges ad_groups and mcommunity_groups.
-- ===========================================================================

-- Drop foreign key constraints that reference silver.groups
ALTER TABLE IF EXISTS silver.computer_groups DROP CONSTRAINT IF EXISTS fk_computer_groups_group;
ALTER TABLE IF EXISTS silver.computer_labs DROP CONSTRAINT IF EXISTS fk_computer_labs_group;
ALTER TABLE IF EXISTS silver.group_members DROP CONSTRAINT IF EXISTS group_members_group_id_fkey;
ALTER TABLE IF EXISTS silver.group_owners DROP CONSTRAINT IF EXISTS group_owners_group_id_fkey;

-- Rename the table
ALTER TABLE silver.groups RENAME TO groups_legacy;

-- Rename indexes to reflect new table name
ALTER INDEX IF EXISTS silver.groups_pkey RENAME TO groups_legacy_pkey;
ALTER INDEX IF EXISTS silver.groups_silver_id_key RENAME TO groups_legacy_silver_id_key;
ALTER INDEX IF EXISTS silver.idx_groups_entity_hash RENAME TO idx_groups_legacy_entity_hash;
ALTER INDEX IF EXISTS silver.idx_groups_name RENAME TO idx_groups_legacy_name;
ALTER INDEX IF EXISTS silver.idx_groups_quality RENAME TO idx_groups_legacy_quality;
ALTER INDEX IF EXISTS silver.idx_groups_source RENAME TO idx_groups_legacy_source;
ALTER INDEX IF EXISTS silver.idx_groups_updated_at RENAME TO idx_groups_legacy_updated_at;
ALTER INDEX IF EXISTS silver.idx_silver_groups_active RENAME TO idx_silver_groups_legacy_active;
ALTER INDEX IF EXISTS silver.idx_silver_groups_ad_object_guid RENAME TO idx_silver_groups_legacy_ad_object_guid;
ALTER INDEX IF EXISTS silver.idx_silver_groups_aliases_gin RENAME TO idx_silver_groups_legacy_aliases_gin;
ALTER INDEX IF EXISTS silver.idx_silver_groups_email RENAME TO idx_silver_groups_legacy_email;
ALTER INDEX IF EXISTS silver.idx_silver_groups_entity_hash RENAME TO idx_silver_groups_legacy_entity_hash;
ALTER INDEX IF EXISTS silver.idx_silver_groups_gid_number RENAME TO idx_silver_groups_legacy_gid_number;
ALTER INDEX IF EXISTS silver.idx_silver_groups_quality RENAME TO idx_silver_groups_legacy_quality;
ALTER INDEX IF EXISTS silver.idx_silver_groups_sam_account RENAME TO idx_silver_groups_legacy_sam_account;
ALTER INDEX IF EXISTS silver.idx_silver_groups_silver_id RENAME TO idx_silver_groups_legacy_silver_id;
ALTER INDEX IF EXISTS silver.idx_silver_groups_source RENAME TO idx_silver_groups_legacy_source;
ALTER INDEX IF EXISTS silver.idx_silver_groups_sync RENAME TO idx_silver_groups_legacy_sync;

-- Rename trigger
DROP TRIGGER IF EXISTS update_silver_groups_updated_at ON silver.groups_legacy;
CREATE TRIGGER update_silver_groups_legacy_updated_at
    BEFORE UPDATE ON silver.groups_legacy
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Rename foreign key constraint
ALTER TABLE silver.groups_legacy RENAME CONSTRAINT groups_ingestion_run_id_fkey TO groups_legacy_ingestion_run_id_fkey;

-- Add comment
COMMENT ON TABLE silver.groups_legacy IS 
'Legacy groups table preserved during migration to source-specific architecture.
This table will be phased out in favor of the new consolidated silver.groups table
that merges data from silver.ad_groups and silver.mcommunity_groups.';

-- ===========================================================================
-- MIGRATION TRACKING
-- ===========================================================================

INSERT INTO meta.schema_migrations (version, description, applied_at)
VALUES ('023a', 'Rename silver.groups to silver.groups_legacy', CURRENT_TIMESTAMP)
ON CONFLICT (version) DO NOTHING;
