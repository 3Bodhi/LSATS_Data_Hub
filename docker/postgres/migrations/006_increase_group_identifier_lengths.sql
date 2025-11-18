-- Migration: Increase VARCHAR lengths for group identifiers
-- Date: 2025-11-18
-- Reason: Group names (cn values) can exceed 50 characters
--         Example: "ITS Application Developer Support University Administrative" (59 chars)
--         With prefixes like "ad_", "mcom_", "cn_", group_id can reach 67+ characters

-- Step 1: Drop views that depend on group_id column
DROP VIEW IF EXISTS silver.group_summary CASCADE;
DROP VIEW IF EXISTS silver.user_group_memberships CASCADE;
DROP VIEW IF EXISTS silver.synced_groups CASCADE;

-- Step 2: Increase group_id in silver.groups from VARCHAR(50) to VARCHAR(100)
-- This is the primary key, so we need to update it in all dependent tables
ALTER TABLE silver.groups
    ALTER COLUMN group_id TYPE VARCHAR(100);

-- Step 3: Update foreign key references in dependent tables
ALTER TABLE silver.group_members
    ALTER COLUMN group_id TYPE VARCHAR(100),
    ALTER COLUMN member_group_id TYPE VARCHAR(100);

ALTER TABLE silver.group_owners
    ALTER COLUMN group_id TYPE VARCHAR(100),
    ALTER COLUMN owner_group_id TYPE VARCHAR(100);

-- Step 4: Recreate the views
-- View: Groups with member and owner counts
CREATE OR REPLACE VIEW silver.group_summary AS
SELECT
    g.group_id,
    g.group_name,
    g.gid_number,
    g.sync_source,
    g.is_active,
    COUNT(DISTINCT gm.membership_id) as total_members,
    COUNT(DISTINCT CASE WHEN gm.member_type = 'user' THEN gm.membership_id END) as user_members,
    COUNT(DISTINCT CASE WHEN gm.member_type = 'group' THEN gm.membership_id END) as nested_groups,
    COUNT(DISTINCT go.ownership_id) as total_owners,
    g.email_address,
    g.description,
    g.data_quality_score
FROM silver.groups g
LEFT JOIN silver.group_members gm ON g.group_id = gm.group_id
LEFT JOIN silver.group_owners go ON g.group_id = go.group_id
GROUP BY g.group_id, g.group_name, g.gid_number, g.sync_source, g.is_active,
         g.email_address, g.description, g.data_quality_score
ORDER BY g.group_name;

-- View: User's group memberships
CREATE OR REPLACE VIEW silver.user_group_memberships AS
SELECT
    gm.member_uniqname as uniqname,
    g.group_id,
    g.group_name,
    g.gid_number,
    gm.is_direct_member,
    gm.source_system,
    g.sync_source,
    g.is_active
FROM silver.group_members gm
JOIN silver.groups g ON gm.group_id = g.group_id
WHERE gm.member_type = 'user'
ORDER BY gm.member_uniqname, g.group_name;

-- View: Groups synced between MCommunity and AD
CREATE OR REPLACE VIEW silver.synced_groups AS
SELECT
    group_id,
    group_name,
    gid_number,
    sync_source,
    is_ad_synced,
    email_address,
    description,
    data_quality_score
FROM silver.groups
WHERE is_ad_synced = true
ORDER BY group_name;

-- Note: No need to rebuild indexes - PostgreSQL handles this automatically
-- The unique indexes and foreign keys will continue to work with the new column type

COMMENT ON COLUMN silver.groups.group_id IS
    'Primary identifier: gidNumber (if available) or prefixed cn (e.g., ad_groupname, mcom_groupname). Increased to VARCHAR(100) to accommodate long group names with prefixes.';

COMMENT ON COLUMN silver.group_members.group_id IS
    'Foreign key to silver.groups(group_id). Increased to VARCHAR(100).';

COMMENT ON COLUMN silver.group_members.member_group_id IS
    'Group identifier (cn) when member is a group. Increased to VARCHAR(100) to accommodate long group names.';

COMMENT ON COLUMN silver.group_owners.group_id IS
    'Foreign key to silver.groups(group_id). Increased to VARCHAR(100).';

COMMENT ON COLUMN silver.group_owners.owner_group_id IS
    'Group identifier (cn) when owner is a group. Increased to VARCHAR(100) to accommodate long group names.';
