-- Migration: Add Silver Layer Groups Tables
-- Purpose: Create tables for cleaned/standardized group data and relationships
-- Dependencies: Run 001_add_bronze_group_indexes.sql first for optimal performance

-- ============================================================================
-- SILVER LAYER: Groups Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.groups (
    -- Primary identifiers
    group_id VARCHAR(50) PRIMARY KEY,                   -- Canonical ID (gidNumber or source-prefixed cn)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),   -- Internal UUID for referencing

    -- Core group information
    group_name VARCHAR(255) NOT NULL,                   -- Primary cn (first element if array)
    group_aliases JSONB DEFAULT '[]'::jsonb,            -- Additional cn values (MCommunity often has multiple)
    gid_number VARCHAR(50),                             -- POSIX gidNumber (for matched groups)

    -- Descriptive information
    description TEXT,                                   -- description (AD) or umichDescription (MCommunity)
    email_address VARCHAR(255),                         -- umichGroupEmail (MCommunity) or mail (AD)

    -- Source-specific identifiers
    ad_object_guid VARCHAR(255),                        -- objectGUID from Active Directory
    ad_sam_account_name VARCHAR(255),                   -- sAMAccountName from Active Directory
    ad_object_sid VARCHAR(255),                         -- objectSid from Active Directory
    mcommunity_dn TEXT,                                 -- Distinguished name from MCommunity

    -- Group configuration (from MCommunity)
    is_joinable BOOLEAN,                                -- joinable attribute
    is_members_only BOOLEAN,                            -- Membersonly attribute
    is_private BOOLEAN,                                 -- umichPrivate attribute
    suppress_no_email_error BOOLEAN,                    -- suppressNoEmailError

    -- Membership metadata (counts, not actual members)
    member_count INTEGER DEFAULT 0,                     -- Count of direct members
    owner_count INTEGER DEFAULT 0,                      -- Count of owners
    has_nested_groups BOOLEAN DEFAULT false,            -- Whether group contains other groups

    -- Synchronization tracking
    is_ad_synced BOOLEAN DEFAULT false,                 -- In OU=MCommADSync
    sync_source VARCHAR(50),                            -- 'mcommunity', 'ad_only', 'both'

    -- Timestamps and expiry (from MCommunity)
    mcommunity_expiry_timestamp TIMESTAMP WITH TIME ZONE,  -- umichExpiryTimestamp
    ad_when_created TIMESTAMP WITH TIME ZONE,           -- whenCreated from AD
    ad_when_changed TIMESTAMP WITH TIME ZONE,           -- whenChanged from AD

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    source_system VARCHAR(100) NOT NULL,                -- 'mcommunity_ldap+active_directory' or single source
    source_entity_id VARCHAR(255) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_groups_updated_at
    BEFORE UPDATE ON silver.groups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying
CREATE INDEX idx_silver_groups_silver_id ON silver.groups (silver_id);
CREATE INDEX idx_silver_groups_gid_number ON silver.groups (gid_number);
CREATE INDEX idx_silver_groups_ad_object_guid ON silver.groups (ad_object_guid);
CREATE INDEX idx_silver_groups_sam_account ON silver.groups (ad_sam_account_name);
CREATE INDEX idx_silver_groups_entity_hash ON silver.groups (entity_hash);
CREATE INDEX idx_silver_groups_source ON silver.groups (source_system, source_entity_id);
CREATE INDEX idx_silver_groups_active ON silver.groups (is_active, group_name);
CREATE INDEX idx_silver_groups_quality ON silver.groups (data_quality_score DESC);
CREATE INDEX idx_silver_groups_aliases_gin ON silver.groups USING gin (group_aliases);
CREATE INDEX idx_silver_groups_sync ON silver.groups (is_ad_synced, sync_source);
CREATE INDEX idx_silver_groups_email ON silver.groups (email_address);

COMMENT ON TABLE silver.groups IS
    'Cleaned and standardized group data merged from MCommunity LDAP and Active Directory';
COMMENT ON COLUMN silver.groups.group_id IS
    'Canonical group identifier: gidNumber for synced groups, source-prefixed cn for unique groups';
COMMENT ON COLUMN silver.groups.sync_source IS
    'Indicates if group exists in both sources (both), or single source (mcommunity, ad_only)';

-- ============================================================================
-- SILVER LAYER: Group Members Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.group_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    group_id VARCHAR(50) NOT NULL REFERENCES silver.groups(group_id) ON DELETE CASCADE,

    -- Member can be either a user or another group
    member_type VARCHAR(20) NOT NULL CHECK (member_type IN ('user', 'group')),
    member_uniqname VARCHAR(50),                        -- If member_type = 'user'
    member_group_id VARCHAR(50),                        -- If member_type = 'group'

    -- Membership metadata
    is_direct_member BOOLEAN DEFAULT true,              -- From umichDirectMember vs nested
    source_system VARCHAR(50) NOT NULL,                 -- Which system reported this membership

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Ensure member reference is valid
    CONSTRAINT check_member_reference CHECK (
        (member_type = 'user' AND member_uniqname IS NOT NULL AND member_group_id IS NULL) OR
        (member_type = 'group' AND member_group_id IS NOT NULL AND member_uniqname IS NULL)
    )
);

-- Prevent duplicate memberships
CREATE UNIQUE INDEX idx_group_members_unique_user ON silver.group_members (
    group_id, member_uniqname, source_system
) WHERE member_type = 'user';

CREATE UNIQUE INDEX idx_group_members_unique_group ON silver.group_members (
    group_id, member_group_id, source_system
) WHERE member_type = 'group';

-- Indexes for membership queries
CREATE INDEX idx_group_members_group ON silver.group_members (group_id);
CREATE INDEX idx_group_members_user ON silver.group_members (member_uniqname) WHERE member_type = 'user';
CREATE INDEX idx_group_members_nested ON silver.group_members (member_group_id) WHERE member_type = 'group';
CREATE INDEX idx_group_members_direct ON silver.group_members (group_id, is_direct_member);
CREATE INDEX idx_group_members_source ON silver.group_members (source_system);

COMMENT ON TABLE silver.group_members IS
    'Group membership relationships supporting both user and nested group members';
COMMENT ON COLUMN silver.group_members.is_direct_member IS
    'True for umichDirectMember, false for inherited/nested memberships';

-- ============================================================================
-- SILVER LAYER: Group Owners Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.group_owners (
    ownership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    group_id VARCHAR(50) NOT NULL REFERENCES silver.groups(group_id) ON DELETE CASCADE,

    -- Owner can be either a user or another group
    owner_type VARCHAR(20) NOT NULL CHECK (owner_type IN ('user', 'group')),
    owner_uniqname VARCHAR(50),
    owner_group_id VARCHAR(50),

    source_system VARCHAR(50) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT check_owner_reference CHECK (
        (owner_type = 'user' AND owner_uniqname IS NOT NULL AND owner_group_id IS NULL) OR
        (owner_type = 'group' AND owner_group_id IS NOT NULL AND owner_uniqname IS NULL)
    )
);

-- Prevent duplicate ownerships
CREATE UNIQUE INDEX idx_group_owners_unique_user ON silver.group_owners (
    group_id, owner_uniqname
) WHERE owner_type = 'user';

CREATE UNIQUE INDEX idx_group_owners_unique_group ON silver.group_owners (
    group_id, owner_group_id
) WHERE owner_type = 'group';

-- Indexes for ownership queries
CREATE INDEX idx_group_owners_group ON silver.group_owners (group_id);
CREATE INDEX idx_group_owners_user ON silver.group_owners (owner_uniqname) WHERE owner_type = 'user';
CREATE INDEX idx_group_owners_nested ON silver.group_owners (owner_group_id) WHERE owner_type = 'group';
CREATE INDEX idx_group_owners_source ON silver.group_owners (source_system);

COMMENT ON TABLE silver.group_owners IS
    'Group ownership relationships from MCommunity (AD does not have owner field)';

-- ============================================================================
-- USEFUL VIEWS
-- ============================================================================

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

COMMENT ON VIEW silver.group_summary IS
    'Summary view of groups with membership and ownership counts';

-- View: User group memberships (for easy user->groups lookup)
CREATE OR REPLACE VIEW silver.user_group_memberships AS
SELECT
    gm.member_uniqname as uniqname,
    g.group_id,
    g.group_name,
    g.gid_number,
    gm.is_direct_member,
    gm.source_system,
    g.description,
    g.is_active as group_is_active
FROM silver.group_members gm
JOIN silver.groups g ON gm.group_id = g.group_id
WHERE gm.member_type = 'user'
ORDER BY gm.member_uniqname, g.group_name;

COMMENT ON VIEW silver.user_group_memberships IS
    'Easy lookup of all groups a user belongs to';

-- View: Synced groups (exist in both MCommunity and AD)
CREATE OR REPLACE VIEW silver.synced_groups AS
SELECT
    group_id,
    group_name,
    gid_number,
    ad_sam_account_name,
    email_address,
    member_count,
    is_active,
    ad_when_changed,
    mcommunity_expiry_timestamp
FROM silver.groups
WHERE sync_source = 'both'
ORDER BY group_name;

COMMENT ON VIEW silver.synced_groups IS
    'Groups that exist in both MCommunity and Active Directory (MCommADSync)';

-- Grant permissions
GRANT ALL ON ALL TABLES IN SCHEMA silver TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO lsats_user;

-- ============================================================================
-- Validation Queries
-- ============================================================================

-- Verify table creation
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'silver' AND tablename LIKE '%group%'
ORDER BY tablename;

-- Show index count
SELECT
    tablename,
    COUNT(*) as index_count
FROM pg_indexes
WHERE schemaname = 'silver' AND tablename LIKE '%group%'
GROUP BY tablename
ORDER BY tablename;
