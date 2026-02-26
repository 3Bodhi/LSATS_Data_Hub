-- Migration: 023b_add_silver_groups.sql
-- Purpose: Create consolidated silver.groups table merging AD and MCommunity groups
-- Date: 2025-11-23
-- Dependencies: silver.ad_groups, silver.mcommunity_groups

-- ===========================================================================
-- CONSOLIDATED SILVER GROUPS TABLE
-- ===========================================================================
-- Merges data from:
--   - silver.ad_groups (8,726 groups)
--   - silver.mcommunity_groups (33,800 groups)
-- Expected output: 41,397 groups (42,526 - 1,129 CN overlaps merged)
--
-- Business Key Strategy:
--   - group_id uses natural CN/group_name from source
--   - All 1,129 CN overlaps are merged records (source_system = 'ad+mcommunity')
--   - No source prefixes needed - all IDs are clean, human-readable names
-- ===========================================================================

CREATE TABLE IF NOT EXISTS silver.groups (
    -- ===== PRIMARY KEY =====
    group_id VARCHAR(100) PRIMARY KEY,              -- Natural CN/group_name (e.g., 'lsa-staff', 'research-team')
    
    -- ===== EXTERNAL SYSTEM IDENTIFIERS =====
    ad_group_guid UUID,                             -- From AD (if source includes AD)
    mcommunity_group_uid UUID,                      -- From MCommunity (if source includes MCommunity)
    
    -- ===== CORE IDENTITY (MERGED FIELDS) =====
    group_name VARCHAR(100),                        -- Priority: AD.name > MCommunity.group_name
    group_email VARCHAR(100),                       -- Priority: MCommunity.group_email > AD.mail
    sam_account_name VARCHAR(100),                  -- AD-specific SAM account name (NULL for MCommunity-only)
    cn VARCHAR(100),                                 -- Common Name (AD.cn or MCommunity.group_name)
    distinguished_name TEXT,                        -- Full DN (AD if available, else MCommunity)
    description TEXT,                               -- Priority: MCommunity.description > AD.description
    display_name VARCHAR(255),                      -- AD-specific display name
    
    -- ===== GROUP TYPE & CLASSIFICATION =====
    group_type INTEGER,                             -- AD groupType value
    is_security_group BOOLEAN,                      -- Derived from AD.group_type
    is_distribution_group BOOLEAN,                  -- Derived from AD.group_type or MCommunity flags
    sam_account_type INTEGER,                       -- AD sAMAccountType
    object_category VARCHAR(255),                   -- AD objectCategory
    
    -- ===== MCOMMUNITY-SPECIFIC FLAGS =====
    is_private BOOLEAN,                             -- MCommunity.is_private
    is_members_only BOOLEAN,                        -- MCommunity.is_members_only
    is_joinable BOOLEAN,                            -- MCommunity.is_joinable
    expiry_timestamp TIMESTAMP WITH TIME ZONE,      -- MCommunity.expiry_timestamp
    
    -- ===== MCOMMADSYNC DETECTION =====
    is_mcomm_adsync BOOLEAN DEFAULT FALSE,          -- TRUE if group in MCommADSync OU
    
    -- ===== ORGANIZATION HIERARCHY (FROM AD) =====
    ou_root VARCHAR(100),                           -- Root OU
    ou_organization VARCHAR(100),                   -- Organization (e.g., 'LSA')
    ou_department VARCHAR(100),                     -- Department (e.g., 'Psychology', 'Chemistry')
    ou_category VARCHAR(100),                       -- Category level
    ou_immediate_parent VARCHAR(100),               -- Immediate parent OU
    ou_full_path JSONB DEFAULT '[]'::jsonb,         -- Complete OU path array
    ou_depth INTEGER,                               -- OU hierarchy depth
    parent_ou_dn TEXT,                              -- Parent OU distinguished name
    
    -- ===== MEMBERSHIP (JSONB ARRAYS) =====
    members JSONB DEFAULT '[]'::jsonb,              -- Merged from both sources (deduplicated DNs)
    owners JSONB DEFAULT '[]'::jsonb,               -- From MCommunity.owners + AD.managed_by
    member_of JSONB DEFAULT '[]'::jsonb,            -- From AD.member_of (groups this group belongs to)
    direct_members JSONB DEFAULT '[]'::jsonb,       -- MCommunity direct members
    nested_members JSONB DEFAULT '[]'::jsonb,       -- MCommunity nested group members
    
    -- ===== MANAGEMENT =====
    managed_by TEXT,                                -- AD.managed_by DN
    
    -- ===== CONTACT INFO (JSONB) =====
    contact_info JSONB DEFAULT '{}'::jsonb,         -- From MCommunity.contact_info
    proxy_addresses TEXT[],                         -- AD proxy addresses
    
    -- ===== TIMESTAMPS =====
    when_created TIMESTAMP WITH TIME ZONE,          -- Priority: AD.when_created > MCommunity.created_at
    when_changed TIMESTAMP WITH TIME ZONE,          -- Latest from either source
    
    -- ===== DATA QUALITY =====
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    
    -- ===== SOURCE TRACKING =====
    source_system VARCHAR(100) NOT NULL,            -- 'ad', 'mcommunity', 'ad+mcommunity'
    source_entity_id VARCHAR(255) NOT NULL,         -- Original ID from source
    entity_hash VARCHAR(64) NOT NULL,               -- SHA-256 content hash for change detection
    
    -- ===== TRACEABILITY =====
    ad_raw_id UUID,                                 -- Link to bronze AD record
    mcommunity_raw_id UUID,                         -- Link to bronze MCommunity record
    
    -- ===== STANDARD METADATA =====
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID                           -- FK to meta.ingestion_runs
);

-- ===========================================================================
-- INDEXES
-- ===========================================================================

-- Primary lookups
CREATE INDEX idx_groups_guid ON silver.groups (ad_group_guid) WHERE ad_group_guid IS NOT NULL;
CREATE INDEX idx_groups_mcom_uid ON silver.groups (mcommunity_group_uid) WHERE mcommunity_group_uid IS NOT NULL;
CREATE INDEX idx_groups_name ON silver.groups (group_name);
CREATE INDEX idx_groups_email ON silver.groups (group_email) WHERE group_email IS NOT NULL;
CREATE INDEX idx_groups_sam ON silver.groups (sam_account_name) WHERE sam_account_name IS NOT NULL;

-- Filtering indexes
CREATE INDEX idx_groups_is_mcomm_adsync ON silver.groups (is_mcomm_adsync) WHERE is_mcomm_adsync = TRUE;
CREATE INDEX idx_groups_source ON silver.groups (source_system);
CREATE INDEX idx_groups_quality ON silver.groups (data_quality_score DESC);

-- Organization hierarchy indexes
CREATE INDEX idx_groups_ou_department ON silver.groups (ou_department) WHERE ou_department IS NOT NULL;
CREATE INDEX idx_groups_ou_organization ON silver.groups (ou_organization) WHERE ou_organization IS NOT NULL;

-- Change detection
CREATE INDEX idx_groups_entity_hash ON silver.groups (entity_hash);
CREATE INDEX idx_groups_updated_at ON silver.groups (updated_at DESC);

-- JSONB GIN indexes for membership queries
CREATE INDEX idx_groups_members_gin ON silver.groups USING gin(members);
CREATE INDEX idx_groups_owners_gin ON silver.groups USING gin(owners);
CREATE INDEX idx_groups_member_of_gin ON silver.groups USING gin(member_of);
CREATE INDEX idx_groups_nested_members_gin ON silver.groups USING gin(nested_members) WHERE nested_members != '[]'::jsonb;

-- ===========================================================================
-- FOREIGN KEY CONSTRAINTS
-- ===========================================================================

ALTER TABLE silver.groups
    ADD CONSTRAINT groups_ingestion_run_id_fkey
    FOREIGN KEY (ingestion_run_id) REFERENCES meta.ingestion_runs(run_id);

-- Optional: FK to bronze (if strict lineage desired)
-- Note: Commented out by default as groups may come from merged sources
-- ALTER TABLE silver.groups
--     ADD CONSTRAINT groups_ad_raw_id_fkey
--     FOREIGN KEY (ad_raw_id) REFERENCES bronze.raw_entities(raw_id);
-- 
-- ALTER TABLE silver.groups
--     ADD CONSTRAINT groups_mcommunity_raw_id_fkey
--     FOREIGN KEY (mcommunity_raw_id) REFERENCES bronze.raw_entities(raw_id);

-- ===========================================================================
-- TRIGGERS
-- ===========================================================================

-- Auto-update updated_at timestamp
CREATE TRIGGER update_silver_groups_updated_at
    BEFORE UPDATE ON silver.groups
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ===========================================================================
-- TABLE COMMENTS
-- ===========================================================================

COMMENT ON TABLE silver.groups IS 
'Consolidated groups table merging AD and MCommunity LDAP groups.
- AD-only groups: 7,597
- MCommunity-only groups: 32,671
- Merged groups (CN overlap): 1,129
- Total: 41,397 groups
Business key (group_id) uses natural CN/group_name with no prefixes.';

COMMENT ON COLUMN silver.groups.group_id IS 
'Natural group identifier (CN or group_name). Clean, human-readable, no prefixes.
Examples: lsa-staff, LSA-Chemistry-Faculty, research-coordinators';

COMMENT ON COLUMN silver.groups.is_mcomm_adsync IS 
'TRUE for groups synchronized between AD and MCommunity via MCommADSync.
Detected by OU containing "MCommADSync" in AD distinguished_name.
1,071 groups have this flag set.';

COMMENT ON COLUMN silver.groups.members IS 
'JSONB array of member DNs. Merged and deduplicated from:
- AD: member attribute
- MCommunity: member attribute
Format: ["CN=user1,OU=...", "CN=user2,OU=...", ...]';

COMMENT ON COLUMN silver.groups.owners IS 
'JSONB array of owner/manager DNs. Merged from:
- MCommunity: owner attribute  
- AD: managedBy attribute (converted to array)
Format: ["uid=user1,ou=...", "uid=user2,ou=...", ...]';

COMMENT ON COLUMN silver.groups.data_quality_score IS 
'Quality score from 0.00 to 1.00 based on field completeness and source richness.
Higher scores for merged groups with complete metadata.';

-- ===========================================================================
-- MIGRATION TRACKING
-- ===========================================================================

INSERT INTO meta.schema_migrations (version, description, applied_at)
VALUES ('023', 'Create consolidated silver.groups table', CURRENT_TIMESTAMP)
ON CONFLICT (version) DO NOTHING;
