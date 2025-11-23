-- Migration: Add silver.ad_computers source-specific table
-- Date: 2025-11-22
-- Purpose: Create source-specific silver table for Active Directory computers
--          Part of two-tier silver architecture (source-specific -> consolidated)
--          Extracted from AD LDAP queries via bronze.raw_entities

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.ad_computers CASCADE;

-- Create silver.ad_computers table
CREATE TABLE IF NOT EXISTS silver.ad_computers (
    -- ============================================
    -- PRIMARY IDENTIFIER
    -- ============================================
    sam_account_name VARCHAR(20) PRIMARY KEY,      -- e.g., "MCDB-5CG0183FB7$" (unique, 100% populated)
                                                    -- CRITICAL: AD unique identifier with $ suffix

    -- ============================================
    -- CORE IDENTITY FIELDS (for cross-system matching)
    -- ============================================
    computer_name VARCHAR(60) NOT NULL,            -- CN field (max 50 + buffer, 100% populated)
                                                    -- CRITICAL: Join with keyconfigure_computers.computer_name

    dns_hostname VARCHAR(60),                      -- Fully qualified domain name (max 42 + buffer, 99.7% populated)
                                                    -- CRITICAL: Network identity for IP resolution

    distinguished_name VARCHAR(300) NOT NULL,      -- Full AD path (max 271 + buffer, 100% populated)
                                                    -- Used for OU extraction and hierarchy queries

    object_guid UUID NOT NULL,                     -- AD GUID (100% populated, immutable)
    object_sid VARCHAR(100) NOT NULL,              -- Security identifier (100% populated)

    -- ============================================
    -- ORGANIZATIONAL UNIT HIERARCHY (UNIVERSAL AD EXTRACTION METHOD)
    -- ============================================
    -- Parsed from root (DC) → leaf (CN) for consistent cross-entity matching
    -- Pattern: UMICH → Organizations → LSA → Category → Division → Department → ...
    -- Compatible with computers, groups, organizational_units (users have simpler structure)

    ou_root VARCHAR(50),                           -- Always "UMICH" (position from end: array[length])
    ou_organization_type VARCHAR(50),              -- "Organizations" for LSA entities (array[length-1])
    ou_organization VARCHAR(50),                   -- "LSA", "UMICH", etc. (array[length-2])
    ou_category VARCHAR(60),                       -- "Workstations", "Users and Groups", "Research and Instrumentation" (array[length-3])
    ou_division VARCHAR(100),                      -- "RSN", "EHTS", "CSG", "LSAIT Computer Support Group" (array[length-4])
    ou_department VARCHAR(150),                    -- Department name: "Biology", "Psychology", etc. (array[length-5])
    ou_subdepartment VARCHAR(150),                 -- Sub-department or lab-specific OU (array[length-6])
    ou_immediate_parent VARCHAR(150),              -- First OU after CN (array[1]) - varies by entity
    ou_full_path JSONB DEFAULT '[]'::jsonb,        -- Complete array of all OUs (leaf→root order) for deep hierarchy queries

    -- CRITICAL: These OU fields enable cross-entity joins:
    -- - silver.ad_computers.ou_department = silver.ad_groups.ou_department (same dept)
    -- - silver.ad_computers.ou_division = "RSN" (all RSN computers)
    -- - Find computers where ou_immediate_parent LIKE '%-lab' (lab-specific machines)

    -- ============================================
    -- OPERATING SYSTEM (for OS-based matching)
    -- ============================================
    operating_system VARCHAR(50),                  -- OS name (max 44 + buffer, 99.6% populated)
                                                    -- CRITICAL: Join with tdx_assets.attr_operating_system_name
                                                    --           and keyconfigure_computers.os

    operating_system_version VARCHAR(20),          -- OS version (max 13 + buffer, 99.6% populated)
    operating_system_service_pack VARCHAR(50),     -- Service pack info (rare, ~1% populated)

    -- ============================================
    -- ACCOUNT STATUS AND FLAGS
    -- ============================================
    is_enabled BOOLEAN NOT NULL,                   -- Computed from userAccountControl bit 2
                                                    -- (94.2% enabled, 5.8% disabled)

    user_account_control INTEGER NOT NULL,         -- Raw UAC flags (100% populated)
                                                    -- Common values: 4096 (normal), 4098 (disabled)

    is_critical_system_object BOOLEAN,             -- Windows critical object flag (rare)

    -- ============================================
    -- NETWORK AND SERVICES
    -- ============================================
    service_principal_names JSONB DEFAULT '[]'::jsonb,  -- Service SPNs (99.7% populated, avg 9 entries)
                                                         -- Array of service identifiers (HOST, TERMSRV, WSMAN, etc.)
                                                         -- Used for Kerberos authentication

    network_addresses JSONB DEFAULT '[]'::jsonb,   -- Network address info (rare, <1% populated)

    -- ============================================
    -- GROUP MEMBERSHIP (for permissions/access control)
    -- ============================================
    member_of_groups JSONB DEFAULT '[]'::jsonb,    -- AD group DNs (48.7% populated as array, max 5 groups)
                                                    -- CRITICAL: Normalize string → array on ingestion
                                                    -- Used for access control and policy queries
                                                    -- Example: ["CN=LSA-Duo-WorkstationG,OU=...", ...]

    -- ============================================
    -- MANAGEMENT AND OWNERSHIP
    -- ============================================
    managed_by VARCHAR(250),                       -- DN of managing user/group (39.4% populated, max 218)
                                                    -- Used to identify responsible party
                                                    -- Links to silver.ad_users or silver.ad_groups

    description VARCHAR(300),                      -- Computer description (8.7% populated, max 290)
    display_name VARCHAR(100),                     -- Display name (rare, <1% populated)

    -- ============================================
    -- AUTHENTICATION AND SECURITY
    -- ============================================
    pwd_last_set TIMESTAMP WITH TIME ZONE NOT NULL, -- Computer password last changed (100% populated)
                                                     -- Range: 2013-2025
                                                     -- Indicates last successful AD authentication

    account_expires TIMESTAMP WITH TIME ZONE,      -- Account expiration (100% populated, mostly 9999-12-31)
    bad_password_time TIMESTAMP WITH TIME ZONE,    -- Last bad password attempt (rare)
    bad_pwd_count INTEGER,                         -- Failed login count (rare)

    ms_laps_password_expiration_time BIGINT,       -- LAPS password expiry (81.3% populated)
                                                    -- Windows LAPS (Local Admin Password Solution)
    ms_mcs_adm_pwd_expiration_time BIGINT,         -- Legacy LAPS (57.3% populated)

    msds_supported_encryption_types INTEGER,       -- Kerberos encryption types (99.6% populated)
    msds_key_credential_link TEXT,                 -- Windows Hello for Business credential (26.4% populated)

    user_certificate TEXT,                         -- Machine certificate (74.9% populated, large binary)
                                                    -- Used for certificate-based authentication

    -- ============================================
    -- LOGON AND SESSION TRACKING
    -- ============================================
    last_logon TIMESTAMP WITH TIME ZONE,           -- Last logon on THIS DC (100% populated)
                                                    -- NOTE: DC-specific, use lastLogonTimestamp for actual activity

    last_logon_timestamp TIMESTAMP WITH TIME ZONE,           -- Replicated last logon (99.8% populated)
                                                             -- Range: 2020-11-13 to 2025-11-21
                                                             -- CRITICAL: Primary activity indicator (replicated across DCs)

    last_logoff TIMESTAMP WITH TIME ZONE,          -- Last logoff (rare, <1% populated)
    logon_count INTEGER,                           -- Total logon count (100% populated)

    -- ============================================
    -- AD METADATA AND VERSIONING
    -- ============================================
    when_created TIMESTAMP WITH TIME ZONE NOT NULL, -- AD object creation (100% populated, 2009-2025)
    when_changed TIMESTAMP WITH TIME ZONE NOT NULL, -- Last AD modification (100% populated)
    usn_created BIGINT NOT NULL,                   -- Update sequence number at creation
    usn_changed BIGINT NOT NULL,                   -- Current USN (for replication tracking)

    ds_core_propagation_data JSONB DEFAULT '[]'::jsonb, -- Replication timestamps (100% populated)
                                                         -- Tracks AD replication events

    -- ============================================
    -- ADDITIONAL METADATA (low population, consolidated into JSONB)
    -- ============================================
    additional_attributes JSONB,                   -- Rare fields consolidated:
                                                    -- - pager, networkAddress (location info)
                                                    -- - msDFSR-ComputerReferenceBL (DFS replication)
                                                    -- - mSMQDigests, mSMQSignCertificates (message queue)
                                                    -- - msDS-AllowedToActOnBehalfOfOtherIdentity (delegation)
                                                    -- - msDS-GroupMSAMembership, msDS-ManagedPasswordId (gMSA)
                                                    -- - netbootSCPBL (network boot)

    -- ============================================
    -- TRACEABILITY (link back to bronze)
    -- ============================================
    raw_id UUID NOT NULL,                          -- Link to bronze.raw_entities.raw_id

    -- ============================================
    -- STANDARD SILVER METADATA
    -- ============================================
    source_system VARCHAR(50) DEFAULT 'active_directory' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,              -- SHA-256 for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Core business key indexes (UNIQUE for matching)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_computers_sam_account_name
    ON silver.ad_computers (sam_account_name);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_computers_computer_name
    ON silver.ad_computers (computer_name);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_computers_object_guid
    ON silver.ad_computers (object_guid);

-- CRITICAL: Cross-system matching indexes
CREATE INDEX IF NOT EXISTS idx_ad_computers_dns_hostname
    ON silver.ad_computers (dns_hostname)
    WHERE dns_hostname IS NOT NULL AND dns_hostname != '';

CREATE INDEX IF NOT EXISTS idx_ad_computers_operating_system
    ON silver.ad_computers (operating_system)
    WHERE operating_system IS NOT NULL;

-- OU hierarchy indexes (for cross-entity matching)
CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_organization
    ON silver.ad_computers (ou_organization)
    WHERE ou_organization IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_division
    ON silver.ad_computers (ou_division)
    WHERE ou_division IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_department
    ON silver.ad_computers (ou_department)
    WHERE ou_department IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_category
    ON silver.ad_computers (ou_category)
    WHERE ou_category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_immediate_parent
    ON silver.ad_computers (ou_immediate_parent)
    WHERE ou_immediate_parent IS NOT NULL;

-- Account status and filtering
CREATE INDEX IF NOT EXISTS idx_ad_computers_is_enabled
    ON silver.ad_computers (is_enabled);

CREATE INDEX IF NOT EXISTS idx_ad_computers_enabled_active
    ON silver.ad_computers (is_enabled, last_logon_timestamp)
    WHERE is_enabled = TRUE;

-- Management and ownership
CREATE INDEX IF NOT EXISTS idx_ad_computers_managed_by
    ON silver.ad_computers (managed_by)
    WHERE managed_by IS NOT NULL;

-- Activity tracking (for compliance queries)
CREATE INDEX IF NOT EXISTS idx_ad_computers_last_logon_timestamp
    ON silver.ad_computers (last_logon_timestamp);

CREATE INDEX IF NOT EXISTS idx_ad_computers_pwd_last_set
    ON silver.ad_computers (pwd_last_set);

CREATE INDEX IF NOT EXISTS idx_ad_computers_when_created
    ON silver.ad_computers (when_created);

CREATE INDEX IF NOT EXISTS idx_ad_computers_when_changed
    ON silver.ad_computers (when_changed);

-- LAPS password management
CREATE INDEX IF NOT EXISTS idx_ad_computers_laps_expiration
    ON silver.ad_computers (ms_laps_password_expiration_time)
    WHERE ms_laps_password_expiration_time IS NOT NULL;

-- GIN indexes for JSONB fields (for containment queries)
CREATE INDEX IF NOT EXISTS idx_ad_computers_ou_full_path_gin
    ON silver.ad_computers USING gin (ou_full_path);

CREATE INDEX IF NOT EXISTS idx_ad_computers_member_of_groups_gin
    ON silver.ad_computers USING gin (member_of_groups);

CREATE INDEX IF NOT EXISTS idx_ad_computers_service_principal_names_gin
    ON silver.ad_computers USING gin (service_principal_names);

CREATE INDEX IF NOT EXISTS idx_ad_computers_additional_attributes_gin
    ON silver.ad_computers USING gin (additional_attributes)
    WHERE additional_attributes IS NOT NULL;

-- Standard traceability indexes
CREATE INDEX IF NOT EXISTS idx_ad_computers_raw_id
    ON silver.ad_computers (raw_id);

CREATE INDEX IF NOT EXISTS idx_ad_computers_entity_hash
    ON silver.ad_computers (entity_hash);

-- Ingestion tracking
CREATE INDEX IF NOT EXISTS idx_ad_computers_ingestion_run_id
    ON silver.ad_computers (ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- ============================================
-- TABLE AND COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE silver.ad_computers IS
    'Source-specific silver table for Active Directory computers. Part of two-tier silver architecture - feeds into consolidated silver.computers table. Contains typed columns extracted from bronze.raw_entities JSONB data with universal OU extraction for cross-entity matching with AD groups, users, and OUs.';

COMMENT ON COLUMN silver.ad_computers.sam_account_name IS
    'Primary key - SAM account name with $ suffix (e.g., MCDB-5CG0183FB7$). Unique AD identifier. 100% populated.';

COMMENT ON COLUMN silver.ad_computers.computer_name IS
    'Computer hostname (CN field). CRITICAL for joining with keyconfigure_computers.computer_name and TDX asset names. 100% populated.';

COMMENT ON COLUMN silver.ad_computers.dns_hostname IS
    'Fully qualified domain name (e.g., mcdb-5cg0183fb7.adsroot.itcs.umich.edu). Used for network identity and IP resolution. 99.7% populated.';

COMMENT ON COLUMN silver.ad_computers.object_guid IS
    'Immutable AD GUID. Survives renames and moves. 100% populated.';

COMMENT ON COLUMN silver.ad_computers.ou_division IS
    'Division OU extracted from DN (e.g., RSN, EHTS, CSG). CRITICAL for cross-entity queries: find all computers, groups, users in same division. Enables departmental analysis.';

COMMENT ON COLUMN silver.ad_computers.ou_department IS
    'Department OU extracted from DN (e.g., Biology, Psychology, Chemistry). CRITICAL for matching computers to departmental groups and users.';

COMMENT ON COLUMN silver.ad_computers.ou_full_path IS
    'Complete OU hierarchy as JSONB array (leaf→root order). Supports deep hierarchy queries and edge cases beyond standard extraction.';

COMMENT ON COLUMN silver.ad_computers.operating_system IS
    'OS name (e.g., Windows 11 Enterprise, Mac OS X). CRITICAL for joining with tdx_assets.attr_operating_system_name and keyconfigure_computers.os. 99.6% populated.';

COMMENT ON COLUMN silver.ad_computers.is_enabled IS
    'Computed from userAccountControl bit 2 (ACCOUNTDISABLE). TRUE = account enabled (94.2%), FALSE = disabled (5.8%). Use for filtering active computers.';

COMMENT ON COLUMN silver.ad_computers.last_logon_timestamp IS
    'Replicated last logon timestamp across all DCs. CRITICAL for identifying active vs stale computers. Prefer this over last_logon (DC-specific). Range: 2020-11-13 to 2025-11-21. 99.8% populated.';

COMMENT ON COLUMN silver.ad_computers.member_of_groups IS
    'AD group DNs (JSONB array). Normalized from string/array inconsistency on ingestion. Used for access control policies and group membership analysis. 48.7% populated, max 5 groups.';

COMMENT ON COLUMN silver.ad_computers.managed_by IS
    'DN of managing user or group. Links to silver.ad_users or silver.ad_groups for ownership tracking. 39.4% populated.';

COMMENT ON COLUMN silver.ad_computers.service_principal_names IS
    'Kerberos service principal names (JSONB array). Common: HOST, TERMSRV, WSMAN, RestrictedKrbHost. Used for authentication services. 99.7% populated, avg 9 entries.';

COMMENT ON COLUMN silver.ad_computers.ms_laps_password_expiration_time IS
    'Windows LAPS (Local Admin Password Solution) password expiry timestamp. 81.3% populated. Used for local admin password rotation compliance.';

COMMENT ON COLUMN silver.ad_computers.pwd_last_set IS
    'Computer account password last changed. Indicates last successful AD authentication. Range: 2013-2025. 100% populated.';

COMMENT ON COLUMN silver.ad_computers.additional_attributes IS
    'Consolidated JSONB for rare attributes (<10% population): pager, networkAddress, msDFSR, mSMQ, delegation, gMSA fields.';

COMMENT ON COLUMN silver.ad_computers.raw_id IS
    'Link to most recent bronze.raw_entities record for this computer - for audit trail and bronze data access.';

COMMENT ON COLUMN silver.ad_computers.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed (incremental processing).';

-- ============================================
-- PERMISSIONS
-- ============================================

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.ad_computers TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.ad_computers TO lsats_user;

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully created silver.ad_computers table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'ad_computers');
    RAISE NOTICE '📊 Table supports 15,325 AD computers with universal OU extraction for cross-entity matching';
    RAISE NOTICE '🔗 Critical join keys:';
    RAISE NOTICE '   - computer_name → keyconfigure_computers.computer_name';
    RAISE NOTICE '   - dns_hostname → network/IP resolution';
    RAISE NOTICE '   - operating_system → tdx_assets.attr_operating_system_name, keyconfigure_computers.os';
    RAISE NOTICE '   - ou_department/ou_division → cross-entity OU matching with groups, users';
    RAISE NOTICE '🏢 OU hierarchy: root → org_type → organization → category → division → department → subdepartment';
END $$;
