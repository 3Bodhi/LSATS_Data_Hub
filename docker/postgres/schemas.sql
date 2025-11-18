-- LSATS Database Schema Definitions
-- Bronze-Silver-Gold architecture for flexible data processing

-- ============================================================================
-- BRONZE LAYER: Raw data exactly as received from source systems
-- ============================================================================

-- Universal raw entity storage - this table can handle any entity type
-- The JSONB column preserves the complete original data structure
CREATE TABLE bronze.raw_entities (
    -- Primary identifier for this specific raw record
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Entity classification
    entity_type VARCHAR(50) NOT NULL,  -- 'department', 'user', 'lab', 'asset', etc.
    source_system VARCHAR(50) NOT NULL,  -- 'tdx', 'lab_csv', 'hr_system', etc.
    external_id VARCHAR(255) NOT NULL,  -- The ID from the source system

    -- Complete raw data from source (this is the magic of JSONB)
    raw_data JSONB NOT NULL,

    -- Tracking and metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    entity_hash VARCHAR(64) GENERATED ALWAYS AS (
        encode(sha256((entity_type || '|' || source_system || '|' || external_id)::bytea), 'hex')
    ) STORED,  -- Computed column for consistent entity identification

    -- Additional metadata about the ingestion
    ingestion_metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for efficient querying
CREATE INDEX idx_bronze_entity_lookup ON bronze.raw_entities (entity_type, source_system, external_id);
CREATE INDEX idx_bronze_entity_hash ON bronze.raw_entities (entity_hash);
CREATE INDEX idx_bronze_ingestion_time ON bronze.raw_entities (entity_type, ingested_at DESC);
CREATE INDEX idx_bronze_raw_data_gin ON bronze.raw_entities USING gin (raw_data);  -- For JSONB queries

-- User data optimization indexes (for silver transformation performance)
-- These indexes dramatically improve user transformation query performance (35-50x speedup)
CREATE INDEX idx_bronze_tdx_alternateid ON bronze.raw_entities (source_system, LOWER(raw_data->>'AlternateID'))
    WHERE entity_type = 'user' AND source_system = 'tdx';

CREATE INDEX idx_bronze_mcom_uid ON bronze.raw_entities (source_system, LOWER(raw_data->>'uid'))
    WHERE entity_type = 'user' AND source_system = 'mcommunity_ldap';

CREATE INDEX idx_bronze_umapi_uniqname ON bronze.raw_entities (source_system, LOWER(raw_data->>'UniqName'))
    WHERE entity_type = 'user' AND source_system = 'umich_api';

CREATE INDEX idx_bronze_ad_uid ON bronze.raw_entities (source_system, LOWER(raw_data->>'uid'))
    WHERE entity_type = 'user' AND source_system = 'active_directory';

CREATE INDEX idx_bronze_user_source_ingested ON bronze.raw_entities (entity_type, source_system, ingested_at DESC)
    WHERE entity_type = 'user';

-- Group data optimization indexes (for silver transformation performance)
-- These indexes optimize group matching, member extraction, and transformation queries
CREATE INDEX idx_bronze_group_cn ON bronze.raw_entities (
    source_system,
    LOWER(CASE
        WHEN jsonb_typeof(raw_data->'cn') = 'array' THEN raw_data->'cn'->>0
        ELSE raw_data->>'cn'
    END)
) WHERE entity_type = 'group';

CREATE INDEX idx_bronze_group_gid ON bronze.raw_entities (
    source_system,
    (raw_data->>'gidNumber')
) WHERE entity_type = 'group' AND raw_data->>'gidNumber' IS NOT NULL;

CREATE INDEX idx_bronze_ad_mcomm_sync ON bronze.raw_entities (
    (raw_data->>'dn')
) WHERE entity_type = 'group'
  AND source_system = 'active_directory'
  AND raw_data->>'dn' ~ 'MCommADSync';

CREATE INDEX idx_bronze_group_source_ingested ON bronze.raw_entities (
    entity_type, source_system, ingested_at DESC
) WHERE entity_type = 'group';

CREATE INDEX idx_bronze_group_members_gin ON bronze.raw_entities USING gin (
    (raw_data->'member')
) WHERE entity_type = 'group';

CREATE INDEX idx_bronze_group_owners_gin ON bronze.raw_entities USING gin (
    (raw_data->'owner')
) WHERE entity_type = 'group' AND source_system = 'mcommunity_ldap';

-- ============================================================================
-- SILVER LAYER: Cleaned and standardized data from pandas processing
-- ============================================================================

-- Departments after bronze merge and cleaning
-- Merges data from umich_api (org hierarchy) and tdx (operational data)
CREATE TABLE silver.departments (
    -- Primary identifiers (dept_id is unique across all sources)
    dept_id VARCHAR(50) PRIMARY KEY,                    -- From DeptId (UMICH) / Code (TDX)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),   -- Internal UUID for referencing
    tdx_id INTEGER,                                     -- From ID (TDX) - for write-back operations

    -- Core department information (prioritize UMICH for names)
    department_name VARCHAR(255) NOT NULL,              -- Cleaned DeptDescription (UMICH)
    department_code VARCHAR(50),                        -- Duplicate of dept_id for compatibility
    description TEXT,                                   -- Notes field if available

    -- Hierarchical organization data (from UMICH_API)
    campus_name VARCHAR(255),                           -- DeptGroupCampusDescr
    college_group VARCHAR(255),                         -- DeptGroupDescription
    vp_area VARCHAR(255),                               -- DeptGroupVPAreaDescr
    hierarchical_path TEXT,                             -- Full org path from root to dept

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,            -- IsActive (TDX)

    -- TDX-specific timestamps (prefixed to indicate source)
    tdx_created_date TIMESTAMP WITH TIME ZONE,          -- CreatedDate (TDX)
    tdx_modified_date TIMESTAMP WITH TIME ZONE,         -- ModifiedDate (TDX)

    -- Location and contact information (from TDX, structured as JSONB)
    location_info JSONB DEFAULT '{}'::jsonb,            -- {city, address, phone, fax, url, postal_code}

    -- Data quality metrics (calculated during transformation)
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,            -- Array of quality issues found

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,                 -- 'umich_api+tdx' for merged records
    source_entity_id VARCHAR(255) NOT NULL,             -- Same as dept_id
    entity_hash VARCHAR(64) NOT NULL,                   -- Hash of merged content

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_departments_updated_at
    BEFORE UPDATE ON silver.departments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying and joining
CREATE INDEX idx_silver_departments_silver_id ON silver.departments (silver_id);
CREATE INDEX idx_silver_departments_tdx_id ON silver.departments (tdx_id);
CREATE INDEX idx_silver_departments_entity_hash ON silver.departments (entity_hash);
CREATE INDEX idx_silver_departments_source ON silver.departments (source_system, source_entity_id);
CREATE INDEX idx_silver_departments_active ON silver.departments (is_active, department_name);
CREATE INDEX idx_silver_departments_quality ON silver.departments (data_quality_score DESC);
CREATE INDEX idx_silver_departments_hierarchy ON silver.departments (campus_name, college_group, vp_area);
CREATE INDEX idx_silver_departments_location_gin ON silver.departments USING gin (location_info);

-- Users after bronze merge and cleaning
-- Merges data from tdx, umich_api, mcommunity_ldap, and active_directory
CREATE TABLE silver.users (
    -- Primary identifiers (uniqname is unique across all sources, normalized to lowercase)
    uniqname VARCHAR(50) PRIMARY KEY,                   -- From uid (LDAP) / UniqName (UMAPI) / AlternateID (TDX)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),   -- Internal UUID for referencing

    -- External system identifiers
    umich_empl_id VARCHAR(50),                          -- EmplId from UMAPI or ExternalID from TDX
    tdx_user_uid UUID,                                  -- UID from TDX - critical for operations
    ldap_uid_number VARCHAR(50),                        -- uidNumber from MCommunity LDAP
    ad_object_guid VARCHAR(255),                        -- objectGUID from Active Directory
    ad_sam_account_name VARCHAR(255),                   -- sAMAccountName from Active Directory

    -- Core person information (prioritize TDX/UMAPI, fallback to LDAP)
    first_name VARCHAR(255),                            -- FirstName (TDX/UMAPI) or givenName (LDAP)
    last_name VARCHAR(255),                             -- LastName (TDX/UMAPI) or sn (LDAP)
    full_name VARCHAR(255),                             -- FullName (TDX), Name (UMAPI), or displayName (LDAP)

    -- Contact information
    primary_email VARCHAR(255),                         -- PrimaryEmail (TDX) or mail (LDAP)
    work_phone VARCHAR(50),                             -- WorkPhone (TDX/UMAPI) or telephoneNumber (LDAP)

    -- Job and department information
    job_title TEXT,                                     -- UniversityJobTitle (UMAPI) or umichTitle (LDAP)
    tdx_job_title TEXT,                                 -- Title from TDX (single value)
    department_job_titles JSONB DEFAULT '[]'::jsonb,    -- Array of DepartmentJobTitle from UMAPI (multiple employment records)
    department_id VARCHAR(50),                          -- Primary DepartmentId (from EmplRcd 0)
    department_ids JSONB DEFAULT '[]'::jsonb,           -- Array of all DepartmentIds (multiple employment records)
    job_codes JSONB DEFAULT '[]'::jsonb,                -- Array of Jobcode from UMAPI (multiple employment records)
    supervisor_ids JSONB DEFAULT '[]'::jsonb,           -- Array of SupervisorID from UMAPI (multiple employment records)

    -- Work location (from TDX or UMAPI)
    work_city VARCHAR(255),                             -- WorkCity (TDX) or Work_City (UMAPI)
    work_state VARCHAR(50),                             -- WorkState (TDX) or Work_State (UMAPI)
    work_postal_code VARCHAR(50),                       -- WorkZip (TDX) or Work_Postal (UMAPI)
    work_country VARCHAR(100),                          -- WorkCountry (TDX) or Work_Country (UMAPI)
    work_address_line1 VARCHAR(255),                    -- WorkAddress (TDX) or Work_Address1 (UMAPI)
    work_address_line2 VARCHAR(255),                    -- Work_Address2 from UMAPI

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,            -- IsActive from TDX
    ad_account_disabled BOOLEAN,                        -- Derived from userAccountControl bit flags
    ad_last_logon VARCHAR(255),                         -- lastLogonTimestamp from Active Directory

    -- MCommunity organizational affiliations
    mcommunity_ou_affiliations JSONB DEFAULT '[]'::jsonb,   -- Array of OU strings from MCommunity LDAP
    ou_department_ids JSONB DEFAULT '[]'::jsonb,            -- Matched dept_ids from OU affiliations

    -- Active Directory group memberships
    ad_group_memberships JSONB DEFAULT '[]'::jsonb,     -- Full array of memberOf DN strings from AD

    -- Data quality metrics (calculated during transformation)
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,            -- Array of quality issues found

    -- Source tracking
    source_system VARCHAR(100) NOT NULL,                -- e.g., 'tdx+umich_api+mcommunity_ldap+active_directory'
    source_entity_id VARCHAR(255) NOT NULL,             -- Most specific ID available
    entity_hash VARCHAR(64) NOT NULL,                   -- Hash of merged content

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_users_updated_at
    BEFORE UPDATE ON silver.users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying and joining
CREATE INDEX idx_silver_users_silver_id ON silver.users (silver_id);
CREATE INDEX idx_silver_users_umich_empl_id ON silver.users (umich_empl_id);
CREATE INDEX idx_silver_users_tdx_user_uid ON silver.users (tdx_user_uid);
CREATE INDEX idx_silver_users_entity_hash ON silver.users (entity_hash);
CREATE INDEX idx_silver_users_source ON silver.users (source_system, source_entity_id);
CREATE INDEX idx_silver_users_active ON silver.users (is_active, full_name);
CREATE INDEX idx_silver_users_quality ON silver.users (data_quality_score DESC);
CREATE INDEX idx_silver_users_email ON silver.users (primary_email);
CREATE INDEX idx_silver_users_department ON silver.users (department_id);
CREATE INDEX idx_silver_users_job_title ON silver.users (job_title);

-- Foreign key to departments (if department exists in silver)
ALTER TABLE silver.users
    ADD CONSTRAINT fk_users_department
    FOREIGN KEY (department_id)
    REFERENCES silver.departments(dept_id)
    ON DELETE SET NULL;

-- GIN indexes for array fields (for efficient JSONB queries)
CREATE INDEX idx_silver_users_dept_ids_gin ON silver.users USING gin (department_ids);
CREATE INDEX idx_silver_users_ou_dept_ids_gin ON silver.users USING gin (ou_department_ids);
CREATE INDEX idx_silver_users_ad_groups_gin ON silver.users USING gin (ad_group_memberships);
CREATE INDEX idx_silver_users_ou_affiliations_gin ON silver.users USING gin (mcommunity_ou_affiliations);

-- Groups after bronze merge and cleaning
-- Merges data from mcommunity_ldap and active_directory
CREATE TABLE silver.groups (
    -- Primary identifiers
    group_id VARCHAR(100) PRIMARY KEY,                  -- Canonical ID (gidNumber or source-prefixed cn)
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

-- Group member relationships (users and groups)
CREATE TABLE silver.group_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    group_id VARCHAR(100) NOT NULL REFERENCES silver.groups(group_id) ON DELETE CASCADE,

    -- Member can be either a user or another group
    member_type VARCHAR(20) NOT NULL CHECK (member_type IN ('user', 'group')),
    member_uniqname VARCHAR(50),                        -- If member_type = 'user'
    member_group_id VARCHAR(100),                       -- If member_type = 'group'

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

-- Group owner relationships
CREATE TABLE silver.group_owners (
    ownership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    group_id VARCHAR(100) NOT NULL REFERENCES silver.groups(group_id) ON DELETE CASCADE,

    -- Owner can be either a user or another group
    owner_type VARCHAR(20) NOT NULL CHECK (owner_type IN ('user', 'group')),
    owner_uniqname VARCHAR(50),
    owner_group_id VARCHAR(100),

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

-- ============================================================================
-- GOLD LAYER: Master records representing authoritative truth
-- ============================================================================

-- Master department records (golden truth)
CREATE TABLE gold.department_masters (
    master_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Authoritative department information (best data from all sources)
    canonical_name VARCHAR(255) NOT NULL,
    canonical_code VARCHAR(50),
    canonical_description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    region VARCHAR(100),

    -- Master record metadata
    confidence_score DECIMAL(3,2) CHECK (confidence_score BETWEEN 0.00 AND 1.00),
    source_count INTEGER NOT NULL DEFAULT 1,  -- How many sources contribute to this master
    primary_source VARCHAR(50) NOT NULL,  -- Which source is considered most authoritative

    -- Reconciliation tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_department_masters_updated_at
    BEFORE UPDATE ON gold.department_masters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Mapping table: Which silver records contribute to each master record
CREATE TABLE gold.department_source_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_id UUID NOT NULL REFERENCES gold.department_masters(master_id) ON DELETE CASCADE,
    silver_id UUID NOT NULL REFERENCES silver.departments(silver_id) ON DELETE CASCADE,
    contribution_weight DECIMAL(3,2) DEFAULT 1.00,  -- How much this source contributes
    is_primary_source BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Master user records
CREATE TABLE gold.user_masters (
    master_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Authoritative user information
    canonical_uniqname VARCHAR(50) NOT NULL UNIQUE,
    canonical_name VARCHAR(255),
    canonical_email VARCHAR(255),
    primary_department_id UUID REFERENCES gold.department_masters(master_id),
    user_type VARCHAR(50),
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Master record metadata
    confidence_score DECIMAL(3,2) CHECK (confidence_score BETWEEN 0.00 AND 1.00),
    source_count INTEGER NOT NULL DEFAULT 1,
    primary_source VARCHAR(50) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_user_masters_updated_at
    BEFORE UPDATE ON gold.user_masters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- User source mapping
CREATE TABLE gold.user_source_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_id UUID NOT NULL REFERENCES gold.user_masters(master_id) ON DELETE CASCADE,
    silver_id UUID NOT NULL REFERENCES silver.users(silver_id) ON DELETE CASCADE,
    contribution_weight DECIMAL(3,2) DEFAULT 1.00,
    is_primary_source BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Lab membership relationships (many-to-many between users and labs)
CREATE TABLE gold.lab_memberships (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_master_id UUID NOT NULL REFERENCES gold.user_masters(master_id) ON DELETE CASCADE,
    lab_name VARCHAR(255) NOT NULL,
    lab_pi_uniqname VARCHAR(50),
    user_role VARCHAR(50),  -- 'PI', 'lab_manager', 'postdoc', 'grad_student', etc.
    department_master_id UUID REFERENCES gold.department_masters(master_id),
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    confidence_score DECIMAL(3,2) DEFAULT 1.00,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_lab_memberships_updated_at
    BEFORE UPDATE ON gold.lab_memberships
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- HELPFUL VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Complete lab picture with all members and their roles
CREATE VIEW gold.lab_rosters AS
SELECT
    lab_name,
    lab_pi_uniqname,
    dept.canonical_name as department_name,
    user_role,
    user_master.canonical_uniqname,
    user_master.canonical_name as user_name,
    user_master.canonical_email,
    membership.is_active as membership_active,
    membership.confidence_score
FROM gold.lab_memberships membership
JOIN gold.user_masters user_master ON membership.user_master_id = user_master.master_id
LEFT JOIN gold.department_masters dept ON membership.department_master_id = dept.master_id
WHERE membership.is_active = true
ORDER BY lab_name, user_role, user_master.canonical_name;

-- View: Department summary with user counts
CREATE VIEW gold.department_summary AS
SELECT
    dept.canonical_name,
    dept.canonical_code,
    dept.is_active,
    COUNT(DISTINCT user_master.master_id) as total_users,
    COUNT(DISTINCT CASE WHEN lab.user_role = 'PI' THEN lab.user_master_id END) as pi_count,
    COUNT(DISTINCT lab.lab_name) as lab_count,
    dept.confidence_score,
    dept.source_count
FROM gold.department_masters dept
LEFT JOIN gold.user_masters user_master ON user_master.primary_department_id = dept.master_id
LEFT JOIN gold.lab_memberships lab ON lab.department_master_id = dept.master_id AND lab.is_active = true
GROUP BY dept.master_id, dept.canonical_name, dept.canonical_code, dept.is_active, dept.confidence_score, dept.source_count
ORDER BY dept.canonical_name;

-- View: Groups with member and owner counts
CREATE VIEW silver.group_summary AS
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

-- View: User group memberships (for easy user->groups lookup)
CREATE VIEW silver.user_group_memberships AS
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

-- View: Synced groups (exist in both MCommunity and AD)
CREATE VIEW silver.synced_groups AS
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

-- Grant permissions for all new tables
GRANT ALL ON ALL TABLES IN SCHEMA bronze TO lsats_user;
GRANT ALL ON ALL TABLES IN SCHEMA silver TO lsats_user;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO lsats_user;

-- Add helpful table comments
COMMENT ON TABLE bronze.raw_entities IS 'Stores raw data exactly as received from any source system';
COMMENT ON TABLE silver.departments IS 'Cleaned and standardized department data from pandas processing';
COMMENT ON TABLE silver.groups IS 'Cleaned and standardized group data merged from MCommunity LDAP and Active Directory';
COMMENT ON TABLE silver.group_members IS 'Group membership relationships supporting both user and nested group members';
COMMENT ON TABLE silver.group_owners IS 'Group ownership relationships from MCommunity (AD does not have owner field)';
COMMENT ON TABLE gold.department_masters IS 'Authoritative master department records combining all sources';
COMMENT ON VIEW gold.lab_rosters IS 'Complete view of lab memberships with user details and roles';
COMMENT ON VIEW gold.department_summary IS 'Summary statistics for each department including user and lab counts';
COMMENT ON VIEW silver.group_summary IS 'Summary view of groups with membership and ownership counts';
COMMENT ON VIEW silver.user_group_memberships IS 'Easy lookup of all groups a user belongs to';
COMMENT ON VIEW silver.synced_groups IS 'Groups that exist in both MCommunity and Active Directory (MCommADSync)';

-- Labs after bronze merge and cleaning
-- Merges data from lab_award and organizational_unit
CREATE TABLE silver.labs (
    -- Primary identifiers
    lab_id VARCHAR(100) PRIMARY KEY,                    -- Same as PI uniqname (lowercase)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),   -- Internal UUID for referencing
    
    -- Principal Investigator (always required)
    pi_uniqname VARCHAR(50) NOT NULL,                   -- Lab owner/PI
    
    -- Core lab information
    lab_name VARCHAR(255),                              -- From OU name or generated from PI name
    lab_display_name VARCHAR(255),                      -- Friendly display name
    
    -- Department affiliation (from multiple sources)
    primary_department_id VARCHAR(50),                  -- Most common dept from awards or OU hierarchy
    department_ids JSONB DEFAULT '[]'::jsonb,           -- Array of all associated dept IDs
    department_names JSONB DEFAULT '[]'::jsonb,         -- Human-readable dept names
    
    -- Financial metrics (aggregated from lab_award)
    total_award_dollars NUMERIC(15,2) DEFAULT 0.00,     -- Sum of all Award Total Dollars
    total_direct_dollars NUMERIC(15,2) DEFAULT 0.00,    -- Sum of all Award Direct Dollars
    total_indirect_dollars NUMERIC(15,2) DEFAULT 0.00,  -- Sum of all Award Indirect Dollars
    award_count INTEGER DEFAULT 0,                      -- Number of active/historical awards
    active_award_count INTEGER DEFAULT 0,               -- Awards active as of transformation date
    
    -- Award date ranges
    earliest_award_start DATE,                          -- Earliest Award Project Start Date
    latest_award_end DATE,                              -- Latest Award Project End Date
    
    -- Active Directory organizational structure (from organizational_unit)
    has_ad_ou BOOLEAN DEFAULT false,                    -- Whether lab has an OU in AD
    ad_ou_dn TEXT,                                      -- Full distinguished name
    ad_ou_hierarchy JSONB DEFAULT '[]'::jsonb,          -- Array of OU levels
    ad_parent_ou TEXT,                                  -- Parent OU DN
    ad_ou_depth INTEGER,                                -- Depth in OU tree
    
    -- Infrastructure metadata (from organizational_unit)
    computer_count INTEGER DEFAULT 0,                   -- Direct computer count from OU
    has_computer_children BOOLEAN DEFAULT false,        -- Whether OU has computers
    has_child_ous BOOLEAN DEFAULT false,                -- Whether OU has sub-OUs
    
    -- AD timestamps (from organizational_unit)
    ad_ou_created TIMESTAMP WITH TIME ZONE,             -- whenCreated from AD
    ad_ou_modified TIMESTAMP WITH TIME ZONE,            -- whenChanged from AD
    
    -- Lab member counts (calculated from junction tables)
    pi_count INTEGER DEFAULT 0,                         -- Count of PIs (from lab_members where role is PI)
    investigator_count INTEGER DEFAULT 0,               -- Count of all investigators
    member_count INTEGER DEFAULT 0,                     -- Total member count
    
    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,            -- Has recent awards OR active AD OU
    has_active_awards BOOLEAN DEFAULT false,            -- Current date within award date range
    has_active_ou BOOLEAN DEFAULT false,                -- OU exists and has computers
    
    -- Data completeness flags
    has_award_data BOOLEAN DEFAULT false,               -- Sourced from lab_award
    has_ou_data BOOLEAN DEFAULT false,                  -- Sourced from organizational_unit
    data_source VARCHAR(50) NOT NULL,                   -- 'award_only', 'ou_only', 'award+ou'
    
    -- Data quality metrics
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,            -- ['no_silver_user', 'no_department', 'no_awards', etc.]
    
    -- Source tracking
    source_system VARCHAR(100) NOT NULL,                -- 'lab_award+organizational_unit' or single
    entity_hash VARCHAR(64) NOT NULL,                   -- Hash of merged content
    
    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    
    -- Foreign key to primary department (PI foreign key removed - quality flags track missing PIs)
    CONSTRAINT fk_labs_primary_department
        FOREIGN KEY (primary_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.labs IS 'Lab records merged from lab_award and organizational_unit sources. One record per PI uniqname.';
COMMENT ON COLUMN silver.labs.lab_id IS 'Primary key: lowercase PI uniqname';
COMMENT ON COLUMN silver.labs.pi_uniqname IS 'Principal Investigator uniqname - may not exist in silver.users (tracked via quality_flags)';
COMMENT ON COLUMN silver.labs.total_award_dollars IS 'Sum of all award total dollars for this lab';
COMMENT ON COLUMN silver.labs.ad_ou_dn IS 'Full AD distinguished name if lab has an OU';
COMMENT ON COLUMN silver.labs.data_source IS 'Indicates which bronze sources contributed: award_only, ou_only, or award+ou';

CREATE TRIGGER update_silver_labs_updated_at
    BEFORE UPDATE ON silver.labs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for silver.labs
CREATE INDEX idx_silver_labs_silver_id ON silver.labs (silver_id);
CREATE INDEX idx_silver_labs_pi_uniqname ON silver.labs (pi_uniqname);
CREATE INDEX idx_silver_labs_entity_hash ON silver.labs (entity_hash);
CREATE INDEX idx_silver_labs_active ON silver.labs (is_active, lab_name);
CREATE INDEX idx_silver_labs_quality ON silver.labs (data_quality_score DESC);
CREATE INDEX idx_silver_labs_primary_dept ON silver.labs (primary_department_id);
CREATE INDEX idx_silver_labs_data_source ON silver.labs (data_source, has_award_data, has_ou_data);
CREATE INDEX idx_silver_labs_with_ou ON silver.labs (has_ad_ou, ad_ou_dn) WHERE has_ad_ou = true;
CREATE INDEX idx_silver_labs_with_awards ON silver.labs (has_award_data, total_award_dollars DESC) WHERE has_award_data = true;
CREATE INDEX idx_silver_labs_active_awards ON silver.labs (has_active_awards, latest_award_end) WHERE has_active_awards = true;
CREATE INDEX idx_silver_labs_award_dollars ON silver.labs (total_award_dollars DESC);
CREATE INDEX idx_silver_labs_award_count ON silver.labs (award_count DESC);
CREATE INDEX idx_silver_labs_member_count ON silver.labs (member_count DESC);
CREATE INDEX idx_silver_labs_computer_count ON silver.labs (computer_count DESC);
CREATE INDEX idx_silver_labs_award_dates ON silver.labs (earliest_award_start, latest_award_end);
CREATE INDEX idx_silver_labs_dept_ids_gin ON silver.labs USING gin (department_ids);
CREATE INDEX idx_silver_labs_dept_names_gin ON silver.labs USING gin (department_names);
CREATE INDEX idx_silver_labs_ou_hierarchy_gin ON silver.labs USING gin (ad_ou_hierarchy);
CREATE INDEX idx_silver_labs_quality_flags_gin ON silver.labs USING gin (quality_flags);

-- Lab member relationships
CREATE TABLE silver.lab_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR(100) NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,
    member_uniqname VARCHAR(50) NOT NULL,               -- Member's uniqname
    
    -- Role information (from lab_award Person Role field)
    member_role VARCHAR(100) NOT NULL,                  -- 'UM Principal Investigator', 'Participating Investigator', etc.
    is_pi BOOLEAN GENERATED ALWAYS AS (
        member_role LIKE '%Principal Investigator%'
    ) STORED,
    
    -- Member details (denormalized for performance)
    member_first_name VARCHAR(255),                     -- From bronze lab_award Person First Name
    member_last_name VARCHAR(255),                      -- From bronze lab_award Person Last Name
    member_full_name VARCHAR(255),                      -- Derived or from silver.users
    member_department_id VARCHAR(50),                   -- Person Appt Department Id
    member_department_name VARCHAR(255),                -- Person Appt Department
    
    -- Job/employment info from silver.users (if available)
    silver_user_exists BOOLEAN DEFAULT false,           -- Whether member has silver.users record
    member_job_title TEXT,                              -- From silver.users.job_title
    
    -- Source tracking
    source_system VARCHAR(50) NOT NULL,                 -- 'lab_award' (future: 'ad_group', 'manual')
    source_award_ids JSONB DEFAULT '[]'::jsonb,         -- Array of Award IDs this person appears in
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key to user (if they exist in silver.users)
    CONSTRAINT fk_lab_members_user
        FOREIGN KEY (member_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE CASCADE,
    
    -- Foreign key to department
    CONSTRAINT fk_lab_members_department
        FOREIGN KEY (member_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.lab_members IS 'Lab membership records linking users to labs with their roles';
COMMENT ON COLUMN silver.lab_members.is_pi IS 'Computed: true if role contains "Principal Investigator"';
COMMENT ON COLUMN silver.lab_members.source_award_ids IS 'Array of Award IDs where this person appears';

-- Prevent duplicate memberships (same person, same lab, same role)
CREATE UNIQUE INDEX idx_lab_members_unique ON silver.lab_members (
    lab_id, member_uniqname, member_role
);

CREATE INDEX idx_lab_members_lab ON silver.lab_members (lab_id);
CREATE INDEX idx_lab_members_uniqname ON silver.lab_members (member_uniqname);
CREATE INDEX idx_lab_members_pi ON silver.lab_members (lab_id, is_pi) WHERE is_pi = true;
CREATE INDEX idx_lab_members_role ON silver.lab_members (member_role);
CREATE INDEX idx_lab_members_department ON silver.lab_members (member_department_id);
CREATE INDEX idx_lab_members_silver_user ON silver.lab_members (silver_user_exists, member_uniqname) WHERE silver_user_exists = true;
CREATE INDEX idx_lab_members_no_user ON silver.lab_members (member_uniqname) WHERE silver_user_exists = false;
CREATE INDEX idx_lab_members_source ON silver.lab_members (source_system);
CREATE INDEX idx_lab_members_source_awards_gin ON silver.lab_members USING gin (source_award_ids);

-- Lab award detail records
CREATE TABLE silver.lab_awards (
    award_record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR(100) NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,
    
    -- Award identification
    award_id VARCHAR(50) NOT NULL,                      -- Award Id (e.g., AWD029634)
    project_grant_id VARCHAR(50),                       -- Project/Grant
    
    -- Award details
    award_title TEXT NOT NULL,                          -- Award Title
    award_class VARCHAR(100),                           -- Award Class
    
    -- Financial information
    award_total_dollars NUMERIC(15,2),                  -- Parsed from "$60,000" format
    award_direct_dollars NUMERIC(15,2),
    award_indirect_dollars NUMERIC(15,2),
    facilities_admin_rate NUMERIC(5,2),                 -- Facilities & Admin Rate (%)
    
    -- Timeline
    award_start_date DATE,                              -- Award Project Start Date
    award_end_date DATE,                                -- Award Project End Date
    pre_nce_end_date DATE,                              -- Pre NCE Project End Date
    award_publish_date DATE,                            -- Award Publish Date
    
    -- Sponsor information
    direct_sponsor_name VARCHAR(255),
    direct_sponsor_category VARCHAR(255),
    direct_sponsor_subcategory VARCHAR(255),
    direct_sponsor_reference VARCHAR(255),
    prime_sponsor_name VARCHAR(255),
    prime_sponsor_category VARCHAR(255),
    prime_sponsor_subcategory VARCHAR(255),
    prime_sponsor_reference VARCHAR(255),
    
    -- Administrative information
    award_admin_department VARCHAR(255),
    award_admin_school_college VARCHAR(255),
    
    -- Person information (duplicated from lab_members for convenience)
    person_uniqname VARCHAR(50) NOT NULL,
    person_role VARCHAR(100) NOT NULL,
    person_first_name VARCHAR(255),
    person_last_name VARCHAR(255),
    person_appt_department VARCHAR(255),
    person_appt_department_id VARCHAR(50),
    person_appt_school_college VARCHAR(255),
    
    -- Activity status
    is_active BOOLEAN DEFAULT false,                    -- Current date within start/end range
    
    -- Source tracking
    bronze_raw_id UUID,                                 -- Link to bronze.raw_entities
    source_file VARCHAR(255),                           -- _source_file from bronze
    content_hash VARCHAR(64),                           -- _content_hash from bronze
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_lab_awards_person
        FOREIGN KEY (person_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_lab_awards_department
        FOREIGN KEY (person_appt_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.lab_awards IS 'Individual award records preserving all details from lab_award bronze source';
COMMENT ON COLUMN silver.lab_awards.is_active IS 'True if current date is between award start and end dates';
COMMENT ON COLUMN silver.lab_awards.bronze_raw_id IS 'Link back to original bronze.raw_entities record';

-- Prevent duplicate award records
CREATE UNIQUE INDEX idx_lab_awards_unique ON silver.lab_awards (
    award_id, person_uniqname, person_role
);

CREATE INDEX idx_lab_awards_lab ON silver.lab_awards (lab_id);
CREATE INDEX idx_lab_awards_award_id ON silver.lab_awards (award_id);
CREATE INDEX idx_lab_awards_person ON silver.lab_awards (person_uniqname);
CREATE INDEX idx_lab_awards_active ON silver.lab_awards (is_active, award_end_date) WHERE is_active = true;
CREATE INDEX idx_lab_awards_dates ON silver.lab_awards (award_start_date, award_end_date);
CREATE INDEX idx_lab_awards_end_date ON silver.lab_awards (award_end_date DESC);
CREATE INDEX idx_lab_awards_dollars ON silver.lab_awards (award_total_dollars DESC);
CREATE INDEX idx_lab_awards_direct_dollars ON silver.lab_awards (award_direct_dollars DESC);
CREATE INDEX idx_lab_awards_direct_sponsor ON silver.lab_awards (direct_sponsor_name);
CREATE INDEX idx_lab_awards_prime_sponsor ON silver.lab_awards (prime_sponsor_name);
CREATE INDEX idx_lab_awards_award_class ON silver.lab_awards (award_class);
CREATE INDEX idx_lab_awards_person_dept ON silver.lab_awards (person_appt_department_id);
CREATE INDEX idx_lab_awards_admin_dept ON silver.lab_awards (award_admin_department);
CREATE INDEX idx_lab_awards_person_role ON silver.lab_awards (person_role);
CREATE INDEX idx_lab_awards_bronze ON silver.lab_awards (bronze_raw_id);
CREATE INDEX idx_lab_awards_source_file ON silver.lab_awards (source_file);
CREATE INDEX idx_lab_awards_content_hash ON silver.lab_awards (content_hash);

