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
COMMENT ON TABLE gold.department_masters IS 'Authoritative master department records combining all sources';
COMMENT ON VIEW gold.lab_rosters IS 'Complete view of lab memberships with user details and roles';
COMMENT ON VIEW gold.department_summary IS 'Summary statistics for each department including user and lab counts';
