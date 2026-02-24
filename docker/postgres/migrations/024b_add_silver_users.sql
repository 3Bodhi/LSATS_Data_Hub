-- Migration: 024b_add_silver_users.sql
-- Purpose: Create consolidated silver.users table merging TDX, AD, UMAPI, MCommunity
-- Date: 2025-11-23
--
-- Consolidates 4 source-specific tables:
--   silver.tdx_users (91K users)
--   silver.ad_users (535K users)
--   silver.umapi_employees (61K employees, 93K records with multi-appointments)
--   silver.mcommunity_users (703K users total, 487K alumni-only)
--
-- Design philosophy: Comprehensive but not redundant
--   - Captures all unique data points from every source
--   - Deduplicates on uniqname (primary key)
--   - Intelligent merge with documented priority rules
--   - Quality scored and tracked

CREATE TABLE IF NOT EXISTS silver.users (
    -- ============================================
    -- PRIMARY BUSINESS KEY
    -- ============================================
    uniqname VARCHAR(50) PRIMARY KEY,  -- Normalized lowercase uniqname
    
    -- ============================================
    -- SURROGATE KEYS
    -- ============================================
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),
    
    -- ============================================
    -- EXTERNAL SYSTEM IDENTIFIERS
    -- ============================================
    tdx_user_uid UUID,                    -- TDX UID for write-back operations
    umich_empl_id VARCHAR(10),            -- Primary UMAPI EmplId (empl_rcd=0)
    umich_empl_ids JSONB DEFAULT '[]'::jsonb,  -- All EmplIds if multiple records [{empl_id, empl_rcd}]
    ldap_uid_number BIGINT,               -- MCommunity POSIX uidNumber
    ldap_gid_number BIGINT,               -- MCommunity POSIX gidNumber
    ad_object_guid VARCHAR(255),          -- AD ObjectGUID
    ad_sam_account_name VARCHAR(255),     -- AD sAMAccountName
    ad_object_sid TEXT,                   -- AD objectSid
    
    -- ============================================
    -- CORE IDENTITY FIELDS
    -- Priority: TDX > UMAPI > MCommunity > AD
    -- ============================================
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    display_name VARCHAR(255),            -- From MCommunity displayName or AD
    
    -- ============================================
    -- CONTACT INFORMATION
    -- Priority: TDX > MCommunity > AD
    -- ============================================
    primary_email VARCHAR(255),
    
    -- Work contact (from UMAPI work_location JSONB or MCommunity)
    work_phone VARCHAR(50),
    work_phone_extension VARCHAR(20),
    mobile_phone VARCHAR(50),
    
    -- Work address (from UMAPI work_location JSONB)
    work_address_line1 VARCHAR(255),
    work_address_line2 VARCHAR(255),
    work_address_line3 VARCHAR(255),
    work_city VARCHAR(100),
    work_state VARCHAR(50),
    work_postal_code VARCHAR(20),
    work_country VARCHAR(100),
    
    -- ============================================
    -- EMPLOYMENT INFORMATION
    -- Priority: UMAPI (empl_rcd=0) > TDX
    -- ============================================
    -- Primary department (from UMAPI empl_rcd=0 or TDX default_account)
    department_id VARCHAR(10),            -- FK to silver.departments(dept_id) - not enforced
    department_name VARCHAR(255),         -- Denormalized for convenience
    
    -- All departments (from UMAPI multiple records) - JSONB array
    department_ids JSONB DEFAULT '[]'::jsonb,  -- [{dept_id, dept_name, empl_rcd}]
    
    -- Job information (from UMAPI primary record or TDX)
    job_title TEXT,                       -- Priority: UMAPI university_job_title > TDX title
    department_job_title VARCHAR(255),    -- From UMAPI primary record
    
    -- Primary job code (from UMAPI empl_rcd=0) for ease of comparison
    primary_job_code VARCHAR(10),         -- Primary jobcode from empl_rcd=0
    job_codes JSONB DEFAULT '[]'::jsonb,  -- All job codes from multiple UMAPI records [{job_code, dept_job_title, empl_rcd}]
    
    -- Supervisor relationship (from UMAPI primary or TDX)
    primary_supervisor_id VARCHAR(10),    -- Primary supervisor EmplId (from empl_rcd=0)
    primary_supervisor_uniqname VARCHAR(50),  -- Resolved to uniqname for easy joins
    supervisor_ids JSONB DEFAULT '[]'::jsonb,  -- All supervisors from multiple records [{empl_id, uniqname, empl_rcd}]
    reports_to_uid UUID,                  -- TDX reports_to_uid for backward compatibility
    
    -- ============================================
    -- ROLE & STATUS FLAGS
    -- ============================================
    is_pi BOOLEAN DEFAULT FALSE,          -- Derived from lab_awards.person_uniqname + ad_organizational_units.extracted_uniqname
    is_active BOOLEAN DEFAULT TRUE,       -- Merged from all sources (active if ANY source shows active)
    is_employee BOOLEAN,                  -- True if in UMAPI or TDX.is_employee=TRUE
    
    -- AD account status
    ad_account_disabled BOOLEAN,
    ad_account_locked BOOLEAN,
    ad_last_logon TIMESTAMP WITH TIME ZONE,
    ad_last_logon_timestamp TIMESTAMP WITH TIME ZONE,
    ad_pwd_last_set TIMESTAMP WITH TIME ZONE,
    
    -- ============================================
    -- ORGANIZATIONAL AFFILIATIONS
    -- ============================================
    -- MCommunity organizational units (affiliations)
    mcommunity_ou_affiliations JSONB DEFAULT '[]'::jsonb,  -- ["LSA - Faculty and Staff", "Alumni", ...]
    
    -- Parsed department IDs from MCommunity OU strings
    ou_department_ids JSONB DEFAULT '[]'::jsonb,
    
    -- AD organizational unit hierarchy
    ad_ou_root VARCHAR(100),
    ad_ou_organization VARCHAR(100),
    ad_ou_department VARCHAR(100),
    ad_ou_full_path JSONB DEFAULT '[]'::jsonb,
    ad_parent_ou_dn VARCHAR(500),
    
    -- ============================================
    -- GROUP MEMBERSHIPS
    -- ============================================
    -- AD group memberships (full DNs)
    ad_group_memberships JSONB DEFAULT '[]'::jsonb,
    ad_primary_group_id INTEGER,
    
    -- TDX group IDs
    tdx_group_ids JSONB DEFAULT '[]'::jsonb,
    
    -- ============================================
    -- POSIX/UNIX INFORMATION
    -- ============================================
    home_directory VARCHAR(255),          -- From MCommunity or AD
    login_shell VARCHAR(50),              -- From MCommunity or AD
    
    -- ============================================
    -- TDX-SPECIFIC FIELDS (for backward compatibility)
    -- ============================================
    tdx_external_id VARCHAR(255),         -- External ID in TDX
    tdx_beid VARCHAR(255),                -- Business Entity ID
    tdx_security_role_id UUID,
    tdx_security_role_name VARCHAR(100),
    tdx_is_employee BOOLEAN,
    tdx_is_confidential BOOLEAN,
    
    -- ============================================
    -- DATA QUALITY & TRACKING
    -- ============================================
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score >= 0.00 AND data_quality_score <= 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,  -- Array of quality issue identifiers
    
    -- Source tracking
    source_system VARCHAR(200) NOT NULL,  -- e.g., "tdx+umapi+mcom+ad" or "mcom+ad"
    source_entity_id VARCHAR(255) NOT NULL,  -- Tracking ID for lineage
    entity_hash VARCHAR(64) NOT NULL,     -- SHA-256 hash for change detection
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- ============================================
-- INDEXES
-- ============================================

-- Primary and surrogate keys
CREATE INDEX idx_users_silver_id ON silver.users(silver_id);

-- External system identifiers (for joins and lookups)
CREATE INDEX idx_users_tdx_user_uid ON silver.users(tdx_user_uid) WHERE tdx_user_uid IS NOT NULL;
CREATE INDEX idx_users_umich_empl_id ON silver.users(umich_empl_id) WHERE umich_empl_id IS NOT NULL;
CREATE INDEX idx_users_ldap_uid_number ON silver.users(ldap_uid_number) WHERE ldap_uid_number IS NOT NULL;
CREATE INDEX idx_users_ad_object_guid ON silver.users(ad_object_guid) WHERE ad_object_guid IS NOT NULL;
CREATE INDEX idx_users_ad_sam_account_name ON silver.users(ad_sam_account_name) WHERE ad_sam_account_name IS NOT NULL;

-- Contact and identity
CREATE INDEX idx_users_email ON silver.users(primary_email) WHERE primary_email IS NOT NULL;
CREATE INDEX idx_users_full_name ON silver.users(full_name);

-- Employment and organizational
CREATE INDEX idx_users_department ON silver.users(department_id) WHERE department_id IS NOT NULL;
CREATE INDEX idx_users_primary_job_code ON silver.users(primary_job_code) WHERE primary_job_code IS NOT NULL;
CREATE INDEX idx_users_primary_supervisor ON silver.users(primary_supervisor_uniqname) WHERE primary_supervisor_uniqname IS NOT NULL;

-- Role flags
CREATE INDEX idx_users_is_pi ON silver.users(is_pi) WHERE is_pi = TRUE;
CREATE INDEX idx_users_is_active ON silver.users(is_active);
CREATE INDEX idx_users_is_employee ON silver.users(is_employee) WHERE is_employee = TRUE;

-- Data quality and tracking
CREATE INDEX idx_users_entity_hash ON silver.users(entity_hash);
CREATE INDEX idx_users_quality_score ON silver.users(data_quality_score DESC);
CREATE INDEX idx_users_source ON silver.users(source_system, source_entity_id);
CREATE INDEX idx_users_ingestion_run ON silver.users(ingestion_run_id);
CREATE INDEX idx_users_updated_at ON silver.users(updated_at);

-- JSONB GIN indexes for array/object searches
CREATE INDEX idx_users_dept_ids_gin ON silver.users USING gin(department_ids);
CREATE INDEX idx_users_job_codes_gin ON silver.users USING gin(job_codes);
CREATE INDEX idx_users_supervisor_ids_gin ON silver.users USING gin(supervisor_ids);
CREATE INDEX idx_users_mcom_ou_gin ON silver.users USING gin(mcommunity_ou_affiliations);
CREATE INDEX idx_users_ou_dept_ids_gin ON silver.users USING gin(ou_department_ids);
CREATE INDEX idx_users_ad_groups_gin ON silver.users USING gin(ad_group_memberships);
CREATE INDEX idx_users_tdx_groups_gin ON silver.users USING gin(tdx_group_ids);
CREATE INDEX idx_users_quality_flags_gin ON silver.users USING gin(quality_flags);

-- ============================================
-- FOREIGN KEY CONSTRAINTS
-- ============================================

-- Logical FK to departments (not enforced per medallion standards)
-- CREATE INDEX already created above for department_id

-- FK to meta.ingestion_runs (enforced)
-- Already defined inline in table definition

-- ============================================
-- TRIGGERS
-- ============================================

CREATE TRIGGER update_silver_users_updated_at
    BEFORE UPDATE ON silver.users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE silver.users IS 
'Consolidated users table merging TDX, AD, UMAPI, and MCommunity sources. Comprehensive but not redundant - contains all unique data points from 4 source systems with intelligent merge priority rules. Created 2025-11-23 to replace legacy silver.users table.';

COMMENT ON COLUMN silver.users.uniqname IS 
'Primary business key - normalized lowercase university unique name. Guaranteed unique across all sources.';

COMMENT ON COLUMN silver.users.is_pi IS 
'Principal Investigator flag derived from silver.lab_awards.person_uniqname UNION silver.ad_organizational_units.extracted_uniqname. Approximately 600 PIs identified.';

COMMENT ON COLUMN silver.users.department_ids IS 
'JSONB array of all departments from UMAPI multiple employment records. Format: [{dept_id, dept_name, empl_rcd}]. Use for users with dual appointments.';

COMMENT ON COLUMN silver.users.job_codes IS 
'JSONB array of all job codes from UMAPI multiple employment records. Format: [{job_code, dept_job_title, empl_rcd}]. Primary job code also available in primary_job_code column.';

COMMENT ON COLUMN silver.users.supervisor_ids IS 
'JSONB array of all supervisors from UMAPI multiple employment records. Format: [{empl_id, uniqname, empl_rcd}]. Primary supervisor also available in primary_supervisor_uniqname column.';

COMMENT ON COLUMN silver.users.mcommunity_ou_affiliations IS 
'JSONB array of MCommunity organizational unit strings (e.g., ["LSA - Faculty and Staff", "Alumni"]). 487K users (69% of MCommunity) have only "Alumni" affiliation.';

COMMENT ON COLUMN silver.users.data_quality_score IS 
'Calculated quality score 0.00-1.00. Scoring: start 1.00, deduct for missing_email (-0.25), missing_name (-0.20), missing_department (-0.15), missing_job_title (-0.10), not_umapi_employee (-0.10), ad_disabled (-0.10), no_tdx_record (-0.05), mcom_only (-0.15).';

COMMENT ON COLUMN silver.users.source_system IS 
'Pipe-delimited list of contributing source systems (e.g., "tdx+umapi+mcom+ad"). Indicates data completeness and provenance.';

COMMENT ON COLUMN silver.users.entity_hash IS 
'SHA-256 content hash of significant fields for change detection. Only transform record if hash changed from previous version.';
