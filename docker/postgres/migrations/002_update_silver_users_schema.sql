-- Migration: Update silver.users table schema for multi-source transformation
-- This migration updates the existing silver.users table to support merging data from
-- tdx, umich_api, mcommunity_ldap, and active_directory sources

-- Drop the old table and recreate with new schema
-- (Alternative: Could use ALTER TABLE to add columns, but DROP/CREATE is cleaner for major schema changes)
DROP TABLE IF EXISTS silver.users CASCADE;

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

-- Add comment
COMMENT ON TABLE silver.users IS 'Cleaned and standardized user data merged from tdx, umich_api, mcommunity_ldap, and active_directory';
