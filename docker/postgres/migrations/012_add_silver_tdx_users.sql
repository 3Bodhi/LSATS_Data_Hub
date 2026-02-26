-- Migration: Add silver.tdx_users source-specific table
-- Date: 2025-11-21
-- Purpose: Create source-specific silver table for TeamDynamix users
--          Part of two-tier silver architecture (source-specific -> consolidated)

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.tdx_users CASCADE;

-- Create silver.tdx_users table
CREATE TABLE IF NOT EXISTS silver.tdx_users (
    -- Primary identifier (TDX native ID)
    tdx_user_uid UUID PRIMARY KEY,
    
    -- Business key (for matching to consolidated silver.users)
    uniqname VARCHAR(50),
    
    -- Core identity fields
    external_id VARCHAR(50),                    -- TDX ExternalID (EmplId)
    username VARCHAR(255),                      -- TDX Username
    first_name VARCHAR(255),
    middle_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    nickname VARCHAR(255),
    
    -- Contact information
    primary_email VARCHAR(255),
    alternate_email VARCHAR(255),
    alert_email VARCHAR(255),
    work_phone VARCHAR(50),
    mobile_phone VARCHAR(50),
    home_phone VARCHAR(50),
    fax VARCHAR(50),
    other_phone VARCHAR(50),
    pager VARCHAR(50),
    im_provider VARCHAR(100),
    im_handle VARCHAR(255),
    
    -- Work location fields
    work_address VARCHAR(255),
    work_city VARCHAR(100),
    work_state VARCHAR(50),
    work_zip VARCHAR(20),
    work_country VARCHAR(100),
    
    -- Note: Home location fields removed (99.9% empty in current data)
    -- If needed, query bronze layer directly
    
    -- Employment and organizational
    title VARCHAR(255),                         -- Job title in TDX
    company VARCHAR(255),
    default_account_id INTEGER,
    default_account_name VARCHAR(255),
    location_id INTEGER,
    location_name VARCHAR(255),
    location_room_id INTEGER,
    location_room_name VARCHAR(255),
    reports_to_uid UUID,                        -- References another tdx_user_uid
    reports_to_full_name VARCHAR(255),
    
    -- Status and authentication
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_employee BOOLEAN,
    is_confidential BOOLEAN,
    authentication_provider_id INTEGER,
    authentication_user_name VARCHAR(255),
    security_role_id UUID,
    security_role_name VARCHAR(100),
    
    -- TDX operational fields
    beid VARCHAR(255),                          -- Badge/ID
    beid_int INTEGER,
    default_priority_id INTEGER,
    default_priority_name VARCHAR(100),
    should_report_time BOOLEAN,
    is_capacity_managed BOOLEAN,
    default_rate DECIMAL(10,2),
    cost_rate DECIMAL(10,2),
    primary_client_portal_application_id INTEGER,
    
    -- Signature and profile
    technician_signature TEXT,
    profile_image_file_name VARCHAR(255),
    apply_technician_signature_to_replies BOOLEAN,
    apply_technician_signature_to_updates_and_comments BOOLEAN,
    
    -- Dates
    end_date TIMESTAMP WITH TIME ZONE,
    report_time_after_date TIMESTAMP WITH TIME ZONE,
    
    -- Complex fields (arrays/objects - keep as JSONB)
    -- Note: Only 2.4-2.8% of users have these fields populated
    attributes JSONB DEFAULT '[]'::jsonb,      -- Custom attributes array
    applications JSONB DEFAULT '[]'::jsonb,    -- Application permissions
    org_applications JSONB DEFAULT '[]'::jsonb,
    group_ids JSONB DEFAULT '[]'::jsonb,       -- TDX group memberships
    permissions JSONB DEFAULT '{}'::jsonb,     -- Permission object (currently unused)
    
    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                      -- Most recent bronze.raw_entities.raw_id
    raw_data_snapshot JSONB,                   -- Optional: full copy for audit
    
    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'tdx' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,          -- For change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE UNIQUE INDEX IF NOT EXISTS idx_tdx_users_uniqname 
    ON silver.tdx_users (uniqname) WHERE uniqname IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tdx_users_raw_id 
    ON silver.tdx_users (raw_id);

CREATE INDEX IF NOT EXISTS idx_tdx_users_entity_hash 
    ON silver.tdx_users (entity_hash);

CREATE INDEX IF NOT EXISTS idx_tdx_users_is_active 
    ON silver.tdx_users (is_active);

CREATE INDEX IF NOT EXISTS idx_tdx_users_external_id 
    ON silver.tdx_users (external_id) WHERE external_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tdx_users_default_account_id 
    ON silver.tdx_users (default_account_id);

CREATE INDEX IF NOT EXISTS idx_tdx_users_reports_to_uid 
    ON silver.tdx_users (reports_to_uid) WHERE reports_to_uid IS NOT NULL;

-- GIN indexes for JSONB fields (for containment queries)
CREATE INDEX IF NOT EXISTS idx_tdx_users_attributes_gin 
    ON silver.tdx_users USING gin (attributes);

CREATE INDEX IF NOT EXISTS idx_tdx_users_applications_gin 
    ON silver.tdx_users USING gin (applications);

CREATE INDEX IF NOT EXISTS idx_tdx_users_group_ids_gin 
    ON silver.tdx_users USING gin (group_ids);

-- Table and column comments for documentation
COMMENT ON TABLE silver.tdx_users IS 
    'Source-specific silver table for TeamDynamix users. Part of two-tier silver architecture - feeds into consolidated silver.users table. Contains typed columns extracted from bronze.raw_entities JSONB data.';

COMMENT ON COLUMN silver.tdx_users.tdx_user_uid IS 
    'Primary key from TDX UID field - unique TeamDynamix user identifier (UUID)';

COMMENT ON COLUMN silver.tdx_users.uniqname IS 
    'Business key from TDX AlternateID - normalized lowercase uniqname for joining to silver.users';

COMMENT ON COLUMN silver.tdx_users.raw_id IS 
    'Link to most recent bronze.raw_entities record for this user - for audit trail';

COMMENT ON COLUMN silver.tdx_users.reports_to_uid IS 
    'Logical FK to silver.tdx_users(tdx_user_uid) - supervisor relationship (not enforced per medallion standards)';

COMMENT ON COLUMN silver.tdx_users.entity_hash IS 
    'SHA-256 hash of significant fields for change detection - only transform if hash changed';

COMMENT ON COLUMN silver.tdx_users.attributes IS 
    'Custom TDX attributes array (JSONB) - only 2.8% of users have this populated';

COMMENT ON COLUMN silver.tdx_users.applications IS 
    'TDX application permissions array (JSONB) - only 2.8% of users have this populated';

COMMENT ON COLUMN silver.tdx_users.group_ids IS 
    'TDX group membership IDs array (JSONB) - only 2.4% of users have this populated';

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.tdx_users TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.tdx_users TO lsats_user;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Successfully created silver.tdx_users table with % indexes', 
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'tdx_users');
END $$;
