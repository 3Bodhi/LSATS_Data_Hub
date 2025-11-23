-- Migration: Add silver.mcommunity_users source-specific table
-- Date: 2025-11-22
-- Purpose: Create source-specific silver table for MCommunity LDAP users
--          Part of two-tier silver architecture (source-specific -> consolidated)

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.mcommunity_users CASCADE;

-- Create silver.mcommunity_users table
CREATE TABLE IF NOT EXISTS silver.mcommunity_users (
    -- Primary identifier (business key - MCommunity uniqname)
    uid VARCHAR(50) PRIMARY KEY,

    -- Core identity fields
    display_name VARCHAR(100),
    given_name VARCHAR(100),
    cn JSONB DEFAULT '[]'::jsonb,              -- Common names array (can have aliases)
    sn JSONB DEFAULT '[]'::jsonb,              -- Surnames array (can have multiple)

    -- Contact information
    mail VARCHAR(255) NOT NULL,                 -- Email address (100% populated)
    telephone_number VARCHAR(50),

    -- Organizational affiliations
    ou JSONB DEFAULT '[]'::jsonb,              -- Organizational units array (affiliations)

    -- Work/Position information
    umich_title TEXT,                          -- Job title (can be very long, up to 611 chars observed)

    -- Postal address (structured and raw)
    umich_postal_address TEXT,                 -- $ delimited address string
    umich_postal_address_data TEXT,            -- Structured key:value format

    -- POSIX/System fields
    uid_number BIGINT NOT NULL,                -- POSIX uidNumber (100% populated)
    gid_number BIGINT NOT NULL,                -- POSIX gidNumber (100% populated)
    home_directory VARCHAR(50),
    login_shell VARCHAR(50),

    -- LDAP metadata
    object_class JSONB DEFAULT '[]'::jsonb,    -- LDAP object classes array

    -- LDAP server metadata (from bronze ingestion)
    ldap_server VARCHAR(255),                  -- _ldap_server from bronze
    search_base VARCHAR(255),                  -- _search_base from bronze

    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                      -- Most recent bronze.raw_entities.raw_id

    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'mcommunity_ldap' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,          -- SHA-256 for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_mcommunity_users_updated_at
    BEFORE UPDATE ON silver.mcommunity_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_raw_id
    ON silver.mcommunity_users (raw_id);

CREATE INDEX IF NOT EXISTS idx_mcommunity_users_entity_hash
    ON silver.mcommunity_users (entity_hash);

-- Email lookup (common join key to TDX and AD)
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_mail
    ON silver.mcommunity_users (mail);

-- POSIX identifiers (for system integration queries)
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_uid_number
    ON silver.mcommunity_users (uid_number);

CREATE INDEX IF NOT EXISTS idx_mcommunity_users_gid_number
    ON silver.mcommunity_users (gid_number);

CREATE INDEX IF NOT EXISTS idx_mcommunity_users_ingestion_run
    ON silver.mcommunity_users (ingestion_run_id);

-- GIN indexes for JSONB fields (for containment queries - e.g., finding users in specific OUs)
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_ou_gin
    ON silver.mcommunity_users USING gin (ou);

CREATE INDEX IF NOT EXISTS idx_mcommunity_users_cn_gin
    ON silver.mcommunity_users USING gin (cn);

CREATE INDEX IF NOT EXISTS idx_mcommunity_users_object_class_gin
    ON silver.mcommunity_users USING gin (object_class);

-- Table and column comments for documentation
COMMENT ON TABLE silver.mcommunity_users IS
    'Source-specific silver table for MCommunity LDAP users. Part of two-tier silver architecture - feeds into consolidated silver.users table. Contains typed columns extracted from bronze.raw_entities JSONB data where source_system=mcommunity_ldap.';

COMMENT ON COLUMN silver.mcommunity_users.uid IS
    'Primary key - MCommunity uniqname (LDAP uid attribute). Normalized to lowercase. Guaranteed unique and immutable.';

COMMENT ON COLUMN silver.mcommunity_users.cn IS
    'Common names array (JSONB) - can include full name and aliases. First value typically matches displayName.';

COMMENT ON COLUMN silver.mcommunity_users.sn IS
    'Surnames array (JSONB) - can include multiple surname variants or maiden names.';

COMMENT ON COLUMN silver.mcommunity_users.mail IS
    'Primary email address - 100% populated in MCommunity. Key field for cross-system matching.';

COMMENT ON COLUMN silver.mcommunity_users.ou IS
    'Organizational units array (JSONB) - contains all affiliations (e.g., "LSA MLB Thayer Events & Comm - Faculty and Staff", "College of Lit, Science & Arts - Faculty and Staff"). Critical for determining user roles and access.';

COMMENT ON COLUMN silver.mcommunity_users.umich_title IS
    'Job title from MCommunity - can be very long (up to 611 chars observed). Only 9% of users have this populated.';

COMMENT ON COLUMN silver.mcommunity_users.umich_postal_address IS
    'Postal address in $ delimited format (e.g., "Dept Name $ Building Room $ City State ZIP").';

COMMENT ON COLUMN silver.mcommunity_users.umich_postal_address_data IS
    'Structured postal address in key:value format (e.g., "{addr1=...}:{addr2=...}:{city=...}:{state=...}").';

COMMENT ON COLUMN silver.mcommunity_users.uid_number IS
    'POSIX uidNumber - unique numeric identifier for system-level operations. 100% populated.';

COMMENT ON COLUMN silver.mcommunity_users.gid_number IS
    'POSIX gidNumber - primary group ID for system-level operations. 100% populated.';

COMMENT ON COLUMN silver.mcommunity_users.home_directory IS
    'Unix home directory path - typically /users/{uniqname}. 100% populated.';

COMMENT ON COLUMN silver.mcommunity_users.login_shell IS
    'Default login shell - typically /bin/csh, /bin/bash, etc. 99.9% populated.';

COMMENT ON COLUMN silver.mcommunity_users.object_class IS
    'LDAP objectClass array - defines what type of entry this is (e.g., inetOrgPerson, posixAccount, umichPerson).';

COMMENT ON COLUMN silver.mcommunity_users.raw_id IS
    'Link to most recent bronze.raw_entities record for this user - for audit trail and full data access.';

COMMENT ON COLUMN silver.mcommunity_users.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed from previous version.';

-- Grant permissions (adjust as needed for your security model)
GRANT SELECT ON silver.mcommunity_users TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.mcommunity_users TO lsats_user;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Successfully created silver.mcommunity_users table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'mcommunity_users');
END $$;
