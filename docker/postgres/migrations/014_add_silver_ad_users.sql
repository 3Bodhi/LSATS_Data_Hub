-- Migration: 014_add_silver_ad_users.sql
-- Description: Create silver.ad_users table for Active Directory user data

CREATE TABLE IF NOT EXISTS silver.ad_users (
    -- Primary Key
    ad_user_guid UUID PRIMARY KEY,
    
    -- Identity & Core
    name TEXT,
    cn TEXT,
    sam_account_name TEXT,
    distinguished_name TEXT,
    user_principal_name TEXT,
    display_name TEXT,
    given_name TEXT,
    sn TEXT,
    initials TEXT,
    title TEXT,
    description TEXT,
    
    -- Contact
    mail TEXT,
    telephone_number TEXT,
    mobile TEXT,
    other_mobile TEXT,
    facsimile_telephone_number TEXT,
    street_address TEXT,
    proxy_addresses JSONB DEFAULT '[]'::jsonb,
    
    -- Organization
    department TEXT,
    umichad_ou JSONB DEFAULT '[]'::jsonb,
    umichad_role JSONB DEFAULT '[]'::jsonb,
    
    -- Account Status & Security
    user_account_control INTEGER,
    account_expires TIMESTAMP WITH TIME ZONE,
    pwd_last_set TIMESTAMP WITH TIME ZONE,
    last_logon TIMESTAMP WITH TIME ZONE,
    last_logon_timestamp TIMESTAMP WITH TIME ZONE,
    last_logoff TIMESTAMP WITH TIME ZONE,
    bad_pwd_count INTEGER,
    bad_password_time TIMESTAMP WITH TIME ZONE,
    logon_count INTEGER,
    lockout_time TIMESTAMP WITH TIME ZONE,
    object_sid TEXT,
    sid_history JSONB DEFAULT '[]'::jsonb,
    
    -- Metadata
    when_created TIMESTAMP WITH TIME ZONE,
    when_changed TIMESTAMP WITH TIME ZONE,
    usn_created BIGINT,
    usn_changed BIGINT,
    object_class JSONB DEFAULT '[]'::jsonb,
    object_category TEXT,
    instance_type INTEGER,
    
    -- Membership
    member_of JSONB DEFAULT '[]'::jsonb,
    primary_group_id INTEGER,
    
    -- Posix & Other
    uid TEXT,
    uid_number BIGINT,
    gid_number BIGINT,
    home_directory TEXT,
    home_drive TEXT,
    login_shell TEXT,
    employee_type TEXT,
    
    -- System Columns
    raw_id UUID NOT NULL,
    entity_hash TEXT NOT NULL,
    ingestion_run_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_silver_ad_users_sam_account_name ON silver.ad_users(sam_account_name);
CREATE INDEX IF NOT EXISTS idx_silver_ad_users_mail ON silver.ad_users(mail);
CREATE INDEX IF NOT EXISTS idx_silver_ad_users_uid ON silver.ad_users(uid);
CREATE INDEX IF NOT EXISTS idx_silver_ad_users_entity_hash ON silver.ad_users(entity_hash);
