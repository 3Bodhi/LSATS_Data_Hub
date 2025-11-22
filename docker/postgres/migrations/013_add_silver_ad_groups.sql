-- Migration: Add Silver Layer AD Groups Table
-- Purpose: Create source-specific silver table for Active Directory groups
-- Dependencies: None

CREATE TABLE IF NOT EXISTS silver.ad_groups (
    -- Primary Identifier (mapped from objectGUID)
    ad_group_guid UUID PRIMARY KEY,
    
    -- Core Identity
    name VARCHAR(255),
    cn VARCHAR(255),
    sam_account_name VARCHAR(255),
    distinguished_name TEXT,
    
    -- Metadata
    description TEXT,
    group_type INTEGER,
    sam_account_type INTEGER,
    object_category VARCHAR(255),
    object_class TEXT[], -- Array of strings
    
    -- Membership (Stored as JSONB arrays of DN strings)
    members JSONB DEFAULT '[]'::jsonb,
    member_of JSONB DEFAULT '[]'::jsonb,
    
    -- Timestamps
    when_created TIMESTAMP WITH TIME ZONE,
    when_changed TIMESTAMP WITH TIME ZONE,
    
    -- Email & Contact
    mail VARCHAR(255),
    display_name VARCHAR(255),
    info TEXT,
    managed_by TEXT,
    proxy_addresses TEXT[],
    
    -- Replication Metadata
    usn_created BIGINT,
    usn_changed BIGINT,
    
    -- Security Identifiers
    object_sid VARCHAR(255),
    sid_history VARCHAR(255), -- Often binary/base64, keeping as string
    
    -- Traceability
    raw_id UUID REFERENCES bronze.raw_entities(raw_id),
    entity_hash VARCHAR(64) NOT NULL, -- SHA-256 hash for change detection
    
    -- Standard Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_ad_groups_updated_at
    BEFORE UPDATE ON silver.ad_groups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying
CREATE INDEX idx_silver_ad_groups_sam_account ON silver.ad_groups (sam_account_name);
CREATE INDEX idx_silver_ad_groups_cn ON silver.ad_groups (cn);
CREATE INDEX idx_silver_ad_groups_entity_hash ON silver.ad_groups (entity_hash);
CREATE INDEX idx_silver_ad_groups_ingestion_run ON silver.ad_groups (ingestion_run_id);

-- Hierarchical query support for DNs
CREATE INDEX idx_silver_ad_groups_dn_pattern ON silver.ad_groups (distinguished_name text_pattern_ops);

-- GIN indexes for JSONB membership fields
CREATE INDEX idx_silver_ad_groups_members ON silver.ad_groups USING GIN (members);
CREATE INDEX idx_silver_ad_groups_member_of ON silver.ad_groups USING GIN (member_of);

COMMENT ON TABLE silver.ad_groups IS 'Source-specific silver table for Active Directory groups, preserving raw structure and relationships in JSONB format.';
