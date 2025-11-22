-- Migration: Add Silver Layer MCommunity Groups Table
-- Purpose: Create source-specific silver table for MCommunity groups
-- Dependencies: None

CREATE TABLE IF NOT EXISTS silver.mcommunity_groups (
    -- Primary Identifier
    mcommunity_group_uid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Business Key
    group_email VARCHAR(255) NOT NULL, -- Mapped from umichGroupEmail
    
    -- Core Identity
    group_name VARCHAR(500), -- Mapped from cn (first value)
    distinguished_name TEXT, -- Mapped from dn
    description TEXT, -- Mapped from umichDescription (first value)
    gid_number BIGINT, -- Mapped from gidNumber
    
    -- Flags & Status
    is_private BOOLEAN, -- Mapped from umichPrivate
    is_members_only BOOLEAN, -- Mapped from Membersonly
    is_joinable BOOLEAN, -- Mapped from joinable
    expiry_timestamp TIMESTAMP WITH TIME ZONE, -- Mapped from umichExpiryTimestamp
    
    -- Membership & Relationships (JSONB for flexibility and large lists)
    owners JSONB DEFAULT '[]'::jsonb, -- Mapped from owner
    members JSONB DEFAULT '[]'::jsonb, -- Mapped from member
    direct_members JSONB DEFAULT '[]'::jsonb, -- Mapped from umichDirectMember
    nested_members JSONB DEFAULT '[]'::jsonb, -- Mapped from umichDirectGroupMember
    requests_to JSONB DEFAULT '[]'::jsonb, -- Mapped from requestsTo
    aliases JSONB DEFAULT '[]'::jsonb, -- Mapped from cn (all values)
    
    -- Contact Info
    contact_info JSONB DEFAULT '{}'::jsonb, -- Consolidated phone, fax, address, uri
    
    -- Traceability
    raw_id UUID REFERENCES bronze.raw_entities(raw_id),
    entity_hash VARCHAR(64) NOT NULL, -- SHA-256 hash for change detection
    
    -- Standard Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_mcommunity_groups_updated_at
    BEFORE UPDATE ON silver.mcommunity_groups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying
CREATE INDEX idx_silver_mcommunity_groups_email ON silver.mcommunity_groups (group_email);
CREATE INDEX idx_silver_mcommunity_groups_name ON silver.mcommunity_groups (group_name);
CREATE INDEX idx_silver_mcommunity_groups_entity_hash ON silver.mcommunity_groups (entity_hash);
CREATE INDEX idx_silver_mcommunity_groups_ingestion_run ON silver.mcommunity_groups (ingestion_run_id);

-- GIN indexes for JSONB membership fields
CREATE INDEX idx_silver_mcommunity_groups_members ON silver.mcommunity_groups USING GIN (members);
CREATE INDEX idx_silver_mcommunity_groups_owners ON silver.mcommunity_groups USING GIN (owners);
CREATE INDEX idx_silver_mcommunity_groups_aliases ON silver.mcommunity_groups USING GIN (aliases);

COMMENT ON TABLE silver.mcommunity_groups IS 'Source-specific silver table for MCommunity groups, preserving raw structure and relationships.';
