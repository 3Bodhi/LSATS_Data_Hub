-- Migration: Add Silver Computers Schema
-- Description: Creates silver.computers and related tables for unified computer/asset data
-- Date: 2025-11-19
-- Dependencies: silver.users, silver.departments, silver.labs, silver.groups

-- ============================================================================
-- PART 1: Bronze Layer Indexes for Performance
-- ============================================================================

-- Computer/asset name indexes (primary matching field)
CREATE INDEX IF NOT EXISTS idx_bronze_computer_name ON bronze.raw_entities (
    LOWER(COALESCE(raw_data->>'Name', raw_data->>'cn'))
) WHERE entity_type IN ('computer', 'asset');

-- MAC address index (normalized for matching)
CREATE INDEX IF NOT EXISTS idx_bronze_computer_mac ON bronze.raw_entities (
    UPPER(REGEXP_REPLACE(
        COALESCE(raw_data->>'MAC', raw_data->'Attributes'->>'MAC Address(es)'),
        '[^A-F0-9]', '', 'g'
    ))
) WHERE entity_type IN ('computer', 'asset')
  AND (raw_data->>'MAC' IS NOT NULL OR raw_data->'Attributes'->>'MAC Address(es)' IS NOT NULL);

-- Serial number index (normalized for matching)
CREATE INDEX IF NOT EXISTS idx_bronze_computer_serial ON bronze.raw_entities (
    UPPER(COALESCE(raw_data->>'SerialNumber', raw_data->>'OEM SN'))
) WHERE entity_type IN ('computer', 'asset')
  AND (raw_data->>'SerialNumber' IS NOT NULL OR raw_data->>'OEM SN' IS NOT NULL);

-- TDX asset indexes
CREATE INDEX IF NOT EXISTS idx_bronze_tdx_asset_id ON bronze.raw_entities (
    CAST(raw_data->>'ID' AS integer)
) WHERE entity_type = 'asset' AND source_system = 'tdx';

CREATE INDEX IF NOT EXISTS idx_bronze_tdx_owner_uid ON bronze.raw_entities (
    (raw_data->>'OwningCustomerID')
) WHERE entity_type = 'asset' AND source_system = 'tdx';

CREATE INDEX IF NOT EXISTS idx_bronze_tdx_dept_id ON bronze.raw_entities (
    CAST(raw_data->>'OwningDepartmentID' AS integer)
) WHERE entity_type = 'asset' AND source_system = 'tdx';

-- AD computer indexes
CREATE INDEX IF NOT EXISTS idx_bronze_ad_computer_dn ON bronze.raw_entities (
    (raw_data->>'dn')
) WHERE entity_type = 'computer' AND source_system = 'active_directory';

CREATE INDEX IF NOT EXISTS idx_bronze_ad_computer_guid ON bronze.raw_entities (
    (raw_data->>'objectGUID')
) WHERE entity_type = 'computer' AND source_system = 'active_directory';

-- Key client indexes
CREATE INDEX IF NOT EXISTS idx_bronze_kc_owner ON bronze.raw_entities (
    (raw_data->>'Owner')
) WHERE entity_type = 'computer' AND source_system = 'key_client';

CREATE INDEX IF NOT EXISTS idx_bronze_kc_last_user ON bronze.raw_entities (
    LOWER(raw_data->>'Last User')
) WHERE entity_type = 'computer' AND source_system = 'key_client';

-- Ingestion timestamp index for incremental processing
CREATE INDEX IF NOT EXISTS idx_bronze_computer_source_ingested ON bronze.raw_entities (
    entity_type, source_system, ingested_at DESC
) WHERE entity_type IN ('computer', 'asset');

-- GIN indexes for arrays
CREATE INDEX IF NOT EXISTS idx_bronze_ad_computer_memberof_gin ON bronze.raw_entities USING gin (
    (raw_data->'memberOf')
) WHERE entity_type = 'computer' AND source_system = 'active_directory';

CREATE INDEX IF NOT EXISTS idx_bronze_tdx_attributes_gin ON bronze.raw_entities USING gin (
    (raw_data->'Attributes')
) WHERE entity_type = 'asset' AND source_system = 'tdx';

-- ============================================================================
-- PART 2: Silver Layer Tables
-- ============================================================================

-- Main table: silver.computers
CREATE TABLE silver.computers (
    -- Primary identifiers
    computer_id VARCHAR(100) PRIMARY KEY,
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),

    -- Computer name variants
    computer_name VARCHAR(255) NOT NULL,
    computer_name_aliases JSONB DEFAULT '[]'::jsonb,

    -- Hardware identifiers (for cross-source matching)
    mac_address VARCHAR(17),
    mac_addresses JSONB DEFAULT '[]'::jsonb,
    serial_number VARCHAR(100),
    serial_numbers JSONB DEFAULT '[]'::jsonb,

    -- TeamDynamix identifiers (critical for write-back)
    tdx_asset_id INTEGER,
    tdx_asset_uid UUID,
    tdx_tag VARCHAR(50),
    tdx_status_id INTEGER,
    tdx_form_id INTEGER,

    -- Active Directory identifiers
    ad_object_guid VARCHAR(255),
    ad_object_sid VARCHAR(255),
    ad_sam_account_name VARCHAR(255),
    ad_dns_hostname VARCHAR(255),

    -- Key Client identifiers
    kc_agid VARCHAR(50),
    kc_idnt VARCHAR(50),

    -- Ownership and assignment
    owner_uniqname VARCHAR(50),
    owner_department_id VARCHAR(50),
    owner_group VARCHAR(100),
    tdx_owning_customer_uid UUID,
    tdx_requesting_customer_uid UUID,

    -- Location information
    tdx_location_id INTEGER,
    tdx_location_room_id INTEGER,

    -- Active Directory organizational structure
    ad_dn TEXT,
    ad_ou_hierarchy JSONB DEFAULT '[]'::jsonb,
    ad_parent_ou TEXT,
    ad_ou_depth INTEGER,

    -- Lab association (can have multiple via junction table)
    primary_lab_id VARCHAR(100),
    primary_lab_method VARCHAR(50),
    lab_association_count INTEGER DEFAULT 0,

    -- Hardware specifications
    cpu VARCHAR(255),
    cpu_speed_mhz INTEGER,
    cpu_cores INTEGER,
    cpu_sockets INTEGER,
    ram_mb INTEGER,
    disk_gb NUMERIC(10,2),
    disk_free_gb NUMERIC(10,2),

    -- Operating system information
    os_family VARCHAR(50),
    os_name VARCHAR(255),
    os_version VARCHAR(100),
    os_build VARCHAR(100),
    os_install_date TIMESTAMP WITH TIME ZONE,
    os_serial_number VARCHAR(100),

    -- Software and client information
    kc_client_version VARCHAR(50),

    -- Usage and activity tracking
    last_user VARCHAR(50),
    last_logon TIMESTAMP WITH TIME ZONE,
    last_logon_timestamp TIMESTAMP WITH TIME ZONE,
    last_audit TIMESTAMP WITH TIME ZONE,
    last_session TIMESTAMP WITH TIME ZONE,
    last_startup TIMESTAMP WITH TIME ZONE,
    base_audit TIMESTAMP WITH TIME ZONE,
    last_seen TIMESTAMP WITH TIME ZONE,

    -- AD timestamps
    ad_pwd_last_set TIMESTAMP WITH TIME ZONE,
    ad_when_created TIMESTAMP WITH TIME ZONE,
    ad_when_changed TIMESTAMP WITH TIME ZONE,

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_ad_enabled BOOLEAN,
    has_recent_activity BOOLEAN,

    -- Data completeness flags
    has_key_client_data BOOLEAN DEFAULT false,
    has_ad_data BOOLEAN DEFAULT false,
    has_tdx_data BOOLEAN DEFAULT false,
    data_source VARCHAR(100) NOT NULL,

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    source_system VARCHAR(100) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),

    -- Foreign keys
    CONSTRAINT fk_computers_owner_user
        FOREIGN KEY (owner_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE SET NULL,

    CONSTRAINT fk_computers_owner_department
        FOREIGN KEY (owner_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL,

    CONSTRAINT fk_computers_primary_lab
        FOREIGN KEY (primary_lab_id)
        REFERENCES silver.labs(lab_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.computers IS 'Unified computer/asset records from key_client, active_directory, and tdx sources';
COMMENT ON COLUMN silver.computers.computer_id IS 'Primary key: normalized computer name (lowercase)';
COMMENT ON COLUMN silver.computers.primary_lab_id IS 'Primary lab association (highest confidence from silver.computer_labs)';
COMMENT ON COLUMN silver.computers.primary_lab_method IS 'Method used for primary lab association';
COMMENT ON COLUMN silver.computers.lab_association_count IS 'Total number of lab associations in silver.computer_labs';
COMMENT ON COLUMN silver.computers.last_seen IS 'Most recent activity from any source (max of last_logon, last_audit, last_session)';
COMMENT ON COLUMN silver.computers.has_recent_activity IS 'Activity within last 90 days';

-- Trigger for automatic timestamp updates
CREATE TRIGGER update_silver_computers_updated_at
    BEFORE UPDATE ON silver.computers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Core indexes
CREATE INDEX idx_silver_computers_silver_id ON silver.computers (silver_id);
CREATE INDEX idx_silver_computers_computer_name ON silver.computers (LOWER(computer_name));
CREATE INDEX idx_silver_computers_entity_hash ON silver.computers (entity_hash);
CREATE INDEX idx_silver_computers_active ON silver.computers (is_active, computer_name);
CREATE INDEX idx_silver_computers_quality ON silver.computers (data_quality_score DESC);

-- Matching indexes
CREATE INDEX idx_silver_computers_mac ON silver.computers (mac_address) WHERE mac_address IS NOT NULL;
CREATE INDEX idx_silver_computers_serial ON silver.computers (UPPER(serial_number)) WHERE serial_number IS NOT NULL;

-- TDX indexes
CREATE INDEX idx_silver_computers_tdx_id ON silver.computers (tdx_asset_id);
CREATE INDEX idx_silver_computers_tdx_uid ON silver.computers (tdx_asset_uid);
CREATE INDEX idx_silver_computers_tdx_tag ON silver.computers (tdx_tag);

-- AD indexes
CREATE INDEX idx_silver_computers_ad_guid ON silver.computers (ad_object_guid);
CREATE INDEX idx_silver_computers_ad_sid ON silver.computers (ad_object_sid);
CREATE INDEX idx_silver_computers_ad_dn ON silver.computers (ad_dn);

-- Relationship indexes
CREATE INDEX idx_silver_computers_owner_user ON silver.computers (owner_uniqname);
CREATE INDEX idx_silver_computers_owner_dept ON silver.computers (owner_department_id);
CREATE INDEX idx_silver_computers_primary_lab ON silver.computers (primary_lab_id);
CREATE INDEX idx_silver_computers_lab_count ON silver.computers (lab_association_count DESC) WHERE lab_association_count > 0;

-- Activity indexes
CREATE INDEX idx_silver_computers_last_seen ON silver.computers (last_seen DESC);
CREATE INDEX idx_silver_computers_last_user ON silver.computers (LOWER(last_user));
CREATE INDEX idx_silver_computers_recent_activity ON silver.computers (has_recent_activity, last_seen DESC) WHERE has_recent_activity = true;

-- Source coverage indexes
CREATE INDEX idx_silver_computers_data_source ON silver.computers (data_source, has_key_client_data, has_ad_data, has_tdx_data);

-- GIN indexes for JSONB arrays
CREATE INDEX idx_silver_computers_ad_ou_hierarchy_gin ON silver.computers USING gin (ad_ou_hierarchy);
CREATE INDEX idx_silver_computers_quality_flags_gin ON silver.computers USING gin (quality_flags);
CREATE INDEX idx_silver_computers_name_aliases_gin ON silver.computers USING gin (computer_name_aliases);
CREATE INDEX idx_silver_computers_mac_addresses_gin ON silver.computers USING gin (mac_addresses);
CREATE INDEX idx_silver_computers_serial_numbers_gin ON silver.computers USING gin (serial_numbers);

-- ============================================================================
-- Junction Table: silver.computer_labs
-- ============================================================================

CREATE TABLE silver.computer_labs (
    association_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    computer_id VARCHAR(100) NOT NULL REFERENCES silver.computers(computer_id) ON DELETE CASCADE,
    lab_id VARCHAR(100) NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,

    -- Association method and metadata
    association_method VARCHAR(50) NOT NULL,
    confidence_score DECIMAL(3,2) DEFAULT 0.50 CHECK (confidence_score BETWEEN 0.00 AND 1.00),

    -- Supporting evidence
    matched_ou TEXT,
    matched_group_id VARCHAR(50),
    matched_user VARCHAR(50),

    -- Metadata
    is_primary BOOLEAN DEFAULT false,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key to group (if association is via group)
    CONSTRAINT fk_computer_labs_group
        FOREIGN KEY (matched_group_id)
        REFERENCES silver.groups(group_id)
        ON DELETE SET NULL,

    CONSTRAINT check_association_method CHECK (
        association_method IN ('ad_ou_nested', 'owner_is_pi', 'group_membership', 'owner_member', 'last_user_member')
    )
);

COMMENT ON TABLE silver.computer_labs IS 'Computer-lab associations with confidence scoring (supports multiple associations per computer)';
COMMENT ON COLUMN silver.computer_labs.association_method IS 'Method used: ad_ou_nested, owner_is_pi, group_membership, owner_member, last_user_member';
COMMENT ON COLUMN silver.computer_labs.confidence_score IS 'Confidence in this association (0.00-1.00)';
COMMENT ON COLUMN silver.computer_labs.is_primary IS 'Whether this is the primary association (highest confidence)';

-- Prevent duplicate associations
CREATE UNIQUE INDEX idx_computer_labs_unique ON silver.computer_labs (
    computer_id, lab_id, association_method
);

-- Indexes for lab association queries
CREATE INDEX idx_computer_labs_computer ON silver.computer_labs (computer_id);
CREATE INDEX idx_computer_labs_lab ON silver.computer_labs (lab_id);
CREATE INDEX idx_computer_labs_method ON silver.computer_labs (association_method);
CREATE INDEX idx_computer_labs_primary ON silver.computer_labs (computer_id, is_primary) WHERE is_primary = true;
CREATE INDEX idx_computer_labs_confidence ON silver.computer_labs (confidence_score DESC);

-- ============================================================================
-- Junction Table: silver.computer_groups
-- ============================================================================

CREATE TABLE silver.computer_groups (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    computer_id VARCHAR(100) NOT NULL REFERENCES silver.computers(computer_id) ON DELETE CASCADE,

    -- Group information
    group_id VARCHAR(50),
    group_dn TEXT NOT NULL,
    group_cn VARCHAR(255),

    -- Membership metadata
    source_system VARCHAR(50) NOT NULL DEFAULT 'active_directory',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key to group (if it exists in silver.groups)
    CONSTRAINT fk_computer_groups_group
        FOREIGN KEY (group_id)
        REFERENCES silver.groups(group_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE silver.computer_groups IS 'Computer AD group memberships extracted from memberOf attribute';

-- Prevent duplicate memberships
CREATE UNIQUE INDEX idx_computer_groups_unique ON silver.computer_groups (
    computer_id, group_dn
);

-- Indexes for group membership queries
CREATE INDEX idx_computer_groups_computer ON silver.computer_groups (computer_id);
CREATE INDEX idx_computer_groups_group ON silver.computer_groups (group_id);
CREATE INDEX idx_computer_groups_cn ON silver.computer_groups (LOWER(group_cn));

-- ============================================================================
-- Detail Table: silver.computer_attributes
-- ============================================================================

CREATE TABLE silver.computer_attributes (
    attribute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    computer_id VARCHAR(100) NOT NULL REFERENCES silver.computers(computer_id) ON DELETE CASCADE,

    -- Attribute information
    attribute_name VARCHAR(255) NOT NULL,
    attribute_value TEXT,
    attribute_value_uid INTEGER,

    -- Source tracking
    source_system VARCHAR(50) NOT NULL DEFAULT 'tdx',
    tdx_form_id INTEGER,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.computer_attributes IS 'TDX custom attributes that vary by form type';

-- Prevent duplicate attributes
CREATE UNIQUE INDEX idx_computer_attributes_unique ON silver.computer_attributes (
    computer_id, attribute_name
);

-- Indexes for attribute queries
CREATE INDEX idx_computer_attributes_computer ON silver.computer_attributes (computer_id);
CREATE INDEX idx_computer_attributes_name ON silver.computer_attributes (LOWER(attribute_name));
CREATE INDEX idx_computer_attributes_value ON silver.computer_attributes (attribute_value);

-- ============================================================================
-- PART 3: Views
-- ============================================================================

-- View 1: Computer Summary with Relationships
CREATE VIEW silver.v_computer_summary AS
SELECT
    c.computer_id,
    c.computer_name,
    c.serial_number,
    c.mac_address,

    -- Owner details
    c.owner_uniqname,
    u.full_name AS owner_name,
    u.primary_email AS owner_email,

    -- Department details
    c.owner_department_id,
    d.department_name,

    -- Lab details
    c.primary_lab_id,
    l.lab_name AS primary_lab_name,
    l.pi_uniqname AS primary_lab_pi,
    c.primary_lab_method,
    c.lab_association_count,

    -- Hardware
    c.cpu,
    c.ram_mb,
    c.disk_gb,
    c.os_name,

    -- Activity
    c.last_user,
    c.last_seen,
    c.has_recent_activity,

    -- Source coverage
    c.has_key_client_data,
    c.has_ad_data,
    c.has_tdx_data,
    c.data_source,

    -- Quality
    c.is_active,
    c.data_quality_score

FROM silver.computers c
LEFT JOIN silver.users u ON c.owner_uniqname = u.uniqname
LEFT JOIN silver.departments d ON c.owner_department_id = d.dept_id
LEFT JOIN silver.labs l ON c.primary_lab_id = l.lab_id;

COMMENT ON VIEW silver.v_computer_summary IS 'Computer summary with owner, department, and lab relationships';

-- View 2: Lab Computers (All Associations)
CREATE VIEW silver.v_lab_computers AS
SELECT
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    c.computer_id,
    c.computer_name,
    c.serial_number,
    c.last_seen,
    c.has_recent_activity,
    cl.association_method,
    cl.confidence_score,
    cl.is_primary,
    cl.matched_ou,
    cl.matched_group_id,
    cl.matched_user
FROM silver.labs l
INNER JOIN silver.computer_labs cl ON l.lab_id = cl.lab_id
INNER JOIN silver.computers c ON cl.computer_id = c.computer_id
WHERE c.is_active = true
ORDER BY l.lab_name, cl.is_primary DESC, cl.confidence_score DESC, c.computer_name;

COMMENT ON VIEW silver.v_lab_computers IS 'All computer-lab associations with confidence scores and association methods';

-- View 3: Department Computers
CREATE VIEW silver.v_department_computers AS
SELECT
    d.dept_id,
    d.department_name,
    COUNT(c.computer_id) AS total_computers,
    COUNT(c.computer_id) FILTER (WHERE c.has_recent_activity) AS active_computers,
    COUNT(c.computer_id) FILTER (WHERE c.has_tdx_data) AS computers_in_tdx,
    COUNT(c.computer_id) FILTER (WHERE c.has_ad_data) AS computers_in_ad,
    COUNT(c.computer_id) FILTER (WHERE c.has_key_client_data) AS computers_in_key_client,
    AVG(c.data_quality_score)::DECIMAL(3,2) AS avg_quality_score
FROM silver.departments d
LEFT JOIN silver.computers c ON d.dept_id = c.owner_department_id
GROUP BY d.dept_id, d.department_name
ORDER BY total_computers DESC;

COMMENT ON VIEW silver.v_department_computers IS 'Department computer counts and statistics';

-- View 4: Computer Group Memberships
CREATE VIEW silver.v_computer_group_memberships AS
SELECT
    c.computer_id,
    c.computer_name,
    g.group_id,
    g.group_name,
    cg.group_dn,
    g.description AS group_description
FROM silver.computer_groups cg
INNER JOIN silver.computers c ON cg.computer_id = c.computer_id
LEFT JOIN silver.groups g ON cg.group_id = g.group_id
ORDER BY c.computer_name, g.group_name;

COMMENT ON VIEW silver.v_computer_group_memberships IS 'Computer AD group memberships with group details';

-- ============================================================================
-- Migration Complete
-- ============================================================================

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Migration 007_add_silver_computers.sql completed successfully';
    RAISE NOTICE 'Created tables: silver.computers, silver.computer_labs, silver.computer_groups, silver.computer_attributes';
    RAISE NOTICE 'Created views: v_computer_summary, v_lab_computers, v_department_computers, v_computer_group_memberships';
    RAISE NOTICE 'Created 15 bronze indexes and 35+ silver indexes';
END $$;
