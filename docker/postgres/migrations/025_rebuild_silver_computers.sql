-- Migration: Rebuild silver.computers with comprehensive schema
-- Date: 2025-11-23
-- Purpose: Replace minimal computer table with comprehensive schema from schemas.sql
--          Adds hard columns for searchability + JSONB for source consolidation
--          Adds foreign keys, proper indexes, and junction tables

-- ============================================================================
-- STEP 1: Drop existing minimal table and dependencies
-- ============================================================================

-- Drop existing table (old minimal version with 24 columns)
DROP TABLE IF EXISTS silver.computers CASCADE;

-- ============================================================================
-- STEP 2: Create comprehensive silver.computers table
-- ============================================================================

CREATE TABLE silver.computers (
    -- ========================================================================
    -- PRIMARY IDENTIFIERS
    -- ========================================================================
    silver_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),  -- Standard PK across all silver tables

    -- Computed stable identifier (normalized computer name)
    computer_id VARCHAR(100) UNIQUE NOT NULL,  -- LOWER(computer_name) for stable lookups

    -- ========================================================================
    -- IDENTITY FIELDS (for matching across sources)
    -- ========================================================================

    -- Computer name (primary identifier)
    computer_name VARCHAR(255) NOT NULL,
    computer_name_aliases JSONB DEFAULT '[]'::jsonb,  -- Alternative names from different sources

    -- Hardware identifiers (critical for cross-source matching)
    serial_number VARCHAR(100),               -- Primary serial (best from TDX > KC)
    serial_numbers JSONB DEFAULT '[]'::jsonb,  -- All serials from all sources
    mac_address VARCHAR(17),                   -- Primary MAC (best from TDX > KC)
    mac_addresses JSONB DEFAULT '[]'::jsonb,   -- All MACs from all sources

    -- ========================================================================
    -- SOURCE SYSTEM IDENTIFIERS (for write-back and audit)
    -- ========================================================================

    -- TeamDynamix identifiers (CRITICAL for write-back operations)
    tdx_asset_id INTEGER,        -- TDX ID field (unique in TDX)
    tdx_tag VARCHAR(50),          -- TDX asset tag/barcode
    tdx_status_id INTEGER,        -- TDX status (for filtering active/retired)
    tdx_status_name VARCHAR(100), -- Human-readable status
    tdx_form_id INTEGER,          -- TDX form type
    tdx_form_name VARCHAR(255),   -- Human-readable form name
    tdx_configuration_item_id INTEGER,  -- Link to TDX CI
    tdx_external_id VARCHAR(100), -- External ID field in TDX
    tdx_uri VARCHAR(255),         -- TDX web UI link

    -- Active Directory identifiers
    ad_object_guid UUID,           -- Immutable AD GUID
    ad_object_sid VARCHAR(100),    -- AD Security Identifier
    ad_sam_account_name VARCHAR(20),  -- SAM account name (with $ suffix)
    ad_dns_hostname VARCHAR(60),   -- Fully qualified domain name
    ad_distinguished_name TEXT,    -- Full DN path

    -- KeyConfigure identifiers
    kc_mac_address VARCHAR(20),    -- Primary key in KC (MAC)

    -- ========================================================================
    -- OWNERSHIP & ASSIGNMENT (with foreign keys)
    -- ========================================================================

    -- Primary owner (resolved to silver.users)
    owner_uniqname VARCHAR(50),    -- FK to silver.users.uniqname

    -- Primary department (resolved to silver.departments)
    owner_department_id VARCHAR(50),  -- FK to silver.departments.dept_id

    -- ========================================================================
    -- HARDWARE & SOFTWARE SPECIFICATIONS (searchable fields)
    -- ========================================================================

    -- Manufacturer and model (from TDX - most complete)
    manufacturer VARCHAR(255),     -- TDX manufacturer_name (57 unique values)
    product_model VARCHAR(255),    -- TDX product_model_name (1,135 unique values)

    -- Operating system (normalized best value)
    os_family VARCHAR(50),         -- Windows, macOS, Linux, etc. (derived)
    os_name VARCHAR(255),          -- Best OS name (KC > AD > TDX)
    os_version VARCHAR(100),       -- OS version string
    os_install_date TIMESTAMP WITH TIME ZONE,  -- From KC

    -- Hardware specs (from KeyConfigure - most accurate)
    cpu VARCHAR(255),              -- CPU description
    cpu_cores SMALLINT,            -- Number of CPU cores
    cpu_sockets SMALLINT,          -- Number of CPU sockets
    cpu_speed_mhz INTEGER,         -- Clock speed
    ram_mb INTEGER,                -- RAM in megabytes
    disk_gb NUMERIC(10,2),         -- Total disk capacity
    disk_free_gb NUMERIC(10,2),    -- Free disk space

    -- ========================================================================
    -- ACTIVITY & STATUS TRACKING
    -- ========================================================================

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,  -- From TDX status
    is_ad_enabled BOOLEAN,                     -- From AD userAccountControl
    has_recent_activity BOOLEAN,               -- Activity within last 90 days

    -- Activity timestamps
    last_seen TIMESTAMP WITH TIME ZONE,        -- Max of all activity timestamps
    last_user VARCHAR(50),                     -- Last logged-in user (from KC)

    -- ========================================================================
    -- CONSOLIDATED SOURCE DATA (JSONB for completeness)
    -- ========================================================================

    -- Location information (from TDX)
    location_info JSONB DEFAULT '{}'::jsonb,
    -- {"location_id": 856, "location_name": "...", "room_id": 123, "room_name": "..."}

    -- Detailed ownership (from all sources)
    ownership_info JSONB DEFAULT '{}'::jsonb,
    -- {"tdx_owning": {...}, "tdx_requesting": {...}, "tdx_financial_owner": {...}, "kc_owner": "...", "ad_managed_by": "..."}

    -- Complete hardware specs (from all sources)
    hardware_specs JSONB DEFAULT '{}'::jsonb,
    -- {"tdx": {"memory": "32GB", ...}, "kc": {"cpu_cores": 4, ...}}

    -- Complete OS details (from all sources)
    os_details JSONB DEFAULT '{}'::jsonb,
    -- {"tdx": {...}, "kc": {...}, "ad": {...}}

    -- Network information (IPs, MACs, DNS)
    network_info JSONB DEFAULT '{}'::jsonb,
    -- {"mac_addresses": [...], "ip_addresses": [...], "dns_hostname": "..."}

    -- AD security attributes
    ad_security_info JSONB DEFAULT '{}'::jsonb,
    -- {"service_principal_names": [...], "ms_laps_password_expiration_time": 123, ...}

    -- AD organizational unit hierarchy
    ad_ou_info JSONB DEFAULT '{}'::jsonb,
    -- {"ou_root": "UMICH", "ou_organization": "LSA", "ou_division": "RSN", ...}

    -- Financial information (from TDX)
    financial_info JSONB DEFAULT '{}'::jsonb,
    -- {"purchase_cost": 2867.38, "acquisition_date": "...", "expected_replacement_date": "..."}

    -- All activity timestamps (from all sources)
    activity_timestamps JSONB DEFAULT '{}'::jsonb,
    -- {"tdx_last_inventoried": "...", "kc_last_audit": "...", "ad_last_logon": "..."}

    -- TDX custom attributes (complete array)
    tdx_attributes JSONB DEFAULT '{}'::jsonb,
    -- {"support_groups": {"ids": [1,2], "text": "..."}, "function": {...}, "all_attributes": [...]}

    -- TDX attachments
    tdx_attachments JSONB DEFAULT '[]'::jsonb,

    -- Source raw IDs (audit trail to bronze layer)
    source_raw_ids JSONB DEFAULT '{}'::jsonb,
    -- {"tdx_raw_id": "uuid", "ad_raw_id": "uuid", "keyconfigure_raw_id": "uuid"}

    -- ========================================================================
    -- DATA QUALITY & METADATA (standard fields)
    -- ========================================================================

    data_quality_score NUMERIC(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    source_system VARCHAR(100) NOT NULL,  -- Comma-separated: "tdx,keyconfigure,ad"
    source_entity_id VARCHAR(255),         -- Primary source ID
    entity_hash VARCHAR(64) NOT NULL,      -- SHA-256 for change detection

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- ============================================================================
-- STEP 3: Add table comments
-- ============================================================================

COMMENT ON TABLE silver.computers IS 'Consolidated computer records from TDX, KeyConfigure, and Active Directory';
COMMENT ON COLUMN silver.computers.silver_id IS 'Primary key - UUID for internal referencing (standard across all silver tables)';
COMMENT ON COLUMN silver.computers.computer_id IS 'Computed stable identifier - LOWER(computer_name) for lookups';
COMMENT ON COLUMN silver.computers.serial_number IS 'Primary serial number (best from TDX > KC)';
COMMENT ON COLUMN silver.computers.mac_address IS 'Primary MAC address (best from TDX > KC)';
COMMENT ON COLUMN silver.computers.owner_uniqname IS 'Primary owner - resolved FK to silver.users.uniqname';
COMMENT ON COLUMN silver.computers.owner_department_id IS 'Primary department - resolved FK to silver.departments.dept_id';
COMMENT ON COLUMN silver.computers.manufacturer IS 'Manufacturer name from TDX (Apple, Dell, HP, Lenovo, etc.)';
COMMENT ON COLUMN silver.computers.product_model IS 'Product model from TDX (ThinkPad X1, MacBook Pro, etc.)';
COMMENT ON COLUMN silver.computers.os_family IS 'Derived OS family: Windows, macOS, Linux, etc.';
COMMENT ON COLUMN silver.computers.os_name IS 'Best OS name from KC > AD > TDX';
COMMENT ON COLUMN silver.computers.is_active IS 'Active status from TDX status_name';
COMMENT ON COLUMN silver.computers.is_ad_enabled IS 'AD account enabled status from userAccountControl';
COMMENT ON COLUMN silver.computers.last_seen IS 'Most recent activity from any source (max of all timestamps)';
COMMENT ON COLUMN silver.computers.location_info IS 'JSONB: Consolidated location data from TDX';
COMMENT ON COLUMN silver.computers.ownership_info IS 'JSONB: Complete ownership details from all sources';
COMMENT ON COLUMN silver.computers.hardware_specs IS 'JSONB: Detailed hardware specifications from all sources';
COMMENT ON COLUMN silver.computers.os_details IS 'JSONB: Complete OS information from all sources';
COMMENT ON COLUMN silver.computers.network_info IS 'JSONB: Network configuration (IPs, MACs, DNS)';
COMMENT ON COLUMN silver.computers.ad_security_info IS 'JSONB: AD security attributes (SPNs, LAPS, etc.)';
COMMENT ON COLUMN silver.computers.ad_ou_info IS 'JSONB: Complete AD OU hierarchy';
COMMENT ON COLUMN silver.computers.financial_info IS 'JSONB: Financial data from TDX (cost, dates)';
COMMENT ON COLUMN silver.computers.activity_timestamps IS 'JSONB: All activity timestamps from all sources';
COMMENT ON COLUMN silver.computers.tdx_attributes IS 'JSONB: TDX custom attributes array';
COMMENT ON COLUMN silver.computers.source_raw_ids IS 'JSONB: Raw IDs linking to bronze.raw_entities for audit trail';

-- ============================================================================
-- STEP 4: Create indexes
-- ============================================================================

-- Primary and unique indexes
CREATE UNIQUE INDEX idx_silver_computers_silver_id ON silver.computers (silver_id);
CREATE UNIQUE INDEX idx_silver_computers_computer_id ON silver.computers (computer_id);

-- Identity matching indexes
CREATE INDEX idx_silver_computers_computer_name ON silver.computers (LOWER(computer_name));
CREATE INDEX idx_silver_computers_serial ON silver.computers (UPPER(serial_number)) WHERE serial_number IS NOT NULL;
CREATE INDEX idx_silver_computers_mac ON silver.computers (mac_address) WHERE mac_address IS NOT NULL;

-- Source system indexes (for write-back and lookups)
CREATE INDEX idx_silver_computers_tdx_asset_id ON silver.computers (tdx_asset_id) WHERE tdx_asset_id IS NOT NULL;
CREATE INDEX idx_silver_computers_tdx_tag ON silver.computers (tdx_tag) WHERE tdx_tag IS NOT NULL;
CREATE INDEX idx_silver_computers_tdx_status ON silver.computers (tdx_status_id, is_active);
CREATE INDEX idx_silver_computers_ad_guid ON silver.computers (ad_object_guid) WHERE ad_object_guid IS NOT NULL;
CREATE INDEX idx_silver_computers_ad_sam ON silver.computers (ad_sam_account_name) WHERE ad_sam_account_name IS NOT NULL;
CREATE INDEX idx_silver_computers_kc_mac ON silver.computers (kc_mac_address) WHERE kc_mac_address IS NOT NULL;

-- Foreign key indexes (for JOINs)
CREATE INDEX idx_silver_computers_owner_user ON silver.computers (owner_uniqname) WHERE owner_uniqname IS NOT NULL;
CREATE INDEX idx_silver_computers_owner_dept ON silver.computers (owner_department_id) WHERE owner_department_id IS NOT NULL;

-- Searchable field indexes
CREATE INDEX idx_silver_computers_manufacturer ON silver.computers (manufacturer) WHERE manufacturer IS NOT NULL;
CREATE INDEX idx_silver_computers_product_model ON silver.computers (product_model) WHERE product_model IS NOT NULL;
CREATE INDEX idx_silver_computers_os_family ON silver.computers (os_family) WHERE os_family IS NOT NULL;
CREATE INDEX idx_silver_computers_os_name ON silver.computers (os_name) WHERE os_name IS NOT NULL;

-- Activity and status indexes
CREATE INDEX idx_silver_computers_active ON silver.computers (is_active, last_seen DESC);
CREATE INDEX idx_silver_computers_last_seen ON silver.computers (last_seen DESC) WHERE last_seen IS NOT NULL;
CREATE INDEX idx_silver_computers_last_user ON silver.computers (LOWER(last_user)) WHERE last_user IS NOT NULL;
CREATE INDEX idx_silver_computers_recent_activity ON silver.computers (has_recent_activity, last_seen DESC) WHERE has_recent_activity = true;

-- Data quality indexes
CREATE INDEX idx_silver_computers_quality_score ON silver.computers (data_quality_score DESC) WHERE data_quality_score IS NOT NULL;
CREATE INDEX idx_silver_computers_entity_hash ON silver.computers (entity_hash);

-- JSONB GIN indexes (for deep querying)
CREATE INDEX idx_silver_computers_location_info_gin ON silver.computers USING gin (location_info);
CREATE INDEX idx_silver_computers_ownership_info_gin ON silver.computers USING gin (ownership_info);
CREATE INDEX idx_silver_computers_hardware_specs_gin ON silver.computers USING gin (hardware_specs);
CREATE INDEX idx_silver_computers_ad_ou_info_gin ON silver.computers USING gin (ad_ou_info);
CREATE INDEX idx_silver_computers_tdx_attributes_gin ON silver.computers USING gin (tdx_attributes);
CREATE INDEX idx_silver_computers_quality_flags_gin ON silver.computers USING gin (quality_flags);
CREATE INDEX idx_silver_computers_name_aliases_gin ON silver.computers USING gin (computer_name_aliases);
CREATE INDEX idx_silver_computers_mac_addresses_gin ON silver.computers USING gin (mac_addresses);
CREATE INDEX idx_silver_computers_serial_numbers_gin ON silver.computers USING gin (serial_numbers);

-- Ingestion tracking
CREATE INDEX idx_silver_computers_ingestion_run ON silver.computers (ingestion_run_id) WHERE ingestion_run_id IS NOT NULL;
CREATE INDEX idx_silver_computers_created_at ON silver.computers (created_at DESC);
CREATE INDEX idx_silver_computers_updated_at ON silver.computers (updated_at DESC);

-- ============================================================================
-- STEP 5: Create trigger for automatic timestamp updates
-- ============================================================================

CREATE TRIGGER update_silver_computers_updated_at
    BEFORE UPDATE ON silver.computers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- STEP 6: Create junction table for computer group memberships
-- ============================================================================

CREATE TABLE silver.computer_groups (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    computer_id VARCHAR(100) NOT NULL REFERENCES silver.computers(computer_id) ON DELETE CASCADE,

    -- Group information (from AD member_of_groups)
    group_id VARCHAR(100),  -- FK to silver.groups.group_id if resolved
    group_dn TEXT NOT NULL,  -- Full distinguished name from AD
    group_cn VARCHAR(255),   -- Extracted CN from DN

    -- Membership metadata
    source_system VARCHAR(50) NOT NULL DEFAULT 'active_directory',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key to groups (soft FK - may not exist in silver.groups)
    CONSTRAINT fk_computer_groups_group
        FOREIGN KEY (group_id)
        REFERENCES silver.groups(group_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.computer_groups IS 'Computer AD group memberships extracted from AD member_of_groups array';
COMMENT ON COLUMN silver.computer_groups.group_id IS 'FK to silver.groups.group_id (if group exists in silver layer)';
COMMENT ON COLUMN silver.computer_groups.group_dn IS 'Full AD distinguished name from memberOf attribute';
COMMENT ON COLUMN silver.computer_groups.group_cn IS 'Extracted CN (common name) from group DN';

-- Prevent duplicate memberships
CREATE UNIQUE INDEX idx_computer_groups_unique ON silver.computer_groups (computer_id, group_dn);

-- Indexes for group membership queries
CREATE INDEX idx_computer_groups_computer ON silver.computer_groups (computer_id);
CREATE INDEX idx_computer_groups_group ON silver.computer_groups (group_id) WHERE group_id IS NOT NULL;
CREATE INDEX idx_computer_groups_cn ON silver.computer_groups (LOWER(group_cn)) WHERE group_cn IS NOT NULL;
CREATE INDEX idx_computer_groups_source ON silver.computer_groups (source_system);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

-- Verification query
SELECT
    'silver.computers' as table_name,
    COUNT(*) as column_count
FROM information_schema.columns
WHERE table_schema = 'silver' AND table_name = 'computers'
UNION ALL
SELECT
    'silver.computer_groups',
    COUNT(*)
FROM information_schema.columns
WHERE table_schema = 'silver' AND table_name = 'computer_groups';
