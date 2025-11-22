-- Migration: Add silver.tdx_assets source-specific table
-- Date: 2025-11-22
-- Purpose: Create source-specific silver table for TeamDynamix assets
--          Part of two-tier silver architecture (source-specific -> consolidated)
--          Extracts critical attributes to typed columns for cross-system matching

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.tdx_assets CASCADE;

-- Create silver.tdx_assets table
CREATE TABLE IF NOT EXISTS silver.tdx_assets (
    -- Primary identifier (TDX native ID)
    tdx_asset_id INTEGER PRIMARY KEY,

    -- Core identity fields
    tag VARCHAR(50) NOT NULL,                -- TDX asset tag (unique barcode/identifier)
    name VARCHAR(255) NOT NULL,              -- Asset name
    uri VARCHAR(255),                        -- TDX API URI (e.g., "api/48/assets/106894")
    external_id VARCHAR(100),                -- External system ID (0.7% populated)
    serial_number VARCHAR(100),              -- Manufacturer serial number (96.5% populated)

    -- Form/Type classification
    app_id INTEGER NOT NULL,                 -- TDX application ID
    app_name VARCHAR(255),                   -- TDX application name (e.g., "LSA-TS Assets/CIs")
    form_id INTEGER NOT NULL,                -- TDX form ID
    form_name VARCHAR(255),                  -- Form name (e.g., "Virtual Machine Form", "Physical Computer Form")
    status_id INTEGER NOT NULL,              -- TDX status ID
    status_name VARCHAR(100),                -- Status name (e.g., "Active", "Disposed", "In Storage")

    -- Hierarchy (rare - only 1.6% have parents)
    parent_id INTEGER,                       -- Parent asset ID (for nested assets)
    parent_tag VARCHAR(50),
    parent_name VARCHAR(255),
    parent_serial_number VARCHAR(100),

    -- Configuration Item relationship (100% populated)
    configuration_item_id INTEGER NOT NULL,  -- Link to TDX CI

    -- Location information
    location_id INTEGER,
    location_name VARCHAR(255),
    location_room_id INTEGER,
    location_room_name VARCHAR(255),

    -- Ownership and responsibility
    owning_customer_id UUID,                 -- TDX UID of owning user
    owning_customer_name VARCHAR(255),
    owning_department_id INTEGER,            -- TDX department ID
    owning_department_name VARCHAR(255),

    -- Requesting party (for asset requests)
    requesting_customer_id UUID,
    requesting_customer_name VARCHAR(255),
    requesting_department_id INTEGER,
    requesting_department_name VARCHAR(255),

    -- Financial information
    purchase_cost DECIMAL(10,2),             -- Max ~$122k observed
    acquisition_date TIMESTAMP WITH TIME ZONE,
    expected_replacement_date TIMESTAMP WITH TIME ZONE,

    -- Manufacturer and model
    manufacturer_id INTEGER,
    manufacturer_name VARCHAR(255),
    product_model_id INTEGER,
    product_model_name VARCHAR(255),
    supplier_id INTEGER,
    supplier_name VARCHAR(255),

    -- Maintenance
    maintenance_schedule_id INTEGER,
    maintenance_schedule_name VARCHAR(255),

    -- External source integration
    external_source_id INTEGER,
    external_source_name VARCHAR(255),

    -- Audit fields
    created_uid UUID,                        -- TDX UID of creator
    created_full_name VARCHAR(255),
    created_date TIMESTAMP WITH TIME ZONE,
    modified_uid UUID,                       -- TDX UID of last modifier
    modified_full_name VARCHAR(255),
    modified_date TIMESTAMP WITH TIME ZONE,

    -- ============================================
    -- EXTRACTED ATTRIBUTES (for matching & queries)
    -- ============================================
    -- These are extracted from the Attributes JSONB array for:
    -- 1. Cross-system matching (MAC, IP, OS with KeyClient, AD, etc.)
    -- 2. Fast indexed queries (compliance, reporting)
    -- 3. Type safety and validation
    -- The complete Attributes array is preserved below

    -- TIER 1: Critical for cross-system matching
    attr_mac_address VARCHAR(100),                       -- Attributes[Name="MAC Address(es)"].Value (88.1% populated)
    attr_ip_address VARCHAR(100),                        -- Attributes[Name="Reserved IP Address(es)"].Value (11% populated)
    attr_operating_system_id INTEGER,                    -- Attributes[Name="Operating System"].Value (choice ID) (89% populated)
    attr_operating_system_name VARCHAR(255),             -- Attributes[Name="Operating System"].ValueText

    -- TIER 2: High usage & compliance/reporting
    attr_last_inventoried_date TIMESTAMP WITH TIME ZONE, -- Attributes[Name="Last Inventoried Date"].Value (97.1% populated)
    attr_purchase_shortcode VARCHAR(100),                -- Attributes[Name="Purchase Shortcode"].Value (97.1% populated)
    attr_function_id INTEGER,                            -- Attributes[Name="Function"].Value (choice ID) (96.1% populated)
    attr_function_name VARCHAR(255),                     -- Attributes[Name="Function"].ValueText
    attr_financial_owner_uid UUID,                       -- Attributes[Name="Financial Owner/Responsible"].Value (person UID) (95.1% populated)
    attr_financial_owner_name VARCHAR(255),              -- Attributes[Name="Financial Owner/Responsible"].ValueText
    attr_support_groups_ids JSONB,                       -- Attributes[Name="Support Group(s)"].Value (multiselect array) (99.9% populated)
    attr_support_groups_text VARCHAR(500),               -- Attributes[Name="Support Group(s)"].ValueText
    attr_memory VARCHAR(50),                             -- Attributes[Name="Memory"].Value (e.g., "8GB") (68.8% populated)
    attr_storage VARCHAR(50),                            -- Attributes[Name="Storage"].Value (e.g., "120GB") (66.6% populated)
    attr_processor_count VARCHAR(50),                    -- Attributes[Name="Processor(s)"].Value (65.8% populated)

    -- Complete attributes array (includes ALL attributes, not just extracted ones)
    -- Preserves audit trail and allows querying of non-extracted attributes
    attributes JSONB DEFAULT '[]'::jsonb,                -- 99.99% populated with 30+ different attribute types
    attachments JSONB DEFAULT '[]'::jsonb,               -- 38.3% populated

    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                                -- Most recent bronze.raw_entities.raw_id
    raw_data_snapshot JSONB,                             -- Optional: full copy for audit

    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'tdx' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,                    -- For change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Core business key indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_tdx_assets_tag
    ON silver.tdx_assets (tag);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_serial_number
    ON silver.tdx_assets (serial_number)
    WHERE serial_number IS NOT NULL AND serial_number != '';

CREATE INDEX IF NOT EXISTS idx_tdx_assets_name
    ON silver.tdx_assets (name);

-- Classification and status indexes
CREATE INDEX IF NOT EXISTS idx_tdx_assets_status_id
    ON silver.tdx_assets (status_id);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_form_id
    ON silver.tdx_assets (form_id);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_form_name
    ON silver.tdx_assets (form_name);

-- Configuration Item relationship
CREATE INDEX IF NOT EXISTS idx_tdx_assets_ci_id
    ON silver.tdx_assets (configuration_item_id);

-- Ownership indexes
CREATE INDEX IF NOT EXISTS idx_tdx_assets_owning_customer_id
    ON silver.tdx_assets (owning_customer_id)
    WHERE owning_customer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tdx_assets_owning_department_id
    ON silver.tdx_assets (owning_department_id)
    WHERE owning_department_id IS NOT NULL;

-- Location indexes
CREATE INDEX IF NOT EXISTS idx_tdx_assets_location_id
    ON silver.tdx_assets (location_id)
    WHERE location_id IS NOT NULL;

-- CRITICAL: Indexes for cross-system matching with KeyClient, AD, etc.
CREATE INDEX IF NOT EXISTS idx_tdx_assets_mac_address
    ON silver.tdx_assets (attr_mac_address)
    WHERE attr_mac_address IS NOT NULL AND attr_mac_address != '';

CREATE INDEX IF NOT EXISTS idx_tdx_assets_ip_address
    ON silver.tdx_assets (attr_ip_address)
    WHERE attr_ip_address IS NOT NULL AND attr_ip_address != '';

CREATE INDEX IF NOT EXISTS idx_tdx_assets_os_name
    ON silver.tdx_assets (attr_operating_system_name)
    WHERE attr_operating_system_name IS NOT NULL;

-- Indexes for high-usage attributes (compliance and reporting)
CREATE INDEX IF NOT EXISTS idx_tdx_assets_last_inventoried
    ON silver.tdx_assets (attr_last_inventoried_date)
    WHERE attr_last_inventoried_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tdx_assets_function_id
    ON silver.tdx_assets (attr_function_id)
    WHERE attr_function_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tdx_assets_financial_owner
    ON silver.tdx_assets (attr_financial_owner_uid)
    WHERE attr_financial_owner_uid IS NOT NULL;

-- GIN indexes for JSONB fields (for querying non-extracted attributes and containment)
CREATE INDEX IF NOT EXISTS idx_tdx_assets_attributes_gin
    ON silver.tdx_assets USING gin (attributes);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_attachments_gin
    ON silver.tdx_assets USING gin (attachments);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_support_groups_gin
    ON silver.tdx_assets USING gin (attr_support_groups_ids);

-- Standard traceability indexes
CREATE INDEX IF NOT EXISTS idx_tdx_assets_raw_id
    ON silver.tdx_assets (raw_id);

CREATE INDEX IF NOT EXISTS idx_tdx_assets_entity_hash
    ON silver.tdx_assets (entity_hash);

-- Ingestion tracking
CREATE INDEX IF NOT EXISTS idx_tdx_assets_ingestion_run_id
    ON silver.tdx_assets (ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- ============================================
-- TABLE AND COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE silver.tdx_assets IS
    'Source-specific silver table for TeamDynamix assets. Part of two-tier silver architecture - feeds into consolidated silver.computers/assets tables. Contains typed columns extracted from bronze.raw_entities JSONB data with critical attributes promoted to columns for cross-system matching.';

COMMENT ON COLUMN silver.tdx_assets.tdx_asset_id IS
    'Primary key from TDX ID field - unique TeamDynamix asset identifier (INTEGER)';

COMMENT ON COLUMN silver.tdx_assets.tag IS
    'Unique business key - TDX asset tag/barcode identifier used by users';

COMMENT ON COLUMN silver.tdx_assets.configuration_item_id IS
    'Link to TDX Configuration Item (CI) - 100% of assets have CI associations';

COMMENT ON COLUMN silver.tdx_assets.attr_mac_address IS
    'Extracted from Attributes array for cross-system matching with KeyClient.MAC, AD network data (88.1% populated)';

COMMENT ON COLUMN silver.tdx_assets.attr_ip_address IS
    'Extracted from Attributes array for network identity matching (11% populated)';

COMMENT ON COLUMN silver.tdx_assets.attr_operating_system_name IS
    'Extracted from Attributes array for OS matching with KeyClient.OS, AD.operatingSystem (89% populated)';

COMMENT ON COLUMN silver.tdx_assets.attr_last_inventoried_date IS
    'Extracted from Attributes array for compliance queries - when asset was last physically verified (97.1% populated)';

COMMENT ON COLUMN silver.tdx_assets.attr_support_groups_ids IS
    'Extracted from Attributes array - multiselect field stored as JSONB array of choice IDs (99.9% populated)';

COMMENT ON COLUMN silver.tdx_assets.attributes IS
    'Complete TDX custom attributes array (JSONB) - includes all 30+ attribute types. Extracted attributes also remain here for audit trail.';

COMMENT ON COLUMN silver.tdx_assets.attachments IS
    'TDX attachments array (JSONB) - 38.3% of assets have attachments';

COMMENT ON COLUMN silver.tdx_assets.raw_id IS
    'Link to most recent bronze.raw_entities record for this asset - for audit trail';

COMMENT ON COLUMN silver.tdx_assets.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed';

-- ============================================
-- PERMISSIONS
-- ============================================

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.tdx_assets TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.tdx_assets TO lsats_user;

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully created silver.tdx_assets table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'tdx_assets');
    RAISE NOTICE '📊 Table supports 41,208+ TDX assets with extracted attributes for cross-system matching';
    RAISE NOTICE '🔗 Critical extracted attributes: MAC address, IP address, OS name for consolidation with KeyClient/AD';
END $$;
