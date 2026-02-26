-- Migration: Add silver.keyconfigure_computers source-specific table
-- Date: 2025-11-22
-- Purpose: Create source-specific silver table for KeyConfigure computer inventory
--          Part of two-tier silver architecture (source-specific -> consolidated)
--          Extracted from KeyConfigure Excel exports via bronze.raw_entities

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.keyconfigure_computers CASCADE;

-- Create silver.keyconfigure_computers table
CREATE TABLE IF NOT EXISTS silver.keyconfigure_computers (
    -- Primary identifier (MAC address as unique key)
    mac_address VARCHAR(20) PRIMARY KEY,           -- Unique MAC address (100% populated)
    
    -- Core identity fields
    computer_name VARCHAR(50) NOT NULL,            -- Computer hostname (max: 40 + buffer, 100% populated)
    oem_serial_number VARCHAR(30),                 -- Manufacturer serial number (max: 24 + buffer, 99% populated)
    owner VARCHAR(100),                            -- Department/lab owner (max: 67 + buffer, 62% populated)
    
    -- Hardware specifications
    cpu VARCHAR(30),                               -- CPU model (max: 23 + buffer, 99.8% populated)
    cpu_cores SMALLINT,                            -- Number of CPU cores (1-64, avg 10)
    cpu_sockets SMALLINT,                          -- Number of CPU sockets (1-6, avg 1)
    clock_speed_mhz INTEGER,                       -- CPU clock speed in MHz (800-5400, avg 2378)
    ram_mb INTEGER,                                -- RAM in megabytes (2040-1031552, avg 21205)
    disk_gb NUMERIC(10,2),                         -- Total disk capacity in GB (0-108032, avg 898)
    disk_free_gb NUMERIC(10,2),                    -- Free disk space in GB (0-17943, avg 486)
    
    -- Operating system information
    os VARCHAR(30),                                -- OS name (max: 20 + buffer, 65.2% populated)
    os_family VARCHAR(30),                         -- OS family category (max: 22 + buffer, 48.5% populated)
    os_version VARCHAR(40),                        -- Full OS version string (max: 28 + buffer, 99.8% populated)
    os_serial_number VARCHAR(30),                  -- OS license key (max: 23 + buffer, 58.9% populated)
    os_install_date TIMESTAMP WITH TIME ZONE,      -- OS installation timestamp (73.9% populated)
    
    -- Network information
    last_ip_address VARCHAR(20) NOT NULL,          -- Last known IP address (max: 15 + buffer, 100% populated)
    last_user VARCHAR(40) NOT NULL,                -- Last logged-in user (max: 31 + buffer, 100% populated)
    
    -- Session and audit information
    login_type VARCHAR(15),                        -- Login session type (max: 9 + buffer, 100% populated)
    last_session TIMESTAMP WITH TIME ZONE,         -- Last user session timestamp (100% populated)
    last_startup TIMESTAMP WITH TIME ZONE,         -- Last computer boot timestamp (99.6% populated)
    last_audit TIMESTAMP WITH TIME ZONE,           -- Last KeyConfigure audit (98.6% populated)
    base_audit TIMESTAMP WITH TIME ZONE,           -- First KeyConfigure audit (98.6% populated)
    
    -- Client information
    keyconfigure_client_version VARCHAR(15),       -- KeyConfigure agent version (max: 7 + buffer, 100% populated)
    
    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                          -- Link to bronze.raw_entities record
    raw_data_snapshot JSONB,                       -- Optional: snapshot for audit
    
    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'key_client' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,              -- SHA-256 for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Core business key indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_keyconfigure_computers_name
    ON silver.keyconfigure_computers (computer_name);

CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_serial
    ON silver.keyconfigure_computers (oem_serial_number)
    WHERE oem_serial_number IS NOT NULL AND oem_serial_number != '';

-- Ownership and filtering
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_owner
    ON silver.keyconfigure_computers (owner)
    WHERE owner IS NOT NULL;

-- Operating system indexes (for analytics and cross-system matching)
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_os
    ON silver.keyconfigure_computers (os)
    WHERE os IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_os_family
    ON silver.keyconfigure_computers (os_family)
    WHERE os_family IS NOT NULL;

-- CRITICAL: Network indexes for cross-system matching with TDX assets
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_ip
    ON silver.keyconfigure_computers (last_ip_address);

-- Hardware specification indexes (for analytics)
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_cpu
    ON silver.keyconfigure_computers (cpu)
    WHERE cpu IS NOT NULL;

-- Audit timestamp indexes (for compliance queries)
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_last_audit
    ON silver.keyconfigure_computers (last_audit)
    WHERE last_audit IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_last_session
    ON silver.keyconfigure_computers (last_session)
    WHERE last_session IS NOT NULL;

-- Standard traceability indexes
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_raw_id
    ON silver.keyconfigure_computers (raw_id);

CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_entity_hash
    ON silver.keyconfigure_computers (entity_hash);

-- Ingestion tracking
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_ingestion_run_id
    ON silver.keyconfigure_computers (ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- GIN index for JSONB snapshot (if used)
CREATE INDEX IF NOT EXISTS idx_keyconfigure_computers_snapshot_gin
    ON silver.keyconfigure_computers USING gin (raw_data_snapshot)
    WHERE raw_data_snapshot IS NOT NULL;

-- ============================================
-- TABLE AND COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE silver.keyconfigure_computers IS
    'Source-specific silver table for KeyConfigure computer inventory. Part of two-tier silver architecture - feeds into consolidated silver.computers table. Contains typed columns extracted from bronze.raw_entities JSONB data with critical fields (MAC, IP, Serial, Name) for cross-system matching with TDX assets and AD computers.';

COMMENT ON COLUMN silver.keyconfigure_computers.mac_address IS
    'Primary key - unique MAC address from KeyConfigure. Used as external_id in bronze. 100% populated. CRITICAL for joining with silver.tdx_assets.attr_mac_address';

COMMENT ON COLUMN silver.keyconfigure_computers.computer_name IS
    'Computer hostname. 7,551 unique names (200 duplicates exist). Used for joining with AD computers by name.';

COMMENT ON COLUMN silver.keyconfigure_computers.oem_serial_number IS
    'Manufacturer serial number. 7,477 unique values (99% populated). Used for joining with silver.tdx_assets.serial_number';

COMMENT ON COLUMN silver.keyconfigure_computers.owner IS
    'Department or lab owner. 146 unique departments (LSA-PSYC, LSA-PHYS, etc.). 62% populated.';

COMMENT ON COLUMN silver.keyconfigure_computers.last_ip_address IS
    'Last known IP address. 100% populated. Used for network-based matching with TDX assets.';

COMMENT ON COLUMN silver.keyconfigure_computers.os IS
    'Operating system name (macOS, Win 11 23H2 Ent, etc.). 22 unique OS values. 65% populated. Used for OS matching with TDX assets.';

COMMENT ON COLUMN silver.keyconfigure_computers.ram_mb IS
    'RAM in megabytes. Range: 2040 MB (2GB) to 1,031,552 MB (1TB). Average: 21,205 MB (~21GB).';

COMMENT ON COLUMN silver.keyconfigure_computers.disk_gb IS
    'Total disk capacity in gigabytes. Range: 0 to 108,032 GB (~108TB). Average: 898 GB.';

COMMENT ON COLUMN silver.keyconfigure_computers.cpu_cores IS
    'Number of CPU cores. Range: 1-64 cores. Average: 10 cores.';

COMMENT ON COLUMN silver.keyconfigure_computers.last_session IS
    'Timestamp of last user session. 100% populated. Used for identifying active vs inactive computers.';

COMMENT ON COLUMN silver.keyconfigure_computers.last_audit IS
    'Timestamp of last KeyConfigure audit. 98.6% populated. Indicates when computer was last scanned.';

COMMENT ON COLUMN silver.keyconfigure_computers.raw_id IS
    'Link to most recent bronze.raw_entities record for this computer - for audit trail.';

COMMENT ON COLUMN silver.keyconfigure_computers.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed.';

-- ============================================
-- PERMISSIONS
-- ============================================

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.keyconfigure_computers TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.keyconfigure_computers TO lsats_user;

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully created silver.keyconfigure_computers table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'keyconfigure_computers');
    RAISE NOTICE '📊 Table supports 7,751 KeyConfigure computers with typed columns for cross-system matching';
    RAISE NOTICE '🔗 Critical join keys: MAC address (100%% match with TDX assets), computer name (for AD), serial number, IP address';
END $$;
