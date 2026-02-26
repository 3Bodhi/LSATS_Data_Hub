-- Migration: Consolidate KeyConfigure Computers (Multi-NIC → Single Computer)
-- Created: 2025-11-26
-- Purpose: Transform silver.keyconfigure_computers from "one record per NIC" to "one record per computer"
--
-- Impact:
-- - Changes primary key from mac_address to computer_id (name + serial)
-- - Consolidates ~266 multi-NIC computers (7,751 NICs → ~7,485 computers)
-- - Adds mac_addresses JSONB array (all MACs per computer)
-- - Adds ip_addresses JSONB array (all IPs per computer)
-- - Adds nic_count metadata field
-- - Adds consolidated_raw_ids for audit trail
--
-- IMPORTANT: This is a breaking change. Ensure 006_transform_keyconfigure_computers.py
-- is updated BEFORE running this migration.

BEGIN;

-- ===========================================================================
-- STEP 1: Backup existing table
-- ===========================================================================

DROP TABLE IF EXISTS silver.keyconfigure_computers_backup CASCADE;

CREATE TABLE silver.keyconfigure_computers_backup AS
SELECT * FROM silver.keyconfigure_computers;

SELECT COUNT(*) as backup_count FROM silver.keyconfigure_computers_backup;
-- Expected: ~7,751 NIC records

-- ===========================================================================
-- STEP 2: Drop old table and create new schema
-- ===========================================================================

DROP TABLE IF EXISTS silver.keyconfigure_computers CASCADE;

CREATE TABLE silver.keyconfigure_computers (
    -- Primary key: computer_id (composite of normalized name + serial)
    computer_id VARCHAR(100) PRIMARY KEY,

    -- Core identity fields
    computer_name VARCHAR(50) NOT NULL,
    oem_serial_number VARCHAR(30),

    -- Multi-NIC consolidation fields (NEW)
    primary_mac_address VARCHAR(20),           -- Most recently active MAC
    mac_addresses JSONB,                       -- Array of all MACs
    ip_addresses JSONB,                        -- Array of all IPs
    nic_count SMALLINT DEFAULT 1,              -- Number of NICs consolidated

    -- Hardware specifications
    cpu VARCHAR(30),
    cpu_cores SMALLINT,
    cpu_sockets SMALLINT,
    clock_speed_mhz INTEGER,
    ram_mb INTEGER,
    disk_gb NUMERIC(10,2),
    disk_free_gb NUMERIC(10,2),

    -- Operating system information
    os VARCHAR(30),
    os_family VARCHAR(30),
    os_version VARCHAR(40),
    os_serial_number VARCHAR(30),
    os_install_date TIMESTAMP WITH TIME ZONE,

    -- User and owner information
    last_user VARCHAR(40),
    owner VARCHAR(100),
    login_type VARCHAR(15),

    -- Session and audit information (most recent across all NICs)
    last_session TIMESTAMP WITH TIME ZONE,
    last_startup TIMESTAMP WITH TIME ZONE,
    last_audit TIMESTAMP WITH TIME ZONE,
    base_audit TIMESTAMP WITH TIME ZONE,

    -- Client information
    keyconfigure_client_version VARCHAR(15),

    -- Traceability (NEW: track all bronze raw_ids)
    consolidated_raw_ids JSONB,                -- Array of all raw_ids for this computer
    raw_id UUID NOT NULL,                      -- Most recent bronze raw_id

    -- Standard metadata
    source_system VARCHAR(50) NOT NULL DEFAULT 'key_client',
    entity_hash VARCHAR(64) NOT NULL,
    ingestion_run_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key
    CONSTRAINT keyconfigure_computers_ingestion_run_fkey
        FOREIGN KEY (ingestion_run_id)
        REFERENCES meta.ingestion_runs(run_id)
);

-- ===========================================================================
-- STEP 3: Create indexes
-- ===========================================================================

-- Primary lookup indexes
CREATE INDEX idx_kc_computers_primary_mac ON silver.keyconfigure_computers(primary_mac_address);
CREATE INDEX idx_kc_computers_name ON silver.keyconfigure_computers(computer_name);
CREATE INDEX idx_kc_computers_serial ON silver.keyconfigure_computers(oem_serial_number)
    WHERE oem_serial_number IS NOT NULL AND oem_serial_number != '';

-- JSONB array indexes (GIN for array containment queries)
CREATE INDEX idx_kc_computers_mac_array ON silver.keyconfigure_computers USING GIN(mac_addresses);
CREATE INDEX idx_kc_computers_ip_array ON silver.keyconfigure_computers USING GIN(ip_addresses);
CREATE INDEX idx_kc_computers_raw_ids_array ON silver.keyconfigure_computers USING GIN(consolidated_raw_ids);

-- Hardware specs indexes
CREATE INDEX idx_kc_computers_cpu ON silver.keyconfigure_computers(cpu) WHERE cpu IS NOT NULL;
CREATE INDEX idx_kc_computers_os ON silver.keyconfigure_computers(os) WHERE os IS NOT NULL;
CREATE INDEX idx_kc_computers_os_family ON silver.keyconfigure_computers(os_family) WHERE os_family IS NOT NULL;
CREATE INDEX idx_kc_computers_owner ON silver.keyconfigure_computers(owner) WHERE owner IS NOT NULL;

-- Activity indexes
CREATE INDEX idx_kc_computers_last_session ON silver.keyconfigure_computers(last_session)
    WHERE last_session IS NOT NULL;
CREATE INDEX idx_kc_computers_last_audit ON silver.keyconfigure_computers(last_audit)
    WHERE last_audit IS NOT NULL;

-- Metadata indexes
CREATE INDEX idx_kc_computers_entity_hash ON silver.keyconfigure_computers(entity_hash);
CREATE INDEX idx_kc_computers_raw_id ON silver.keyconfigure_computers(raw_id);
CREATE INDEX idx_kc_computers_ingestion_run_id ON silver.keyconfigure_computers(ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- ===========================================================================
-- STEP 4: Migrate data (consolidate multi-NIC records)
-- ===========================================================================

INSERT INTO silver.keyconfigure_computers (
    computer_id, computer_name, oem_serial_number,
    primary_mac_address, mac_addresses, ip_addresses, nic_count,
    cpu, cpu_cores, cpu_sockets, clock_speed_mhz, ram_mb, disk_gb, disk_free_gb,
    os, os_family, os_version, os_serial_number, os_install_date,
    last_user, owner, login_type,
    last_session, last_startup, last_audit, base_audit,
    keyconfigure_client_version,
    consolidated_raw_ids, raw_id,
    source_system, entity_hash, ingestion_run_id, created_at, updated_at
)
SELECT
    -- computer_id: Normalized name + serial (matching Python logic)
    -- Use the normalized grouping columns
    CASE
        WHEN UPPER(TRIM(COALESCE((array_agg(oem_serial_number ORDER BY last_session DESC NULLS LAST))[1], ''))) != ''
             AND UPPER(TRIM((array_agg(oem_serial_number ORDER BY last_session DESC NULLS LAST))[1])) NOT IN ('N/A', 'NONE', 'UNKNOWN')
        THEN UPPER(TRIM((array_agg(computer_name ORDER BY last_session DESC NULLS LAST))[1])) || '-' ||
             UPPER(TRIM((array_agg(oem_serial_number ORDER BY last_session DESC NULLS LAST))[1]))
        ELSE UPPER(TRIM((array_agg(computer_name ORDER BY last_session DESC NULLS LAST))[1]))
    END as computer_id,

    -- Use most common case variant of computer_name (prefer uppercase)
    (array_agg(computer_name ORDER BY
        CASE WHEN computer_name = UPPER(computer_name) THEN 1 ELSE 2 END,
        last_session DESC NULLS LAST, created_at DESC))[1] as computer_name,

    -- Use most common case variant of serial (prefer uppercase)
    (array_agg(oem_serial_number ORDER BY
        CASE WHEN oem_serial_number = UPPER(oem_serial_number) OR oem_serial_number IS NULL THEN 1 ELSE 2 END,
        last_session DESC NULLS LAST, created_at DESC))[1] as oem_serial_number,

    -- Primary MAC: first in array (ordered by most recent last_session)
    (array_agg(mac_address ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as primary_mac_address,

    -- All MACs as JSONB array (remove duplicates)
    to_jsonb(array_agg(DISTINCT mac_address ORDER BY mac_address)) as mac_addresses,

    -- All IPs as JSONB array (remove duplicates, filter nulls)
    CASE
        WHEN COUNT(last_ip_address) FILTER (WHERE last_ip_address IS NOT NULL) > 0
        THEN to_jsonb(array_agg(DISTINCT last_ip_address ORDER BY last_ip_address)
                      FILTER (WHERE last_ip_address IS NOT NULL))
        ELSE '[]'::jsonb
    END as ip_addresses,

    -- NIC count
    COUNT(*)::SMALLINT as nic_count,

    -- Take values from most recently active NIC (highest last_session)
    (array_agg(cpu ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as cpu,
    (array_agg(cpu_cores ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as cpu_cores,
    (array_agg(cpu_sockets ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as cpu_sockets,
    (array_agg(clock_speed_mhz ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as clock_speed_mhz,
    (array_agg(ram_mb ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as ram_mb,
    (array_agg(disk_gb ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as disk_gb,
    (array_agg(disk_free_gb ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as disk_free_gb,
    (array_agg(os ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as os,
    (array_agg(os_family ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as os_family,
    (array_agg(os_version ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as os_version,
    (array_agg(os_serial_number ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as os_serial_number,
    (array_agg(os_install_date ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as os_install_date,
    (array_agg(last_user ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as last_user,
    (array_agg(owner ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as owner,
    (array_agg(login_type ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as login_type,

    -- Timestamps: take most recent
    MAX(last_session) as last_session,
    MAX(last_startup) as last_startup,
    MAX(last_audit) as last_audit,
    MIN(base_audit) as base_audit,  -- First audit

    (array_agg(keyconfigure_client_version ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as keyconfigure_client_version,

    -- Audit trail: collect all raw_ids
    to_jsonb(array_agg(raw_id::text ORDER BY last_session DESC NULLS LAST, created_at DESC)) as consolidated_raw_ids,
    (array_agg(raw_id ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as raw_id,

    -- Metadata
    'key_client' as source_system,
    -- Entity hash: aggregate of all entity hashes
    MD5(string_agg(entity_hash, '|' ORDER BY entity_hash)) as entity_hash,
    (array_agg(ingestion_run_id ORDER BY last_session DESC NULLS LAST, created_at DESC))[1] as ingestion_run_id,
    MIN(created_at) as created_at,
    MAX(updated_at) as updated_at
FROM silver.keyconfigure_computers_backup
GROUP BY UPPER(TRIM(computer_name)), UPPER(TRIM(COALESCE(oem_serial_number, '')));

-- ===========================================================================
-- STEP 5: Verify migration
-- ===========================================================================

DO $$
DECLARE
    old_count INTEGER;
    new_count INTEGER;
    total_nics INTEGER;
    multi_nic_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO old_count FROM silver.keyconfigure_computers_backup;
    SELECT COUNT(*) INTO new_count FROM silver.keyconfigure_computers;
    SELECT SUM(nic_count) INTO total_nics FROM silver.keyconfigure_computers;
    SELECT COUNT(*) INTO multi_nic_count FROM silver.keyconfigure_computers WHERE nic_count > 1;

    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Migration Results:';
    RAISE NOTICE '  Old table (NICs): %', old_count;
    RAISE NOTICE '  New table (Computers): %', new_count;
    RAISE NOTICE '  Total NICs consolidated: %', total_nics;
    RAISE NOTICE '  Multi-NIC computers: %', multi_nic_count;
    RAISE NOTICE '  Consolidation ratio: %.2f NICs/computer', total_nics::NUMERIC / new_count;
    RAISE NOTICE '=================================================================';

    -- Validation: total NICs should match
    IF total_nics != old_count THEN
        RAISE EXCEPTION 'NIC count mismatch: expected %, got %', old_count, total_nics;
    END IF;

    -- Validation: should have fewer computers than NICs (unless all single-NIC)
    IF new_count > old_count THEN
        RAISE EXCEPTION 'Computer count increased: old=%, new=%', old_count, new_count;
    END IF;

    -- Validation: expected range based on analysis (~7,485 computers from ~7,751 NICs)
    IF new_count < 7000 OR new_count > 8000 THEN
        RAISE WARNING 'Computer count outside expected range: %', new_count;
    END IF;
END $$;

-- ===========================================================================
-- STEP 6: Sample multi-NIC computers for verification
-- ===========================================================================

SELECT
    computer_id,
    computer_name,
    nic_count,
    jsonb_array_length(mac_addresses) as mac_count,
    jsonb_array_length(COALESCE(ip_addresses, '[]'::jsonb)) as ip_count,
    primary_mac_address
FROM silver.keyconfigure_computers
WHERE nic_count > 1
ORDER BY nic_count DESC, computer_name
LIMIT 20;

-- ===========================================================================
-- STEP 7: Grant permissions
-- ===========================================================================

-- Ensure permissions are maintained
GRANT SELECT ON silver.keyconfigure_computers TO PUBLIC;

-- ===========================================================================
-- NOTES FOR ROLLBACK
-- ===========================================================================

-- If migration fails or needs rollback:
-- 1. DROP TABLE silver.keyconfigure_computers;
-- 2. ALTER TABLE silver.keyconfigure_computers_backup RENAME TO keyconfigure_computers;
-- 3. Recreate indexes on keyconfigure_computers

-- After validation period (recommended: 1 week), drop backup table:
-- DROP TABLE silver.keyconfigure_computers_backup;

COMMIT;

-- Migration complete!
-- NEXT STEPS:
-- 1. Verify multi-NIC consolidation is correct (check sample above)
-- 2. Run: python scripts/database/silver/006_transform_keyconfigure_computers.py --full-sync --dry-run
-- 3. If dry-run looks good, run without --dry-run
-- 4. Run: python scripts/database/silver/013_transform_computers.py --full-sync
-- 5. After 1 week of validation, drop backup table: DROP TABLE silver.keyconfigure_computers_backup;
