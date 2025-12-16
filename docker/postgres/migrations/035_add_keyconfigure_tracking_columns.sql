-- Migration: Add KeyConfigure tracking columns to silver.computers
-- Purpose: Track which consolidated KeyConfigure record was matched and NIC count
-- Date: 2025-11-26

BEGIN;

-- Add KeyConfigure tracking columns
ALTER TABLE silver.computers
    ADD COLUMN IF NOT EXISTS kc_computer_id VARCHAR(100),
    ADD COLUMN IF NOT EXISTS kc_primary_mac VARCHAR(20),
    ADD COLUMN IF NOT EXISTS kc_nic_count SMALLINT;

-- Add indexes for the new columns
CREATE INDEX IF NOT EXISTS idx_silver_computers_kc_computer_id
    ON silver.computers(kc_computer_id)
    WHERE kc_computer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_silver_computers_kc_primary_mac
    ON silver.computers(kc_primary_mac)
    WHERE kc_primary_mac IS NOT NULL;

-- Add comments
COMMENT ON COLUMN silver.computers.kc_computer_id IS 'Primary key from silver.keyconfigure_computers (consolidated computer record)';
COMMENT ON COLUMN silver.computers.kc_primary_mac IS 'Primary MAC address from KeyConfigure consolidation';
COMMENT ON COLUMN silver.computers.kc_nic_count IS 'Number of NICs found in KeyConfigure for this computer';

COMMIT;
