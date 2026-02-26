-- Drop old legacy table if it exists (from previous migration attempts)
-- CASCADE will drop dependent foreign keys and views
DROP TABLE IF EXISTS silver.computers_legacy CASCADE;

-- Rename existing consolidated computers table to legacy if it exists
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'silver' AND table_name = 'computers') THEN
        -- Check if this is the OLD computers table (has computer_id column) vs the NEW one we're creating
        -- If it has computer_id as UUID, it's likely the new schema, so we'll drop it instead
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'silver' 
                AND table_name = 'computers' 
                AND column_name = 'computer_id' 
                AND data_type = 'uuid'
        ) THEN
            -- This is the new schema, just drop it to recreate
            DROP TABLE silver.computers CASCADE;
        ELSE
            -- This is the old schema, rename it to legacy
            ALTER TABLE silver.computers RENAME TO computers_legacy_old;
        END IF;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS silver.computers (
    computer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    serial_number VARCHAR(255),
    mac_address VARCHAR(255),
    computer_name VARCHAR(255),
    
    -- Source IDs
    tdx_asset_id INTEGER,
    keyconfigure_id UUID,
    ad_object_guid UUID,
    
    -- Foreign Keys
    department_id VARCHAR(100), -- Links to silver.departments.dept_id (AD OU can be 52+ chars)
    primary_user_uniqname VARCHAR(100), -- Links to silver.users.uniqname (KC owner can be 67+ chars)
    
    -- Attributes
    manufacturer VARCHAR(255),
    model VARCHAR(255),
    operating_system VARCHAR(255),
    os_version VARCHAR(255),
    ram_gb NUMERIC(10, 2),
    storage_gb NUMERIC(10, 2),
    processor VARCHAR(255),
    last_seen TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    data_quality_score NUMERIC(3, 2),
    sources JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}',
    
    -- Standard Audit Columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID,
    entity_hash VARCHAR(64) -- SHA256 hash for change detection
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_computers_serial ON silver.computers(serial_number);
CREATE INDEX IF NOT EXISTS idx_computers_mac ON silver.computers(mac_address);
CREATE INDEX IF NOT EXISTS idx_computers_name ON silver.computers(computer_name);
CREATE INDEX IF NOT EXISTS idx_computers_dept ON silver.computers(department_id);
CREATE INDEX IF NOT EXISTS idx_computers_user ON silver.computers(primary_user_uniqname);
CREATE INDEX IF NOT EXISTS idx_computers_tdx_id ON silver.computers(tdx_asset_id);
