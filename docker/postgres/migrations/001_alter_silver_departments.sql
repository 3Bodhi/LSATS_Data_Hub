-- Migration: Alter silver.departments to support merged UMICH/TDX department data
-- Version: 001
-- Date: 2025-11-14
-- Description: Adds hierarchical org data, TDX operational fields, and enforces dept_id as primary key

-- ============================================================================
-- STEP 1: Backup existing data (optional but recommended)
-- ============================================================================
-- Uncomment to create backup table before migration:
-- CREATE TABLE silver.departments_backup_20251114 AS SELECT * FROM silver.departments;

-- ============================================================================
-- STEP 2: Add new columns
-- ============================================================================

-- Add dept_id as unique identifier (will become primary key later)
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS dept_id VARCHAR(50);

-- Add TDX operational ID for write-back
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS tdx_id INTEGER;

-- Add hierarchical organization fields (from UMICH_API)
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS campus_name VARCHAR(255);
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS college_group VARCHAR(255);
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS vp_area VARCHAR(255);
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS hierarchical_path TEXT;

-- Add TDX-specific timestamps (prefixed to indicate source system)
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS tdx_created_date TIMESTAMP WITH TIME ZONE;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS tdx_modified_date TIMESTAMP WITH TIME ZONE;

-- Add location information as structured JSONB
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS location_info JSONB DEFAULT '{}'::jsonb;

-- Remove region_name if it exists (replaced by campus_name)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'silver'
        AND table_name = 'departments'
        AND column_name = 'region_name'
    ) THEN
        EXECUTE 'ALTER TABLE silver.departments DROP COLUMN region_name';
    END IF;
END $$;

-- ============================================================================
-- STEP 3: Populate dept_id for existing records (if any exist)
-- ============================================================================

-- Option A: If department_code contains the DeptID, copy it
UPDATE silver.departments
SET dept_id = department_code
WHERE dept_id IS NULL AND department_code IS NOT NULL;

-- Option B: If using source_entity_id as dept_id
UPDATE silver.departments
SET dept_id = source_entity_id
WHERE dept_id IS NULL AND source_entity_id IS NOT NULL;

-- Option C: Generate temporary IDs for records without proper identifiers
-- (These should be re-transformed from bronze after migration)
UPDATE silver.departments
SET dept_id = 'TEMP_' || silver_id::text
WHERE dept_id IS NULL;

-- ============================================================================
-- STEP 4: Handle foreign key constraints before modifying primary key
-- ============================================================================

-- Drop foreign key constraints that reference silver.departments.silver_id
DO $$
BEGIN
    -- Drop FK from gold.department_source_mapping if it exists
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'gold'
        AND table_name = 'department_source_mapping'
        AND constraint_name = 'department_source_mapping_silver_id_fkey'
    ) THEN
        EXECUTE 'ALTER TABLE gold.department_source_mapping DROP CONSTRAINT department_source_mapping_silver_id_fkey';
    END IF;
END $$;

-- ============================================================================
-- STEP 5: Modify primary key constraint
-- ============================================================================

-- Drop the old primary key constraint on silver_id (now safe since FK is removed)
ALTER TABLE silver.departments DROP CONSTRAINT IF EXISTS departments_pkey;

-- silver_id is no longer primary key but remains unique for FK references
-- Add unique constraint on silver_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = 'silver'
        AND table_name = 'departments'
        AND constraint_name = 'silver_departments_silver_id_unique'
    ) THEN
        EXECUTE 'ALTER TABLE silver.departments ADD CONSTRAINT silver_departments_silver_id_unique UNIQUE (silver_id)';
    END IF;
END $$;

-- Add primary key constraint on dept_id
ALTER TABLE silver.departments ADD PRIMARY KEY (dept_id);

-- ============================================================================
-- STEP 6: Recreate foreign key references
-- ============================================================================

-- Recreate FK on gold.department_source_mapping (still references silver_id, not dept_id)
DO $$
BEGIN
    -- Only recreate if the gold.department_source_mapping table exists
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'gold'
        AND table_name = 'department_source_mapping'
    ) THEN
        EXECUTE 'ALTER TABLE gold.department_source_mapping
                 ADD CONSTRAINT department_source_mapping_silver_id_fkey
                 FOREIGN KEY (silver_id)
                 REFERENCES silver.departments(silver_id)
                 ON DELETE CASCADE';
    END IF;
END $$;

-- ============================================================================
-- STEP 7: Add new indexes for performance
-- ============================================================================

-- Index on silver_id (still needed for FK lookups)
CREATE INDEX IF NOT EXISTS idx_silver_departments_silver_id ON silver.departments (silver_id);

-- Index on tdx_id for write-back operations
CREATE INDEX IF NOT EXISTS idx_silver_departments_tdx_id ON silver.departments (tdx_id);

-- Composite index on hierarchical fields for org queries
CREATE INDEX IF NOT EXISTS idx_silver_departments_hierarchy
ON silver.departments (campus_name, college_group, vp_area);

-- GIN index on location_info JSONB for flexible queries
CREATE INDEX IF NOT EXISTS idx_silver_departments_location_gin
ON silver.departments USING gin (location_info);

-- ============================================================================
-- STEP 8: Update table comments
-- ============================================================================

COMMENT ON TABLE silver.departments IS
'Cleaned and standardized department data merged from umich_api (org hierarchy) and tdx (operational data)';

COMMENT ON COLUMN silver.departments.dept_id IS
'Primary identifier from DeptId (UMICH_API) or Code (TDX) - unique across all sources';

COMMENT ON COLUMN silver.departments.silver_id IS
'Internal UUID for referencing from gold layer (maintained for backward compatibility)';

COMMENT ON COLUMN silver.departments.tdx_id IS
'TeamDynamix internal ID for API write-back operations';

COMMENT ON COLUMN silver.departments.hierarchical_path IS
'Full organizational path from root (Campus) to department';

COMMENT ON COLUMN silver.departments.location_info IS
'JSONB structure containing city, address, phone, fax, url, postal_code from TDX';

COMMENT ON COLUMN silver.departments.tdx_created_date IS
'Record creation timestamp from TeamDynamix (source-specific)';

COMMENT ON COLUMN silver.departments.tdx_modified_date IS
'Record modification timestamp from TeamDynamix (source-specific)';

-- ============================================================================
-- STEP 9: Final notices
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Migration 001 completed successfully!';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run validation queries (see migration file)';
    RAISE NOTICE '2. Run: python scripts/database/transform_silver_departments.py';
    RAISE NOTICE '3. Check for TEMP_ prefixed dept_ids';
    RAISE NOTICE '========================================';
END $$;

-- ============================================================================
-- VALIDATION QUERIES (run these manually after migration)
-- ============================================================================

-- Check for any NULL dept_id values (should be none)
-- SELECT COUNT(*) FROM silver.departments WHERE dept_id IS NULL;

-- Check for duplicate dept_id values (should be none)
-- SELECT dept_id, COUNT(*) FROM silver.departments GROUP BY dept_id HAVING COUNT(*) > 1;

-- Verify primary key constraint
-- SELECT constraint_name, constraint_type
-- FROM information_schema.table_constraints
-- WHERE table_schema = 'silver' AND table_name = 'departments' AND constraint_type = 'PRIMARY KEY';

-- Check silver_id is still unique
-- SELECT constraint_name, constraint_type
-- FROM information_schema.table_constraints
-- WHERE table_schema = 'silver' AND table_name = 'departments'
-- AND constraint_name = 'silver_departments_silver_id_unique';

-- Verify FK was recreated
-- SELECT constraint_name, table_name
-- FROM information_schema.table_constraints
-- WHERE constraint_schema = 'gold'
-- AND constraint_name = 'department_source_mapping_silver_id_fkey';

-- Count records by source system
-- SELECT source_system, COUNT(*) FROM silver.departments GROUP BY source_system;

-- Check for records needing re-transformation (those with TEMP_ prefix)
-- SELECT COUNT(*) FROM silver.departments WHERE dept_id LIKE 'TEMP_%';
