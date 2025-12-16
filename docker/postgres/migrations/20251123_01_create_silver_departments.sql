-- Created: 2025-11-23
-- Updates existing silver.departments to new consolidated schema

-- Add new columns if they don't exist
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS dept_name TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS parent_dept_id TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS campus_name TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS vp_area_name TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS college_name TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS dept_group TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS dept_group_campus TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS dept_group_vp_area TEXT;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS tdx_manager_uid UUID;
ALTER TABLE silver.departments ADD COLUMN IF NOT EXISTS sources JSONB DEFAULT '[]'::jsonb;

-- Migrate existing data to new column names
UPDATE silver.departments SET dept_name = department_name WHERE dept_name IS NULL AND department_name IS NOT NULL;
UPDATE silver.departments SET dept_name = department_code WHERE dept_name IS NULL AND department_code IS NOT NULL;
UPDATE silver.departments SET vp_area_name = vp_area WHERE vp_area_name IS NULL AND vp_area IS NOT NULL;
UPDATE silver.departments SET college_name = college_group WHERE college_name IS NULL AND college_group IS NOT NULL;

-- Set NOT NULL constraint on dept_name after data migration
ALTER TABLE silver.departments ALTER COLUMN dept_name SET NOT NULL;

-- Note: Keeping legacy columns for backward compatibility with existing views
-- Legacy columns: department_name, department_code, description, vp_area, college_group, 
-- hierarchical_path, tdx_created_date, tdx_modified_date, silver_id, source_system, source_entity_id


-- Update primary key to dept_id (if not already)
DO $$
BEGIN
    -- Drop existing primary key if it exists
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'departments_pkey') THEN
        ALTER TABLE silver.departments DROP CONSTRAINT departments_pkey;
    END IF;
    
    -- Add new primary key
    ALTER TABLE silver.departments ADD PRIMARY KEY (dept_id);
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_silver_departments_tdx_id ON silver.departments(tdx_id);
CREATE INDEX IF NOT EXISTS idx_silver_departments_parent_id ON silver.departments(parent_dept_id);
CREATE INDEX IF NOT EXISTS idx_silver_departments_college ON silver.departments(college_name);
CREATE INDEX IF NOT EXISTS idx_silver_departments_updated_at ON silver.departments(updated_at);

-- Comments
COMMENT ON TABLE silver.departments IS 'Consolidated department records from UMAPI and TeamDynamix';
COMMENT ON COLUMN silver.departments.dept_id IS '6-digit department code or TDX fallback code';
COMMENT ON COLUMN silver.departments.sources IS 'List of source systems contributing to this record';
