-- Migration: Add silver.umapi_departments source-specific table
-- Created: 2025-11-22
-- Purpose: Store UMich API department data with organizational hierarchy, separate from consolidated silver.departments

-- Create the silver.umapi_departments table
CREATE TABLE IF NOT EXISTS silver.umapi_departments (
    -- Primary identifier (UMAPI DeptId)
    dept_id VARCHAR(10) PRIMARY KEY,
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),

    -- Core department information
    dept_description VARCHAR(50) NOT NULL,

    -- Organizational hierarchy codes (machine-readable)
    dept_group VARCHAR(50),
    dept_group_campus VARCHAR(20),
    dept_group_vp_area VARCHAR(50),

    -- Organizational hierarchy descriptions (human-readable)
    college_name VARCHAR(50),
    campus_name VARCHAR(50),
    vp_area_name VARCHAR(50),

    -- Computed hierarchical path
    hierarchical_path VARCHAR(255),

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) DEFAULT 1.00,
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    entity_hash VARCHAR(64) NOT NULL,
    source_bronze_id UUID,

    -- Standard audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),

    -- Constraints
    CONSTRAINT umapi_departments_dept_id_check CHECK (dept_id <> ''),
    CONSTRAINT umapi_departments_dept_description_check CHECK (dept_description <> ''),
    CONSTRAINT umapi_departments_quality_score_range CHECK (data_quality_score >= 0.00 AND data_quality_score <= 1.00)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_umapi_departments_dept_group ON silver.umapi_departments(dept_group);
CREATE INDEX IF NOT EXISTS idx_umapi_departments_campus ON silver.umapi_departments(dept_group_campus);
CREATE INDEX IF NOT EXISTS idx_umapi_departments_vp_area ON silver.umapi_departments(dept_group_vp_area);
CREATE INDEX IF NOT EXISTS idx_umapi_departments_updated_at ON silver.umapi_departments(updated_at);
CREATE INDEX IF NOT EXISTS idx_umapi_departments_hierarchy ON silver.umapi_departments(dept_group_campus, dept_group_vp_area, dept_group);

-- Add table and column comments for documentation
COMMENT ON TABLE silver.umapi_departments IS 'Source-specific table for UMich API department data with organizational hierarchy and campus information';
COMMENT ON COLUMN silver.umapi_departments.dept_id IS 'UMich API DeptId, matches dept_code in TDX departments for cross-referencing';
COMMENT ON COLUMN silver.umapi_departments.dept_description IS 'Department name from UMAPI (e.g., "LSA Physics")';
COMMENT ON COLUMN silver.umapi_departments.dept_group IS 'Machine-readable college/group code (e.g., "COLLEGE_OF_LSA")';
COMMENT ON COLUMN silver.umapi_departments.dept_group_campus IS 'Machine-readable campus code (e.g., "UM_ANN-ARBOR")';
COMMENT ON COLUMN silver.umapi_departments.dept_group_vp_area IS 'Machine-readable VP area code (e.g., "PRVST_EXC_VP_ACA_AFF")';
COMMENT ON COLUMN silver.umapi_departments.college_name IS 'Human-readable college/group name (e.g., "College of Lit, Science & Arts")';
COMMENT ON COLUMN silver.umapi_departments.campus_name IS 'Human-readable campus name (e.g., "Univ of Mich-Ann-Arbor")';
COMMENT ON COLUMN silver.umapi_departments.vp_area_name IS 'Human-readable VP area name (e.g., "Provost & Exec VP Academic Aff")';
COMMENT ON COLUMN silver.umapi_departments.hierarchical_path IS 'Full organizational path from campus to department (e.g., "Univ of Mich-Ann-Arbor -> Provost & Exec VP Academic Aff -> College of Lit, Science & Arts -> LSA Physics")';
COMMENT ON COLUMN silver.umapi_departments.data_quality_score IS 'Calculated quality score from 0.00 to 1.00 based on completeness of hierarchy fields';
COMMENT ON COLUMN silver.umapi_departments.quality_flags IS 'JSONB array of quality issue identifiers';

-- Grant permissions (adjust as needed for your deployment)
-- GRANT SELECT ON silver.umapi_departments TO readonly_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON silver.umapi_departments TO readwrite_role;
