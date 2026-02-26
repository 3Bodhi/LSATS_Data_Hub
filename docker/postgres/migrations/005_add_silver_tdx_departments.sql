-- Migration: Add silver.tdx_departments source-specific table
-- Created: 2025-11-21
-- Purpose: Store TeamDynamix department data with full fidelity, separate from consolidated silver.departments

-- Create the silver.tdx_departments table
CREATE TABLE IF NOT EXISTS silver.tdx_departments (
    -- Primary identifiers
    tdx_id INTEGER PRIMARY KEY,
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),

    -- Core department information
    dept_code VARCHAR(50) NOT NULL,
    dept_name VARCHAR(255) NOT NULL,
    dept_notes TEXT,

    -- Status and hierarchy
    is_active BOOLEAN NOT NULL DEFAULT true,
    parent_id INTEGER,
    manager_uid UUID,

    -- Timestamps from TDX
    tdx_created_date TIMESTAMP WITH TIME ZONE,
    tdx_modified_date TIMESTAMP WITH TIME ZONE,

    -- Location and contact information (JSONB consolidation)
    location_info JSONB DEFAULT '{}'::jsonb,

    -- Custom TDX attributes (complete array structure)
    attributes JSONB DEFAULT '[]'::jsonb,

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) DEFAULT 1.00,
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    entity_hash VARCHAR(64) NOT NULL,
    is_enriched BOOLEAN DEFAULT false,
    source_bronze_id UUID,

    -- Standard audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),

    -- Constraints
    CONSTRAINT tdx_departments_dept_code_check CHECK (dept_code <> ''),
    CONSTRAINT tdx_departments_dept_name_check CHECK (dept_name <> ''),
    CONSTRAINT tdx_departments_quality_score_range CHECK (data_quality_score >= 0.00 AND data_quality_score <= 1.00)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_tdx_departments_dept_code ON silver.tdx_departments(dept_code);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_is_active ON silver.tdx_departments(is_active);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_is_enriched ON silver.tdx_departments(is_enriched);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_updated_at ON silver.tdx_departments(updated_at);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_location_info ON silver.tdx_departments USING gin(location_info);
CREATE INDEX IF NOT EXISTS idx_tdx_departments_attributes ON silver.tdx_departments USING gin(attributes);

-- Add table and column comments for documentation
COMMENT ON TABLE silver.tdx_departments IS 'Source-specific table for TeamDynamix department data with complete fidelity including custom attributes';
COMMENT ON COLUMN silver.tdx_departments.tdx_id IS 'TeamDynamix Account ID, primary key for write-back operations';
COMMENT ON COLUMN silver.tdx_departments.dept_code IS 'Department code matching DeptId in UMICH API for cross-referencing';
COMMENT ON COLUMN silver.tdx_departments.location_info IS 'JSONB object containing address, city, state, postal_code, country, phone, fax, url fields';
COMMENT ON COLUMN silver.tdx_departments.attributes IS 'JSONB array of TDX custom attributes with id, name, value, value_text structure';
COMMENT ON COLUMN silver.tdx_departments.is_enriched IS 'Boolean flag indicating whether Attributes field has been populated via enrichment';
COMMENT ON COLUMN silver.tdx_departments.manager_uid IS 'TDX User UID of department manager (00000000-0000-0000-0000-000000000000 treated as NULL)';
COMMENT ON COLUMN silver.tdx_departments.data_quality_score IS 'Calculated quality score from 0.00 to 1.00 based on completeness and consistency';
COMMENT ON COLUMN silver.tdx_departments.quality_flags IS 'JSONB array of quality issue identifiers';

-- Grant permissions (adjust as needed for your deployment)
-- GRANT SELECT ON silver.tdx_departments TO readonly_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON silver.tdx_departments TO readwrite_role;
