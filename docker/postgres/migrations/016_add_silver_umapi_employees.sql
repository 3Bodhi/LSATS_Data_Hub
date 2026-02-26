-- Migration: Add silver.umapi_employees source-specific table
-- Date: 2025-11-22
-- Purpose: Create source-specific silver table for UMich API employee records
--          Part of two-tier silver architecture (source-specific -> consolidated)
--
-- Key Design Decisions:
-- - Composite PK on (empl_id, empl_rcd) to store each employment record separately
-- - Handles multi-department employees (e.g., dual appointments, multiple job codes)
-- - Nullable uniqname since 29% of UMAPI employees lack this field
-- - JSONB work_location for 8 address/phone fields (follows silver.tdx_users pattern)
-- - TEXT for university_job_title to handle 611-character outlier

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.umapi_employees CASCADE;

-- Create silver.umapi_employees table
CREATE TABLE IF NOT EXISTS silver.umapi_employees (
    -- Composite primary key (unique employment record)
    empl_id VARCHAR(10) NOT NULL,
    empl_rcd SMALLINT NOT NULL,
    PRIMARY KEY (empl_id, empl_rcd),

    -- Additional unique identifier for individual records
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),

    -- Business key for joining to consolidated silver.users
    -- Note: 29% of UMAPI employees lack uniqname (temporary workers, students, affiliates)
    uniqname VARCHAR(10),

    -- Core identity fields
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(35),
    full_name VARCHAR(60),                     -- Computed: "LastName, FirstName"

    -- Employment and organizational
    department_id VARCHAR(10) NOT NULL,        -- References silver.departments(dept_id)
    dept_description VARCHAR(50),              -- Department name from UMAPI
    supervisor_id VARCHAR(10),                 -- Nullable (6.9% empty), references another empl_id

    -- Job title fields
    jobcode VARCHAR(10),                       -- Job classification code
    department_job_title VARCHAR(50),          -- Department-specific job title
    university_job_title TEXT,                 -- University-wide job title (max 611 chars for emeritus titles)

    -- Work location (consolidated JSONB following tdx_users pattern)
    -- Contains: address1, address2, address3, city, state, postal, country, phone, phone_extension
    work_location JSONB DEFAULT '{}'::jsonb,

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) DEFAULT 1.00,
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                      -- Most recent bronze.raw_entities.raw_id
    entity_hash VARCHAR(64) NOT NULL,          -- For change detection

    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'umich_api' NOT NULL,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT umapi_employees_empl_id_check CHECK (empl_id <> ''),
    CONSTRAINT umapi_employees_empl_rcd_check CHECK (empl_rcd >= 0),
    CONSTRAINT umapi_employees_quality_score_range CHECK (data_quality_score >= 0.00 AND data_quality_score <= 1.00)
);

-- Indexes for performance

-- Primary access patterns (joining to consolidated silver.users)
CREATE UNIQUE INDEX IF NOT EXISTS idx_umapi_employees_uniqname_empl_rcd
    ON silver.umapi_employees (uniqname, empl_rcd)
    WHERE uniqname IS NOT NULL;  -- 71% of records have uniqname

CREATE INDEX IF NOT EXISTS idx_umapi_employees_uniqname
    ON silver.umapi_employees (uniqname)
    WHERE uniqname IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_umapi_employees_empl_id
    ON silver.umapi_employees (empl_id);

-- Foreign key lookups (not enforced per medallion standards)
CREATE INDEX IF NOT EXISTS idx_umapi_employees_department_id
    ON silver.umapi_employees (department_id);

CREATE INDEX IF NOT EXISTS idx_umapi_employees_supervisor_id
    ON silver.umapi_employees (supervisor_id)
    WHERE supervisor_id IS NOT NULL;

-- Traceability
CREATE INDEX IF NOT EXISTS idx_umapi_employees_raw_id
    ON silver.umapi_employees (raw_id);

CREATE INDEX IF NOT EXISTS idx_umapi_employees_entity_hash
    ON silver.umapi_employees (entity_hash);

-- JSONB work location (for querying by city, state, etc.)
CREATE INDEX IF NOT EXISTS idx_umapi_employees_work_location_gin
    ON silver.umapi_employees USING gin (work_location);

-- Standard metadata
CREATE INDEX IF NOT EXISTS idx_umapi_employees_updated_at
    ON silver.umapi_employees (updated_at);

-- Quality filtering
CREATE INDEX IF NOT EXISTS idx_umapi_employees_quality_score
    ON silver.umapi_employees (data_quality_score DESC);

-- Table and column comments for documentation
COMMENT ON TABLE silver.umapi_employees IS
    'Source-specific silver table for UMich API employee records. Part of two-tier silver architecture - feeds into consolidated silver.users table. Stores each employment record separately to handle multi-department employees (dual appointments, multiple job codes).';

COMMENT ON COLUMN silver.umapi_employees.empl_id IS
    'UMich Employee ID (EmplId) - primary identifier for an employee. Max 8 chars observed, sized to 10 for future growth.';

COMMENT ON COLUMN silver.umapi_employees.empl_rcd IS
    'Employment Record number (EmplRcd) - 0 for primary, 1+ for additional appointments. Range 0-511 observed. Employees with multiple records have dual appointments or multiple job codes.';

COMMENT ON COLUMN silver.umapi_employees.uniqname IS
    'Business key from UniqName field - normalized lowercase uniqname for joining to silver.users. NULLABLE: 29% of UMAPI employees lack uniqname (temporary workers, students, affiliates).';

COMMENT ON COLUMN silver.umapi_employees.full_name IS
    'Full name in "LastName, FirstName" format from UMAPI Name field';

COMMENT ON COLUMN silver.umapi_employees.department_id IS
    'Logical FK to silver.departments(dept_id) - department code (not enforced per medallion standards)';

COMMENT ON COLUMN silver.umapi_employees.supervisor_id IS
    'Logical FK to another empl_id - supervisor relationship. Nullable (6.9% of employees lack supervisor).';

COMMENT ON COLUMN silver.umapi_employees.university_job_title IS
    'University-wide job title from UniversityJobTitle field. TEXT type to handle 611-character outlier (Provost Emeritus with concatenated titles).';

COMMENT ON COLUMN silver.umapi_employees.work_location IS
    'JSONB object containing work address and contact info: {address1, address2, address3, city, state, postal, country, phone, phone_extension}';

COMMENT ON COLUMN silver.umapi_employees.data_quality_score IS
    'Calculated quality score 0.00-1.00 based on: has_uniqname (0.30), has_supervisor (0.15), has_phone (0.15), complete_location (0.20), has_jobcode (0.10), has_univ_job_title (0.10)';

COMMENT ON COLUMN silver.umapi_employees.quality_flags IS
    'JSONB array of quality issue identifiers: missing_uniqname, missing_supervisor, missing_work_phone, incomplete_work_location, missing_jobcode, missing_university_job_title';

COMMENT ON COLUMN silver.umapi_employees.raw_id IS
    'Link to most recent bronze.raw_entities record for this employment record - for audit trail';

COMMENT ON COLUMN silver.umapi_employees.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed';

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.umapi_employees TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.umapi_employees TO lsats_user;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Successfully created silver.umapi_employees table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'umapi_employees');
END $$;
