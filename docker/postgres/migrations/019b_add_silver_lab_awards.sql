-- Migration 019b: Create source-specific silver.lab_awards table
-- Purpose: New tier-1 source-specific table following medallion architecture
-- Date: 2025-11-22

-- ============================================================================
-- Create silver.lab_awards (source-specific)
-- ============================================================================

CREATE TABLE silver.lab_awards (
    -- Primary identifier (UUID for each record)
    award_record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Business keys (unique per award-person-role)
    award_id VARCHAR(15) NOT NULL,                  -- Award Id (max 9 → 15 with buffer)
    person_uniqname VARCHAR(50) NOT NULL,           -- Person Uniqname (max 8 → 50 standard)
    person_role VARCHAR(60) NOT NULL,               -- Person Role (max 48 → 60 with buffer)
    
    -- Award identification
    project_grant_id VARCHAR(15),                   -- Project/Grant (max 7 → 15)
    award_title VARCHAR(250) NOT NULL,              -- Award Title (max 193 → 250 with buffer)
    award_class VARCHAR(30),                        -- Award Class (max 24 → 30)
    
    -- Financial (parsed from "$X,XXX" strings)
    award_total_dollars NUMERIC(15,2),              -- Award Total Dollars
    award_direct_dollars NUMERIC(15,2),             -- Award Direct Dollars  
    award_indirect_dollars NUMERIC(15,2),           -- Award Indirect Dollars
    facilities_admin_rate NUMERIC(5,2),             -- Facilities & Admin Rate (%) (max 5 digits)
    
    -- Timeline (parsed from date strings)
    award_start_date DATE,                          -- Award Project Start Date
    award_end_date DATE,                            -- Award Project End Date
    pre_nce_end_date DATE,                          -- Pre NCE Project End Date
    award_publish_date DATE,                        -- Award Publish Date
    
    -- Sponsor information
    direct_sponsor_name VARCHAR(120),               -- Direct Sponsor Name (max 100 → 120)
    direct_sponsor_category VARCHAR(100),           -- Direct Sponsor Category (max 28 → 100)
    direct_sponsor_subcategory VARCHAR(100),        -- Direct Sponsor Subcategory (max 30 → 100)
    direct_sponsor_reference VARCHAR(100),          -- Reference Number (max 71 → 100)
    prime_sponsor_name VARCHAR(120),                -- Prime Sponsor Name (max 98 → 120)
    prime_sponsor_category VARCHAR(100),            -- Prime Sponsor Category (max 28 → 100)
    prime_sponsor_subcategory VARCHAR(100),         -- Prime Sponsor Subcategory (max 30 → 100)
    prime_sponsor_reference VARCHAR(100),           -- Reference Number (max 40 → 100)
    
    -- Administrative
    award_admin_department VARCHAR(100),            -- Award Admin Department (max 30 → 100)
    award_admin_school_college VARCHAR(100),        -- School/College (max 30 → 100)
    
    -- Person information (extracted from bronze, NOT enriched)
    person_first_name VARCHAR(100),                 -- Person First Name (max 13 → 100)
    person_last_name VARCHAR(100),                  -- Person Last Name (max 19 → 100)
    person_appt_department VARCHAR(100),            -- Person Appt Department (max 30 → 100)
    person_appt_department_id VARCHAR(50),          -- Person Appt Department Id (stored as string)
    person_appt_school_college VARCHAR(100),        -- School/College (max 30 → 100)
    
    -- Traceability (link back to bronze)
    raw_id UUID NOT NULL,                           -- bronze.raw_entities.raw_id
    raw_data_snapshot JSONB,                        -- Optional: preserve full bronze record
    
    -- Standard silver metadata
    source_system VARCHAR(50) DEFAULT 'lab_awards' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,               -- SHA-256 for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Indexes
-- ============================================================================

-- Unique constraint on business key
CREATE UNIQUE INDEX idx_lab_awards_unique 
    ON silver.lab_awards (award_id, person_uniqname, person_role);

-- Primary lookup indexes
CREATE INDEX idx_lab_awards_award_id ON silver.lab_awards (award_id);
CREATE INDEX idx_lab_awards_person ON silver.lab_awards (person_uniqname);
CREATE INDEX idx_lab_awards_person_role ON silver.lab_awards (person_role);

-- Date indexes for filtering
CREATE INDEX idx_lab_awards_dates ON silver.lab_awards (award_start_date, award_end_date);
CREATE INDEX idx_lab_awards_end_date ON silver.lab_awards (award_end_date DESC);

-- Financial indexes
CREATE INDEX idx_lab_awards_total_dollars ON silver.lab_awards (award_total_dollars DESC)
    WHERE award_total_dollars IS NOT NULL;

-- Sponsor indexes
CREATE INDEX idx_lab_awards_direct_sponsor ON silver.lab_awards (direct_sponsor_name)
    WHERE direct_sponsor_name IS NOT NULL;
CREATE INDEX idx_lab_awards_prime_sponsor ON silver.lab_awards (prime_sponsor_name)
    WHERE prime_sponsor_name IS NOT NULL;

-- Department index
CREATE INDEX idx_lab_awards_person_dept ON silver.lab_awards (person_appt_department_id)
    WHERE person_appt_department_id IS NOT NULL;

-- Traceability indexes
CREATE INDEX idx_lab_awards_raw_id ON silver.lab_awards (raw_id);
CREATE INDEX idx_lab_awards_entity_hash ON silver.lab_awards (entity_hash);
CREATE INDEX idx_lab_awards_ingestion_run ON silver.lab_awards (ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- GIN index for JSONB snapshot (if used)
CREATE INDEX idx_lab_awards_snapshot_gin ON silver.lab_awards USING gin (raw_data_snapshot)
    WHERE raw_data_snapshot IS NOT NULL;

-- ============================================================================
-- Table and Column Comments
-- ============================================================================

COMMENT ON TABLE silver.lab_awards IS
    'Source-specific silver table for lab awards (TIER 1). Extracts all fields from bronze.raw_entities where entity_type=lab_award. Part of medallion two-tier architecture. One record per (Award Id, Person Uniqname, Person Role) combination. Total records: 1,043 from bronze.';

COMMENT ON COLUMN silver.lab_awards.award_record_id IS
    'Primary key UUID for each record.';

COMMENT ON COLUMN silver.lab_awards.award_id IS
    'Award identifier from source system (e.g., AWD029634). Max length observed: 9 chars.';

COMMENT ON COLUMN silver.lab_awards.person_uniqname IS
    'Person uniqname (lowercase). Max length observed: 8 chars.';

COMMENT ON COLUMN silver.lab_awards.person_role IS
    'Person role on award (e.g., "UM Principal Investigator"). Max length observed: 48 chars.';

COMMENT ON COLUMN silver.lab_awards.award_total_dollars IS
    'Total award dollars parsed from "$X,XXX" format. Range: $0 to $10M+.';

COMMENT ON COLUMN silver.lab_awards.award_start_date IS
    'Award project start date parsed from "M/D/YYYY" format. 100% populated.';

COMMENT ON COLUMN silver.lab_awards.entity_hash IS
    'SHA-256 hash of significant fields for change detection. Excludes metadata (_content_hash, _source_file, _ingestion_timestamp).';

COMMENT ON COLUMN silver.lab_awards.raw_id IS
    'Link to bronze.raw_entities record for audit trail and lineage tracking.';

COMMENT ON COLUMN silver.lab_awards.raw_data_snapshot IS
    'Optional JSONB snapshot of complete bronze record for audit purposes.';

-- ============================================================================
-- Permissions
-- ============================================================================

GRANT SELECT ON silver.lab_awards TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.lab_awards TO lsats_user;

-- ============================================================================
-- Success Message
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Created silver.lab_awards (source-specific table)';
    RAISE NOTICE '📊 Ready for transformation from bronze.raw_entities';
    RAISE NOTICE '🔗 Indexes created: %', (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'lab_awards');
    RAISE NOTICE '📝 Next: Create transformation script 008_transform_lab_awards.py';
END $$;
