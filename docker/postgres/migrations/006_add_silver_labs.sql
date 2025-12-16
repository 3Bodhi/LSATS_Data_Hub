-- Migration 006: Add silver.labs and supporting tables
-- Purpose: Create tables for lab records merged from lab_award and organizational_unit sources
-- Author: LSATS Data Hub
-- Date: 2025-11-18

-- ============================================================================
-- PART 1: Bronze Layer Indexes for Performance
-- ============================================================================

-- Lab award indexes for efficient transformation queries
CREATE INDEX IF NOT EXISTS idx_bronze_lab_award_uniqname
ON bronze.raw_entities (LOWER(raw_data->>'Person Uniqname'))
WHERE entity_type = 'lab_award';

CREATE INDEX IF NOT EXISTS idx_bronze_lab_award_dept
ON bronze.raw_entities ((raw_data->>'Person Appt Department Id'))
WHERE entity_type = 'lab_award';

CREATE INDEX IF NOT EXISTS idx_bronze_lab_award_role
ON bronze.raw_entities ((raw_data->>'Person Role'))
WHERE entity_type = 'lab_award';

CREATE INDEX IF NOT EXISTS idx_bronze_lab_award_ingested
ON bronze.raw_entities (entity_type, source_system, ingested_at DESC)
WHERE entity_type = 'lab_award';

-- Organizational unit indexes for lab OU queries
CREATE INDEX IF NOT EXISTS idx_bronze_ou_extracted_uniqname
ON bronze.raw_entities (LOWER(raw_data->>'_extracted_uniqname'))
WHERE entity_type = 'organizational_unit'
  AND raw_data->>'_extracted_uniqname' IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bronze_ou_hierarchy_gin
ON bronze.raw_entities USING gin ((raw_data->'_ou_hierarchy'))
WHERE entity_type = 'organizational_unit';

CREATE INDEX IF NOT EXISTS idx_bronze_ou_ingested
ON bronze.raw_entities (entity_type, source_system, ingested_at DESC)
WHERE entity_type = 'organizational_unit';

-- ============================================================================
-- PART 2: Main Silver Table - silver.labs
-- ============================================================================

CREATE TABLE silver.labs (
    -- Primary identifiers
    lab_id VARCHAR(100) PRIMARY KEY,                    -- Same as PI uniqname (lowercase)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),   -- Internal UUID for referencing

    -- Principal Investigator (always required)
    pi_uniqname VARCHAR(50) NOT NULL,                   -- Lab owner/PI

    -- Core lab information
    lab_name VARCHAR(255),                              -- From OU name or generated from PI name
    lab_display_name VARCHAR(255),                      -- Friendly display name

    -- Department affiliation (from multiple sources)
    primary_department_id VARCHAR(50),                  -- Most common dept from awards or OU hierarchy
    department_ids JSONB DEFAULT '[]'::jsonb,           -- Array of all associated dept IDs
    department_names JSONB DEFAULT '[]'::jsonb,         -- Human-readable dept names

    -- Financial metrics (aggregated from lab_award)
    total_award_dollars NUMERIC(15,2) DEFAULT 0.00,     -- Sum of all Award Total Dollars
    total_direct_dollars NUMERIC(15,2) DEFAULT 0.00,    -- Sum of all Award Direct Dollars
    total_indirect_dollars NUMERIC(15,2) DEFAULT 0.00,  -- Sum of all Award Indirect Dollars
    award_count INTEGER DEFAULT 0,                      -- Number of active/historical awards
    active_award_count INTEGER DEFAULT 0,               -- Awards active as of transformation date

    -- Award date ranges
    earliest_award_start DATE,                          -- Earliest Award Project Start Date
    latest_award_end DATE,                              -- Latest Award Project End Date

    -- Active Directory organizational structure (from organizational_unit)
    has_ad_ou BOOLEAN DEFAULT false,                    -- Whether lab has an OU in AD
    ad_ou_dn TEXT,                                      -- Full distinguished name
    ad_ou_hierarchy JSONB DEFAULT '[]'::jsonb,          -- Array of OU levels
    ad_parent_ou TEXT,                                  -- Parent OU DN
    ad_ou_depth INTEGER,                                -- Depth in OU tree

    -- Infrastructure metadata (from organizational_unit)
    computer_count INTEGER DEFAULT 0,                   -- Direct computer count from OU
    has_computer_children BOOLEAN DEFAULT false,        -- Whether OU has computers
    has_child_ous BOOLEAN DEFAULT false,                -- Whether OU has sub-OUs

    -- AD timestamps (from organizational_unit)
    ad_ou_created TIMESTAMP WITH TIME ZONE,             -- whenCreated from AD
    ad_ou_modified TIMESTAMP WITH TIME ZONE,            -- whenChanged from AD

    -- Lab member counts (calculated from junction tables)
    pi_count INTEGER DEFAULT 0,                         -- Count of PIs (from lab_members where role is PI)
    investigator_count INTEGER DEFAULT 0,               -- Count of all investigators
    member_count INTEGER DEFAULT 0,                     -- Total member count

    -- Operational status
    is_active BOOLEAN NOT NULL DEFAULT true,            -- Has recent awards OR active AD OU
    has_active_awards BOOLEAN DEFAULT false,            -- Current date within award date range
    has_active_ou BOOLEAN DEFAULT false,                -- OU exists and has computers

    -- Data completeness flags
    has_award_data BOOLEAN DEFAULT false,               -- Sourced from lab_award
    has_ou_data BOOLEAN DEFAULT false,                  -- Sourced from organizational_unit
    data_source VARCHAR(50) NOT NULL,                   -- 'award_only', 'ou_only', 'award+ou'

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,            -- ['no_silver_user', 'no_department', 'no_awards', etc.]

    -- Source tracking
    source_system VARCHAR(100) NOT NULL,                -- 'lab_award+organizational_unit' or single
    entity_hash VARCHAR(64) NOT NULL,                   -- Hash of merged content

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),

    -- Foreign key to PI user
    CONSTRAINT fk_labs_pi_user
        FOREIGN KEY (pi_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE RESTRICT,  -- Cannot delete user if they're a PI

    -- Foreign key to primary department
    CONSTRAINT fk_labs_primary_department
        FOREIGN KEY (primary_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.labs IS 'Lab records merged from lab_award and organizational_unit sources. One record per PI uniqname.';
COMMENT ON COLUMN silver.labs.lab_id IS 'Primary key: lowercase PI uniqname';
COMMENT ON COLUMN silver.labs.pi_uniqname IS 'Principal Investigator uniqname - must exist in silver.users';
COMMENT ON COLUMN silver.labs.total_award_dollars IS 'Sum of all award total dollars for this lab';
COMMENT ON COLUMN silver.labs.ad_ou_dn IS 'Full AD distinguished name if lab has an OU';
COMMENT ON COLUMN silver.labs.data_source IS 'Indicates which bronze sources contributed: award_only, ou_only, or award+ou';

-- Trigger for automatic timestamp updates
CREATE TRIGGER update_silver_labs_updated_at
    BEFORE UPDATE ON silver.labs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Indexes for silver.labs
-- ============================================================================

-- Core lookup indexes
CREATE INDEX idx_silver_labs_silver_id ON silver.labs (silver_id);
CREATE INDEX idx_silver_labs_pi_uniqname ON silver.labs (pi_uniqname);
CREATE INDEX idx_silver_labs_entity_hash ON silver.labs (entity_hash);

-- Query optimization indexes
CREATE INDEX idx_silver_labs_active ON silver.labs (is_active, lab_name);
CREATE INDEX idx_silver_labs_quality ON silver.labs (data_quality_score DESC);
CREATE INDEX idx_silver_labs_primary_dept ON silver.labs (primary_department_id);
CREATE INDEX idx_silver_labs_data_source ON silver.labs (data_source, has_award_data, has_ou_data);

-- Filtered indexes for common queries
CREATE INDEX idx_silver_labs_with_ou ON silver.labs (has_ad_ou, ad_ou_dn)
    WHERE has_ad_ou = true;
CREATE INDEX idx_silver_labs_with_awards ON silver.labs (has_award_data, total_award_dollars DESC)
    WHERE has_award_data = true;
CREATE INDEX idx_silver_labs_active_awards ON silver.labs (has_active_awards, latest_award_end)
    WHERE has_active_awards = true;

-- Financial and metric indexes
CREATE INDEX idx_silver_labs_award_dollars ON silver.labs (total_award_dollars DESC);
CREATE INDEX idx_silver_labs_award_count ON silver.labs (award_count DESC);
CREATE INDEX idx_silver_labs_member_count ON silver.labs (member_count DESC);
CREATE INDEX idx_silver_labs_computer_count ON silver.labs (computer_count DESC);

-- Date range index for active award queries
CREATE INDEX idx_silver_labs_award_dates ON silver.labs (earliest_award_start, latest_award_end);

-- GIN indexes for JSONB fields
CREATE INDEX idx_silver_labs_dept_ids_gin ON silver.labs USING gin (department_ids);
CREATE INDEX idx_silver_labs_dept_names_gin ON silver.labs USING gin (department_names);
CREATE INDEX idx_silver_labs_ou_hierarchy_gin ON silver.labs USING gin (ad_ou_hierarchy);
CREATE INDEX idx_silver_labs_quality_flags_gin ON silver.labs USING gin (quality_flags);

-- ============================================================================
-- PART 3: Junction Table - silver.lab_members
-- ============================================================================

CREATE TABLE silver.lab_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR(100) NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,
    member_uniqname VARCHAR(50) NOT NULL,               -- Member's uniqname

    -- Role information (from lab_award Person Role field)
    member_role VARCHAR(100) NOT NULL,                  -- 'UM Principal Investigator', 'Participating Investigator', etc.
    is_pi BOOLEAN GENERATED ALWAYS AS (
        member_role LIKE '%Principal Investigator%'
    ) STORED,

    -- Member details (denormalized for performance)
    member_first_name VARCHAR(255),                     -- From bronze lab_award Person First Name
    member_last_name VARCHAR(255),                      -- From bronze lab_award Person Last Name
    member_full_name VARCHAR(255),                      -- Derived or from silver.users
    member_department_id VARCHAR(50),                   -- Person Appt Department Id
    member_department_name VARCHAR(255),                -- Person Appt Department

    -- Job/employment info from silver.users (if available)
    silver_user_exists BOOLEAN DEFAULT false,           -- Whether member has silver.users record
    member_job_title TEXT,                              -- From silver.users.job_title

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,                 -- 'lab_award' (future: 'ad_group', 'manual')
    source_award_ids JSONB DEFAULT '[]'::jsonb,         -- Array of Award IDs this person appears in

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key to user (if they exist in silver.users)
    CONSTRAINT fk_lab_members_user
        FOREIGN KEY (member_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE CASCADE,

    -- Foreign key to department
    CONSTRAINT fk_lab_members_department
        FOREIGN KEY (member_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.lab_members IS 'Lab membership records linking users to labs with their roles';
COMMENT ON COLUMN silver.lab_members.is_pi IS 'Computed: true if role contains "Principal Investigator"';
COMMENT ON COLUMN silver.lab_members.source_award_ids IS 'Array of Award IDs where this person appears';

-- Prevent duplicate memberships (same person, same lab, same role)
CREATE UNIQUE INDEX idx_lab_members_unique ON silver.lab_members (
    lab_id, member_uniqname, member_role
);

-- ============================================================================
-- Indexes for silver.lab_members
-- ============================================================================

-- Primary lookup indexes
CREATE INDEX idx_lab_members_lab ON silver.lab_members (lab_id);
CREATE INDEX idx_lab_members_uniqname ON silver.lab_members (member_uniqname);

-- Role-based indexes
CREATE INDEX idx_lab_members_pi ON silver.lab_members (lab_id, is_pi) WHERE is_pi = true;
CREATE INDEX idx_lab_members_role ON silver.lab_members (member_role);

-- Department and user existence indexes
CREATE INDEX idx_lab_members_department ON silver.lab_members (member_department_id);
CREATE INDEX idx_lab_members_silver_user ON silver.lab_members (silver_user_exists, member_uniqname)
    WHERE silver_user_exists = true;
CREATE INDEX idx_lab_members_no_user ON silver.lab_members (member_uniqname)
    WHERE silver_user_exists = false;

-- Source tracking index
CREATE INDEX idx_lab_members_source ON silver.lab_members (source_system);
CREATE INDEX idx_lab_members_source_awards_gin ON silver.lab_members USING gin (source_award_ids);

-- ============================================================================
-- PART 4: Detail Table - silver.lab_awards
-- ============================================================================

CREATE TABLE silver.lab_awards (
    award_record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR(100) NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,

    -- Award identification
    award_id VARCHAR(50) NOT NULL,                      -- Award Id (e.g., AWD029634)
    project_grant_id VARCHAR(50),                       -- Project/Grant

    -- Award details
    award_title TEXT NOT NULL,                          -- Award Title
    award_class VARCHAR(100),                           -- Award Class

    -- Financial information
    award_total_dollars NUMERIC(15,2),                  -- Parsed from "$60,000" format
    award_direct_dollars NUMERIC(15,2),
    award_indirect_dollars NUMERIC(15,2),
    facilities_admin_rate NUMERIC(5,2),                 -- Facilities & Admin Rate (%)

    -- Timeline
    award_start_date DATE,                              -- Award Project Start Date
    award_end_date DATE,                                -- Award Project End Date
    pre_nce_end_date DATE,                              -- Pre NCE Project End Date
    award_publish_date DATE,                            -- Award Publish Date

    -- Sponsor information
    direct_sponsor_name VARCHAR(255),
    direct_sponsor_category VARCHAR(255),
    direct_sponsor_subcategory VARCHAR(255),
    direct_sponsor_reference VARCHAR(255),
    prime_sponsor_name VARCHAR(255),
    prime_sponsor_category VARCHAR(255),
    prime_sponsor_subcategory VARCHAR(255),
    prime_sponsor_reference VARCHAR(255),

    -- Administrative information
    award_admin_department VARCHAR(255),
    award_admin_school_college VARCHAR(255),

    -- Person information (duplicated from lab_members for convenience)
    person_uniqname VARCHAR(50) NOT NULL,
    person_role VARCHAR(100) NOT NULL,
    person_first_name VARCHAR(255),
    person_last_name VARCHAR(255),
    person_appt_department VARCHAR(255),
    person_appt_department_id VARCHAR(50),
    person_appt_school_college VARCHAR(255),

    -- Activity status
    is_active BOOLEAN DEFAULT false,                    -- Current date within start/end range

    -- Source tracking
    bronze_raw_id UUID,                                 -- Link to bronze.raw_entities
    source_file VARCHAR(255),                           -- _source_file from bronze
    content_hash VARCHAR(64),                           -- _content_hash from bronze

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_lab_awards_person
        FOREIGN KEY (person_uniqname)
        REFERENCES silver.users(uniqname)
        ON DELETE CASCADE,

    CONSTRAINT fk_lab_awards_department
        FOREIGN KEY (person_appt_department_id)
        REFERENCES silver.departments(dept_id)
        ON DELETE SET NULL
);

COMMENT ON TABLE silver.lab_awards IS 'Individual award records preserving all details from lab_award bronze source';
COMMENT ON COLUMN silver.lab_awards.is_active IS 'True if current date is between award start and end dates';
COMMENT ON COLUMN silver.lab_awards.bronze_raw_id IS 'Link back to original bronze.raw_entities record';

-- Prevent duplicate award records
CREATE UNIQUE INDEX idx_lab_awards_unique ON silver.lab_awards (
    award_id, person_uniqname, person_role
);

-- ============================================================================
-- Indexes for silver.lab_awards
-- ============================================================================

-- Primary lookup indexes
CREATE INDEX idx_lab_awards_lab ON silver.lab_awards (lab_id);
CREATE INDEX idx_lab_awards_award_id ON silver.lab_awards (award_id);
CREATE INDEX idx_lab_awards_person ON silver.lab_awards (person_uniqname);

-- Activity and date indexes
CREATE INDEX idx_lab_awards_active ON silver.lab_awards (is_active, award_end_date)
    WHERE is_active = true;
CREATE INDEX idx_lab_awards_dates ON silver.lab_awards (award_start_date, award_end_date);
CREATE INDEX idx_lab_awards_end_date ON silver.lab_awards (award_end_date DESC);

-- Financial indexes
CREATE INDEX idx_lab_awards_dollars ON silver.lab_awards (award_total_dollars DESC);
CREATE INDEX idx_lab_awards_direct_dollars ON silver.lab_awards (award_direct_dollars DESC);

-- Sponsor and category indexes
CREATE INDEX idx_lab_awards_direct_sponsor ON silver.lab_awards (direct_sponsor_name);
CREATE INDEX idx_lab_awards_prime_sponsor ON silver.lab_awards (prime_sponsor_name);
CREATE INDEX idx_lab_awards_award_class ON silver.lab_awards (award_class);

-- Department and person indexes
CREATE INDEX idx_lab_awards_person_dept ON silver.lab_awards (person_appt_department_id);
CREATE INDEX idx_lab_awards_admin_dept ON silver.lab_awards (award_admin_department);
CREATE INDEX idx_lab_awards_person_role ON silver.lab_awards (person_role);

-- Source tracking indexes
CREATE INDEX idx_lab_awards_bronze ON silver.lab_awards (bronze_raw_id);
CREATE INDEX idx_lab_awards_source_file ON silver.lab_awards (source_file);
CREATE INDEX idx_lab_awards_content_hash ON silver.lab_awards (content_hash);

-- ============================================================================
-- PART 5: Views for Common Queries
-- ============================================================================

-- View 1: Lab Summary with PI Details
CREATE OR REPLACE VIEW silver.v_lab_summary AS
SELECT
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    u.full_name AS pi_full_name,
    u.primary_email AS pi_email,
    u.job_title AS pi_job_title,
    l.primary_department_id,
    d.department_name AS primary_department_name,
    l.total_award_dollars,
    l.award_count,
    l.active_award_count,
    l.member_count,
    l.computer_count,
    l.is_active,
    l.data_source,
    l.data_quality_score
FROM silver.labs l
LEFT JOIN silver.users u ON l.pi_uniqname = u.uniqname
LEFT JOIN silver.departments d ON l.primary_department_id = d.dept_id;

COMMENT ON VIEW silver.v_lab_summary IS 'Lab overview with PI and department details for reporting';

-- View 2: Department Lab Aggregates
CREATE OR REPLACE VIEW silver.v_department_labs AS
SELECT
    d.dept_id,
    d.department_name,
    COUNT(l.lab_id) AS lab_count,
    SUM(l.total_award_dollars) AS total_department_funding,
    SUM(l.member_count) AS total_lab_members,
    SUM(l.computer_count) AS total_lab_computers,
    COUNT(l.lab_id) FILTER (WHERE l.is_active) AS active_lab_count,
    COUNT(l.lab_id) FILTER (WHERE l.has_award_data) AS labs_with_awards,
    COUNT(l.lab_id) FILTER (WHERE l.has_ou_data) AS labs_with_ou
FROM silver.departments d
LEFT JOIN silver.labs l ON d.dept_id = l.primary_department_id
GROUP BY d.dept_id, d.department_name;

COMMENT ON VIEW silver.v_department_labs IS 'Aggregated lab statistics per department';

-- View 3: Lab Members with Job Details
CREATE OR REPLACE VIEW silver.v_lab_members_detailed AS
SELECT
    lm.lab_id,
    l.lab_name,
    lm.member_uniqname,
    lm.member_full_name,
    lm.member_role,
    lm.is_pi,
    u.job_title,
    u.department_job_titles,
    u.job_codes,
    lm.member_department_name,
    lm.silver_user_exists
FROM silver.lab_members lm
INNER JOIN silver.labs l ON lm.lab_id = l.lab_id
LEFT JOIN silver.users u ON lm.member_uniqname = u.uniqname;

COMMENT ON VIEW silver.v_lab_members_detailed IS 'Lab membership with enriched user employment data';

-- View 4: Active Awards by Lab
CREATE OR REPLACE VIEW silver.v_lab_active_awards AS
SELECT
    l.lab_id,
    l.lab_name,
    la.award_id,
    la.award_title,
    la.award_total_dollars,
    la.award_start_date,
    la.award_end_date,
    la.direct_sponsor_name,
    la.person_uniqname,
    la.person_role
FROM silver.labs l
INNER JOIN silver.lab_awards la ON l.lab_id = la.lab_id
WHERE la.is_active = true
ORDER BY l.lab_id, la.award_end_date DESC;

COMMENT ON VIEW silver.v_lab_active_awards IS 'Currently active awards across all labs';

-- View 5: Lab Groups (via pattern matching)
CREATE OR REPLACE VIEW silver.v_lab_groups AS
SELECT
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    g.group_id,
    g.group_name,
    g.description,
    g.member_count AS group_member_count,
    CASE
        WHEN g.group_name ILIKE '%' || l.pi_uniqname || '%' THEN 'name_regex_match'
        WHEN g.mcommunity_dn ILIKE '%OU=' || l.pi_uniqname || '%' THEN 'dn_ou_match'
        ELSE 'other'
    END AS match_type,
    g.is_ad_synced,
    g.email_address AS group_email
FROM silver.labs l
INNER JOIN silver.groups g ON (
    g.group_name ILIKE '%' || l.pi_uniqname || '%'
    OR g.mcommunity_dn ILIKE '%OU=' || l.pi_uniqname || '%'
)
WHERE l.is_active = true;

COMMENT ON VIEW silver.v_lab_groups IS 'Groups potentially associated with labs via name/DN pattern matching';

-- ============================================================================
-- PART 6: Helper Functions
-- ============================================================================

-- Function to update lab member counts
CREATE OR REPLACE FUNCTION silver.update_lab_member_counts(p_lab_id VARCHAR(100))
RETURNS VOID AS $$
BEGIN
    UPDATE silver.labs
    SET
        member_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id
        ),
        pi_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id AND is_pi = true
        ),
        investigator_count = (
            SELECT COUNT(*)
            FROM silver.lab_members
            WHERE lab_id = p_lab_id
              AND member_role LIKE '%Investigator%'
        )
    WHERE lab_id = p_lab_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION silver.update_lab_member_counts IS 'Recalculates member counts for a specific lab';

-- ============================================================================
-- Migration Complete
-- ============================================================================

-- Validation: Verify all tables and indexes exist
DO $$
BEGIN
    ASSERT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'silver' AND tablename = 'labs'),
        'Table silver.labs was not created';
    ASSERT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'silver' AND tablename = 'lab_members'),
        'Table silver.lab_members was not created';
    ASSERT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'silver' AND tablename = 'lab_awards'),
        'Table silver.lab_awards was not created';

    RAISE NOTICE 'Migration 006 completed successfully';
    RAISE NOTICE 'Created tables: silver.labs, silver.lab_members, silver.lab_awards';
    RAISE NOTICE 'Created views: v_lab_summary, v_department_labs, v_lab_members_detailed, v_lab_active_awards, v_lab_groups';
    RAISE NOTICE 'Created indexes: 40+ indexes for performance optimization';
END $$;
