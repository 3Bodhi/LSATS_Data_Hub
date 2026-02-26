-- Migration 011: Add Lab Manager Identification System
-- Description: Creates views and tables for identifying up to 3 lab managers per legitimate lab
-- Author: LSATS Data Hub Team
-- Date: 2025-11-20

-- ============================================================================
-- PART 1: Views for Lab Manager Identification
-- ============================================================================

-- View: Legitimate labs (monitored with TDX-connected departments)
CREATE OR REPLACE VIEW silver.v_legitimate_labs AS
SELECT
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    l.member_count,
    l.computer_count,
    l.primary_department_id,
    d.tdx_id as department_tdx_id
FROM silver.labs l
INNER JOIN silver.departments d ON l.primary_department_id = d.dept_id
WHERE l.is_active = true
    AND l.computer_count > 0
    AND d.tdx_id IS NOT NULL;

COMMENT ON VIEW silver.v_legitimate_labs IS
'Labs that are actively monitored (have computers) and connected to TDX-enabled departments. Used for lab manager identification.';


-- View: Eligible lab members for manager identification
CREATE OR REPLACE VIEW silver.v_eligible_lab_members AS
SELECT
    lm.membership_id,
    lm.lab_id,
    lm.member_uniqname,
    lm.member_role,
    lm.member_job_title,
    lm.is_pi,
    lm.is_investigator,
    u.job_codes,
    u.tdx_user_uid
FROM silver.lab_members lm
INNER JOIN silver.users u ON lm.member_uniqname = u.uniqname
WHERE lm.member_role IS NOT NULL  -- Must have a role
    AND lm.member_department_id NOT IN (
        '171240','481477','171210','171220','171245','171230','481207',
        '309980','309982','309981','315834','231640','211600','481450',
        '676785','309919','309921','380002'
    )  -- Exclude support departments (LSATS, ITS, etc.)
    AND lm.member_role NOT ILIKE '%Chief Administrator%'  -- Exclude Chief Administrators
    AND lm.member_role NOT ILIKE '%Professor%'  -- Exclude professors (likely PIs/co-PIs)
    AND lm.is_pi = false;  -- Exclude PIs explicitly

COMMENT ON VIEW silver.v_eligible_lab_members IS
'Lab members eligible for manager identification. Excludes PIs, professors, Chief Administrators, and support department staff.';


-- ============================================================================
-- PART 2: Lab Managers Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.lab_managers (
    lab_manager_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR(100) NOT NULL,
    manager_uniqname VARCHAR(50) NOT NULL,
    manager_tdx_uid UUID,  -- For TDX write-back operations (matches users.tdx_user_uid)
    manager_role TEXT,
    manager_job_codes JSONB,  -- Store for reference
    manager_confidence_score INTEGER NOT NULL,  -- 1 (highest) to 10 (lowest)
    manager_rank INTEGER NOT NULL,  -- 1, 2, or 3 (up to 3 managers per lab)
    detection_reason TEXT NOT NULL,  -- Human-readable explanation
    is_verified BOOLEAN DEFAULT false,  -- Manual verification flag
    verification_notes TEXT,  -- Human override notes
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(lab_id, manager_uniqname),
    CHECK (manager_rank BETWEEN 1 AND 3),
    CHECK (manager_confidence_score BETWEEN 1 AND 10),

    FOREIGN KEY (lab_id) REFERENCES silver.labs(lab_id) ON DELETE CASCADE,
    FOREIGN KEY (manager_uniqname) REFERENCES silver.users(uniqname) ON DELETE CASCADE
);

CREATE INDEX idx_lab_managers_lab ON silver.lab_managers(lab_id, manager_rank);
CREATE INDEX idx_lab_managers_uniqname ON silver.lab_managers(manager_uniqname);
CREATE INDEX idx_lab_managers_confidence ON silver.lab_managers(manager_confidence_score);
CREATE INDEX idx_lab_managers_verified ON silver.lab_managers(is_verified) WHERE is_verified = true;
CREATE INDEX idx_lab_managers_tdx_uid ON silver.lab_managers(manager_tdx_uid) WHERE manager_tdx_uid IS NOT NULL;

CREATE TRIGGER update_lab_managers_updated_at
    BEFORE UPDATE ON silver.lab_managers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE silver.lab_managers IS
'Identified lab managers (up to 3 per lab) with confidence scoring and verification tracking. Used for TDX ticket assignment and lab communication.';

COMMENT ON COLUMN silver.lab_managers.manager_confidence_score IS
'Confidence score 1-10 (lower is higher confidence). 1=explicit manager role/code, 5=Research Fellow, 8=Research Scientist, 9=fallback roles, 10=grad students.';

COMMENT ON COLUMN silver.lab_managers.is_verified IS
'Manual verification flag. Set to true after human review confirms manager assignment is correct.';

COMMENT ON COLUMN silver.lab_managers.verification_notes IS
'Human-entered notes for manual overrides or verification context.';


-- ============================================================================
-- PART 3: Scoring Functions
-- ============================================================================

-- Function: Calculate confidence score for a potential lab manager
CREATE OR REPLACE FUNCTION calculate_lab_manager_score(
    p_member_role TEXT,
    p_job_codes JSONB
) RETURNS TABLE(
    confidence_score INTEGER,
    detection_reason TEXT
) AS $$
BEGIN
    -- Priority 1 - Confidence Score 1 (Automatic Managers)
    -- Use OR logic: either role name OR job code qualifies
    IF p_member_role ILIKE '%Lab Manager%' OR p_job_codes::text LIKE '%102945%' THEN
        RETURN QUERY SELECT 1, 'Explicit Lab Manager (role or job code 102945)'::TEXT;
        RETURN;
    END IF;

    IF p_member_role ILIKE '%Lab Coordinator%' OR p_job_codes::text LIKE '%102946%' THEN
        RETURN QUERY SELECT 1, 'Lab Coordinator (role or job code 102946)'::TEXT;
        RETURN;
    END IF;

    IF p_member_role ILIKE '%Laboratory Manager%' OR p_job_codes::text LIKE '%102929%' THEN
        RETURN QUERY SELECT 1, 'Laboratory Manager (role or job code 102929)'::TEXT;
        RETURN;
    END IF;

    -- Priority 2 - Confidence Score 2
    IF p_member_role = 'Admin Coord/Project Coord' THEN
        RETURN QUERY SELECT 2, 'Administrative/Project Coordinator'::TEXT;
        RETURN;
    END IF;

    IF p_member_role ILIKE '%Project Coordinator%' THEN
        RETURN QUERY SELECT 2, 'Project Coordinator (variant)'::TEXT;
        RETURN;
    END IF;

    IF p_member_role ILIKE '%Administrative Coordinator%' THEN
        RETURN QUERY SELECT 2, 'Administrative Coordinator'::TEXT;
        RETURN;
    END IF;

    -- Priority 3 - Confidence Score 3
    IF p_member_role = 'Research Lab Specialist Lead' OR p_job_codes::text LIKE '%102909%' THEN
        RETURN QUERY SELECT 3, 'Research Lab Specialist Lead (role or job code 102909)'::TEXT;
        RETURN;
    END IF;

    -- Priority 4 - Confidence Score 4
    IF p_member_role ILIKE '%Research Lab Specialist Lead%' THEN
        RETURN QUERY SELECT 4, 'Research Lab Specialist Lead (variant)'::TEXT;
        RETURN;
    END IF;

    -- Priority 5 - Confidence Score 5 (Research Fellow moved up per request)
    IF p_member_role ILIKE 'Research Fellow%' THEN
        RETURN QUERY SELECT 5, 'Research Fellow'::TEXT;
        RETURN;
    END IF;

    -- Priority 6 - Confidence Score 6
    IF p_member_role ILIKE '%Tech%Sr%' OR p_job_codes::text LIKE '%102944%' THEN
        RETURN QUERY SELECT 6, 'Senior Technician (Tech Sr or job code 102944)'::TEXT;
        RETURN;
    END IF;

    -- Priority 7 - Confidence Score 7
    IF p_member_role ILIKE '%Lead%' THEN
        RETURN QUERY SELECT 7, 'Leadership role (contains "Lead")'::TEXT;
        RETURN;
    END IF;

    -- Priority 8 - Confidence Score 8 (Research Scientist - common in labs)
    IF p_member_role ILIKE '%Research Scientist%' THEN
        RETURN QUERY SELECT 8, 'Research Scientist'::TEXT;
        RETURN;
    END IF;

    -- Priority 9 - Confidence Score 9 (Fallback roles for labs without clear managers)
    IF p_member_role = 'Graduate Student Instructor and Graduate Student Research Assistant'
       OR p_member_role = 'Graduate Student Research Assistant and Graduate Student Instructor' THEN
        RETURN QUERY SELECT 9, 'Graduate Student (dual GSI/GSRA)'::TEXT;
        RETURN;
    END IF;

    IF p_member_role ILIKE '%Research Lab Specialist Senior%' THEN
        RETURN QUERY SELECT 9, 'Research Lab Specialist Senior'::TEXT;
        RETURN;
    END IF;

    -- Priority 10 - Confidence Score 10 (Last resort - any grad student)
    IF p_member_role ILIKE '%Graduate Student Instructor%'
       OR p_member_role ILIKE '%Graduate Student Research Assistant%' THEN
        RETURN QUERY SELECT 10, 'Graduate Student (GSI or GSRA)'::TEXT;
        RETURN;
    END IF;

    -- No match - return NULL to exclude
    RETURN QUERY SELECT NULL::INTEGER, NULL::TEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION calculate_lab_manager_score IS
'Calculates confidence score (1-10) for potential lab managers based on role name and job codes. Lower scores indicate higher confidence. Returns NULL for non-qualifying members.';


-- Function: Populate lab_managers table with identified managers
CREATE OR REPLACE FUNCTION populate_lab_managers(
    p_lab_id VARCHAR DEFAULT NULL  -- NULL = all labs, specific lab_id for incremental
) RETURNS TABLE(
    labs_processed INTEGER,
    managers_identified INTEGER,
    managers_added INTEGER,
    managers_removed INTEGER
) AS $$
DECLARE
    v_labs_processed INTEGER := 0;
    v_managers_identified INTEGER := 0;
    v_managers_added INTEGER := 0;
    v_managers_removed INTEGER := 0;
    v_batch_inserted INTEGER := 0;
    v_lab RECORD;
    v_member_count INTEGER;
    v_pi_count INTEGER;
    v_small_lab_threshold INTEGER;
BEGIN
    -- If specific lab_id provided, delete existing managers for that lab
    -- Otherwise, clear all (full recalculation)
    IF p_lab_id IS NOT NULL THEN
        DELETE FROM silver.lab_managers WHERE lab_id = p_lab_id;
        GET DIAGNOSTICS v_managers_removed = ROW_COUNT;
    ELSE
        DELETE FROM silver.lab_managers;
        GET DIAGNOSTICS v_managers_removed = ROW_COUNT;
    END IF;

    -- Process each legitimate lab
    FOR v_lab IN
        SELECT lab_id, member_count
        FROM silver.v_legitimate_labs
        WHERE (p_lab_id IS NULL OR lab_id = p_lab_id)
    LOOP
        v_labs_processed := v_labs_processed + 1;

        -- Count PIs for this lab
        SELECT COUNT(*) INTO v_pi_count
        FROM silver.lab_members
        WHERE lab_id = v_lab.lab_id AND is_pi = true;

        -- Check for small lab case: (member_count - PI_count) <= 3
        v_small_lab_threshold := v_lab.member_count - v_pi_count;

        -- Score and insert managers
        WITH scored_members AS (
            SELECT
                elm.lab_id,
                elm.member_uniqname,
                elm.tdx_user_uid,
                elm.member_role,
                elm.job_codes,
                score.confidence_score,
                score.detection_reason,
                ROW_NUMBER() OVER (
                    PARTITION BY elm.lab_id
                    ORDER BY score.confidence_score ASC NULLS LAST, elm.member_role
                ) as rank
            FROM silver.v_eligible_lab_members elm
            CROSS JOIN LATERAL calculate_lab_manager_score(elm.member_role, elm.job_codes) score
            WHERE elm.lab_id = v_lab.lab_id
                AND score.confidence_score IS NOT NULL
        ),
        -- Special case: small labs get all eligible members as managers
        small_lab_managers AS (
            SELECT
                elm.lab_id,
                elm.member_uniqname,
                elm.tdx_user_uid,
                elm.member_role,
                elm.job_codes,
                9 as confidence_score,
                'Small lab: all eligible members assigned'::TEXT as detection_reason,
                ROW_NUMBER() OVER (ORDER BY elm.member_role) as rank
            FROM silver.v_eligible_lab_members elm
            WHERE elm.lab_id = v_lab.lab_id
                AND v_small_lab_threshold <= 3
        ),
        -- Check if any high-priority (score 1-4) managers exist
        has_high_priority AS (
            SELECT EXISTS(
                SELECT 1 FROM scored_members
                WHERE lab_id = v_lab.lab_id AND confidence_score <= 4
            ) as has_managers
        ),
        -- Final selection logic
        final_managers AS (
            -- If small lab, use all eligible members (up to 3)
            SELECT * FROM small_lab_managers WHERE v_small_lab_threshold <= 3 AND rank <= 3

            UNION ALL

            -- Otherwise, use scored members
            -- Include all scores, but only use lower-priority (5-10) if no high-priority managers exist
            SELECT * FROM scored_members
            WHERE v_small_lab_threshold > 3
                AND rank <= 3
                AND (
                    confidence_score <= 4  -- Always include high-priority managers (scores 1-4)
                    OR (confidence_score >= 5 AND NOT (SELECT has_managers FROM has_high_priority))  -- Include lower-priority only if no high-priority exists
                )
        )
        INSERT INTO silver.lab_managers (
            lab_id,
            manager_uniqname,
            manager_tdx_uid,
            manager_role,
            manager_job_codes,
            manager_confidence_score,
            manager_rank,
            detection_reason
        )
        SELECT
            lab_id,
            member_uniqname,
            tdx_user_uid,
            member_role,
            job_codes,
            confidence_score,
            rank::INTEGER,
            detection_reason
        FROM final_managers;

        -- Accumulate the count of inserted rows
        GET DIAGNOSTICS v_batch_inserted = ROW_COUNT;
        v_managers_added := v_managers_added + v_batch_inserted;
    END LOOP;

    v_managers_identified := v_managers_added;

    RETURN QUERY SELECT v_labs_processed, v_managers_identified, v_managers_added, v_managers_removed;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION populate_lab_managers IS
'Populates silver.lab_managers table with identified managers. Can run for all labs (NULL) or incrementally for specific lab_id. Returns processing statistics.';


-- ============================================================================
-- PART 4: Initial Population (commented out - run manually after validation)
-- ============================================================================

-- To populate lab managers, run:
-- SELECT * FROM populate_lab_managers(NULL);

-- To update a specific lab:
-- SELECT * FROM populate_lab_managers('csmonk');
