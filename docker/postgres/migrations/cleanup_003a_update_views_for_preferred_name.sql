-- Migration: Update views to use preferred_name instead of display_name
-- Description: Drop and recreate v_lab_managers_detailed to use preferred_name
-- Date: 2026-01-12
-- Status: Phase 1 Cleanup - Pre-requisite for dropping display_name column
-- Dependencies: Must run before cleanup_003_drop_old_consolidated_user_columns.sql

BEGIN;

-- Drop the view that depends on display_name
DROP VIEW IF EXISTS silver.v_lab_managers_detailed CASCADE;

-- Recreate with preferred_name
CREATE OR REPLACE VIEW silver.v_lab_managers_detailed AS
SELECT
    d_manager.department_name AS manager_department_full_name,
    lm.lab_id,
    lm.manager_uniqname,
    u.preferred_name,
    u.job_title,
    d_lab.department_name AS lab_department_full_name,
    lm.manager_tdx_uid,
    lm.detection_reason,
    lm.manager_rank,
    lm.manager_confidence_score,
    lm.is_verified,
    lm.verification_notes
FROM
    silver.lab_managers AS lm
JOIN
    silver.departments AS d_lab ON lm.lab_department_id = d_lab.department_code
JOIN
    silver.departments AS d_manager ON lm.manager_department_id = d_manager.department_code
JOIN
    silver.users AS u ON lm.manager_uniqname = u.uniqname
ORDER BY
    lm.lab_id,
    lm.manager_rank;

COMMENT ON VIEW silver.v_lab_managers_detailed IS 'Detailed view of lab managers with department names and user information - Updated to use preferred_name';

COMMIT;

-- Verify view was created successfully
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.views
        WHERE table_schema = 'silver'
        AND table_name = 'v_lab_managers_detailed'
    ) THEN
        RAISE EXCEPTION 'Failed to create v_lab_managers_detailed view';
    END IF;

    RAISE NOTICE '✅ Successfully updated v_lab_managers_detailed to use preferred_name';
END $$;
