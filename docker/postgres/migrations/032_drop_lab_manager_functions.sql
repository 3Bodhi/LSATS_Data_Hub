-- Migration 032: Drop Lab Manager Database Functions
-- Description: Removes database functions replaced by Python service
-- Author: LSATS Data Hub Team
-- Date: 2025-11-24
-- Related: Phase 4 of lab modernization plan

-- ============================================================================
-- IMPORTANT: Run Python transformation BEFORE applying this migration
-- ============================================================================
--
-- 1. Run the Python transformation to populate silver.lab_managers:
--    python scripts/database/silver/014_transform_lab_managers.py
--
-- 2. Verify the results:
--    SELECT COUNT(*) FROM silver.lab_managers;
--    Expected: ~500-600 manager assignments across ~220-250 labs
--
-- 3. Then apply this migration to drop the old database functions
--
-- ============================================================================

-- Drop the population function (orchestration logic)
DROP FUNCTION IF EXISTS populate_lab_managers(VARCHAR);

-- Drop the scoring function (rule evaluation logic)
DROP FUNCTION IF EXISTS calculate_lab_manager_score(TEXT, JSONB);

-- Update table comment to reflect new Python-based approach
COMMENT ON TABLE silver.lab_managers IS
'Identified lab managers (up to 3 per lab) with confidence scoring and verification tracking.

Populated by Python service:
- Service: services/lab_manager_identification_service.py
- Transformation: scripts/database/silver/014_transform_lab_managers.py
- Documentation: docs/LAB_MANAGERS_README.md

The Python service replaces the previous database function approach,
providing better testability, maintainability, and observability while
maintaining 100% scoring logic fidelity.

Last migration: 032_drop_lab_manager_functions.sql';

-- Verify the functions are dropped
DO $$
BEGIN
    -- Check if functions still exist
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
        AND p.proname IN ('populate_lab_managers', 'calculate_lab_manager_score')
    ) THEN
        RAISE EXCEPTION 'Lab manager functions still exist after drop attempt';
    END IF;

    RAISE NOTICE '✅ Lab manager database functions successfully dropped';
    RAISE NOTICE '   Scoring logic now handled by: services/lab_manager_identification_service.py';
    RAISE NOTICE '   Transformation script: scripts/database/silver/014_transform_lab_managers.py';
END $$;
