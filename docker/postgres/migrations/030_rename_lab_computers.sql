-- Migration 030: Rename computer_labs → lab_computers with enhanced scoring
-- Date: 2025-01-24
-- Phase: 6 of Lab Modernization Plan
-- Purpose: Refactor lab-computer associations with additive multi-criteria scoring

-- Key Changes:
--   1. Rename table to match naming convention (lab_computers vs computer_labs)
--   2. Add 5 boolean fields to track scoring criteria
--   3. Support additive scoring model (multiple positive indicators)
--   4. Enable function-based scoring (Research positive, Admin/Dev negative)

BEGIN;

-- ============================================================================
-- STEP 1: Rename Table
-- ============================================================================

ALTER TABLE silver.computer_labs RENAME TO lab_computers;

-- Note: Indexes, constraints, and triggers automatically renamed by PostgreSQL
-- Expected renames:
--   - computer_labs_pkey → lab_computers_pkey
--   - idx_computer_labs_* → idx_lab_computers_*
--   - computer_labs_lab_id_fkey → lab_computers_lab_id_fkey

-- ============================================================================
-- STEP 2: Add New Scoring Criteria Fields
-- ============================================================================

-- Add boolean fields for additive scoring model
ALTER TABLE silver.lab_computers
ADD COLUMN owner_is_pi BOOLEAN DEFAULT false,
ADD COLUMN fin_owner_is_pi BOOLEAN DEFAULT false,
ADD COLUMN owner_is_member BOOLEAN DEFAULT false,
ADD COLUMN fin_owner_is_member BOOLEAN DEFAULT false,
ADD COLUMN function_is_research BOOLEAN DEFAULT false,
ADD COLUMN function_is_classroom BOOLEAN DEFAULT false;

-- Note: All existing rows will have these set to FALSE (updated by transformation script)

-- ============================================================================
-- STEP 3: Update Column Comments
-- ============================================================================

COMMENT ON TABLE silver.lab_computers IS
'Lab-to-computer associations with multi-criteria confidence scoring. Each computer can be associated with multiple labs, with one marked as primary (highest confidence). Uses additive scoring model where multiple positive indicators increase confidence.';

COMMENT ON COLUMN silver.lab_computers.confidence_score IS
'Confidence score 0.00-1.00 using additive model. Start at 1.0, subtract penalties for missing criteria. Multiple positive indicators increase confidence (e.g., PI owner + Research function). Typical scores: all criteria (1.0), PI owner + Research (0.85), member owner (0.55), no criteria (0.30). Computers with no discovery criteria are filtered out.';

COMMENT ON COLUMN silver.lab_computers.owner_is_pi IS
'TRUE if computer.owner_uniqname = lab.pi_uniqname. Strong positive indicator (prevents -0.15 penalty).';

COMMENT ON COLUMN silver.lab_computers.fin_owner_is_pi IS
'TRUE if computer.financial_owner_uniqname = lab.pi_uniqname. Good positive indicator (prevents -0.10 penalty).';

COMMENT ON COLUMN silver.lab_computers.owner_is_member IS
'TRUE if computer.owner_uniqname is in lab_members but not the PI. Moderate positive indicator (prevents -0.20 penalty).';

COMMENT ON COLUMN silver.lab_computers.fin_owner_is_member IS
'TRUE if computer.financial_owner_uniqname is in lab_members but not the PI. Moderate positive indicator (prevents -0.15 penalty).';

COMMENT ON COLUMN silver.lab_computers.function_is_research IS
'TRUE if TDX Function attribute = "Research" (ID 27316). Strong positive indicator (prevents -0.10 penalty). Admin/Staff and Dev/Testing functions apply -0.20 penalty.';

COMMENT ON COLUMN silver.lab_computers.function_is_classroom IS
'TRUE if TDX Function attribute = "Classroom/Computer Lab" (ID 27312). Minor positive indicator (+0.05 bonus, sometimes used for research labs).';

COMMENT ON COLUMN silver.lab_computers.association_method IS
'Primary discovery method that initially identified this association: ad_ou_nested (computer in lab OU), owner_is_pi (owner = PI), group_membership (computer in lab groups), owner_member (owner in lab), last_user_member (last user in lab). Kept for backward compatibility and debugging.';

COMMENT ON COLUMN silver.lab_computers.is_primary IS
'TRUE for the single highest-confidence lab association per computer. Used for quick lookup of primary lab assignment. Null/FALSE for secondary associations.';

-- ============================================================================
-- STEP 4: Create Indexes for New Columns
-- ============================================================================

-- Performance indexes for filtering high-confidence associations
CREATE INDEX idx_lab_computers_owner_pi
ON silver.lab_computers(owner_is_pi)
WHERE owner_is_pi = true;

CREATE INDEX idx_lab_computers_fin_owner_pi
ON silver.lab_computers(fin_owner_is_pi)
WHERE fin_owner_is_pi = true;

CREATE INDEX idx_lab_computers_function_research
ON silver.lab_computers(function_is_research)
WHERE function_is_research = true;

CREATE INDEX idx_lab_computers_function_classroom
ON silver.lab_computers(function_is_classroom)
WHERE function_is_classroom = true;

-- Composite index for multi-criteria queries
CREATE INDEX idx_lab_computers_criteria_composite
ON silver.lab_computers(lab_id, owner_is_pi, fin_owner_is_pi, function_is_research)
WHERE owner_is_pi = true OR fin_owner_is_pi = true OR function_is_research = true;

-- ============================================================================
-- STEP 5: Update Constraint Check (if needed)
-- ============================================================================

-- Verify association_method constraint exists (should have been renamed automatically)
-- If not, recreate it:
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'check_association_method'
        AND conrelid = 'silver.lab_computers'::regclass
    ) THEN
        ALTER TABLE silver.lab_computers
        ADD CONSTRAINT check_association_method CHECK (
            association_method IN (
                'ad_ou_nested',
                'owner_is_pi',
                'group_membership',
                'owner_member',
                'last_user_member'
            )
        );
    END IF;
END $$;

-- ============================================================================
-- STEP 6: Verify Foreign Key Constraints
-- ============================================================================

-- Should have been automatically renamed:
--   computer_labs_lab_id_fkey → lab_computers_lab_id_fkey

-- Verify it exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'lab_computers_lab_id_fkey'
        AND conrelid = 'silver.lab_computers'::regclass
    ) THEN
        -- Recreate if missing
        ALTER TABLE silver.lab_computers
        ADD CONSTRAINT lab_computers_lab_id_fkey
        FOREIGN KEY (lab_id)
        REFERENCES silver.labs(lab_id)
        ON DELETE CASCADE;
    END IF;
END $$;

-- ============================================================================
-- STEP 7: Update Statistics
-- ============================================================================

-- Analyze table for query planner
ANALYZE silver.lab_computers;

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Run these manually after migration to verify success:

-- 1. Check table exists with new name
-- SELECT COUNT(*) FROM silver.lab_computers;

-- 2. Check new columns exist
-- \d silver.lab_computers

-- 3. Check indexes were renamed/created
-- \di silver.lab_computers*

-- 4. Check constraints
-- SELECT conname, contype FROM pg_constraint
-- WHERE conrelid = 'silver.lab_computers'::regclass;

COMMIT;

-- ============================================================================
-- Migration Complete
-- ============================================================================

-- Summary:
-- ✓ Renamed computer_labs → lab_computers
-- ✓ Added 6 boolean criteria fields (owner_is_pi, fin_owner_is_pi, etc.)
-- ✓ Created 5 new indexes for criteria filtering
-- ✓ Updated table and column comments
-- ✓ Verified constraints and foreign keys

-- Next Step: Run transformation script (014_transform_lab_computers.py) to:
--   1. Recalculate all associations with new scoring
--   2. Populate new boolean fields
--   3. Filter out computers with no discovery criteria
--   4. Apply function-based scoring adjustments
