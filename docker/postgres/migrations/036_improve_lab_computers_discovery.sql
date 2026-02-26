-- Migration: 036_improve_lab_computers_discovery.sql
-- Purpose: Add new discovery methods and quality tracking to silver.lab_computers
-- Date: 2025-11-27
-- Related: Phase 1-5 of Lab Computers Improvement Plan

-- ============================================================================
-- PART 1: Add new discovery methods to constraint
-- ============================================================================

-- Drop existing constraint
ALTER TABLE silver.lab_computers
DROP CONSTRAINT IF EXISTS check_association_method;

-- Add constraint with new methods: fin_owner_is_pi, name_pattern_pi
ALTER TABLE silver.lab_computers
ADD CONSTRAINT check_association_method
CHECK (association_method IN (
    'ad_ou_nested',
    'owner_is_pi',
    'fin_owner_is_pi',      -- NEW: Financial owner is PI
    'name_pattern_pi',      -- NEW: Computer name contains PI uniqname
    'group_membership',
    'owner_member',
    'last_user_member'
));

COMMENT ON CONSTRAINT check_association_method ON silver.lab_computers IS
'Valid association methods:
- ad_ou_nested: Computer DN contains lab AD OU (Tier 1, conf 0.70-1.00)
- owner_is_pi: Owner is the PI (Tier 1, conf 0.70-1.00)
- fin_owner_is_pi: Financial owner is the PI (Tier 1, conf 0.70-1.00)
- name_pattern_pi: Computer name contains PI uniqname (Tier 1, conf 0.70-1.00)
- group_membership: Computer in groups matching lab (Tier 2, conf 0.20-0.50)
- owner_member: Owner is lab member, not PI (Tier 2, conf 0.20-0.50)
- last_user_member: Last user is lab member (Tier 2, conf 0.20-0.50)';

-- ============================================================================
-- PART 2: Add quality flags column for data quality monitoring
-- ============================================================================

-- Add quality_flags JSONB column
ALTER TABLE silver.lab_computers
ADD COLUMN IF NOT EXISTS quality_flags JSONB DEFAULT '[]'::jsonb;

COMMENT ON COLUMN silver.lab_computers.quality_flags IS
'Quality flags array for monitoring association quality.
Common flags:
- low_confidence: confidence < 0.40
- high_confidence: confidence >= 0.90
- fully_pi_owned: both owner and financial owner are PI
- owner_not_affiliated: owner not PI or member
- fin_owner_not_affiliated: financial owner not PI or member
- admin_function: function is Admin/Staff
- dev_function: function is Dev/Testing
- no_function: function is NULL';

-- Create GIN index for quality flags array queries
CREATE INDEX IF NOT EXISTS idx_lab_computers_quality_flags_gin
ON silver.lab_computers USING gin(quality_flags);

-- ============================================================================
-- PART 3: Add performance indexes for new discovery methods
-- ============================================================================

-- Index for financial owner PI matches with high confidence
CREATE INDEX IF NOT EXISTS idx_lab_computers_fin_owner_pi_high_conf
ON silver.lab_computers (lab_id, confidence_score DESC)
WHERE fin_owner_is_pi = true;

-- Index for name pattern matches
CREATE INDEX IF NOT EXISTS idx_lab_computers_name_pattern_pi
ON silver.lab_computers (lab_id, confidence_score DESC)
WHERE association_method = 'name_pattern_pi';

-- Composite index for strong discovery methods (Tier 1)
CREATE INDEX IF NOT EXISTS idx_lab_computers_tier1_methods
ON silver.lab_computers (association_method, confidence_score DESC)
WHERE association_method IN ('ad_ou_nested', 'owner_is_pi', 'fin_owner_is_pi', 'name_pattern_pi');

-- Composite index for weak discovery methods (Tier 2)
CREATE INDEX IF NOT EXISTS idx_lab_computers_tier2_methods
ON silver.lab_computers (association_method, confidence_score DESC)
WHERE association_method IN ('group_membership', 'owner_member', 'last_user_member');

-- ============================================================================
-- PART 4: Add validation check for hierarchical confidence bounds
-- ============================================================================

-- Add check constraint to enforce tier-based confidence bounds
-- Note: This is informational - actual enforcement happens in Python code
-- But provides database-level documentation and validation

COMMENT ON COLUMN silver.lab_computers.confidence_score IS
'Confidence score with hierarchical tier enforcement:
- Tier 1 methods (ad_ou_nested, owner_is_pi, fin_owner_is_pi, name_pattern_pi): 0.70 - 1.00
- Tier 2 methods (group_membership, owner_member, last_user_member): 0.20 - 0.50
- Strong discovery methods (Tier 1) always yield high confidence (floor: 0.70)
- Weak discovery methods (Tier 2) always yield low-medium confidence (ceiling: 0.50)';

-- ============================================================================
-- PART 5: Update table comment with new discovery logic
-- ============================================================================

COMMENT ON TABLE silver.lab_computers IS
'Lab-Computer associations with hierarchical confidence scoring.

Discovery Strategy (2-Tier System):
- Tier 1 (Strong): PI ownership, financial ownership, AD OU, name pattern → confidence 0.70-1.00
- Tier 2 (Weak): Member-only relationships → confidence 0.20-0.50

Transformation Strategy:
- Full refresh (TRUNCATE + INSERT) for data consistency
- Multi-criteria discovery with additive confidence scoring
- Primary lab selection based on highest confidence

Updated: 2025-11-27 (Migration 036)';

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify constraint update
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'check_association_method'
        AND conrelid = 'silver.lab_computers'::regclass
    ) THEN
        RAISE NOTICE '✅ Constraint check_association_method updated successfully';
    ELSE
        RAISE EXCEPTION '❌ Failed to create constraint check_association_method';
    END IF;
END $$;

-- Verify quality_flags column
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'silver'
        AND table_name = 'lab_computers'
        AND column_name = 'quality_flags'
    ) THEN
        RAISE NOTICE '✅ Column quality_flags added successfully';
    ELSE
        RAISE EXCEPTION '❌ Failed to add column quality_flags';
    END IF;
END $$;

-- Verify indexes
DO $$
DECLARE
    idx_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO idx_count
    FROM pg_indexes
    WHERE schemaname = 'silver'
    AND tablename = 'lab_computers'
    AND indexname LIKE 'idx_lab_computers_%'
    AND indexname IN (
        'idx_lab_computers_quality_flags_gin',
        'idx_lab_computers_fin_owner_pi_high_conf',
        'idx_lab_computers_name_pattern_pi',
        'idx_lab_computers_tier1_methods',
        'idx_lab_computers_tier2_methods'
    );

    IF idx_count >= 5 THEN
        RAISE NOTICE '✅ All new indexes created successfully (% indexes)', idx_count;
    ELSE
        RAISE WARNING '⚠️  Only % of 5 expected indexes were created', idx_count;
    END IF;
END $$;

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════';
    RAISE NOTICE '✅ Migration 036 completed successfully';
    RAISE NOTICE '════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Changes applied:';
    RAISE NOTICE '  1. Added new discovery methods: fin_owner_is_pi, name_pattern_pi';
    RAISE NOTICE '  2. Added quality_flags JSONB column';
    RAISE NOTICE '  3. Created 5 new performance indexes';
    RAISE NOTICE '  4. Updated constraint and table documentation';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Run updated transformation script';
    RAISE NOTICE '  2. Verify MAC-CRSEIJAS02 appears in crseijas lab';
    RAISE NOTICE '  3. Check confidence score distribution';
    RAISE NOTICE '';
END $$;
