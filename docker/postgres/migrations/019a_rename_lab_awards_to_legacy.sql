-- Migration 019a: Rename silver.lab_awards to silver.lab_awards_legacy
-- Purpose: Preserve existing lab awards table while creating new source-specific table
-- Date: 2025-11-22

BEGIN;

-- Step 1: Rename main table
ALTER TABLE silver.lab_awards RENAME TO lab_awards_legacy;

-- Step 2: Rename primary key constraint
ALTER TABLE silver.lab_awards_legacy 
    RENAME CONSTRAINT lab_awards_pkey TO lab_awards_legacy_pkey;

-- Step 3: Rename foreign key constraints (only if they exist)
ALTER TABLE silver.lab_awards_legacy
    RENAME CONSTRAINT lab_awards_lab_id_fkey TO lab_awards_legacy_lab_id_fkey;
    
ALTER TABLE silver.lab_awards_legacy
    RENAME CONSTRAINT fk_lab_awards_department TO fk_lab_awards_legacy_department;

-- Step 4: Rename indexes
ALTER INDEX silver.idx_lab_awards_unique RENAME TO idx_lab_awards_legacy_unique;
ALTER INDEX silver.idx_lab_awards_lab RENAME TO idx_lab_awards_legacy_lab;
ALTER INDEX silver.idx_lab_awards_award_id RENAME TO idx_lab_awards_legacy_award_id;
ALTER INDEX silver.idx_lab_awards_person RENAME TO idx_lab_awards_legacy_person;
ALTER INDEX silver.idx_lab_awards_active RENAME TO idx_lab_awards_legacy_active;
ALTER INDEX silver.idx_lab_awards_dates RENAME TO idx_lab_awards_legacy_dates;
ALTER INDEX silver.idx_lab_awards_end_date RENAME TO idx_lab_awards_legacy_end_date;
ALTER INDEX silver.idx_lab_awards_dollars RENAME TO idx_lab_awards_legacy_dollars;
ALTER INDEX silver.idx_lab_awards_direct_dollars RENAME TO idx_lab_awards_legacy_direct_dollars;
ALTER INDEX silver.idx_lab_awards_direct_sponsor RENAME TO idx_lab_awards_legacy_direct_sponsor;
ALTER INDEX silver.idx_lab_awards_prime_sponsor RENAME TO idx_lab_awards_legacy_prime_sponsor;
ALTER INDEX silver.idx_lab_awards_award_class RENAME TO idx_lab_awards_legacy_award_class;
ALTER INDEX silver.idx_lab_awards_person_dept RENAME TO idx_lab_awards_legacy_person_dept;
ALTER INDEX silver.idx_lab_awards_admin_dept RENAME TO idx_lab_awards_legacy_admin_dept;
ALTER INDEX silver.idx_lab_awards_person_role RENAME TO idx_lab_awards_legacy_person_role;
ALTER INDEX silver.idx_lab_awards_bronze RENAME TO idx_lab_awards_legacy_bronze;
ALTER INDEX silver.idx_lab_awards_source_file RENAME TO idx_lab_awards_legacy_source_file;
ALTER INDEX silver.idx_lab_awards_content_hash RENAME TO idx_lab_awards_legacy_content_hash;

-- Step 5: Rename dependent view
DROP VIEW IF EXISTS silver.v_lab_active_awards;

CREATE OR REPLACE VIEW silver.v_lab_active_awards_legacy AS
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
INNER JOIN silver.lab_awards_legacy la ON l.lab_id = la.lab_id
WHERE la.is_active = true
ORDER BY l.lab_id, la.award_end_date DESC;

COMMENT ON TABLE silver.lab_awards_legacy IS 
    'LEGACY TABLE - Preserved for backward compatibility. Use silver.lab_awards (source-specific) going forward. Will be deprecated once new consolidated lab tables are stable.';

COMMENT ON VIEW silver.v_lab_active_awards_legacy IS 
    'LEGACY VIEW - Active awards from legacy lab_awards table. Will be deprecated.';

COMMIT;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Renamed silver.lab_awards to silver.lab_awards_legacy';
    RAISE NOTICE '📝 Next: Update transform_silver_labs.py to reference lab_awards_legacy';
    RAISE NOTICE '📊 Record count: %', (SELECT COUNT(*) FROM silver.lab_awards_legacy);
END $$;
