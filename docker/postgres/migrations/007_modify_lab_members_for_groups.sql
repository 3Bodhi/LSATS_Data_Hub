-- Migration 007: Modify silver.lab_members for group-based membership
--
-- Purpose: Modify lab_members table to support group-based membership as primary source
-- with award data as enrichment. Changes is_pi from computed column to regular boolean,
-- adds investigator tracking, and adjusts constraints.
--
-- Author: AI Enhancement Plan V2
-- Date: 2025-01-19

BEGIN;

-- 1. Drop the view that depends on is_pi column
DROP VIEW IF EXISTS silver.v_lab_members_detailed CASCADE;

-- 2. Drop the computed is_pi column
ALTER TABLE silver.lab_members
DROP COLUMN IF EXISTS is_pi;

-- 3. Add is_pi as a regular boolean column
ALTER TABLE silver.lab_members
ADD COLUMN is_pi BOOLEAN DEFAULT false NOT NULL;

-- 4. Add is_investigator column
ALTER TABLE silver.lab_members
ADD COLUMN is_investigator BOOLEAN DEFAULT false NOT NULL;

-- 5. Add award_role column (separate from member_role which is job_title)
ALTER TABLE silver.lab_members
ADD COLUMN award_role VARCHAR(100);

-- 6. Add source_group_ids column to track which groups member belongs to
ALTER TABLE silver.lab_members
ADD COLUMN source_group_ids JSONB DEFAULT '[]'::jsonb;

-- 7. Drop the old unique constraint (lab_id, member_uniqname, member_role)
ALTER TABLE silver.lab_members
DROP CONSTRAINT IF EXISTS idx_lab_members_unique;

-- 8. Create new unique constraint (lab_id, member_uniqname) - one record per person per lab
CREATE UNIQUE INDEX idx_lab_members_unique_v2
ON silver.lab_members (lab_id, member_uniqname);

-- 9. Add index for investigator queries
CREATE INDEX idx_lab_members_investigator
ON silver.lab_members (lab_id, is_investigator)
WHERE is_investigator = true;

-- 10. Add index for award_role
CREATE INDEX idx_lab_members_award_role
ON silver.lab_members (award_role)
WHERE award_role IS NOT NULL;

-- 11. Add index for source_group_ids
CREATE INDEX idx_lab_members_source_groups_gin
ON silver.lab_members USING gin (source_group_ids);

-- 12. Drop foreign key to users (many group members don't exist in silver.users)
ALTER TABLE silver.lab_members
DROP CONSTRAINT IF EXISTS fk_lab_members_user;

-- 13. Update column comments
COMMENT ON COLUMN silver.lab_members.is_pi IS 'PI status: true if uniqname=lab_id OR award_role contains Principal Investigator';
COMMENT ON COLUMN silver.lab_members.is_investigator IS 'Investigator status: true if award_role contains Investigator';
COMMENT ON COLUMN silver.lab_members.award_role IS 'Role from award data (if person appears in lab_award records). Examples: UM Principal Investigator, Participating Investigator';
COMMENT ON COLUMN silver.lab_members.member_role IS 'Primary role derived from silver.users.job_title. Examples: Graduate Student, Research Fellow, Professor';
COMMENT ON COLUMN silver.lab_members.source_group_ids IS 'Array of group_ids where this person is a member (from silver.groups)';
COMMENT ON COLUMN silver.lab_members.source_award_ids IS 'Array of Award IDs where this person appears (from bronze lab_award data)';

-- 14. Update table comment
COMMENT ON TABLE silver.lab_members IS 'Lab membership records from group membership (primary source) enriched with award data. One record per unique person per lab. Member role comes from job_title, award_role is separate.';

-- 15. Recreate the view with new schema
CREATE VIEW silver.v_lab_members_detailed AS
SELECT
    lm.lab_id,
    l.lab_name,
    lm.member_uniqname,
    lm.member_full_name,
    lm.member_role,
    lm.award_role,
    lm.is_pi,
    lm.is_investigator,
    u.job_title,
    u.department_job_titles,
    u.job_codes,
    lm.member_department_name,
    lm.silver_user_exists
FROM silver.lab_members lm
JOIN silver.labs l ON lm.lab_id = l.lab_id
LEFT JOIN silver.users u ON lm.member_uniqname = u.uniqname;

COMMENT ON VIEW silver.v_lab_members_detailed IS 'Detailed lab membership view showing all members with their roles, job information, and investigator status';

COMMIT;

-- Verification queries
SELECT
    'Migration 007 completed' as status,
    COUNT(*) as current_member_count
FROM silver.lab_members;

-- Show new schema
\d silver.lab_members
