-- Migration: Fix v_lab_groups Matching Logic
-- Purpose: Replace substring matching with structured prefix matching to eliminate false positives
-- Issue: 'comm' lab matches 1,078 groups (TeamDynamix Community, etc.) instead of ~71-110 actual lab groups
-- Solution: Use ad_sam_account_name with structured LSA prefix patterns

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
        -- Pattern 1: lsa-{pi_uniqname}-* (standard lab prefix)
        WHEN g.ad_sam_account_name LIKE 'lsa-' || l.pi_uniqname || '-%' THEN 'sam_prefix_standard'

        -- Pattern 2: lsa-{dept}-{pi_uniqname}-* (department-prefixed lab groups)
        -- Covers patterns like: lsa-psyc-parl-*, lsa-chem-aabol-*, etc.
        WHEN g.ad_sam_account_name LIKE 'lsa-%' || l.pi_uniqname || '-%' THEN 'sam_prefix_dept'

        -- Pattern 3: lsa-{pi_uniqname} (exact match, no suffix)
        WHEN g.ad_sam_account_name = 'lsa-' || l.pi_uniqname THEN 'sam_exact_match'

        -- Pattern 4: OU-based matching (for groups without AD sync but in MCommunity OU)
        WHEN g.mcommunity_dn ILIKE '%OU=' || l.pi_uniqname || ',%' THEN 'dn_ou_match'

        -- Fallback: Should not occur with new matching logic
        ELSE 'other'
    END AS match_type,
    g.is_ad_synced,
    g.email_address AS group_email
FROM silver.labs l
JOIN silver.groups g ON (
    -- Match using structured AD SAM account name patterns
    g.ad_sam_account_name LIKE 'lsa-' || l.pi_uniqname || '-%'          -- Pattern 1
    OR g.ad_sam_account_name LIKE 'lsa-%' || l.pi_uniqname || '-%'      -- Pattern 2
    OR g.ad_sam_account_name = 'lsa-' || l.pi_uniqname                  -- Pattern 3
    OR g.mcommunity_dn ILIKE '%OU=' || l.pi_uniqname || ',%'            -- Pattern 4
)
WHERE l.is_active = true;

COMMENT ON VIEW silver.v_lab_groups IS
'Lab-to-group relationships using structured prefix matching on ad_sam_account_name.
Eliminates false positives from substring matches (e.g., "comm" matching "Community" groups).
Patterns:
  1. lsa-{pi_uniqname}-* (standard lab prefix)
  2. lsa-{dept}-{pi_uniqname}-* (department-prefixed groups)
  3. lsa-{pi_uniqname} (exact match)
  4. OU={pi_uniqname} in DN (MCommunity OU match)';

-- Create validation query to compare before/after counts
-- (This is a comment for manual execution, not part of the migration)
--
-- SELECT
--     lab_id,
--     pi_uniqname,
--     COUNT(DISTINCT group_id) as group_count,
--     COUNT(DISTINCT CASE WHEN match_type = 'sam_prefix_standard' THEN group_id END) as standard_prefix,
--     COUNT(DISTINCT CASE WHEN match_type = 'sam_prefix_dept' THEN group_id END) as dept_prefix,
--     COUNT(DISTINCT CASE WHEN match_type = 'sam_exact_match' THEN group_id END) as exact_match,
--     COUNT(DISTINCT CASE WHEN match_type = 'dn_ou_match' THEN group_id END) as ou_match
-- FROM silver.v_lab_groups
-- WHERE lab_id IN ('comm', 'parl', 'csmonk', 'kramer', 'gwk')
-- GROUP BY lab_id, pi_uniqname
-- ORDER BY group_count DESC;
