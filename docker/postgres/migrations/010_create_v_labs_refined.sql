-- Migration: Create v_labs_refined View
-- Purpose: Refined lab view filtering by ou_depth_category for higher-quality lab identification
-- Context: Filters to 'potential_lab' depth category to exclude shallow department OUs
--          and focus on actual lab-level organizational units

CREATE OR REPLACE VIEW silver.v_labs_refined AS
SELECT
    lab_id,
    pi_uniqname,
    lab_name,
    lab_display_name,
    primary_department_id,
    department_ids,
    total_award_dollars,
    total_direct_dollars,
    total_indirect_dollars,
    award_count,
    active_award_count,
    earliest_award_start,
    latest_award_end,
    has_ad_ou,
    ad_ou_dn,
    ad_ou_hierarchy,
    ad_ou_depth,
    ou_depth_category,
    computer_count,
    member_count,
    pi_count,
    investigator_count,
    is_active,
    has_active_awards,
    has_active_ou,
    has_award_data,
    has_ou_data,
    data_source,
    data_quality_score,
    quality_flags,
    created_at,
    updated_at
FROM silver.labs
WHERE ou_depth_category = 'potential_lab'
  AND is_active = true
ORDER BY lab_name;

COMMENT ON VIEW silver.v_labs_refined IS
'Refined lab view showing only high-confidence labs based on OU depth categorization.
Filters to ou_depth_category = ''potential_lab'' (deep OUs likely representing actual labs)
and is_active = true. Excludes shallow department OUs and inactive labs.
Use this view for lab-focused queries and reporting.';
