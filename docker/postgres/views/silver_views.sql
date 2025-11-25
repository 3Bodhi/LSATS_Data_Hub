-- ============================================================================
-- LSATS Data Hub - Silver Layer Views
-- ============================================================================
-- 
-- This file contains all silver schema views consolidated from migrations.
-- Views are organized by functional domain for easier maintenance.
--
-- Usage:
--   - Initial setup: Run as part of database initialization
--   - Updates: Can be re-run safely (all views use CREATE OR REPLACE)
--   - Testing: psql -U lsats_user -d lsats_db -f docker/postgres/views/silver_views.sql
--
-- Migration History:
--   - Consolidated from migrations 006, 007, 008, 010, 011, 019a
--   - Also includes views from schemas.sql
--   - Created: 2025-01-24
--
-- ============================================================================

-- ============================================================================
-- LAB-RELATED VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- v_lab_summary
-- Comprehensive summary view of all labs with key metrics
-- Dependencies: silver.labs
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_summary AS
SELECT l.lab_id,
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
LEFT JOIN silver.users_legacy u ON l.pi_uniqname::text = u.uniqname::text
LEFT JOIN silver.departments d ON l.primary_department_id::text = d.dept_id::text;
;

-- ----------------------------------------------------------------------------
-- v_lab_groups
-- Shows lab-to-group associations with group details
-- Dependencies: silver.labs, silver.lab_members, silver.groups_legacy
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_groups AS
SELECT l.lab_id,
l.lab_name,
l.pi_uniqname,
g.group_id,
g.group_name,
g.description,
g.member_count AS group_member_count,
CASE
WHEN g.ad_sam_account_name::text ~~ (('lsa-'::text || l.pi_uniqname::text) || '-%'::text) THEN 'sam_prefix_standard'::text
WHEN g.ad_sam_account_name::text ~~ (('lsa-%'::text || l.pi_uniqname::text) || '-%'::text) THEN 'sam_prefix_dept'::text
WHEN g.ad_sam_account_name::text = ('lsa-'::text || l.pi_uniqname::text) THEN 'sam_exact_match'::text
WHEN g.mcommunity_dn ~~* (('%OU='::text || l.pi_uniqname::text) || ',%'::text) THEN 'dn_ou_match'::text
ELSE 'other'::text
END AS match_type,
g.is_ad_synced,
g.email_address AS group_email
FROM silver.labs l
JOIN silver.groups_legacy g ON g.ad_sam_account_name::text ~~ (('lsa-'::text || l.pi_uniqname::text) || '-%'::text) OR g.ad_sam_account_name::text ~~ (('lsa-%'::text || l.pi_uniqname::text) || '-%'::text) OR g.ad_sam_account_name::text = ('lsa-'::text || l.pi_uniqname::text) OR g.mcommunity_dn ~~* (('%OU='::text || l.pi_uniqname::text) || ',%'::text)
WHERE l.is_active = true;
;

-- ----------------------------------------------------------------------------
-- v_lab_members_detailed
-- Detailed lab membership with user information
-- Dependencies: silver.lab_members, silver.users_legacy
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_members_detailed AS
SELECT lm.lab_id,
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
JOIN silver.labs l ON lm.lab_id::text = l.lab_id::text
LEFT JOIN silver.users_legacy u ON lm.member_uniqname::text = u.uniqname::text;
;

-- ----------------------------------------------------------------------------
-- v_department_labs
-- Department-level aggregation of lab statistics
-- Dependencies: silver.departments, silver.labs
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_department_labs AS
SELECT d.dept_id,
d.department_name,
count(l.lab_id) AS lab_count,
sum(l.total_award_dollars) AS total_department_funding,
sum(l.member_count) AS total_lab_members,
sum(l.computer_count) AS total_lab_computers,
count(l.lab_id) FILTER (WHERE l.is_active) AS active_lab_count,
count(l.lab_id) FILTER (WHERE l.has_award_data) AS labs_with_awards,
count(l.lab_id) FILTER (WHERE l.has_ou_data) AS labs_with_ou
FROM silver.departments d
LEFT JOIN silver.labs l ON d.dept_id::text = l.primary_department_id::text
GROUP BY d.dept_id, d.department_name;
;

-- ----------------------------------------------------------------------------
-- v_labs_monitored
-- Labs that are actively monitored by LSATS (have computers)
-- Dependencies: silver.labs
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_labs_monitored AS
SELECT l.lab_id,
l.lab_name,
l.lab_display_name,
l.pi_uniqname,
l.primary_department_id,
l.department_ids,
l.department_names,
l.member_count,
l.computer_count,
l.ad_ou_dn,
l.ad_ou_hierarchy,
l.ad_parent_ou,
l.ad_ou_depth,
l.has_award_data,
l.has_ad_ou,
l.is_active,
l.source_system,
l.data_quality_score,
l.quality_flags,
l.total_award_dollars,
l.active_award_count,
l.created_at,
l.updated_at
FROM silver.labs l
WHERE l.ad_ou_dn IS NOT NULL AND l.primary_department_id IS NOT NULL AND l.computer_count > 0
ORDER BY l.member_count DESC;
;

-- ----------------------------------------------------------------------------
-- v_labs_refined
-- Refined lab list with quality filters
-- Dependencies: silver.labs
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_labs_refined AS
SELECT labs.lab_id,
labs.pi_uniqname,
labs.lab_name,
labs.lab_display_name,
labs.primary_department_id,
labs.department_ids,
labs.total_award_dollars,
labs.total_direct_dollars,
labs.total_indirect_dollars,
labs.award_count,
labs.active_award_count,
labs.earliest_award_start,
labs.latest_award_end,
labs.has_ad_ou,
labs.ad_ou_dn,
labs.ad_ou_hierarchy,
labs.ad_ou_depth,
labs.ou_depth_category,
labs.computer_count,
labs.member_count,
labs.pi_count,
labs.investigator_count,
labs.is_active,
labs.has_active_awards,
labs.has_active_ou,
labs.has_award_data,
labs.has_ou_data,
labs.data_source,
labs.data_quality_score,
labs.quality_flags,
labs.created_at,
labs.updated_at
FROM silver.labs
WHERE labs.ou_depth_category::text = 'potential_lab'::text AND labs.is_active = true
ORDER BY labs.lab_name;
;

-- ----------------------------------------------------------------------------
-- v_lab_active_awards_legacy
-- Active awards associated with labs (legacy table)
-- Dependencies: silver.labs, silver.lab_awards_legacy
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_active_awards_legacy AS
SELECT l.lab_id,
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
JOIN silver.lab_awards_legacy la ON l.lab_id::text = la.lab_id::text
WHERE la.is_active = true
ORDER BY l.lab_id, la.award_end_date DESC;
;

-- ============================================================================
-- LAB MANAGER IDENTIFICATION VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- v_legitimate_labs
-- Labs eligible for manager identification (active, have computers, TDX-enabled)
-- Dependencies: silver.labs, silver.departments
-- Used by: Lab manager transformation scripts
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_legitimate_labs AS
SELECT l.lab_id,
l.lab_name,
l.pi_uniqname,
l.member_count,
l.computer_count,
l.primary_department_id,
d.tdx_id AS department_tdx_id
FROM silver.labs l
JOIN silver.departments d ON l.primary_department_id::text = d.dept_id::text
WHERE l.is_active = true AND l.computer_count > 0 AND d.tdx_id IS NOT NULL;
;

-- ----------------------------------------------------------------------------
-- v_eligible_lab_members
-- Lab members eligible for manager identification (excludes PIs, professors, support staff)
-- Dependencies: silver.lab_members, silver.users_legacy
-- Used by: Lab manager transformation scripts
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_eligible_lab_members AS
SELECT lm.membership_id,
lm.lab_id,
lm.member_uniqname,
lm.member_role,
lm.member_job_title,
lm.is_pi,
lm.is_investigator,
u.job_codes,
u.tdx_user_uid
FROM silver.lab_members lm
JOIN silver.users_legacy u ON lm.member_uniqname::text = u.uniqname::text
WHERE lm.member_role IS NOT NULL AND (lm.member_department_id::text <> ALL (ARRAY['171240'::character varying, '481477'::character varying, '171210'::character varying, '171220'::character varying, '171245'::character varying, '171230'::character varying, '481207'::character varying, '309980'::character varying, '309982'::character varying, '309981'::character varying, '315834'::character varying, '231640'::character varying, '211600'::character varying, '481450'::character varying, '676785'::character varying, '309919'::character varying, '309921'::character varying, '380002'::character varying]::text[])) AND lm.member_role !~~* '%Chief Administrator%'::text AND lm.member_role !~~* '%Professor%'::text AND lm.is_pi = false;
;

-- ============================================================================
-- END OF FILE
-- ============================================================================
