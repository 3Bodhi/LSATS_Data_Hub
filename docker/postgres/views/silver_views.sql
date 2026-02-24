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
LEFT JOIN silver.users u ON l.pi_uniqname::text = u.uniqname::text
LEFT JOIN silver.departments d ON l.primary_department_id::text = d.dept_id::text;
;

-- ----------------------------------------------------------------------------
-- v_lab_groups
-- Shows lab-to-group associations with group details
-- Dependencies: silver.labs, silver.lab_members, silver.groups
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_groups AS
SELECT l.lab_id,
l.lab_name,
l.pi_uniqname,
g.group_id,
g.group_name,
g.description,
CASE
WHEN g.sam_account_name::text ~~ (('lsa-'::text || l.pi_uniqname::text) || '-%'::text) THEN 'sam_prefix_standard'::text
WHEN g.sam_account_name::text ~~ (('lsa-%'::text || l.pi_uniqname::text) || '-%'::text) THEN 'sam_prefix_dept'::text
WHEN g.sam_account_name::text = ('lsa-'::text || l.pi_uniqname::text) THEN 'sam_exact_match'::text
WHEN g.distinguished_name ~~* (('%OU='::text || l.pi_uniqname::text) || ',%'::text) THEN 'dn_ou_match'::text
ELSE 'other'::text
END AS match_type,
g.is_mcomm_adsync,
g.group_email
FROM silver.labs l
JOIN silver.groups g ON
    g.sam_account_name::text ~~ (('lsa-'::text || l.pi_uniqname::text) || '-%'::text)
    OR g.sam_account_name::text ~~ (('lsa-%'::text || l.pi_uniqname::text) || '-%'::text)
    OR g.sam_account_name::text = ('lsa-'::text || l.pi_uniqname::text)
    OR g.distinguished_name ~~* (('%OU='::text || l.pi_uniqname::text) || ',%'::text)
WHERE l.is_active = true;
;

-- ----------------------------------------------------------------------------
-- v_lab_members_detailed
-- Detailed lab membership with user information
-- Dependencies: silver.lab_members, silver.users
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
u.job_codes,
lm.member_department_name,
lm.silver_user_exists
FROM silver.lab_members lm
JOIN silver.labs l ON lm.lab_id::text = l.lab_id::text
LEFT JOIN silver.users u ON lm.member_uniqname::text = u.uniqname::text;
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
l.tdx_ci_id,
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
-- v_lab_active_awards
-- Active awards associated with labs, joined via PI uniqname
-- Dependencies: silver.labs, silver.lab_awards
-- Note: lab_awards is per-person; joined to labs via pi_uniqname.
--       is_active derived from award dates.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_active_awards AS
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
JOIN silver.lab_awards la ON l.pi_uniqname::text = la.person_uniqname::text
WHERE la.award_end_date >= CURRENT_DATE
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
-- v_lab_members_all_tdx_reference
-- All lab members with TDX UIDs for ticket search operations
-- Dependencies: silver.lab_members, silver.users
-- Purpose: Provides RequestorUIDs for searching lab-related tickets in TDX
-- Note: Includes PIs and all members, but excludes non-PI professors and
--       chief administrators from excluded departments
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_members_all_tdx_reference AS
SELECT
    lm.membership_id,
    lm.lab_id,
    lm.member_uniqname,
    lm.member_role,
    lm.member_job_title,
    lm.is_pi,
    lm.is_investigator,
    u.job_codes,
    u.tdx_user_uid,
    l.tdx_ci_id

FROM silver.lab_members AS lm
JOIN silver.users AS u ON lm.member_uniqname = u.uniqname
JOIN silver.labs AS l ON lm.lab_id = l.lab_id
WHERE
    lm.member_role IS NOT NULL
    AND (lm.member_department_id::text <> ALL (ARRAY[
        '171240'::character varying, '481477'::character varying,
        '171210'::character varying, '171220'::character varying,
        '171245'::character varying, '171230'::character varying,
        '481207'::character varying, '309980'::character varying,
        '309982'::character varying, '309981'::character varying,
        '315834'::character varying, '231640'::character varying,
        '211600'::character varying, '481450'::character varying,
        '676785'::character varying, '309919'::character varying,
        '309921'::character varying, '380002'::character varying
    ]::text[]))
    AND lm.member_role !~~* '%Chief Administrator%'::text
    AND (
        -- Include all PIs regardless of title
        lm.is_pi = true
        OR
        -- Include non-professors
        u.job_title NOT ILIKE '%professor%'
    );
;

-- ----------------------------------------------------------------------------
-- v_eligible_lab_members
-- Lab members eligible for manager identification (excludes PIs, professors, support staff)
-- Dependencies: silver.lab_members, silver.users
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
JOIN silver.users u ON lm.member_uniqname::text = u.uniqname::text
WHERE lm.member_role IS NOT NULL AND (lm.member_department_id::text <> ALL (ARRAY['171240'::character varying, '481477'::character varying, '171210'::character varying, '171220'::character varying, '171245'::character varying, '171230'::character varying, '481207'::character varying, '309980'::character varying, '309982'::character varying, '309981'::character varying, '315834'::character varying, '231640'::character varying, '211600'::character varying, '481450'::character varying, '676785'::character varying, '309919'::character varying, '309921'::character varying, '380002'::character varying]::text[])) AND lm.member_role !~~* '%Chief Administrator%'::text AND lm.member_role !~~* '%Professor%'::text AND lm.is_pi = false;
;

-- ----------------------------------------------------------------------------
-- v_lab_managers_detailed
-- Human-readable view of lab managers with full context
-- Dependencies: silver.lab_managers, silver.departments, silver.users
-- Purpose: Provides both human-useful and machine-useful information for lab managers
-- ----------------------------------------------------------------------------
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
    lm.lab_id ASC, lm.manager_rank ASC;
;

-- ----------------------------------------------------------------------------
-- v_lab_managers_tdx_reference
-- TDX UIDs and department IDs for writing lab information to TeamDynamix
-- Dependencies: silver.lab_managers, silver.departments, silver.users
-- Purpose: Machine-optimized view with all TDX identifiers needed for API operations
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_managers_tdx_reference AS
SELECT
    lm.lab_id,
    u.tdx_user_uid AS pi_tdx_uid,
    lm.manager_tdx_uid,
    d_lab.tdx_id AS lab_department_tdx_id,
    d_manager.tdx_id AS manager_department_tdx_id
FROM
    silver.lab_managers AS lm
JOIN
    silver.departments AS d_lab ON lm.lab_department_id = d_lab.department_code
JOIN
    silver.departments AS d_manager ON lm.manager_department_id = d_manager.department_code
JOIN
    silver.users AS u ON lm.lab_id = u.uniqname
ORDER BY
    lm.lab_id ASC;
;

-- ============================================================================
-- LAB COMPUTER LOCATION VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- v_lab_locations_detailed
-- Human-readable view of computer counts by lab and location
-- Dependencies: silver.computers, silver.lab_computers, silver.users
-- Purpose: Shows where each lab's computers are physically located
-- Note: Filters by confidence_score >= 0.65 to exclude low-confidence matches
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_locations_detailed AS
SELECT
    u.department_name,
    lc.lab_id,
    CONCAT(c.location_info->>'location_name', ' ', c.location_info->>'room_name', '') AS location_description,
    COUNT(CONCAT(c.location_info->>'location_name', ' ', c.location_info->>'room_name', '')) AS computers_with_location_description
FROM
    silver.computers AS c
JOIN
    silver.lab_computers AS lc ON lc.computer_id = c.computer_id
JOIN
    silver.users AS u ON u.uniqname = lc.lab_id
WHERE
    lc.confidence_score >= 0.65
GROUP BY
    u.department_name,
    lc.lab_id,
    location_description
ORDER BY
    lc.lab_id ASC,
    computers_with_location_description DESC;
;

-- ----------------------------------------------------------------------------
-- v_lab_locations_tdx_reference
-- TDX-optimized view of lab locations with identifiers for API operations
-- Dependencies: silver.computers, silver.lab_computers, silver.users, silver.departments
-- Purpose: Provides TDX IDs and room/location IDs for writing location data to TeamDynamix
-- Note: Filters by confidence_score >= 0.65 to exclude low-confidence matches
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_locations_tdx_reference AS
SELECT
    u.department_name,
    d.tdx_id AS department_tdx_id,
    lc.lab_id,
    c.location_info->>'room_id' AS room_id,
    c.location_info->>'location_id' AS location_id,
    CONCAT(c.location_info->>'location_name', ' ', c.location_info->>'room_name', '') AS location_description,
    COUNT(CONCAT(c.location_info->>'location_name', ' ', c.location_info->>'room_name', '')) AS computers_with_location_description
FROM
    silver.computers AS c
JOIN
    silver.lab_computers AS lc ON lc.computer_id = c.computer_id
JOIN
    silver.users AS u ON u.uniqname = lc.lab_id
JOIN
    silver.departments AS d ON d.department_code = u.department_id
WHERE
    lc.confidence_score >= 0.65
GROUP BY
    u.department_name,
    d.tdx_id,
    lc.lab_id,
    room_id,
    location_id,
    location_description
ORDER BY
    lc.lab_id ASC,
    computers_with_location_description DESC;
;

-- ============================================================================
-- LAB COMPUTER PURCHASE INFORMATION VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- v_lab_purchase_shortcodes
-- Shows which purchase shortcodes labs have used for computer purchases
-- Dependencies: silver.computers, silver.lab_computers, silver.users, silver.departments
-- Purpose: Tracks funding sources for lab computers via TDX Purchase Shortcode attribute
-- Note: Filters by confidence_score >= 0.65 and only includes active computers
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_purchase_shortcodes AS
SELECT
    u.department_name,
    lc.lab_id,
    attribute_object->>'Value' AS shortcode,
    COUNT(attribute_object->>'Value') AS num_computers_purchased_on_shortcode
FROM
    silver.computers AS c
JOIN
    silver.lab_computers AS lc ON lc.computer_id = c.computer_id
JOIN
    silver.users AS u ON u.uniqname = lc.lab_id
JOIN
    silver.departments AS d ON d.department_code = u.department_id
CROSS JOIN LATERAL
    jsonb_array_elements(c.tdx_attributes -> 'all_attributes') AS attribute_object
WHERE
    lc.confidence_score >= 0.65 AND
    attribute_object ->> 'Name' = 'Purchase Shortcode' AND
    c.tdx_status_name = 'Active'
GROUP BY
    u.department_name,
    d.tdx_id,
    lc.lab_id,
    shortcode
ORDER BY
    lc.lab_id ASC;
;

-- ----------------------------------------------------------------------------
-- v_lab_computers_tdx_reference
-- TDX-optimized view of lab computers with all TDX identifiers
-- Dependencies: silver.lab_computers, silver.computers, silver.departments
-- Purpose: Provides TDX IDs needed for API operations on lab computers
-- Note: Filters by confidence_score >= 0.65 to exclude low-confidence matches
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.v_lab_computers_tdx_reference AS
SELECT
    d.tdx_id AS department_tdx_id,
    lc.lab_id,
    lc.computer_id,
    c.tdx_configuration_item_id,
    c.tdx_asset_id AS tdx_computer_id,
    l.tdx_ci_id AS lab_department_tdx_id
FROM
    silver.lab_computers AS lc
JOIN
    silver.computers AS c ON c.computer_id = lc.computer_id
JOIN
    silver.departments AS d ON (c.ownership_info->'tdx_owning'->>'department_id')::numeric::int = d.tdx_id
JOIN
    silver.labs as l on lc.lab_id = l.lab_id
WHERE
    lc.confidence_score >= 0.65;
;

-- ============================================================================
-- GROUP UTILITY VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- group_summary
-- Summary of group membership and ownership counts per group
-- Dependencies: silver.groups, silver.group_members, silver.group_owners
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.group_summary AS
SELECT
    g.group_id,
    g.group_name,
    g.sam_account_name,
    g.is_mcomm_adsync,
    COUNT(DISTINCT gm.membership_id) AS total_members,
    COUNT(DISTINCT CASE WHEN gm.member_type = 'user'  THEN gm.membership_id END) AS user_members,
    COUNT(DISTINCT CASE WHEN gm.member_type = 'group' THEN gm.membership_id END) AS nested_groups,
    COUNT(DISTINCT go.ownership_id) AS total_owners,
    g.group_email,
    g.description,
    g.data_quality_score
FROM silver.groups g
LEFT JOIN silver.group_members gm ON g.group_id::text = gm.group_id::text
LEFT JOIN silver.group_owners  go ON g.group_id::text = go.group_id::text
GROUP BY
    g.group_id, g.group_name, g.sam_account_name, g.is_mcomm_adsync,
    g.group_email, g.description, g.data_quality_score
ORDER BY g.group_name;
;

-- ----------------------------------------------------------------------------
-- user_group_memberships
-- Flat view of which users belong to which groups
-- Dependencies: silver.group_members, silver.groups
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.user_group_memberships AS
SELECT
    gm.member_uniqname AS uniqname,
    g.group_id,
    g.group_name,
    g.sam_account_name,
    gm.is_direct_member,
    gm.source_system,
    g.is_mcomm_adsync,
    g.source_system AS group_source_system
FROM silver.group_members gm
JOIN silver.groups g ON gm.group_id::text = g.group_id::text
WHERE gm.member_type = 'user'
ORDER BY gm.member_uniqname, g.group_name;
;

-- ----------------------------------------------------------------------------
-- synced_groups
-- Groups synced between AD and MCommunity
-- Dependencies: silver.groups
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.synced_groups AS
SELECT
    group_id,
    group_name,
    sam_account_name,
    cn,
    group_email,
    description,
    data_quality_score
FROM silver.groups
WHERE is_mcomm_adsync = true
ORDER BY group_name;
;

-- ============================================================================
-- END OF FILE
-- ============================================================================
