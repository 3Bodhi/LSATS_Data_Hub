-- Migration 007: Add v_labs_monitored filtered view
-- Purpose: Create production-ready view of meaningful labs for monitoring
-- Date: 2025-11-20
-- Phase: Phase 1 (Quick Win - Filtered View)

-- This view filters silver.labs to show only meaningful labs that should be monitored
-- Filtering criteria:
--   1. ad_ou_dn IS NOT NULL - Lab has Active Directory OU presence
--   2. primary_department_id IS NOT NULL - Lab is properly associated with a department
--   3. computer_count > 0 - Lab has computers to monitor
--
-- This excludes department-level false positives (comm, biology, physics)
-- while retaining actual research labs with infrastructure

CREATE OR REPLACE VIEW silver.v_labs_monitored AS
SELECT
    l.lab_id,
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
WHERE l.ad_ou_dn IS NOT NULL
  AND l.primary_department_id IS NOT NULL
  AND l.computer_count > 0
ORDER BY l.member_count DESC;

COMMENT ON VIEW silver.v_labs_monitored IS
'Production-ready filtered view of labs for compliance monitoring. Excludes department-level entities and labs without computers. Use this view for ticket automation and compliance workflows.';
