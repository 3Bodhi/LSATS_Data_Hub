-- Migration: Add Bronze Layer Indexes for Group Transformation Performance
-- Purpose: Optimize group data queries during silver layer transformation
-- Estimated Impact: 35-50x speedup on group matching and member extraction
-- Run this BEFORE running the silver groups transformation

-- ============================================================================
-- Group CN Lookups
-- ============================================================================
-- Handles both array and string cn values from MCommunity and AD
-- Critical for group matching by name
CREATE INDEX IF NOT EXISTS idx_bronze_group_cn ON bronze.raw_entities (
    source_system,
    LOWER(CASE
        WHEN jsonb_typeof(raw_data->'cn') = 'array' THEN raw_data->'cn'->>0
        ELSE raw_data->>'cn'
    END)
) WHERE entity_type = 'group';

COMMENT ON INDEX bronze.idx_bronze_group_cn IS
    'Optimizes group lookups by cn (common name), handles MCommunity array format';

-- ============================================================================
-- GID Number Matching
-- ============================================================================
-- Critical index for matching groups across MCommunity and Active Directory
-- ~1,500 groups share gidNumber between sources (MCommADSync groups)
CREATE INDEX IF NOT EXISTS idx_bronze_group_gid ON bronze.raw_entities (
    source_system,
    (raw_data->>'gidNumber')
) WHERE entity_type = 'group' AND raw_data->>'gidNumber' IS NOT NULL;

COMMENT ON INDEX bronze.idx_bronze_group_gid IS
    'Optimizes cross-source group matching by POSIX gidNumber';

-- ============================================================================
-- AD MCommADSync Groups
-- ============================================================================
-- Identifies AD groups that are synced from MCommunity
-- Approximately 1,526 groups in OU=MCommADSync
CREATE INDEX IF NOT EXISTS idx_bronze_ad_mcomm_sync ON bronze.raw_entities (
    (raw_data->>'dn')
) WHERE entity_type = 'group'
  AND source_system = 'active_directory'
  AND raw_data->>'dn' ~ 'MCommADSync';

COMMENT ON INDEX bronze.idx_bronze_ad_mcomm_sync IS
    'Identifies AD groups synchronized from MCommunity';

-- ============================================================================
-- Incremental Transformation Support
-- ============================================================================
-- Enables efficient incremental transformations by tracking ingestion time
CREATE INDEX IF NOT EXISTS idx_bronze_group_source_ingested ON bronze.raw_entities (
    entity_type, source_system, ingested_at DESC
) WHERE entity_type = 'group';

COMMENT ON INDEX bronze.idx_bronze_group_source_ingested IS
    'Supports incremental group transformations by ingestion timestamp';

-- ============================================================================
-- Member Array Extraction
-- ============================================================================
-- GIN index for fast member array searches and extraction
-- Estimated ~380,000 total membership relationships across all groups
CREATE INDEX IF NOT EXISTS idx_bronze_group_members_gin ON bronze.raw_entities USING gin (
    (raw_data->'member')
) WHERE entity_type = 'group';

COMMENT ON INDEX bronze.idx_bronze_group_members_gin IS
    'Optimizes member array extraction during transformation';

-- ============================================================================
-- Owner Array Extraction (MCommunity only)
-- ============================================================================
-- GIN index for owner array searches
-- MCommunity groups have owner relationships, AD does not
CREATE INDEX IF NOT EXISTS idx_bronze_group_owners_gin ON bronze.raw_entities USING gin (
    (raw_data->'owner')
) WHERE entity_type = 'group' AND source_system = 'mcommunity_ldap';

COMMENT ON INDEX bronze.idx_bronze_group_owners_gin IS
    'Optimizes owner array extraction from MCommunity groups';

-- ============================================================================
-- Validation Queries
-- ============================================================================

-- Verify index creation
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'raw_entities'
  AND indexname LIKE '%group%'
ORDER BY indexname;

-- Show statistics
SELECT
    'Total Groups' as metric,
    COUNT(*) as count
FROM bronze.raw_entities
WHERE entity_type = 'group'

UNION ALL

SELECT
    'Groups with gidNumber' as metric,
    COUNT(*) as count
FROM bronze.raw_entities
WHERE entity_type = 'group' AND raw_data->>'gidNumber' IS NOT NULL

UNION ALL

SELECT
    'AD MCommADSync Groups' as metric,
    COUNT(*) as count
FROM bronze.raw_entities
WHERE entity_type = 'group'
  AND source_system = 'active_directory'
  AND raw_data->>'dn' ~ 'MCommADSync'

UNION ALL

SELECT
    'Groups by Source: ' || source_system as metric,
    COUNT(*) as count
FROM bronze.raw_entities
WHERE entity_type = 'group'
GROUP BY source_system;
