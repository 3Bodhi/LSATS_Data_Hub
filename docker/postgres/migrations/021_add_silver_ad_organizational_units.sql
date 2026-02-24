-- Migration: Add silver.ad_organizational_units source-specific table
-- Date: 2025-11-23
-- Purpose: Create source-specific silver table for Active Directory organizational units
--          Part of medallion architecture - extracted from bronze.raw_entities
--          Enables lab identification and organizational structure analysis

-- Drop table if exists (for development/testing)
-- DROP TABLE IF EXISTS silver.ad_organizational_units CASCADE;

-- Create silver.ad_organizational_units table
CREATE TABLE IF NOT EXISTS silver.ad_organizational_units (
    -- ============================================
    -- PRIMARY IDENTIFIER
    -- ============================================
    object_guid UUID PRIMARY KEY,                  -- AD GUID (immutable, 100% populated)
                                                     -- CRITICAL: Survives OU renames and moves

    -- ============================================
    -- CORE AD IDENTITY FIELDS
    -- ============================================
    distinguished_name VARCHAR(250) UNIQUE NOT NULL, -- Full DN (max 228 + buffer, 100% populated)
                                                      -- Example: "OU=psyc-danweiss,OU=Psychology,..."
    
    ou_name VARCHAR(60) NOT NULL,                  -- OU name from 'ou' field (max 48 + buffer)
                                                    -- Example: "psyc-danweiss", "Biology", "Current"
    
    name VARCHAR(60) NOT NULL,                     -- Name field (usually same as ou_name)
    
    description VARCHAR(100),                      -- OU description (max 70 + buffer, 2% populated)
                                                    -- User-provided description text
    
    managed_by VARCHAR(200),                       -- DN of managing user/group (max 173 + buffer, 7.3% populated)
                                                    -- Links to silver.ad_users or silver.ad_groups
    
    gp_link TEXT,                                  -- Group policy links (max 2507, 49.8% populated)
                                                    -- LDAP DN references to GPOs
                                                    -- Can be very long with multiple policies
    
    gp_options VARCHAR(10),                        -- Group policy options flags
    
    object_category VARCHAR(150),                  -- AD object category
                                                    -- Example: "CN=Organizational-Unit,CN=Schema,..."
    
    object_class JSONB DEFAULT '[]'::jsonb,        -- Object classes array
                                                    -- Typically: ["organizationalUnit", "top"]
    
    instance_type INTEGER,                         -- AD instance type (usually 4)
    
    system_flags INTEGER,                          -- System flags for protected objects

    -- ============================================
    -- OU HIERARCHY (UNIVERSAL EXTRACTION PATTERN)
    -- ============================================
    -- Parsed from root (DC) → leaf (CN) for consistent cross-entity matching
    -- Compatible with ad_computers, ad_groups, ad_users hierarchy extraction
    -- Pattern: UMICH → Organizations → LSA → Category → Division → Department → Subdepartment

    ou_root VARCHAR(50),                           -- Always "UMICH" (position from end: array[length-1])
    ou_organization_type VARCHAR(50),              -- "Organizations" for LSA entities (array[length-2])
    ou_organization VARCHAR(50),                   -- "LSA", "UMICH", etc. (array[length-3])
    ou_category VARCHAR(100),                      -- "Research and Instrumentation", "Workstations" (array[length-4])
    ou_status VARCHAR(20),                         -- Status/lifecycle: "Current", "Legacy", "Staging" (array[length-5])
                                                    -- CRITICAL: Organizational lifecycle stage
    ou_division VARCHAR(100),                      -- "RSN", "EHTS", "CSG", "Randall", "LSAIT Computer Support Group" (array[length-6])
    ou_department VARCHAR(150),                    -- Department name: "Biology", "Psychology", "Chemistry" (array[length-7])
    ou_subdepartment VARCHAR(150),                 -- Sub-department or lab-specific OU (array[length-8])
    ou_immediate_parent VARCHAR(150),              -- First OU parent (array[1]) - NOT array[0] which is the OU itself
    ou_full_path JSONB DEFAULT '[]'::jsonb,        -- Complete array of all OUs (leaf→root order)
                                                    -- CRITICAL: For OU objects, array[0] is the OU itself
                                                    -- For deep hierarchy queries and edge cases
    
    ou_depth INTEGER NOT NULL,                     -- Hierarchy depth count (4-11, avg 8)
                                                    -- Number of OU components in distinguished_name
    
    parent_ou_dn VARCHAR(250),                     -- Parent OU distinguished name
                                                    -- Everything after first OU= component

    -- CRITICAL: These OU hierarchy fields enable cross-entity joins:
    -- - silver.ad_organizational_units.ou_department = silver.ad_computers.ou_department (same dept)
    -- - silver.ad_organizational_units.ou_division = "RSN" (all RSN organizational structure)
    -- - Find lab OUs where ou_immediate_parent LIKE '%-lab' or extracted_uniqname IS NOT NULL

    -- ============================================
    -- ENRICHMENT METADATA (from bronze ingestion)
    -- ============================================
    -- These fields are computed during bronze ingestion via expensive LDAP queries
    -- and must be preserved in silver for analysis

    direct_computer_count INTEGER DEFAULT 0,       -- Computers directly in this OU (0-27, avg 2)
                                                    -- Computed via LDAP query: (objectClass=computer) at LEVEL scope
    
    has_computer_children BOOLEAN DEFAULT false,   -- True if direct_computer_count > 0
                                                    -- CRITICAL: Identifies OUs with managed computers
    
    child_ou_count INTEGER DEFAULT 0,              -- Sub-OUs count (0-41, avg 1)
                                                    -- Computed via LDAP query: (objectClass=organizationalUnit) at LEVEL scope
    
    has_child_ous BOOLEAN DEFAULT false,           -- True if child_ou_count > 0
                                                    -- Indicates organizational hierarchy depth
    
    depth_category VARCHAR(20),                    -- Depth classification for filtering
                                                    -- Values: 'potential_lab' (962, depth 8+),
                                                    --         'department' (184, depth 7),
                                                    --         'region' (37, depth 6),
                                                    --         'high_level' (20, depth <6)
    
    extracted_uniqname VARCHAR(20),                -- Extracted uniqname from OU name patterns (61% populated)
                                                    -- CRITICAL: For lab matching with silver.lab_awards
                                                    -- Pattern extraction: "psyc-danweiss" → "danweiss"
                                                    --                     "Vsih-Lab" → "vsih"
                                                    --                     "nagorny" → "nagorny"
                                                    -- 85 matches with lab award PIs (49% match rate)
    
    name_patterns JSONB,                           -- Name pattern flags for classification
                                                    -- Structure: {
                                                    --   "dept_uniqname": bool,  -- Pattern: "psyc-danweiss"
                                                    --   "uniqname_only": bool,  -- Pattern: "nagorny"
                                                    --   "lab_suffix": bool,     -- Pattern: "Vsih-Lab"
                                                    --   "has_hyphen": bool      -- Contains hyphen
                                                    -- }

    -- ============================================
    -- AD TIMESTAMPS AND VERSIONING
    -- ============================================
    when_created TIMESTAMP WITH TIME ZONE NOT NULL, -- AD object creation (100% populated)
    when_changed TIMESTAMP WITH TIME ZONE NOT NULL, -- Last AD modification (100% populated)
    usn_created BIGINT NOT NULL,                   -- Update sequence number at creation
    usn_changed BIGINT NOT NULL,                   -- Current USN (for replication tracking)

    -- ============================================
    -- AD METADATA
    -- ============================================
    ds_core_propagation_data JSONB DEFAULT '[]'::jsonb, -- Replication timestamps array
                                                         -- Tracks AD replication events

    -- ============================================
    -- TRACEABILITY (link back to bronze)
    -- ============================================
    raw_id UUID NOT NULL,                          -- Link to bronze.raw_entities.raw_id
                                                    -- For audit trail and access to complete bronze data

    -- ============================================
    -- STANDARD SILVER METADATA
    -- ============================================
    source_system VARCHAR(50) DEFAULT 'active_directory' NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,              -- SHA-256 for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Primary key index (automatically created)
-- CREATE UNIQUE INDEX idx_ad_organizational_units_object_guid ON silver.ad_organizational_units (object_guid);

-- Core business key indexes (UNIQUE for matching)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_organizational_units_distinguished_name
    ON silver.ad_organizational_units (distinguished_name);

-- Lab matching (CRITICAL for lab identification)
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_extracted_uniqname
    ON silver.ad_organizational_units (extracted_uniqname)
    WHERE extracted_uniqname IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_depth_category
    ON silver.ad_organizational_units (depth_category)
    WHERE depth_category IS NOT NULL;

-- OU hierarchy indexes (for cross-entity matching)
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_organization
    ON silver.ad_organizational_units (ou_organization)
    WHERE ou_organization IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_division
    ON silver.ad_organizational_units (ou_division)
    WHERE ou_division IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_department
    ON silver.ad_organizational_units (ou_department)
    WHERE ou_department IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_category
    ON silver.ad_organizational_units (ou_category)
    WHERE ou_category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_immediate_parent
    ON silver.ad_organizational_units (ou_immediate_parent)
    WHERE ou_immediate_parent IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_status
    ON silver.ad_organizational_units (ou_status)
    WHERE ou_status IS NOT NULL;

-- Activity and infrastructure
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_has_computer_children
    ON silver.ad_organizational_units (has_computer_children)
    WHERE has_computer_children = TRUE;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_direct_computer_count
    ON silver.ad_organizational_units (direct_computer_count)
    WHERE direct_computer_count > 0;

-- Management
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_managed_by
    ON silver.ad_organizational_units (managed_by)
    WHERE managed_by IS NOT NULL;

-- Timestamps
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_when_created
    ON silver.ad_organizational_units (when_created);

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_when_changed
    ON silver.ad_organizational_units (when_changed);

-- GIN indexes for JSONB fields (for containment queries)
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ou_full_path_gin
    ON silver.ad_organizational_units USING gin (ou_full_path);

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_name_patterns_gin
    ON silver.ad_organizational_units USING gin (name_patterns)
    WHERE name_patterns IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_object_class_gin
    ON silver.ad_organizational_units USING gin (object_class);

-- Standard traceability indexes
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_raw_id
    ON silver.ad_organizational_units (raw_id);

CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_entity_hash
    ON silver.ad_organizational_units (entity_hash);

-- Ingestion tracking
CREATE INDEX IF NOT EXISTS idx_ad_organizational_units_ingestion_run_id
    ON silver.ad_organizational_units (ingestion_run_id)
    WHERE ingestion_run_id IS NOT NULL;

-- ============================================
-- TABLE AND COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE silver.ad_organizational_units IS
    'Source-specific silver table for Active Directory organizational units. Contains typed columns extracted from bronze.raw_entities JSONB data with universal OU hierarchy extraction for cross-entity matching. Critical for lab identification and organizational structure analysis. Preserves enrichment metadata (computer counts, uniqname extraction) computed during bronze ingestion.';

COMMENT ON COLUMN silver.ad_organizational_units.object_guid IS
    'Primary key - Immutable AD GUID. Survives OU renames and moves. 100% populated.';

COMMENT ON COLUMN silver.ad_organizational_units.distinguished_name IS
    'Full distinguished name (DN). Unique but can change if OU is renamed/moved. Example: OU=psyc-danweiss,OU=Psychology,OU=EHTS,... Max length: 228 chars. 100% populated.';

COMMENT ON COLUMN silver.ad_organizational_units.ou_name IS
    'OU name from ''ou'' LDAP field. Examples: "psyc-danweiss", "Biology", "Current", "Research and Instrumentation". Max length: 48 chars. 100% populated.';

COMMENT ON COLUMN silver.ad_organizational_units.extracted_uniqname IS
    'Uniqname extracted from OU name using pattern matching. CRITICAL for lab matching with silver.lab_awards. Examples: "psyc-danweiss" → "danweiss", "Vsih-Lab" → "vsih". 736 populated (61%), 85 matches with lab awards (49% match rate).';

COMMENT ON COLUMN silver.ad_organizational_units.depth_category IS
    'Depth-based classification: "potential_lab" (962, depth 8+), "department" (184, depth 7), "region" (37, depth 6), "high_level" (20, depth <6). Used for filtering lab OUs.';

COMMENT ON COLUMN silver.ad_organizational_units.ou_division IS
    'Division OU extracted from DN (e.g., RSN, EHTS, CSG, Randall). CRITICAL for cross-entity queries: find all OUs, computers, groups, users in same division. Position from end: array[length-4].';

COMMENT ON COLUMN silver.ad_organizational_units.ou_department IS
    'Department OU extracted from DN (e.g., Biology, Psychology, Chemistry). CRITICAL for matching OUs to departmental groups, users, computers. Position from end: array[length-5].';

COMMENT ON COLUMN silver.ad_organizational_units.ou_category IS
    'Category OU extracted from DN (e.g., "Research and Instrumentation", "Workstations", "Users and Groups"). Position from end: array[length-4].';

COMMENT ON COLUMN silver.ad_organizational_units.ou_status IS
    'OU status/lifecycle stage (e.g., "Current", "Legacy", "Staging"). Position from end: array[length-5]. CRITICAL for filtering active vs legacy organizational structures.';

COMMENT ON COLUMN silver.ad_organizational_units.ou_subdepartment IS
    'Sub-department or lab-specific OU. Position from end: array[length-8]. Often NULL for higher-level OUs.';

COMMENT ON COLUMN silver.ad_organizational_units.ou_full_path IS
    'Complete OU hierarchy as JSONB array (leaf→root order). Example: ["psyc-danweiss", "Psychology", "EHTS", "Current", "Research and Instrumentation", "LSA", "Organizations", "UMICH"]. Supports deep hierarchy queries and edge cases.';

COMMENT ON COLUMN silver.ad_organizational_units.direct_computer_count IS
    'Number of computers directly in this OU (not recursive). Range: 0-27, avg 2. Computed during bronze ingestion via LDAP query. CRITICAL for identifying lab OUs with managed computers.';

COMMENT ON COLUMN silver.ad_organizational_units.has_computer_children IS
    'True if direct_computer_count > 0. Quick filter for OUs with managed computers.';

COMMENT ON COLUMN silver.ad_organizational_units.child_ou_count IS
    'Number of immediate sub-OUs (not recursive). Range: 0-41, avg 1. Computed during bronze ingestion via LDAP query.';

COMMENT ON COLUMN silver.ad_organizational_units.name_patterns IS
    'JSONB object with boolean flags for name pattern classification. Structure: {"dept_uniqname": bool, "uniqname_only": bool, "lab_suffix": bool, "has_hyphen": bool}. Used for pattern-based lab identification.';

COMMENT ON COLUMN silver.ad_organizational_units.raw_id IS
    'Link to most recent bronze.raw_entities record for this OU - for audit trail and complete bronze data access.';

COMMENT ON COLUMN silver.ad_organizational_units.entity_hash IS
    'SHA-256 hash of significant fields for change detection - only transform if hash changed (incremental processing).';

-- ============================================
-- PERMISSIONS
-- ============================================

-- Grant permissions (adjust as needed)
GRANT SELECT ON silver.ad_organizational_units TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON silver.ad_organizational_units TO lsats_user;

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully created silver.ad_organizational_units table with % indexes',
        (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'silver' AND tablename = 'ad_organizational_units');
    RAISE NOTICE '📊 Table supports 1,203 AD organizational units with enrichment metadata';
    RAISE NOTICE '🔗 Critical features:';
    RAISE NOTICE '   - extracted_uniqname → silver.lab_awards.person_uniqname (85 current matches)';
    RAISE NOTICE '   - ou_department/ou_division → cross-entity OU matching with computers, groups, users';
    RAISE NOTICE '   - depth_category → filter for potential_lab (962 OUs)';
    RAISE NOTICE '   - direct_computer_count → identify lab OUs with managed computers';
    RAISE NOTICE '🏢 OU hierarchy: root → org_type → organization → category → division → department → subdepartment';
END $$;
