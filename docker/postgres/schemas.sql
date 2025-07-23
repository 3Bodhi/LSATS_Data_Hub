-- LSATS Database Schema Definitions
-- Bronze-Silver-Gold architecture for flexible data processing

-- ============================================================================
-- BRONZE LAYER: Raw data exactly as received from source systems
-- ============================================================================

-- Universal raw entity storage - this table can handle any entity type
-- The JSONB column preserves the complete original data structure
CREATE TABLE bronze.raw_entities (
    -- Primary identifier for this specific raw record
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Entity classification
    entity_type VARCHAR(50) NOT NULL,  -- 'department', 'user', 'lab', 'asset', etc.
    source_system VARCHAR(50) NOT NULL,  -- 'tdx', 'lab_csv', 'hr_system', etc.
    external_id VARCHAR(255) NOT NULL,  -- The ID from the source system

    -- Complete raw data from source (this is the magic of JSONB)
    raw_data JSONB NOT NULL,

    -- Tracking and metadata
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    entity_hash VARCHAR(64) GENERATED ALWAYS AS (
        encode(sha256((entity_type || '|' || source_system || '|' || external_id)::bytea), 'hex')
    ) STORED,  -- Computed column for consistent entity identification

    -- Additional metadata about the ingestion
    ingestion_metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for efficient querying
CREATE INDEX idx_bronze_entity_lookup ON bronze.raw_entities (entity_type, source_system, external_id);
CREATE INDEX idx_bronze_entity_hash ON bronze.raw_entities (entity_hash);
CREATE INDEX idx_bronze_ingestion_time ON bronze.raw_entities (entity_type, ingested_at DESC);
CREATE INDEX idx_bronze_raw_data_gin ON bronze.raw_entities USING gin (raw_data);  -- For JSONB queries

-- ============================================================================
-- SILVER LAYER: Cleaned and standardized data from pandas processing
-- ============================================================================

-- Departments after pandas cleaning pipeline
CREATE TABLE silver.departments (
    silver_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Standardized department fields (cleaned by pandas)
    department_name VARCHAR(255) NOT NULL,
    department_code VARCHAR(50),
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    region_name VARCHAR(100),

    -- Data quality metrics (calculated by pandas pipeline)
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,  -- Array of quality issues found

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    source_entity_id VARCHAR(255) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_silver_departments_updated_at
    BEFORE UPDATE ON silver.departments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Indexes for efficient querying and joining
CREATE INDEX idx_silver_departments_entity_hash ON silver.departments (entity_hash);
CREATE INDEX idx_silver_departments_source ON silver.departments (source_system, source_entity_id);
CREATE INDEX idx_silver_departments_active ON silver.departments (is_active, department_name);
CREATE INDEX idx_silver_departments_quality ON silver.departments (data_quality_score DESC);

-- Users after pandas cleaning pipeline
CREATE TABLE silver.users (
    silver_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Standardized user fields
    uniqname VARCHAR(50) NOT NULL,
    display_name VARCHAR(255),
    email VARCHAR(255),
    department_affiliation VARCHAR(255),
    user_type VARCHAR(50),  -- 'faculty', 'staff', 'student', 'external', etc.
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Data quality metrics
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    source_entity_id VARCHAR(255) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,

    -- Standard timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

CREATE TRIGGER update_silver_users_updated_at
    BEFORE UPDATE ON silver.users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE INDEX idx_silver_users_uniqname ON silver.users (uniqname);
CREATE INDEX idx_silver_users_entity_hash ON silver.users (entity_hash);

-- ============================================================================
-- GOLD LAYER: Master records representing authoritative truth
-- ============================================================================

-- Master department records (golden truth)
CREATE TABLE gold.department_masters (
    master_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Authoritative department information (best data from all sources)
    canonical_name VARCHAR(255) NOT NULL,
    canonical_code VARCHAR(50),
    canonical_description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    region VARCHAR(100),

    -- Master record metadata
    confidence_score DECIMAL(3,2) CHECK (confidence_score BETWEEN 0.00 AND 1.00),
    source_count INTEGER NOT NULL DEFAULT 1,  -- How many sources contribute to this master
    primary_source VARCHAR(50) NOT NULL,  -- Which source is considered most authoritative

    -- Reconciliation tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_department_masters_updated_at
    BEFORE UPDATE ON gold.department_masters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Mapping table: Which silver records contribute to each master record
CREATE TABLE gold.department_source_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_id UUID NOT NULL REFERENCES gold.department_masters(master_id) ON DELETE CASCADE,
    silver_id UUID NOT NULL REFERENCES silver.departments(silver_id) ON DELETE CASCADE,
    contribution_weight DECIMAL(3,2) DEFAULT 1.00,  -- How much this source contributes
    is_primary_source BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Master user records
CREATE TABLE gold.user_masters (
    master_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Authoritative user information
    canonical_uniqname VARCHAR(50) NOT NULL UNIQUE,
    canonical_name VARCHAR(255),
    canonical_email VARCHAR(255),
    primary_department_id UUID REFERENCES gold.department_masters(master_id),
    user_type VARCHAR(50),
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Master record metadata
    confidence_score DECIMAL(3,2) CHECK (confidence_score BETWEEN 0.00 AND 1.00),
    source_count INTEGER NOT NULL DEFAULT 1,
    primary_source VARCHAR(50) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_user_masters_updated_at
    BEFORE UPDATE ON gold.user_masters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- User source mapping
CREATE TABLE gold.user_source_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_id UUID NOT NULL REFERENCES gold.user_masters(master_id) ON DELETE CASCADE,
    silver_id UUID NOT NULL REFERENCES silver.users(silver_id) ON DELETE CASCADE,
    contribution_weight DECIMAL(3,2) DEFAULT 1.00,
    is_primary_source BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Lab membership relationships (many-to-many between users and labs)
CREATE TABLE gold.lab_memberships (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_master_id UUID NOT NULL REFERENCES gold.user_masters(master_id) ON DELETE CASCADE,
    lab_name VARCHAR(255) NOT NULL,
    lab_pi_uniqname VARCHAR(50),
    user_role VARCHAR(50),  -- 'PI', 'lab_manager', 'postdoc', 'grad_student', etc.
    department_master_id UUID REFERENCES gold.department_masters(master_id),
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    confidence_score DECIMAL(3,2) DEFAULT 1.00,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_gold_lab_memberships_updated_at
    BEFORE UPDATE ON gold.lab_memberships
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- HELPFUL VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Complete lab picture with all members and their roles
CREATE VIEW gold.lab_rosters AS
SELECT
    lab_name,
    lab_pi_uniqname,
    dept.canonical_name as department_name,
    user_role,
    user_master.canonical_uniqname,
    user_master.canonical_name as user_name,
    user_master.canonical_email,
    membership.is_active as membership_active,
    membership.confidence_score
FROM gold.lab_memberships membership
JOIN gold.user_masters user_master ON membership.user_master_id = user_master.master_id
LEFT JOIN gold.department_masters dept ON membership.department_master_id = dept.master_id
WHERE membership.is_active = true
ORDER BY lab_name, user_role, user_master.canonical_name;

-- View: Department summary with user counts
CREATE VIEW gold.department_summary AS
SELECT
    dept.canonical_name,
    dept.canonical_code,
    dept.is_active,
    COUNT(DISTINCT user_master.master_id) as total_users,
    COUNT(DISTINCT CASE WHEN lab.user_role = 'PI' THEN lab.user_master_id END) as pi_count,
    COUNT(DISTINCT lab.lab_name) as lab_count,
    dept.confidence_score,
    dept.source_count
FROM gold.department_masters dept
LEFT JOIN gold.user_masters user_master ON user_master.primary_department_id = dept.master_id
LEFT JOIN gold.lab_memberships lab ON lab.department_master_id = dept.master_id AND lab.is_active = true
GROUP BY dept.master_id, dept.canonical_name, dept.canonical_code, dept.is_active, dept.confidence_score, dept.source_count
ORDER BY dept.canonical_name;

-- Grant permissions for all new tables
GRANT ALL ON ALL TABLES IN SCHEMA bronze TO lsats_user;
GRANT ALL ON ALL TABLES IN SCHEMA silver TO lsats_user;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO lsats_user;

-- Add helpful table comments
COMMENT ON TABLE bronze.raw_entities IS 'Stores raw data exactly as received from any source system';
COMMENT ON TABLE silver.departments IS 'Cleaned and standardized department data from pandas processing';
COMMENT ON TABLE gold.department_masters IS 'Authoritative master department records combining all sources';
COMMENT ON VIEW gold.lab_rosters IS 'Complete view of lab memberships with user details and roles';
COMMENT ON VIEW gold.department_summary IS 'Summary statistics for each department including user and lab counts';
