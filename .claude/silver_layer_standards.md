# Silver Layer Standards

**Version:** 1.0  
**Last Updated:** 2025-01-24  
**Purpose:** Comprehensive silver layer reference with composite entity patterns and technology decision guidance

---

## Table of Contents

1. [Overview & Philosophy](#overview--philosophy)
2. [Technology Decision Matrix](#technology-decision-matrix) ⭐
3. [Three-Tier Silver Architecture](#three-tier-silver-architecture)
4. [Composite Entity Patterns](#composite-entity-patterns) ⭐
5. [View Patterns](#view-patterns)
6. [Database Functions vs Application Logic](#database-functions-vs-application-logic) ⭐
7. [Transformation Patterns](#transformation-patterns)
8. [Reference Implementations](#reference-implementations)
9. [Field Merge Priority Rules](#field-merge-priority-rules)
10. [Data Quality Framework](#data-quality-framework)
11. [JSONB Field Usage](#jsonb-field-usage)
12. [Foreign Key & Referential Integrity](#foreign-key--referential-integrity)
13. [Performance Optimization](#performance-optimization)
14. [Testing Strategies](#testing-strategies)
15. [Migration Guide: Python → dbt](#migration-guide-python--dbt)

---

## Overview & Philosophy

**Purpose:** Cleaned, unified, analysis-ready data with entity resolution and business-level aggregations

**Key Principles:**
1. **Three-tier architecture** — Source-specific → Consolidated → Composite
2. **SQL transformations preferred** — dbt future state, Python current implementation
3. **ETL-managed integrity** — No database foreign key constraints
4. **Data quality scoring** — All tables have quality metrics (0.00-1.00)
5. **Incremental by default** — Full-sync as override option

**Current State (2025):** Python-based transformations  
**Future State (2026+):** dbt SQL models with Python for complex algorithms

**Reference:** See [bronze_layer_standards.md](bronze_layer_standards.md) for ingestion patterns and [medallion_standards.md](medallion_standards.md) for cross-layer standards.

---

## Technology Decision Matrix

### Quick Decision Guide

```
┌─────────────────────────────────────┐
│ What type of work is this?          │
└────────────┬────────────────────────┘
             │
    ┌────────┴────────┐
    │                 │
    ▼                 ▼
┌──────────┐     ┌──────────────┐
│ SQL-like │     │ Algorithm    │
│ (joins,  │     │ (ML, complex │
│  agg,    │     │  scoring,    │
│  filter) │     │  hashing)    │
└────┬─────┘     └───────┬──────┘
     │                   │
     ▼                   ▼
┌─────────────┐    ┌──────────────┐
│ **Use dbt** │    │ **Use Python**│
│ (preferred) │    │   (required)  │
│             │    │               │
│ Current:    │    │ Examples:     │
│ Python      │    │ - API calls   │
│ works but   │    │ - Lab manager │
│ migrate     │    │   scoring     │
│ to dbt      │    │ - ML models   │
│ when able   │    │ - Hashing     │
└─────────────┘    └──────────────┘
```

### Detailed Decision Matrix

| Use Case | dbt SQL | Python Service | Current Implementation |
|----------|---------|----------------|----------------------|
| **Multi-table joins** | ✅ **PREFERRED** | ❌ Inefficient | Python (migrate to dbt) |
| **Field merge priority (COALESCE)** | ✅ **PREFERRED** | ❌ Verbose | Python (migrate to dbt) |
| **Aggregations (SUM, COUNT, AVG)** | ✅ **PREFERRED** | ❌ Slow | Python (migrate to dbt) |
| **GROUP BY operations** | ✅ **PREFERRED** | ❌ Complex | Python (migrate to dbt) |
| **Data quality tests** | ✅ **PREFERRED** (dbt tests) | ⚠️ Manual | Python manual queries |
| **Documentation** | ✅ **PREFERRED** (auto-generated) | ⚠️ Manual | Python comments |
| **Version control** | ✅ **PREFERRED** (SQL in Git) | ✅ Same | Python in Git |
| **Complex scoring algorithm** | ❌ Hard to maintain | ✅ **REQUIRED** | Python (keep) |
| **Content hashing** | ❌ Unsupported | ✅ **REQUIRED** | Python (keep) |
| **API calls** | ❌ Not applicable | ✅ **REQUIRED** | Python (keep) |
| **ML model inference** | ⚠️ dbt Python models | ✅ **PREFERRED** | N/A (future) |
| **Cross-table calculations** | ✅ Join logic | ✅ Post-query processing | Python (hybrid future) |

### Why dbt?

**Current Pain Points with Python:**
- Transformation logic embedded in code (hard to review)
- Manual testing with verification queries
- No automatic lineage documentation
- Incremental logic requires custom `--full-sync` flags
- SQL spread across Python strings

**dbt Benefits:**
- ✅ **SQL as code** — Transformations are visible, not hidden in Python
- ✅ **Automatic testing** — `unique`, `not_null`, `relationships` tests built-in
- ✅ **Lineage graphs** — Auto-generated DAG showing dependencies
- ✅ **Incremental refresh** — Built-in `is_incremental()` macro
- ✅ **Faster iteration** — `dbt run --select model_name` vs full pipeline
- ✅ **Better reviews** — SQL easier for team to understand than psycopg2 code

**dbt Limitations:**
- ❌ Doesn't do ingestion (still need Python for API calls)
- ❌ Limited for complex algorithms (use Python for scoring logic)
- ❌ Learning curve (Jinja templating, new concepts)

**Verdict:** Use dbt for SQL transformations, Python for algorithms

### Migration Priority

| Table | Complexity | Priority | Target Timeline | Notes |
|-------|-----------|----------|-----------------|-------|
| **silver.tdx_users** | Low | High | Q2 2025 | Pilot project |
| **silver.departments** | Low | High | Q2 2025 | Simple 2-source merge |
| **silver.users** | High | High | Q3 2025 | 4-source consolidation |
| **silver.groups** | High | Medium | Q3 2025 | CN overlap detection |
| **silver.computers** | Very High | Medium | Q3 2025 | Complex matching logic |
| **silver.labs** | Very High | Medium | Q4 2025 | Multi-table composite |
| **Lab manager scoring** | Algorithm | N/A | **Keep in Python** | Complex conditional logic |

**Current Focus (Q1 2025):** Complete composite tables in Python before refactoring to dbt.

---

## Three-Tier Silver Architecture

### Architecture Overview

```
Bronze Layer (bronze.raw_entities)
          │
          │ Python ingestion (current)
          ▼
┌─────────────────────────────────────────┐
│ TIER 1: Source-Specific Silver Tables   │
│                                          │
│ Purpose: Clean & type single source     │
│ Tech: dbt SQL (future) / Python (now)   │
│                                          │
│ Examples:                                │
│ - silver.tdx_users                      │
│ - silver.umapi_employees                │
│ - silver.ad_users                       │
│ - silver.mcommunity_users               │
│ - silver.lab_awards                     │
│ - silver.ad_organizational_units        │
└──────────┬──────────────────────────────┘
           │
           │ dbt SQL (future) / Python (current)
           ▼
┌─────────────────────────────────────────┐
│ TIER 2: Consolidated Entity Tables      │
│                                          │
│ Purpose: Cross-source entity resolution │
│ Tech: dbt SQL (future) / Python (now)   │
│                                          │
│ Examples:                                │
│ - silver.users                          │
│ - silver.groups                         │
│ - silver.computers                      │
│ - silver.departments                    │
└──────────┬──────────────────────────────┘
           │
           │ dbt SQL aggregation + Python algorithms
           ▼
┌─────────────────────────────────────────┐
│ TIER 3: Composite Business Entities ⭐  │
│                                          │
│ Purpose: Business-level aggregations    │
│ Tech: dbt SQL + Python (hybrid)         │
│                                          │
│ Examples:                                │
│ - silver.labs                           │
│ - silver.lab_members (junction)         │
│ - silver.lab_managers (algorithm)       │
│                                          │
│ Future composite entities:              │
│ - silver.pi_research_groups             │
│ - silver.department_portfolios          │
│ - silver.compliance_units               │
└─────────────────────────────────────────┘
           │
           │ Future: dbt SQL (star schema)
           ▼
     Gold Layer (analytics)
```

### Tier 1: Source-Specific Tables

**Purpose:** Extract and type data from single bronze source

**Characteristics:**
- One table per (source_system, entity_type) combination
- Extracts JSONB fields into typed columns
- Minimal transformation (clean, type, standardize format)
- Preserves source-specific fields
- Links back to bronze via `raw_id`

**Naming:** `silver.{source}_{entity}` (e.g., `tdx_users`, `umapi_employees`)

**Example Schema:**

```sql
CREATE TABLE silver.tdx_users (
    tdx_user_uid UUID PRIMARY KEY,           -- Source's native ID
    uniqname VARCHAR(50) NOT NULL,           -- Normalized business key
    
    -- Core fields (typed from JSONB)
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    primary_email VARCHAR(255),
    
    -- Source-specific fields
    tdx_is_active BOOLEAN,
    tdx_security_role_id INTEGER,
    tdx_authentication_provider_id INTEGER,
    
    -- Traceability
    raw_id UUID NOT NULL,                    -- Link to bronze.raw_entities
    raw_data_snapshot JSONB,                 -- Copy of raw_data for audit
    
    -- Standard metadata
    source_system VARCHAR(50) DEFAULT 'tdx',
    entity_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Index on business key for joining to consolidated layer
CREATE UNIQUE INDEX idx_tdx_users_uniqname ON silver.tdx_users (uniqname);
CREATE INDEX idx_tdx_users_raw_id ON silver.tdx_users (raw_id);
```

**Transformation Pattern (dbt - PREFERRED):**

```sql
-- models/silver/source_specific/tdx_users.sql
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['tdx_user_uid'], 'unique': True},
        {'columns': ['uniqname'], 'unique': True}
    ]
) }}

SELECT
    (raw_data->>'UID')::UUID AS tdx_user_uid,
    LOWER(TRIM(raw_data->>'UserName')) AS uniqname,
    raw_data->>'FirstName' AS first_name,
    raw_data->>'LastName' AS last_name,
    raw_data->>'FullName' AS full_name,
    raw_data->>'PrimaryEmail' AS primary_email,
    (raw_data->>'IsActive')::BOOLEAN AS tdx_is_active,
    (raw_data->>'SecurityRoleID')::INTEGER AS tdx_security_role_id,
    
    -- Traceability
    raw_id,
    raw_data AS raw_data_snapshot,
    
    -- Standard metadata
    'tdx' AS source_system,
    entity_hash,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    ingestion_run_id
    
FROM {{ source('bronze', 'raw_entities') }}
WHERE entity_type = 'user'
  AND source_system = 'tdx'

{% if is_incremental() %}
  -- Only process new bronze records
  AND ingested_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

**Transformation Pattern (Python - CURRENT):**

```python
class TDXUserTransformationService:
    """Transform TDX users from bronze to source-specific silver table."""
    
    def transform_to_silver(self):
        # Fetch bronze records
        bronze_records = self.db.query("""
            SELECT raw_id, raw_data, entity_hash, ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user' AND source_system = 'tdx'
        """)
        
        for record in bronze_records:
            data = record['raw_data']
            
            silver_record = {
                'tdx_user_uid': data.get('UID'),
                'uniqname': data.get('UserName', '').lower().strip(),
                'first_name': data.get('FirstName'),
                'last_name': data.get('LastName'),
                'full_name': data.get('FullName'),
                'primary_email': data.get('PrimaryEmail'),
                'tdx_is_active': data.get('IsActive'),
                'tdx_security_role_id': data.get('SecurityRoleID'),
                'raw_id': record['raw_id'],
                'raw_data_snapshot': data,
                'source_system': 'tdx',
                'entity_hash': record['entity_hash'],
                'ingestion_run_id': record['ingestion_run_id'],
            }
            
            self._upsert_silver_record(silver_record)
```

### Tier 2: Consolidated Entity Tables

**Purpose:** Merge multiple source-specific tables for same entity type

**Characteristics:**
- One table per entity type (users, groups, computers, departments)
- Merges 2+ source-specific tables
- Field merge priority rules (COALESCE chains)
- Data quality scoring across sources
- External system IDs for write-back

**Naming:** `silver.{entity}` (plural, e.g., `users`, `groups`, `departments`)

**Example Schema:**

```sql
CREATE TABLE silver.users (
    -- Business primary key
    uniqname VARCHAR(50) PRIMARY KEY,
    
    -- Surrogate keys (optional)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),
    user_key BIGSERIAL UNIQUE,               -- For performance-critical joins
    
    -- External system identifiers (for write-back)
    tdx_user_uid UUID,
    umich_empl_id VARCHAR(50),
    ldap_uid_number VARCHAR(50),
    ad_object_guid VARCHAR(255),
    
    -- Core fields (merged with priority)
    first_name VARCHAR(255),                 -- Priority: TDX > UMAPI > LDAP > AD
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    primary_email VARCHAR(255),
    
    -- Foreign keys (nullable, no enforcement)
    department_id VARCHAR(50),               -- Logical FK to silver.departments
    
    -- Complex fields (JSONB for arrays)
    department_ids JSONB DEFAULT '[]'::jsonb,
    job_codes JSONB DEFAULT '[]'::jsonb,
    mcommunity_ou_affiliations JSONB DEFAULT '[]'::jsonb,
    
    -- Data quality
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    
    -- Source tracking
    source_system VARCHAR(100) NOT NULL,     -- 'tdx+umapi+mcom+ad'
    entity_hash VARCHAR(64) NOT NULL,
    
    -- Standard metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);
```

**Transformation Pattern (dbt - PREFERRED):**

```sql
-- models/silver/consolidated/users.sql
{{ config(materialized='table') }}

WITH tdx AS (
    SELECT * FROM {{ ref('tdx_users') }}
),

umapi AS (
    SELECT * FROM {{ ref('umapi_employees') }}
),

mcom AS (
    SELECT * FROM {{ ref('mcommunity_users') }}
),

ad AS (
    SELECT * FROM {{ ref('ad_users') }}
),

merged_users AS (
    SELECT
        -- Business key (first non-null from any source)
        COALESCE(tdx.uniqname, umapi.uniqname, mcom.uniqname, ad.uniqname) AS uniqname,
        
        -- External IDs (for write-back)
        tdx.tdx_user_uid,
        umapi.empl_id AS umich_empl_id,
        mcom.uid_number AS ldap_uid_number,
        ad.object_guid AS ad_object_guid,
        
        -- Field merge with priority (TDX > UMAPI > MCom > AD)
        COALESCE(tdx.first_name, umapi.first_name, mcom.given_name, ad.given_name) AS first_name,
        COALESCE(tdx.last_name, umapi.last_name, mcom.sn, ad.sn) AS last_name,
        COALESCE(tdx.full_name, umapi.full_name, mcom.display_name, ad.display_name) AS full_name,
        COALESCE(tdx.primary_email, umapi.email, mcom.mail, ad.mail) AS primary_email,
        
        -- Department (from multiple sources)
        COALESCE(tdx.department_id, umapi.department_id) AS department_id,
        
        -- JSONB arrays
        COALESCE(umapi.department_ids, '[]'::jsonb) AS department_ids,
        COALESCE(umapi.job_codes, '[]'::jsonb) AS job_codes,
        COALESCE(mcom.ou_affiliations, '[]'::jsonb) AS mcommunity_ou_affiliations,
        
        -- Source tracking
        CONCAT_WS('+',
            CASE WHEN tdx.uniqname IS NOT NULL THEN 'tdx' END,
            CASE WHEN umapi.uniqname IS NOT NULL THEN 'umapi' END,
            CASE WHEN mcom.uniqname IS NOT NULL THEN 'mcom' END,
            CASE WHEN ad.uniqname IS NOT NULL THEN 'ad' END
        ) AS source_system,
        
        -- Quality score
        {{ calculate_quality_score(
            has_tdx='tdx.uniqname IS NOT NULL',
            has_umapi='umapi.uniqname IS NOT NULL',
            has_email='COALESCE(tdx.primary_email, umapi.email) IS NOT NULL',
            has_department='COALESCE(tdx.department_id, umapi.department_id) IS NOT NULL'
        ) }} AS data_quality_score,
        
        -- Quality flags
        ARRAY_REMOVE(ARRAY[
            CASE WHEN tdx.uniqname IS NULL THEN 'missing_tdx_source' END,
            CASE WHEN umapi.uniqname IS NULL THEN 'missing_umapi_source' END,
            CASE WHEN COALESCE(tdx.primary_email, umapi.email) IS NULL THEN 'missing_email' END
        ], NULL)::jsonb AS quality_flags,
        
        -- Content hash
        {{ dbt_utils.surrogate_key([
            'COALESCE(tdx.uniqname, umapi.uniqname, mcom.uniqname, ad.uniqname)',
            'source_system'
        ]) }} AS entity_hash,
        
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM tdx
    FULL OUTER JOIN umapi ON tdx.uniqname = umapi.uniqname
    FULL OUTER JOIN mcom ON COALESCE(tdx.uniqname, umapi.uniqname) = mcom.uniqname
    FULL OUTER JOIN ad ON COALESCE(tdx.uniqname, umapi.uniqname, mcom.uniqname) = ad.uniqname
)

SELECT * FROM merged_users
```

**dbt Helper Macros:**

```sql
-- macros/calculate_quality_score.sql
{% macro calculate_quality_score(has_tdx, has_umapi, has_email, has_department) %}
    (1.0
     - CASE WHEN NOT ({{ has_tdx }}) THEN 0.10 ELSE 0.0 END
     - CASE WHEN NOT ({{ has_umapi }}) THEN 0.10 ELSE 0.0 END
     - CASE WHEN NOT ({{ has_email }}) THEN 0.05 ELSE 0.0 END
     - CASE WHEN NOT ({{ has_department }}) THEN 0.05 ELSE 0.0 END
    )::DECIMAL(3,2)
{% endmacro %}
```

### Tier 3: Composite Business Entity Tables

**Purpose:** Business-level aggregations that combine multiple consolidated tables

**Characteristics:**
- Represents business concepts (labs, projects, portfolios)
- Aggregates data from multiple Tier 2 tables
- Has many-to-many relationships (via junction tables)
- Includes pre-computed metrics for performance
- May combine dbt SQL (aggregation) + Python (algorithms)
- Requires data quality scoring

**Examples:**
- `silver.labs` — Research labs (awards + OUs + members + computers)
- `silver.lab_members` — Junction table (labs ↔ users)
- `silver.lab_managers` — Identified managers (algorithm-generated)
- Future: `silver.pi_research_groups`, `silver.department_portfolios`

**See [Composite Entity Patterns](#composite-entity-patterns) section for detailed patterns.**

---

## Composite Entity Patterns

### Definition

**Composite silver entities** are business-level aggregations that combine data from multiple consolidated silver tables to represent higher-order business concepts.

They differ from consolidated tables:

| Aspect | Consolidated Table | Composite Table |
|--------|-------------------|-----------------|
| **Input** | Multiple source-specific tables (same entity) | Multiple consolidated tables (different entities) |
| **Purpose** | Cross-source entity resolution | Business-level aggregation |
| **Example** | `silver.users` (TDX+UMAPI+MCom+AD users) | `silver.labs` (awards+OUs+members+computers) |
| **Relationships** | 1:1 with source records | Many-to-many (junctions needed) |
| **Metrics** | Source fields merged | Pre-computed aggregates |

### When to Create Composite Tables

**Decision Matrix:**

✅ **CREATE TABLE when:**
- Represents recognized business concept (lab, project, compliance unit)
- Requires aggregation across multiple consolidated tables
- Has complex business rules for entity identification
- Needs data quality scoring across merged sources
- High query frequency justifies materialization (>100/day)
- Batch updates acceptable (hourly/daily refresh)

❌ **CREATE VIEW when:**
- Simple joins without aggregation logic
- Ad-hoc reporting queries
- Low query frequency (<100/day)
- No complex business rules
- Real-time data freshness required

**Examples:**

| Entity | Type | Rationale |
|--------|------|-----------|
| **silver.labs** | ✅ TABLE | Complex (awards+OUs), frequent queries, quality scoring |
| **silver.v_lab_summary** | ✅ VIEW | Simple join (labs+users+depts), enrichment only |
| **silver.lab_members** | ✅ TABLE | Junction table, many-to-many relationship |
| **silver.v_labs_monitored** | ✅ VIEW | Filtered subset, low overhead |
| **silver.pi_research_groups** | ✅ TABLE (future) | Aggregates across labs+groups+computers |
| **silver.v_department_labs** | ✅ VIEW | Roll-up query, not frequently used |

### Standard Composite Entity Schema

```sql
CREATE TABLE silver.{composite_entity} (
    -- Business primary key
    {entity}_id VARCHAR PRIMARY KEY,
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),  -- Optional surrogate
    
    -- Core identifying fields
    {key_attribute} VARCHAR NOT NULL,
    {entity}_name VARCHAR(255),
    
    -- Aggregated metrics (from related entities)
    {related}_count INTEGER DEFAULT 0,
    total_{metric} NUMERIC(15,2) DEFAULT 0.00,
    
    -- Relationship tracking (JSONB arrays of IDs)
    related_{entity}_ids JSONB DEFAULT '[]'::jsonb,
    
    -- Multi-source data flags
    has_{source1}_data BOOLEAN DEFAULT false,
    has_{source2}_data BOOLEAN DEFAULT false,
    data_source VARCHAR(50) NOT NULL,  -- 'source1+source2', 'source1_only'
    
    -- Activity status
    is_active BOOLEAN DEFAULT true,
    
    -- Data quality (REQUIRED for all composite entities)
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    
    -- Source tracking (REQUIRED)
    source_system VARCHAR(100) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,
    
    -- Standard timestamps (REQUIRED)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);

-- Standard indexes
CREATE INDEX idx_{entity}_silver_id ON silver.{entity} (silver_id);
CREATE INDEX idx_{entity}_quality ON silver.{entity} (data_quality_score DESC);
CREATE INDEX idx_{entity}_source ON silver.{entity} (source_system, data_source);
CREATE INDEX idx_{entity}_active ON silver.{entity} (is_active) WHERE is_active = true;
CREATE INDEX idx_{entity}_related_gin ON silver.{entity} USING gin (related_{entity}_ids);

-- Update trigger
CREATE TRIGGER update_{entity}_updated_at
    BEFORE UPDATE ON silver.{entity}
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

### Junction Table Pattern

**Purpose:** Represent many-to-many relationships between composite entities and other entities

**Standard Schema:**

```sql
CREATE TABLE silver.{composite}_{related} (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Foreign keys
    {composite}_id VARCHAR NOT NULL REFERENCES silver.{composite}({composite}_id) ON DELETE CASCADE,
    {related}_id VARCHAR NOT NULL REFERENCES silver.{related}({related}_id) ON DELETE CASCADE,
    
    -- Relationship metadata
    relationship_type VARCHAR,  -- Optional: 'manager', 'member', 'owner'
    relationship_role VARCHAR,  -- Optional: job title, award role
    
    -- Denormalized fields for performance (optional)
    {related}_name VARCHAR(255),
    {related}_email VARCHAR(255),
    
    -- Flags
    is_primary BOOLEAN DEFAULT false,
    is_verified BOOLEAN DEFAULT false,
    
    -- Source tracking
    source_system VARCHAR(50) NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicates
    UNIQUE({composite}_id, {related}_id, relationship_type)
);

-- Indexes
CREATE INDEX idx_{composite}_{related}_composite 
ON silver.{composite}_{related} ({composite}_id);

CREATE INDEX idx_{composite}_{related}_related 
ON silver.{composite}_{related} ({related}_id);

CREATE INDEX idx_{composite}_{related}_type 
ON silver.{composite}_{related} (relationship_type) 
WHERE relationship_type IS NOT NULL;
```

**Example: silver.lab_members**

```sql
CREATE TABLE silver.lab_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lab_id VARCHAR NOT NULL REFERENCES silver.labs(lab_id) ON DELETE CASCADE,
    member_uniqname VARCHAR NOT NULL REFERENCES silver.users(uniqname) ON DELETE CASCADE,
    
    -- Role information
    member_role VARCHAR,      -- From job_title (user's actual role)
    award_role VARCHAR,       -- From lab_award (PI, Co-I, Research Assistant)
    is_pi BOOLEAN DEFAULT false,
    is_investigator BOOLEAN DEFAULT false,
    
    -- Denormalized for performance
    member_full_name VARCHAR(255),
    member_department_id VARCHAR(50),
    member_job_title TEXT,
    
    -- Source tracking
    source_system VARCHAR(50),  -- 'lab_groups', 'lab_groups+lab_award'
    source_group_ids JSONB DEFAULT '[]'::jsonb,
    source_award_ids JSONB DEFAULT '[]'::jsonb,
    
    -- Flags
    silver_user_exists BOOLEAN DEFAULT true,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(lab_id, member_uniqname, member_role)
);

-- Indexes
CREATE INDEX idx_lab_members_lab ON silver.lab_members (lab_id);
CREATE INDEX idx_lab_members_uniqname ON silver.lab_members (member_uniqname);
CREATE INDEX idx_lab_members_pi ON silver.lab_members (lab_id, is_pi) WHERE is_pi = true;
```

### Aggregated Metrics Pattern

**Purpose:** Pre-compute expensive aggregations for performance

**Pattern (dbt - PREFERRED):**

```sql
-- models/silver/composite/labs.sql

-- CTE: Compute aggregated metrics
WITH lab_member_counts AS (
    SELECT
        lab_id,
        COUNT(*) AS member_count,
        COUNT(*) FILTER (WHERE is_pi = true) AS pi_count,
        COUNT(*) FILTER (WHERE is_investigator = true) AS investigator_count
    FROM {{ ref('lab_members') }}
    GROUP BY lab_id
),

lab_computer_counts AS (
    SELECT
        lab_id,
        COUNT(*) AS computer_count,
        COUNT(*) FILTER (WHERE is_compliant = true) AS compliant_computer_count
    FROM {{ ref('computer_labs') }}  -- Junction table
    GROUP BY lab_id
),

-- Main entity
labs_base AS (
    SELECT * FROM {{ ref('labs_from_awards_and_ous') }}  -- Previous CTE
)

-- Merge aggregations
SELECT
    l.*,
    COALESCE(lm.member_count, 0) AS member_count,
    COALESCE(lm.pi_count, 0) AS pi_count,
    COALESCE(lm.investigator_count, 0) AS investigator_count,
    COALESCE(lc.computer_count, 0) AS computer_count
FROM labs_base l
LEFT JOIN lab_member_counts lm ON l.lab_id = lm.lab_id
LEFT JOIN lab_computer_counts lc ON l.lab_id = lc.lab_id
```

**Pattern (Python - CURRENT):**

```python
def _update_lab_member_counts(self):
    """Update aggregated member counts for all labs."""
    query = """
        UPDATE silver.labs l
        SET
            member_count = (
                SELECT COUNT(*) FROM silver.lab_members 
                WHERE lab_id = l.lab_id
            ),
            pi_count = (
                SELECT COUNT(*) FROM silver.lab_members 
                WHERE lab_id = l.lab_id AND is_pi = true
            ),
            investigator_count = (
                SELECT COUNT(*) FROM silver.lab_members 
                WHERE lab_id = l.lab_id AND is_investigator = true
            ),
            updated_at = CURRENT_TIMESTAMP
    """
    self.db.execute(query)
```

**Decision: Inline vs Function**

| Approach | Pros | Cons | Recommendation |
|----------|------|------|----------------|
| **Inline (dbt CTE)** | Single transaction, always consistent | Recomputes every time | ✅ **PREFERRED for dbt** |
| **Separate UPDATE** | Incremental updates possible | Can get out of sync | ⚠️ Acceptable for Python |
| **Database function** | Reusable | Hard to test, vendor lock-in | ❌ Avoid |
| **Materialized view** | Automatic refresh | PostgreSQL refresh limitations | ⚠️ Consider for read-heavy |

### Multi-Source Merge Pattern

**Purpose:** Track which sources contributed to composite record

```sql
-- Track data sources
data_source VARCHAR(50) NOT NULL,  -- 'award+ou', 'award_only', 'ou_only'
has_award_data BOOLEAN DEFAULT false,
has_ou_data BOOLEAN DEFAULT false,
source_system VARCHAR(100) NOT NULL,  -- 'lab_award+organizational_unit'

-- Merge logic
CASE
    WHEN award.lab_id IS NOT NULL AND ou.lab_id IS NOT NULL THEN 'award+ou'
    WHEN award.lab_id IS NOT NULL THEN 'award_only'
    WHEN ou.lab_id IS NOT NULL THEN 'ou_only'
END AS data_source,

CONCAT_WS('+',
    CASE WHEN award.lab_id IS NOT NULL THEN 'lab_award' END,
    CASE WHEN ou.lab_id IS NOT NULL THEN 'organizational_unit' END
) AS source_system
```

**Quality Scoring for Multi-Source:**

```sql
CASE
    WHEN has_award_data AND has_ou_data THEN 1.00          -- Perfect: Both sources
    WHEN has_award_data AND computer_count = 0 THEN 0.85   -- Good: Awards but no infrastructure
    WHEN has_ou_data AND award_count = 0 THEN 0.75         -- Fair: Infrastructure but no funding
    WHEN has_award_data ONLY THEN 0.70                      -- Acceptable: One source
    WHEN has_ou_data ONLY THEN 0.65
    ELSE 0.50                                               -- Poor: Missing critical data
END AS data_quality_score
```

---

## View Patterns

### View Categories

**1. Enrichment Views** — Join composite with related entities for display

```sql
-- silver.v_lab_summary
CREATE OR REPLACE VIEW silver.v_lab_summary AS
SELECT
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    u.full_name AS pi_full_name,
    u.primary_email AS pi_email,
    d.department_name AS primary_department_name,
    l.total_award_dollars,
    l.member_count,
    l.computer_count,
    l.data_quality_score
FROM silver.labs l
LEFT JOIN silver.users u ON l.pi_uniqname = u.uniqname
LEFT JOIN silver.departments d ON l.primary_department_id = d.dept_id;
```

**Use case:** Display-ready data for UIs, simple reports

**2. Aggregation Views** — Roll up metrics to higher level

```sql
-- silver.v_department_labs
CREATE OR REPLACE VIEW silver.v_department_labs AS
SELECT
    d.dept_id,
    d.department_name,
    COUNT(l.lab_id) AS lab_count,
    SUM(l.total_award_dollars) AS total_funding,
    SUM(l.member_count) AS total_lab_members,
    AVG(l.data_quality_score) AS avg_data_quality
FROM silver.departments d
LEFT JOIN silver.labs l ON d.dept_id = l.primary_department_id
GROUP BY d.dept_id, d.department_name;
```

**Use case:** Executive dashboards, departmental reporting

**3. Filtered Production Views** — Apply business rules for specific use cases

```sql
-- silver.v_labs_monitored
CREATE OR REPLACE VIEW silver.v_labs_monitored AS
SELECT *
FROM silver.labs
WHERE is_active = true
  AND computer_count > 0
  AND primary_department_id IN (
      SELECT dept_id FROM silver.departments WHERE tdx_id IS NOT NULL
  )
  AND data_quality_score >= 0.70;

COMMENT ON VIEW silver.v_labs_monitored IS 
'Production-ready filtered view of labs for compliance monitoring. 
Excludes inactive labs, labs without computers, and low-quality records.';
```

**Use case:** Production workflows, compliance automation

### View vs Materialized View vs Table

| Criterion | View | Materialized View | Table |
|-----------|------|-------------------|-------|
| **Data freshness** | Real-time | Refresh interval | Transformation interval |
| **Query performance** | Slow (recomputes) | Fast (pre-computed) | Fast (pre-computed) |
| **Storage cost** | Zero | Medium | Medium-High |
| **Maintenance** | Automatic | `REFRESH MATERIALIZED VIEW` | Transformation script |
| **Indexes** | Not supported | Supported | Supported |
| **Use case** | Ad-hoc queries | High-frequency dashboards | Core entities |

**Decision Matrix:**

```
How often is this queried?
├─ Low (<10/day) → Regular VIEW
└─ High (>100/day)
   Can data be slightly stale?
   ├─ YES (minutes/hours) → MATERIALIZED VIEW
   └─ NO (must be real-time) → Regular VIEW (optimize query)
```

**Materialized View Example:**

```sql
CREATE MATERIALIZED VIEW silver.mv_lab_summary AS
SELECT
    l.lab_id,
    l.lab_name,
    COUNT(lm.membership_id) AS member_count,
    SUM(c.computer_count) AS total_computers
FROM silver.labs l
LEFT JOIN silver.lab_members lm ON l.lab_id = lm.lab_id
LEFT JOIN silver.computer_labs cl ON l.lab_id = cl.lab_id
GROUP BY l.lab_id, l.lab_name;

-- Index materialized view
CREATE INDEX idx_mv_lab_summary_lab_id ON silver.mv_lab_summary(lab_id);

-- Refresh strategy (in orchestration)
REFRESH MATERIALIZED VIEW CONCURRENTLY silver.mv_lab_summary;
```

### View Naming Conventions

```
silver.v_{entity}_{purpose}

Enrichment:   silver.v_lab_summary
Aggregation:  silver.v_department_labs
Filtered:     silver.v_labs_monitored, silver.v_labs_refined

Materialized: silver.mv_{entity}_{purpose}
```

---

## Database Functions vs Application Logic

### Modern Best Practice: Minimize DB Functions

**Industry Consensus (2024-2025):**
- Prefer application-layer logic (Python) or dbt models
- Reserve database functions for simple helpers only
- Complex business logic belongs in version-controlled code

**Rationale:**

✅ **Application Logic (Python):**
- Unit testable with pytest
- Version controlled in Git with PR reviews
- Easier to debug (standard debuggers, logging)
- Team can collaborate (familiar languages)
- Can integrate ML libraries, external APIs

❌ **Database Functions (PL/pgSQL):**
- Hard to test (requires manual verification queries)
- Difficult to debug (limited error messages)
- Vendor lock-in (PostgreSQL specific)
- Specialized skill set required
- No built-in CI/CD integration

### Decision Matrix

| Use Case | DB Function | Python Service | dbt Model |
|----------|-------------|----------------|-----------|
| **Simple aggregate update** | ✅ Acceptable | ❌ Overhead | ✅ **PREFERRED** |
| **Complex scoring (50+ lines)** | ❌ Hard to maintain | ✅ **REQUIRED** | ❌ Not suitable |
| **Multi-table aggregation** | ❌ Hard to debug | ❌ Inefficient | ✅ **PREFERRED** |
| **Row-level security** | ✅ DB native | ❌ Security risk | ❌ Wrong layer |
| **Audit triggers** | ✅ DB native | ❌ Can miss events | ❌ Not applicable |
| **ML model scoring** | ❌ Unsupported | ✅ **REQUIRED** | ⚠️ dbt Python models |
| **Content hashing** | ❌ Slow | ✅ **REQUIRED** | ❌ Unsupported |
| **Data quality tests** | ❌ Manual | ✅ pytest | ✅ **PREFERRED** (dbt tests) |

### Case Study: Lab Manager Identification

**Current Implementation (Anti-Pattern):**

`docker/postgres/migrations/011_add_lab_managers.sql` contains a 150+ line PL/pgSQL function:

```sql
CREATE OR REPLACE FUNCTION populate_lab_managers(p_lab_id VARCHAR DEFAULT NULL)
RETURNS TABLE(...) AS $$
DECLARE
    -- 20+ variable declarations
BEGIN
    -- 150+ lines of PL/pgSQL with nested IF statements
    
    IF p_member_role ILIKE '%Lab Manager%' OR p_job_codes::text LIKE '%102945%' THEN
        -- Score = 1
    ELSIF p_member_role ILIKE '%Coordinator%' THEN
        -- Score = 2
    -- ... 10 more conditions
    END IF;
END;
$$ LANGUAGE plpgsql;
```

**Problems:**
- ❌ No unit tests (requires manual SQL verification)
- ❌ Hard to debug (no console.log, limited error messages)
- ❌ Can't iterate on scoring (requires migration for every change)
- ❌ Tightly coupled to PostgreSQL
- ❌ Business logic hidden in database

**Recommended Pattern (Python):**

`scripts/database/enrichment/identify_lab_managers.py`:

```python
from dataclasses import dataclass
from typing import List, Tuple, Optional, Callable

@dataclass
class ScoringRule:
    """Represents a manager identification scoring rule."""
    priority: int
    matcher: Callable[[str, List[str]], bool]
    reason: str

class LabManagerIdentificationService:
    """
    Identifies lab managers using configurable scoring rules.
    
    Scoring Algorithm:
    - Priority 1 (Score 1): Explicit manager roles or job codes
    - Priority 2 (Score 2): Coordinators
    - Priority 5 (Score 5): Research Fellows
    - Priority 10 (Score 10): Graduate students (fallback)
    """
    
    # Scoring rules are version controlled and easy to modify
    SCORING_RULES = [
        ScoringRule(1, 
                   lambda r, c: 'Lab Manager' in r or '102945' in c,
                   'Explicit Lab Manager'),
        ScoringRule(1,
                   lambda r, c: 'Lab Coordinator' in r or '102946' in c,
                   'Lab Coordinator'),
        ScoringRule(2,
                   lambda r, c: r == 'Admin Coord/Project Coord',
                   'Administrative/Project Coordinator'),
        ScoringRule(5,
                   lambda r, c: r.startswith('Research Fellow'),
                   'Research Fellow'),
        ScoringRule(10,
                   lambda r, c: 'Graduate Student' in r,
                   'Graduate Student (fallback)'),
    ]
    
    def calculate_score(
        self, 
        member_role: str, 
        job_codes: List[str]
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Calculate confidence score for potential lab manager.
        
        Args:
            member_role: Member's job title
            job_codes: List of job codes (e.g., ['102945', '041111'])
        
        Returns:
            Tuple of (confidence_score, detection_reason) or (None, None)
        
        Examples:
            >>> svc.calculate_score('Lab Manager', ['102945'])
            (1, 'Explicit Lab Manager')
            
            >>> svc.calculate_score('Graduate Student Research Assistant', [])
            (10, 'Graduate Student (fallback)')
        """
        for rule in self.SCORING_RULES:
            if rule.matcher(member_role, job_codes):
                return (rule.priority, rule.reason)
        
        return (None, None)  # Not eligible
    
    def identify_managers(
        self, 
        lab_id: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Identify up to 3 managers per lab.
        
        Algorithm:
        1. Query eligible members from silver.v_eligible_lab_members
        2. Score each member using calculate_score()
        3. Rank by score (lower is better), then by role name
        4. Select top 3 per lab
        5. Insert/update silver.lab_managers
        
        Args:
            lab_id: Specific lab to process (None = all labs)
            dry_run: If True, log but don't insert
        
        Returns:
            Statistics dict
        """
        # Implementation...
```

**Benefits:**
- ✅ Unit testable (pytest with fixtures)
- ✅ Version controlled (Git)
- ✅ Easy to debug (print statements, debugger)
- ✅ Easy to modify scoring weights
- ✅ Can add ML models later
- ✅ Team collaboration (Python familiar)

### When Functions Are Acceptable

✅ **Simple aggregate updaters:**

```sql
CREATE FUNCTION silver.update_lab_member_counts(p_lab_id VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE silver.labs
    SET member_count = (SELECT COUNT(*) FROM silver.lab_members WHERE lab_id = p_lab_id)
    WHERE lab_id = p_lab_id;
END;
$$ LANGUAGE plpgsql;
```

**Why acceptable:** Simple (10 lines), no complex logic, called from application

✅ **Audit triggers:**

```sql
CREATE TRIGGER audit_lab_changes
AFTER UPDATE ON silver.labs
FOR EACH ROW
EXECUTE FUNCTION log_table_change();
```

✅ **Row-level security:**

```sql
CREATE POLICY lab_department_access ON silver.labs
FOR SELECT
USING (department_id = current_setting('app.user_department'));
```

---

## Transformation Patterns

### dbt Model Structure (PREFERRED - Future)

**File structure:**

```
models/silver/
├── source_specific/
│   ├── tdx_users.sql
│   ├── umapi_employees.sql
│   └── schema.yml
├── consolidated/
│   ├── users.sql
│   ├── departments.sql
│   └── schema.yml
├── composite/
│   ├── labs.sql
│   ├── lab_members.sql
│   └── schema.yml
└── views/
    ├── v_lab_summary.sql
    └── v_labs_monitored.sql
```

**Model template:**

```sql
{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['id'], 'unique': True},
            {'columns': ['quality_score']}
        ],
        tags=['silver', 'consolidated']
    )
}}

-- Import dependencies using ref()
WITH source_1 AS (
    SELECT * FROM {{ ref('source_specific_table_1') }}
),

source_2 AS (
    SELECT * FROM {{ ref('source_specific_table_2') }}
),

-- Transformation logic
merged AS (
    SELECT
        COALESCE(s1.id, s2.id) AS id,
        COALESCE(s1.name, s2.name) AS name,
        -- Field merge logic
    FROM source_1 s1
    FULL OUTER JOIN source_2 s2 ON s1.id = s2.id
),

-- Quality scoring
final AS (
    SELECT
        *,
        {{ calculate_quality_score(...) }} AS data_quality_score
    FROM merged
)

SELECT * FROM final
```

**Testing (models/silver/schema.yml):**

```yaml
version: 2

models:
  - name: users
    description: "Consolidated user records"
    columns:
      - name: uniqname
        description: "Primary business key"
        tests:
          - unique
          - not_null
      
      - name: tdx_user_uid
        tests:
          - relationships:
              to: ref('tdx_users')
              field: tdx_user_uid
      
      - name: data_quality_score
        tests:
          - dbt_utils.accepted_range:
              min_value: 0.0
              max_value: 1.0
```

### Python Service Structure (CURRENT)

**File:** `scripts/database/silver/012_transform_users.py`

```python
class UserConsolidationService:
    """Transform bronze users into silver.users (consolidated)."""
    
    def __init__(self, db_config: Dict[str, str]):
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
    
    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """Get timestamp of last successful transformation."""
        # Query meta.ingestion_runs
    
    def _fetch_source_records(self) -> Tuple[Dict, Dict, Dict, Dict]:
        """Fetch records from source-specific tables."""
        # Query silver.tdx_users, silver.umapi_employees, etc.
    
    def _merge_user_records(self, tdx, umapi, mcom, ad) -> Dict:
        """Merge records with priority rules."""
        merged = {
            'uniqname': tdx.get('uniqname') or umapi.get('uniqname') or ...,
            'first_name': (
                tdx.get('first_name') or
                umapi.get('first_name') or
                mcom.get('given_name') or
                ad.get('given_name')
            ),
        }
        return merged
    
    def _calculate_data_quality(self, merged, sources) -> Tuple[float, List]:
        """Calculate quality score and flags."""
        score = 1.0
        flags = []
        if not sources.get('tdx'):
            score -= 0.10
            flags.append('missing_tdx_source')
        return (round(score, 2), flags)
    
    def consolidate_users(self, full_sync=False, dry_run=False):
        """Main orchestration method."""
        # Fetch, merge, upsert
```

---

## Reference Implementations

### silver.users (Consolidated Entity)

**Purpose:** Merge users from TDX, UMAPI, MCommunity, Active Directory

**Current:** Python implementation in `scripts/database/silver/012_transform_users.py`

**Future:** dbt model (see Tier 2 example above)

### silver.labs (Composite Entity - Desired State)

**Purpose:** Research labs merged from awards + OUs with member/computer associations

**Current Implementation:** Legacy Python (bypasses source-specific tables)

**Desired Flow:**

```
silver.lab_awards (source-specific)
silver.ad_organizational_units (source-specific)
    ↓ (dbt SQL aggregation)
silver.labs (composite table)
    ↓ (Python for complex member extraction)
silver.lab_members (junction table)
    ↓ (Python for scoring algorithm)
silver.lab_managers (algorithm-generated)
```

**See [Composite Entity Patterns](#composite-entity-patterns) for detailed patterns.**

---

## Field Merge Priority Rules

Use **cascading fill pattern** — always try to fill NULL values from other sources:

```python
# ✅ CORRECT: Cascading fill pattern
silver_record['first_name'] = (
    tdx_data.get('FirstName') or           # Priority 1
    umapi_data.get('FirstName') or         # Priority 2
    mcom_data.get('givenName') or          # Priority 3
    ad_data.get('givenName') or            # Priority 4
    None                                    # Only NULL if all missing
)
```

**dbt equivalent:**

```sql
COALESCE(
    tdx.first_name,
    umapi.first_name,
    mcom.given_name,
    ad.given_name
) AS first_name
```

**Key Principle:** Maximize data completeness by attempting to fill every field from all available sources.

---

## Data Quality Framework

**Quality Score Calculation (0.00 - 1.00):**

```python
score = 1.0
- 0.10 for each missing critical source
- 0.05 for each missing important field
- 0.05 for cross-source inconsistencies
- 0.05 for invalid FK references
```

**Standard Quality Flags:**

| Category | Flag Examples |
|----------|---------------|
| **Missing Sources** | `missing_tdx_source`, `missing_umapi_source` |
| **Missing Fields** | `missing_email`, `missing_department` |
| **Invalid References** | `invalid_department_reference`, `pi_not_in_users` |
| **Data Inconsistencies** | `name_mismatch_across_sources` |

**See [medallion_standards.md § Data Quality](medallion_standards.md#data-quality-framework) for complete framework.**

---

## JSONB Field Usage

**When to use JSONB:**
- ✅ Sparse data (many records have NULL)
- ✅ Heterogeneous structure (varies by record)
- ✅ Nested objects
- ✅ Arrays of objects

**When to use typed columns:**
- ❌ High-cardinality keys (primary keys, frequent WHERE/JOIN)
- ❌ Aggregations (SUM, AVG)
- ❌ Foreign keys

**See [medallion_standards.md § JSONB Usage](medallion_standards.md#jsonb-field-usage-standards) for complete patterns.**

---

## Foreign Key & Referential Integrity

**LSATS Standard:** Do NOT use PostgreSQL foreign key constraints in silver layer.

**Use:** Nullable FK columns + quality flags

```sql
-- ✅ CORRECT
ALTER TABLE silver.users
    ADD COLUMN department_id VARCHAR(50);  -- No FK constraint

-- ❌ WRONG
ALTER TABLE silver.users
    ADD CONSTRAINT fk_users_department
    FOREIGN KEY (department_id) REFERENCES silver.departments(dept_id);
```

**See [medallion_standards.md § Foreign Keys](medallion_standards.md#foreign-key--referential-integrity) for rationale.**

---

## Performance Optimization

### Indexes

```sql
-- Business key
CREATE INDEX idx_{table}_{business_key} ON silver.{table} ({business_key});

-- Quality filtering
CREATE INDEX idx_{table}_quality ON silver.{table} (data_quality_score DESC);

-- Foreign keys (even though not enforced)
CREATE INDEX idx_{table}_{fk_column} ON silver.{table} ({fk_column});

-- JSONB fields (GIN indexes)
CREATE INDEX idx_{table}_{jsonb_field}_gin ON silver.{table} USING gin ({jsonb_field});
```

### Batching

Use `execute_values` for bulk operations (1000x faster than individual inserts).

**See [bronze_layer_standards.md § Performance](bronze_layer_standards.md#performance-optimization) for patterns.**

---

## Testing Strategies

### dbt Tests (PREFERRED - Future)

```yaml
# models/silver/schema.yml
models:
  - name: users
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - uniqname
    columns:
      - name: uniqname
        tests:
          - unique
          - not_null
      
      - name: data_quality_score
        tests:
          - dbt_utils.accepted_range:
              min_value: 0.0
              max_value: 1.0
```

**Run tests:**

```bash
dbt test --select users
```

### Python Manual Tests (CURRENT)

```python
# Dry-run mode
python scripts/database/silver/012_transform_users.py --dry-run

# Manual verification queries
psql -c "SELECT COUNT(*) FROM silver.users WHERE first_name IS NULL"
psql -c "SELECT AVG(data_quality_score) FROM silver.users"
```

---

## Migration Guide: Python → dbt

### Phase 1: Setup (Week 1)

```bash
# Install dbt-postgres
pip install dbt-core dbt-postgres

# Initialize project
dbt init lsats_data_hub

# Configure profiles.yml
vim ~/.dbt/profiles.yml
```

**profiles.yml:**

```yaml
lsats_data_hub:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: lsats_user
      password: "{{ env_var('DB_PASSWORD') }}"
      port: 5432
      dbname: lsats_db
      schema: silver
      threads: 4
```

### Phase 2: Pilot (Week 2-3)

Migrate `silver.departments` (simplest):

```sql
-- models/silver/consolidated/departments.sql
{{ config(materialized='table') }}

WITH tdx AS (
    SELECT * FROM {{ ref('tdx_departments') }}
),

umapi AS (
    SELECT * FROM {{ ref('umapi_departments') }}
)

SELECT
    COALESCE(tdx.dept_id, umapi.dept_id) AS dept_id,
    COALESCE(tdx.department_name, umapi.department_name) AS department_name,
    -- Merge logic
FROM tdx
FULL OUTER JOIN umapi ON tdx.dept_id = umapi.dept_id
```

### Phase 3: Expand (Q3 2025)

Migrate users, groups, computers.

### Phase 4: Composite Entities (Q4 2025)

Migrate labs, keep manager scoring in Python.

### Phase 5: Gold Layer (2026)

Implement star schema analytics tables.

---

## Summary

**Silver layer follows these principles:**

1. **Three-tier architecture** — Source-specific → Consolidated → Composite
2. **SQL for transformations** — dbt preferred (future), Python current
3. **Python for algorithms** — Complex scoring, ML models, hashing
4. **Composite entities** — Business-level aggregations with junction tables
5. **Views for convenience** — Enrichment, aggregation, filtered subsets
6. **Quality scoring required** — All tables have 0.00-1.00 scores
7. **No DB constraints** — ETL-managed integrity with quality flags

**Key takeaways:**
- Use dbt for SQL transformations (future)
- Use Python for algorithms (current and future)
- Create composite tables for business concepts
- Use junction tables for many-to-many
- Minimize database functions
- Test automatically (dbt tests preferred)

**Next steps:**
- See [bronze_layer_standards.md](bronze_layer_standards.md) for ingestion patterns
- See [medallion_standards.md](medallion_standards.md) for cross-layer standards
- Complete composite tables in Python (Q1 2025)
- Begin dbt migration (Q2 2025)
