# LSATS Data Hub Medallion Architecture Standards

**Version:** 1.0  
**Last Updated:** 2025-01-21  
**Purpose:** Comprehensive standards for building and maintaining the Bronze-Silver-Gold data warehouse

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Naming Conventions](#naming-conventions)
3. [Bronze Layer Standards](#bronze-layer-standards)
4. [Silver Layer Standards](#silver-layer-standards)
5. [Foreign Key & Referential Integrity](#foreign-key--referential-integrity)
6. [Data Quality Framework](#data-quality-framework)
7. [Incremental Processing Requirements](#incremental-processing-requirements)
8. [Script Structure Standards](#script-structure-standards)
9. [Logging & Emoji Standards](#logging--emoji-standards)
10. [Testing & Validation](#testing--validation)

---

## Architecture Overview

LSATS Data Hub implements a **three-tier medallion architecture** for structured data warehousing:

| Layer | Purpose | Characteristics | Storage Model |
|-------|---------|----------------|---------------|
| **Bronze** | Raw data landing zone | Immutable, complete history, zero transformation | Single universal `bronze.raw_entities` table with JSONB |
| **Silver** | Cleaned & unified data | Standardized schema, entity resolution, quality-scored | Entity-specific typed tables |
| **Gold** | Business-ready aggregates | Denormalized, optimized for analytics, master records | *(Future implementation)* |

### Key Design Principles

1. **Bronze is append-only** — Never delete or modify bronze records
2. **Bronze uses single universal table** — Modern best practice for simplified management, streaming integration, and better scalability
3. **Silver is the source of truth** — All business logic queries use silver layer
4. **ETL-managed integrity** — No enforced FK constraints; use quality flags instead
5. **Incremental by default** — All scripts support incremental processing with `--full-sync` override
6. **Observable transformations** — Comprehensive logging with standardized emojis

---

## Naming Conventions

### Schema Naming

- **Bronze:** `bronze` (single schema for all raw data)
- **Silver:** `silver` (entity-specific tables organized in one schema)
- **Gold:** `gold` (future use)
- **Metadata:** `meta` (ingestion tracking, action logs)

### Table Naming

Follow **snake_case** exclusively:

| Layer | Pattern | Examples |
|-------|---------|----------|
| Bronze | `raw_entities` | Single universal table |
| Silver (Source-Specific) | `[source]_[entity]` | `tdx_users`, `mcom_groups`, `ad_computers` |
| Silver (Consolidated) | `[entity]` (plural) | `users`, `groups`, `departments`, `labs`, `computers` |
| Silver (Junction) | `[entity1]_[entity2]` | `group_members`, `lab_members`, `computer_labs` |

**Examples:**
```sql
-- Source-specific silver tables (NEW STANDARD)
silver.tdx_users          -- Users from TeamDynamix only
silver.umapi_users        -- Users from UMich API only
silver.mcom_users         -- Users from MCommunity LDAP only
silver.ad_users           -- Users from Active Directory only

-- Consolidated silver tables (merge source-specific tables)
silver.users              -- Final merged user records
silver.groups             -- Final merged group records
silver.departments        -- Final merged department records
```

### Column Naming

| Column Type | Naming Pattern | Example | Notes |
|-------------|---------------|---------|-------|
| **Business Key (Primary)** | `[entity]_id` or `uniqname` | `dept_id`, `uniqname`, `group_id` | Used as PRIMARY KEY in consolidated tables |
| **Surrogate Key** | `[entity]_key` or `silver_id` | `user_key BIGINT`, `silver_id UUID` | Optional; use for performance-critical joins |
| **Foreign Key** | `[referenced_entity]_id` | `department_id`, `pi_uniqname` | Must match referenced table's business key |
| **External System ID** | `[system]_[entity]_uid` | `tdx_user_uid`, `ad_object_guid` | For write-back operations |
| **Source Tracking** | `source_system`, `source_entity_id` | Always VARCHAR | Tracks which sources contributed |
| **Quality Metrics** | `data_quality_score`, `quality_flags` | DECIMAL(3,2), JSONB | Required on all silver tables |
| **Timestamps** | `created_at`, `updated_at`, `ingested_at` | TIMESTAMP WITH TIME ZONE | Always use timezone-aware timestamps |

**Reserved Column Names:**
- `entity_hash` — SHA-256 content hash for change detection
- `ingestion_run_id` — UUID linking to `meta.ingestion_runs`
- `raw_id` — UUID of bronze record (in source-specific tables)

---

## Bronze Layer Standards

### Universal Raw Entity Table (Industry Best Practice)

Bronze uses a **single universal table** for all entity types. This modern approach offers:

- **Simplified Management**: One set of ACLs, lineage graphs, and retention policies instead of hundreds
- **Streaming Integration**: Single Kafka/Kinesis sink instead of dozens of independent streams  
- **Better Scalability**: PostgreSQL optimizes one large partitioned table better than many small tables
- **Operational Simplicity**: Policies like "delete records older than 90 days" apply universally

```sql
CREATE TABLE bronze.raw_entities (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type VARCHAR(50) NOT NULL,        -- 'user', 'group', 'department', etc.
    source_system VARCHAR(50) NOT NULL,      -- 'tdx', 'mcommunity_ldap', 'umich_api', etc.
    external_id VARCHAR(255) NOT NULL,       -- ID from source system
    raw_data JSONB NOT NULL,                 -- Complete unmodified data
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    entity_hash VARCHAR(64) GENERATED ALWAYS AS (...) STORED,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    ingestion_metadata JSONB DEFAULT '{}'::jsonb
);
```

### Bronze Ingestion Rules

1. **Store complete records** — Include ALL fields from source, even if seemingly irrelevant
2. **Never transform** — Store data exactly as received (except metadata enrichment)
3. **Content hashing** — Calculate `_content_hash` for sources without timestamps:
   ```python
   # IMPORTANT: Only include fields that represent meaningful data changes
   # EXCLUDE metadata fields that change automatically (timestamps, counters, sync fields)
   significant_fields = {
       'DeptId': data.get('DeptId', '').strip(),
       'DeptDescription': data.get('DeptDescription', '').strip(),
       # Include all business-relevant fields
       # EXCLUDE: 'uSNChanged', 'whenChanged', 'modifyTimestamp', internal IDs
   }
   normalized_json = json.dumps(significant_fields, sort_keys=True, separators=(',', ':'))
   content_hash = hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()
   ```
   
   **Fields to EXCLUDE from content hashing:**
   - Active Directory: `uSNChanged`, `uSNCreated`, `dSCorePropagationData`, `lastLogonTimestamp`
   - LDAP: `modifyTimestamp`, `entryCSN`, internal sequence numbers
   - Database: Auto-increment IDs, `last_modified_by`, sync tracking fields
   - Reason: These change on every sync without meaningful data updates
4. **Metadata enrichment** — Add computed fields prefixed with `_`:
   - `_content_hash` — For change detection
   - `_hierarchical_path` — For organizational data
   - `_extracted_uniqname` — For parsed identifiers
   - `_source_file` — For CSV/batch imports

### Change Detection Strategies

| Source Type | Method | Implementation |
|-------------|--------|----------------|
| **APIs without timestamps** | Content hashing | Calculate hash of significant fields; only insert if hash changed |
| **LDAP/AD** | Timestamp-based | Use `whenChanged` / `modifyTimestamp` fields |
| **Batch files** | File hashing | Hash entire file; skip if file hash unchanged |

**Example: Content Hash Ingestion**
```python
existing_hashes = self._get_existing_department_hashes()
current_hash = self._calculate_department_content_hash(dept_data)

if existing_hash := existing_hashes.get(dept_id):
    if existing_hash == current_hash:
        logger.debug(f"⏭️  Department unchanged, skipping: {dept_name}")
        stats['records_skipped_unchanged'] += 1
        continue
    else:
        logger.info(f"📝 Department changed: {dept_name}")
        stats['changed_departments'] += 1
else:
    logger.info(f"🆕 New department detected: {dept_name}")
    stats['new_departments'] += 1
```

---

## Silver Layer Standards

### Two-Tier Silver Architecture (NEW STANDARD)

The silver layer now uses a **two-tier approach** for complex multi-source entities:

```
Bronze Layer (raw_entities)
    ↓
Tier 1: Source-Specific Silver Tables
    ├─ silver.tdx_users
    ├─ silver.umapi_users
    ├─ silver.mcom_users
    └─ silver.ad_users
    ↓
Tier 2: Consolidated Silver Tables
    └─ silver.users (merges all source-specific tables)
```

### When to Use Source-Specific Tables

**Use source-specific silver tables when:**
- Entity data comes from **2 or more sources**
- Sources have conflicting field structures
- Complex merge logic is required
- You need to audit which specific source provided each field
- Sources may be added/removed over time

**Skip directly to consolidated when:**
- Entity has only **1 source**
- No future sources are planned

### Source-Specific Silver Table Structure

```sql
-- Example: silver.tdx_users (source-specific)
CREATE TABLE silver.tdx_users (
    -- Primary identifier (source-specific)
    tdx_user_uid UUID PRIMARY KEY,           -- Source's native ID
    
    -- Business key (for matching across sources)
    uniqname VARCHAR(50) NOT NULL,           -- Normalized identifier
    
    -- Source-specific fields (typed, not JSONB)
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    primary_email VARCHAR(255),
    tdx_is_active BOOLEAN,
    -- ... all TDX-specific fields typed out
    
    -- Traceability
    raw_id UUID NOT NULL,                    -- Link to bronze.raw_entities
    raw_data_snapshot JSONB,                 -- Optional: copy of raw_data for audit
    
    -- Standard metadata
    source_system VARCHAR(50) DEFAULT 'tdx',
    entity_hash VARCHAR(64) NOT NULL,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index on business key for joining
CREATE UNIQUE INDEX idx_tdx_users_uniqname ON silver.tdx_users (uniqname);
CREATE INDEX idx_tdx_users_raw_id ON silver.tdx_users (raw_id);
```

### Consolidated Silver Table Structure

```sql
-- Example: silver.users (consolidated)
CREATE TABLE silver.users (
    -- Primary business key
    uniqname VARCHAR(50) PRIMARY KEY,
    
    -- Surrogate key (optional)
    silver_id UUID UNIQUE DEFAULT uuid_generate_v4(),
    user_key BIGSERIAL UNIQUE,               -- For performance-critical joins
    
    -- External system identifiers (for write-back)
    tdx_user_uid UUID,
    umich_empl_id VARCHAR(50),
    ldap_uid_number VARCHAR(50),
    ad_object_guid VARCHAR(255),
    
    -- Core fields (merged with priority rules)
    first_name VARCHAR(255),                 -- Priority: TDX > UMAPI > LDAP
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    primary_email VARCHAR(255),
    
    -- Foreign keys (nullable, no enforcement)
    department_id VARCHAR(50),               -- References silver.departments(dept_id)
    
    -- Complex fields (JSONB for arrays)
    department_ids JSONB DEFAULT '[]'::jsonb,
    job_codes JSONB DEFAULT '[]'::jsonb,
    
    -- Data quality
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    
    -- Source tracking
    source_system VARCHAR(100) NOT NULL,     -- e.g., 'tdx+umapi+mcom+ad'
    source_entity_id VARCHAR(255) NOT NULL,
    entity_hash VARCHAR(64) NOT NULL,
    
    -- Standard metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
);
```

### Field Merge Priority Rules

Document merge priority explicitly in transformation scripts. Use **cascading fill pattern** — always try to fill NULL/None values from other sources:

```python
# CORRECT: Cascading fill pattern - try all sources to fill missing data
silver_record['first_name'] = (
    tdx_data.get('FirstName') or           # Priority 1: TDX
    umapi_data.get('FirstName') or         # Priority 2: UMAPI
    mcom_data.get('givenName') or          # Priority 3: MCommunity
    ad_data.get('givenName') or            # Priority 4: Active Directory
    None                                    # Only NULL if all sources missing
)

# CORRECT: More explicit version showing the pattern
def merge_field(field_name: str, source_priority: List[Tuple[Dict, str]]) -> Any:
    """
    Merge a field from multiple sources using priority order.
    
    Args:
        field_name: Logical field name (for logging)
        source_priority: List of (source_dict, source_field_name) tuples in priority order
    
    Returns:
        First non-null value found, or None if all sources are null
    """
    for source_data, source_field in source_priority:
        if source_data and source_data.get(source_field):
            return source_data[source_field]
    return None

# Usage:
silver_record['primary_email'] = merge_field('primary_email', [
    (tdx_data, 'PrimaryEmail'),      # Highest priority
    (umapi_data, 'Email'),
    (mcom_data, 'mail'),
    (ad_data, 'mail')                # Lowest priority
])

# WRONG: Don't stop at first source
# This would leave email NULL if TDX has no email, even if LDAP has one
silver_record['email'] = tdx_data.get('PrimaryEmail')  # ❌ Bad - doesn't fill from other sources
```

**Key Principle:** Maximize data completeness by attempting to fill every field from all available sources, respecting priority order.

### Required Indexes for Silver Tables

Every consolidated silver table MUST have:

```sql
-- Primary business key
CREATE INDEX idx_[table]_[business_key] ON silver.[table] ([business_key]);

-- Surrogate keys (if present)
CREATE INDEX idx_[table]_silver_id ON silver.[table] (silver_id);
CREATE INDEX idx_[table]_key ON silver.[table] ([entity]_key);

-- Entity hash (for change detection)
CREATE INDEX idx_[table]_entity_hash ON silver.[table] (entity_hash);

-- Source tracking
CREATE INDEX idx_[table]_source ON silver.[table] (source_system, source_entity_id);

-- Quality filtering
CREATE INDEX idx_[table]_quality ON silver.[table] (data_quality_score DESC);

-- Foreign keys (even though not enforced)
CREATE INDEX idx_[table]_[fk_column] ON silver.[table] ([fk_column]);

-- JSONB fields (GIN indexes)
CREATE INDEX idx_[table]_[jsonb_field]_gin ON silver.[table] USING gin ([jsonb_field]);
```

---

## JSONB Field Usage Standards

### When to Use JSONB vs Typed Columns

**Use JSONB for:**
- ✅ **Sparse data** — Many records have NULL/empty values (e.g., location fields)
- ✅ **Heterogeneous data** — Structure varies by record (e.g., custom attributes)
- ✅ **Nested objects** — Complex hierarchical data (e.g., API responses)
- ✅ **Flexible schemas** — Source system adds fields over time
- ✅ **Low-frequency queries** — Field queried occasionally, not in hot path
- ✅ **Arrays of objects** — Lists of structured items (e.g., attributes, memberships)

**Use Typed Columns for:**
- ❌ **High-cardinality keys** — Frequently used in WHERE/JOIN (e.g., primary keys)
- ❌ **Indexed lookups** — Heavy filtering/sorting requirements
- ❌ **Aggregations** — SUM, AVG, statistical queries
- ❌ **Foreign keys** — Referenced by other tables
- ❌ **Business-critical fields** — Core entities (dept_id, uniqname, etc.)

### JSONB Consolidation Patterns

**Pattern 1: Location/Contact Consolidation**

Instead of 10+ nullable columns with high NULL density, consolidate into JSONB:

```sql
-- ❌ BAD: Sparse nullable columns (90% NULL values)
address1 VARCHAR(255),
address2 VARCHAR(255),
address3 VARCHAR(255),
address4 VARCHAR(255),
city VARCHAR(100),
state_abbr VARCHAR(2),
state_name VARCHAR(50),
postal_code VARCHAR(20),
country VARCHAR(100),
phone VARCHAR(50),
fax VARCHAR(50),
url VARCHAR(255)

-- ✅ GOOD: JSONB consolidation
location_info JSONB DEFAULT '{}'::jsonb
```

**Transformation code:**
```python
def _build_location_info(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
    """Build location JSONB from sparse source fields."""
    location = {}
    
    # Only add fields that have values
    field_mapping = {
        "address1": "Address1",
        "address2": "Address2",
        "city": "City",
        "state_abbr": "StateAbbr",
        "postal_code": "PostalCode",
        "phone": "Phone",
        "url": "Url",
    }
    
    for json_key, source_key in field_mapping.items():
        value = source_data.get(source_key)
        if value:  # Only store non-empty values
            location[json_key] = value
    
    return location
```

**Pattern 2: Array of Custom Attributes**

For extensible custom fields (e.g., TDX Attributes, custom properties):

```sql
-- ✅ GOOD: Preserve complete API structure
attributes JSONB DEFAULT '[]'::jsonb

-- Example data:
[
  {
    "id": 1777,
    "name": "VP Area",
    "value": "Exec VP & CFO",
    "value_text": "Exec VP & CFO"
  },
  {
    "id": 1778,
    "name": "Campus Code",
    "value": "Ann Arbor",
    "value_text": "Ann Arbor"
  }
]
```

**Transformation code:**
```python
def _extract_attributes(self, source_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract attributes array from source, preserving complete structure."""
    attributes = source_data.get("Attributes")
    
    if not attributes or not isinstance(attributes, list):
        return []
    
    return attributes  # Preserve complete structure
```

**Pattern 3: Array Fields (Simple Lists)**

For lists of IDs, tags, or simple values:

```sql
-- ✅ GOOD: JSONB arrays for flexible lists
department_ids JSONB DEFAULT '[]'::jsonb,
job_codes JSONB DEFAULT '[]'::jsonb,
group_memberships JSONB DEFAULT '[]'::jsonb

-- Example data:
department_ids: ["190300", "211000", "222512"]
job_codes: ["041111", "042010"]
```

### JSONB Querying Patterns

**Check if key exists:**
```sql
-- Has location data
WHERE location_info != '{}'::jsonb

-- Has specific field
WHERE location_info ? 'city'
```

**Extract values:**
```sql
-- Text value
SELECT location_info->>'city' AS city

-- Nested value
SELECT attributes->0->>'name' AS first_attribute_name
```

**Array operations:**
```sql
-- Contains specific attribute
WHERE attributes @> '[{"name": "VP Area"}]'::jsonb

-- Array length
WHERE jsonb_array_length(attributes) > 0

-- Expand array to rows
SELECT jsonb_array_elements(department_ids) AS dept_id
```

**GIN index for performance:**
```sql
CREATE INDEX idx_table_location_gin ON silver.table USING gin(location_info);
CREATE INDEX idx_table_attributes_gin ON silver.table USING gin(attributes);
```

### JSONB Evolution Strategies

**Decision Tree: Simple vs Hybrid Approach**

1. **Start Simple** (JSONB only)
   - Use when query patterns unknown
   - Lowest implementation effort
   - Full data preservation
   - **Example:** New source-specific table

2. **Upgrade to Hybrid** (JSONB + flattened columns)
   - Add when heavy querying emerges
   - Keep original JSONB for audit
   - Add computed/flattened columns for performance
   - **Example:** Frequently-queried attributes

**Hybrid Pattern Example:**
```sql
-- Preserve complete structure
attributes_raw JSONB DEFAULT '[]'::jsonb,

-- Flattened for common queries (can be computed/generated)
attributes JSONB DEFAULT '{}'::jsonb,  -- {"vp_area": "CFO", "campus": "Ann Arbor"}

-- Or individual typed columns for hot queries
vp_area VARCHAR(255) GENERATED ALWAYS AS (attributes->>'vp_area') STORED
```

**When to migrate from Simple → Hybrid:**
- Query performance becomes bottleneck (>100ms)
- Specific fields queried in >50% of queries
- Attribute becomes business-critical (e.g., compliance flag)

### JSONB Best Practices

**✅ DO:**
- Use empty object `{}` or array `[]` as defaults (not NULL)
- Validate JSONB structure during transformation
- Add GIN indexes for queried JSONB fields
- Document expected schema structure in table comments
- Use consistent key naming (snake_case for JSON keys)

**❌ DON'T:**
- Store high-cardinality primary/foreign keys in JSONB
- Use JSONB for fields requiring frequent aggregation (SUM, AVG)
- Embed large binary data (use file storage instead)
- Create deeply nested structures (>3 levels)
- Mix data types for same key across records

**Example: JSONB Field Documentation**
```sql
COMMENT ON COLUMN silver.tdx_departments.location_info IS 
'JSONB object with address/contact fields: address1, address2, city, state_abbr, 
state_name, postal_code, country, phone, fax, url. All fields optional.';

COMMENT ON COLUMN silver.tdx_departments.attributes IS 
'JSONB array of TDX custom attributes. Each object contains: id (int), name (string), 
value (string), value_text (string). Populated via enrichment process.';
```

### Handling Missing/Invalid Data in JSONB

**Empty String Handling:**
```python
# ❌ BAD: Store empty strings
if source_data.get("City"):
    location["city"] = source_data["City"]  # Stores "" as valid value

# ✅ GOOD: Filter empty strings
value = source_data.get("City", "").strip()
if value:  # Only non-empty values
    location["city"] = value
```

**Type Validation:**
```python
def _extract_attributes(self, source_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract attributes with type validation."""
    attributes = source_data.get("Attributes")
    
    # Validate it's actually a list
    if not attributes or not isinstance(attributes, list):
        logger.warning(f"Invalid Attributes format: {type(attributes)}")
        return []
    
    return attributes
```

**Quality Flag for JSONB Issues:**
```python
# Flag records with problematic JSONB
if not silver_record.get("location_info") or silver_record["location_info"] == {}:
    quality_flags.append("no_location_data")

if not silver_record.get("is_enriched"):
    quality_flags.append("missing_attributes")
```

---

## Foreign Key & Referential Integrity

### Industry Best Practice: ETL-Managed Integrity

**LSATS Standard:** Do NOT use PostgreSQL foreign key constraints in the silver layer.

**Rationale:**
1. **Load Performance** — FK constraints slow down bulk inserts significantly
2. **ETL Flexibility** — Parent records may arrive after child records
3. **Partial Data** — Not all source systems provide complete reference data
4. **Data Quality Tracking** — Quality flags provide better observability than constraint violations

### Foreign Key Column Standards

```sql
-- ✅ CORRECT: Nullable FK with quality flag tracking
ALTER TABLE silver.users
    ADD COLUMN department_id VARCHAR(50);  -- No FK constraint

-- ❌ WRONG: Enforced FK constraint
ALTER TABLE silver.users
    ADD CONSTRAINT fk_users_department
    FOREIGN KEY (department_id) REFERENCES silver.departments(dept_id);
```

### Handling Missing Foreign Key References

Use the **nullable FK + quality_flags** pattern:

```python
def _validate_foreign_keys(self, silver_record: Dict) -> List[str]:
    """
    Validate foreign key references and return quality flags.
    
    This replaces database FK constraints with ETL-managed validation.
    """
    flags = []
    
    # Check department reference
    dept_id = silver_record.get('department_id')
    if dept_id and dept_id not in self.dept_cache:
        flags.append('invalid_department_reference')
        logger.warning(f"User {silver_record['uniqname']} references non-existent dept: {dept_id}")
    elif not dept_id:
        flags.append('missing_department')
    
    # Check PI reference (for labs)
    pi_uniqname = silver_record.get('pi_uniqname')
    if pi_uniqname and pi_uniqname not in self.user_cache:
        flags.append('pi_not_in_silver_users')
        logger.warning(f"Lab {silver_record['lab_id']} references non-existent PI: {pi_uniqname}")
    
    return flags
```

### Foreign Key Documentation

Add comments to tables documenting logical FK relationships:

```sql
COMMENT ON COLUMN silver.users.department_id IS 
    'Logical FK to silver.departments(dept_id). No constraint enforced; use quality_flags to track missing references.';

COMMENT ON COLUMN silver.labs.pi_uniqname IS 
    'Logical FK to silver.users(uniqname). May reference users not yet in silver.users (tracked via quality_flags).';
```

### Reconciliation Pattern

For late-arriving parent records:

```sql
-- Query to find orphaned child records
SELECT 
    u.uniqname,
    u.department_id,
    u.quality_flags
FROM silver.users u
WHERE u.department_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.departments d 
      WHERE d.dept_id = u.department_id
  );

-- After parent records arrive, re-run transformation to update quality scores
-- Transformation scripts should automatically clear 'invalid_*_reference' flags
-- when the referenced entity becomes available
```

---

## Data Quality Framework

### Quality Score Calculation

All consolidated silver tables MUST calculate `data_quality_score` (0.00 to 1.00):

```python
def _calculate_data_quality(self, silver_record: Dict, 
                           source_records: Dict[str, Any]) -> Tuple[float, List[str]]:
    """
    Calculate data quality score based on completeness and consistency.
    
    Scoring rubric:
    - Start at 1.0 (perfect)
    - Deduct 0.10 for each missing critical source
    - Deduct 0.05 for each missing important field
    - Deduct 0.05 for cross-source inconsistencies
    - Deduct 0.05 for invalid FK references
    
    Returns:
        Tuple of (quality_score, quality_flags_list)
    """
    score = 1.0
    flags = []
    
    # Check for missing sources
    if not source_records.get('tdx'):
        score -= 0.10
        flags.append('missing_tdx_source')
    
    if not source_records.get('umapi'):
        score -= 0.10
        flags.append('missing_umapi_source')
    
    # Check for missing critical fields
    if not silver_record.get('primary_email'):
        score -= 0.05
        flags.append('missing_email')
    
    if not silver_record.get('department_id'):
        score -= 0.05
        flags.append('missing_department')
    
    # Check for cross-source name mismatches
    if source_records.get('tdx') and source_records.get('mcom'):
        tdx_name = source_records['tdx'].get('FullName', '').lower()
        mcom_name = source_records['mcom'].get('displayName', '').lower()
        if tdx_name and mcom_name and tdx_name != mcom_name:
            score -= 0.05
            flags.append('name_mismatch_across_sources')
    
    # Validate foreign keys
    fk_flags = self._validate_foreign_keys(silver_record)
    for flag in fk_flags:
        score -= 0.05
        flags.append(flag)
    
    return max(0.0, round(score, 2)), flags
```

### Standard Quality Flags

Use consistent flag naming across all entities:

| Category | Flag Examples | Meaning |
|----------|---------------|---------|
| **Missing Sources** | `missing_tdx_source`, `missing_umapi_source` | Expected source data not found |
| **Missing Fields** | `missing_email`, `missing_department`, `missing_job_title` | Critical field is NULL |
| **Invalid References** | `invalid_department_reference`, `pi_not_in_silver_users` | FK points to non-existent record |
| **Data Inconsistencies** | `name_mismatch_across_sources`, `email_format_invalid` | Data doesn't pass validation |
| **Structural Issues** | `no_location_data`, `no_award_data`, `no_ou_data` | Expected complex data missing |

### Quality Reporting

Every transformation script should log quality distribution:

```python
# In transformation summary
quality_ranges = [
    (1.0, 1.0, "Perfect"),
    (0.9, 0.99, "Excellent"),
    (0.8, 0.89, "Good"),
    (0.7, 0.79, "Acceptable"),
    (0.0, 0.69, "Needs Review"),
]

logger.info("📊 Quality Distribution:")
for low, high, label in quality_ranges:
    count = len(silver_df[
        (silver_df['data_quality_score'] >= low) & 
        (silver_df['data_quality_score'] <= high)
    ])
    if count > 0:
        logger.info(f"   ├─ {label} ({low}-{high}): {count} records")
```

---

## Incremental Processing Requirements

### Mandatory Features

ALL ingestion and transformation scripts MUST support:

1. **Incremental processing by default** — Only process records newer than last successful run
2. **Full sync override** — `--full-sync` flag to process all records
3. **Dry run mode** — `--dry-run` flag to preview changes without committing
4. **Run tracking** — Create `meta.ingestion_runs` record for every execution

### Command-Line Arguments

```python
import argparse

parser = argparse.ArgumentParser(description='Transform silver users')
parser.add_argument('--full-sync', action='store_true',
                   help='Process all records (ignore last transformation timestamp)')
parser.add_argument('--dry-run', action='store_true',
                   help='Preview changes without committing to database')
parser.add_argument('--batch-size', type=int, default=500,
                   help='Number of records to process per batch')
args = parser.parse_args()
```

### Incremental Processing Pattern

```python
def transform_incremental(self, full_sync: bool = False) -> Dict[str, Any]:
    """
    Transform bronze records to silver layer incrementally.
    
    Args:
        full_sync: If True, process all records. If False, only process
                   records with bronze data newer than last successful run.
    """
    # Step 1: Get last transformation timestamp (unless full sync)
    last_timestamp = None if full_sync else self._get_last_transformation_timestamp()
    
    if full_sync:
        logger.info("🔄 Full sync mode: Processing ALL records")
    elif last_timestamp:
        logger.info(f"⚡ Incremental mode: Processing records since {last_timestamp}")
    else:
        logger.info("🆕 First run: Processing ALL records")
    
    # Step 2: Create ingestion run
    run_id = self.create_transformation_run(
        incremental_since=last_timestamp,
        metadata={'full_sync': full_sync}
    )
    
    # Step 3: Find records needing transformation
    entity_ids = self._get_entities_needing_transformation(since_timestamp=last_timestamp)
    
    if not entity_ids:
        logger.info("✨ All records up to date - no transformation needed")
        self.complete_transformation_run(run_id, 0, 0, 0)
        return {'run_id': run_id, 'records_processed': 0}
    
    # Step 4: Process records...
    # (transformation logic here)
    
    return stats
```

### Dry Run Pattern

```python
def _upsert_silver_record(self, silver_record: Dict, run_id: str, dry_run: bool = False):
    """
    Insert or update a silver record.
    
    Args:
        silver_record: Record to upsert
        run_id: Ingestion run ID
        dry_run: If True, log what would be done but don't commit
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would upsert: {silver_record['uniqname']}")
        logger.debug(f"[DRY RUN] Record data: {json.dumps(silver_record, indent=2, default=str)}")
        return
    
    # Actual upsert logic
    with self.db_adapter.engine.connect() as conn:
        conn.execute(upsert_query, silver_record)
        conn.commit()
```

---

## Script Structure Standards

### File Organization

Database scripts are organized by layer in numbered subdirectories for easy cron scheduling:

```
scripts/database/
├── bronze/                    # Bronze layer scripts (ingestion & enrichment)
│   ├── 001_ingest_umapi_departments.py
│   ├── 002_ingest_tdx_accounts.py
│   ├── 003_ingest_mcommunity_users.py
│   ├── 004_ingest_ad_users.py
│   ├── 005_ingest_mcommunity_groups.py
│   ├── 010_enrich_tdx_accounts.py
│   └── ...
├── silver/                    # Silver layer transformations
│   ├── 001_transform_silver_departments.py
│   ├── 002_transform_silver_users.py
│   ├── 003_transform_silver_groups.py
│   ├── 004_transform_silver_labs.py
│   └── ...
└── gold/                      # Gold layer aggregations (future)
    └── (future gold scripts)
```

**Naming Convention:** `###_[action]_[entity].py`
- `###` — Three-digit prefix for execution order (001, 002, 003...)
- `[action]` — `ingest`, `enrich`, or `transform`
- `[entity]` — Entity type being processed

**Numbering Guidelines:**
- **001-099:** Core entity ingestion (departments, users, groups)
- **100-199:** Secondary entity ingestion (computers, assets, organizational units)
- **200-299:** Enrichment scripts (add missing data to existing bronze records)
- **300-399:** Reserved for future bronze operations

For silver transformations:
- **001-099:** Foundation entities (departments, users, groups)
- **100-199:** Dependent entities (labs, computers)
- **200-299:** Junction tables and relationships

**Why Numbered?**
- Establishes clear dependency order (departments before users, users before labs)
- Simplifies cron scheduling: `for script in scripts/database/bronze/*.py; do python $script; done`
- Makes it obvious which scripts must run first
- Allows gaps for inserting new scripts without renumbering everything

### Standard Service Class Structure

```python
class EntityTransformationService:
    """
    Service for transforming bronze entity records into silver layer.
    
    Transformation Logic:
    1. Source-specific: bronze.raw_entities → silver.[source]_[entity]
    2. Consolidated: silver.[source]_[entity] → silver.[entity]
    
    Merge Priority:
    - Field X: source1 > source2 > source3
    - Field Y: source2 > source1
    (Document all merge rules here)
    """
    
    def __init__(self, database_url: str, **api_credentials):
        """Initialize with database adapter and API clients."""
        self.db_adapter = PostgresAdapter(database_url)
        # Initialize API clients if needed (for enrichment scripts)
        
    # === PRIVATE METHODS (prefixed with _) ===
    
    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """Get timestamp of last successful transformation."""
        pass
    
    def _fetch_bronze_records(self, since_timestamp: Optional[datetime]) -> List[Dict]:
        """Fetch bronze records needing transformation."""
        pass
    
    def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """Calculate SHA-256 hash for change detection."""
        pass
    
    def _merge_source_records(self, source_records: Dict[str, Dict]) -> Dict[str, Any]:
        """Merge records from multiple sources with priority rules."""
        pass
    
    def _calculate_data_quality(self, silver_record: Dict) -> Tuple[float, List[str]]:
        """Calculate quality score and identify quality flags."""
        pass
    
    def _validate_foreign_keys(self, silver_record: Dict) -> List[str]:
        """Validate logical FK references and return quality flags."""
        pass
    
    def _upsert_silver_record(self, silver_record: Dict, run_id: str, dry_run: bool):
        """Insert or update silver record (with dry-run support)."""
        pass
    
    # === PUBLIC METHODS ===
    
    def create_transformation_run(self, incremental_since: Optional[datetime]) -> str:
        """Create meta.ingestion_runs record and return run_id."""
        pass
    
    def complete_transformation_run(self, run_id: str, stats: Dict):
        """Update meta.ingestion_runs with results."""
        pass
    
    def transform_incremental(self, full_sync: bool = False, 
                            dry_run: bool = False) -> Dict[str, Any]:
        """
        Main entry point for transformation.
        
        Args:
            full_sync: Process all records (ignore incremental timestamp)
            dry_run: Preview changes without committing
            
        Returns:
            Dictionary with comprehensive statistics
        """
        pass
    
    def get_transformation_summary(self) -> pd.DataFrame:
        """Get summary DataFrame of silver records for analysis."""
        pass
    
    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
```

### Main Function Pattern

```python
def main():
    """Command-line entry point with argument parsing."""
    load_dotenv()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Transform entity to silver layer')
    parser.add_argument('--full-sync', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--batch-size', type=int, default=500)
    args = parser.parse_args()
    
    # Get environment variables
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable required")
    
    # Initialize service
    service = EntityTransformationService(database_url)
    
    try:
        # Run transformation
        logger.info("="*80)
        logger.info(f"🚀 Starting entity transformation")
        logger.info(f"   Mode: {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}")
        logger.info(f"   Dry Run: {args.dry_run}")
        logger.info("="*80)
        
        results = service.transform_incremental(
            full_sync=args.full_sync,
            dry_run=args.dry_run
        )
        
        # Display summary
        print("\n" + "="*80)
        print("📊 TRANSFORMATION SUMMARY")
        print("="*80)
        print(f"Run ID:              {results['run_id']}")
        print(f"Records Processed:   {results['records_processed']}")
        print(f"Records Created:     {results['records_created']}")
        print(f"Records Updated:     {results['records_updated']}")
        print(f"Errors:              {len(results.get('errors', []))}")
        print(f"Duration:            {results.get('duration_seconds', 0):.2f}s")
        print("="*80)
        
        if args.dry_run:
            print("\n⚠️  DRY RUN MODE - No changes committed to database")
        else:
            print("\n✅ Transformation completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        service.close()

if __name__ == '__main__':
    main()
```

---

## Logging & Emoji Standards

### Emoji Vocabulary

Use emojis consistently to make logs scannable:

| Emoji | Meaning | Usage |
|-------|---------|-------|
| 🚀 | Starting process | Script initialization |
| 🔄 | Syncing/transforming | Data processing in progress |
| ⚡ | Incremental mode | Processing only new records |
| 🆕 | New record | First time seeing this entity |
| 📝 | Updated record | Entity changed since last run |
| ⏭️  | Skipped unchanged | Entity unchanged, skipping |
| 🔬 | Fetching/loading | Loading data from database/API |
| 📚 | Loading cache | Building in-memory lookup tables |
| 🏗️  | Building/constructing | Creating complex structures |
| 📊 | Statistics/summary | Reporting metrics |
| ✅ | Success | Operation completed successfully |
| ⚠️  | Warning | Non-fatal issue detected |
| ❌ | Error/failure | Fatal error occurred |
| 🎉 | Celebration | Major milestone completed |
| 🔍 | Searching/analyzing | Querying or investigating |
| 💾 | Saving/persisting | Writing to database |
| 🧹 | Cleanup | Removing temporary data |
| 🔌 | Connection | Database/API connection events |
| 📈 | Progress | Periodic progress updates |

### Log Level Guidelines

```python
# DEBUG: Detailed diagnostic information
logger.debug(f"⏭️  Record unchanged, skipping: {entity_id} (hash: {current_hash})")

# INFO: Normal operational messages
logger.info(f"🔬 Fetching bronze records for {entity_type}")
logger.info(f"📝 Processing entity {entity_id}: {name}")

# WARNING: Unexpected but handled situations
logger.warning(f"⚠️  Department reference not found: {dept_id} for user {uniqname}")

# ERROR: Errors that need attention
logger.error(f"❌ Failed to transform record {entity_id}: {error}", exc_info=True)
```

### Progress Logging Pattern

```python
for i, entity_id in enumerate(entity_ids, 1):
    # Process entity...
    
    # Log progress every 50 records
    if i % 50 == 0:
        logger.info(f"📈 Progress: {i}/{len(entity_ids)} entities processed "
                   f"({stats['created']} created, {stats['updated']} updated, "
                   f"{stats['skipped']} skipped)")
```

### Summary Statistics Format

```python
# End-of-script summary with emoji headers
logger.info("="*80)
logger.info("🎉 TRANSFORMATION COMPLETED")
logger.info("="*80)
logger.info(f"📊 Results Summary:")
logger.info(f"   Total Processed:      {stats['records_processed']:>6,}")
logger.info(f"   ├─ New Created:       {stats['records_created']:>6,}")
logger.info(f"   ├─ Updated:           {stats['records_updated']:>6,}")
logger.info(f"   └─ Skipped:           {stats['records_skipped']:>6,}")
logger.info(f"")
logger.info(f"   Source Distribution:")
logger.info(f"   ├─ Source A Only:     {stats['source_a_only']:>6,}")
logger.info(f"   ├─ Source B Only:     {stats['source_b_only']:>6,}")
logger.info(f"   └─ Merged (A+B):      {stats['merged']:>6,}")
logger.info(f"")
logger.info(f"   Data Quality:")
logger.info(f"   ├─ Perfect (1.0):     {quality_counts['perfect']:>6,}")
logger.info(f"   ├─ Excellent (0.9+):  {quality_counts['excellent']:>6,}")
logger.info(f"   ├─ Good (0.8+):       {quality_counts['good']:>6,}")
logger.info(f"   └─ Needs Review (<0.8): {quality_counts['review']:>6,}")
logger.info(f"")
logger.info(f"   Errors:               {len(stats['errors']):>6,}")
logger.info(f"   Duration:             {duration:.2f}s")
logger.info("="*80)
```

### Log File Organization

Logs are organized by layer in subdirectories under project root:

```
<project_root>/
├── logs/
│   ├── bronze/          # Bronze ingestion logs
│   │   ├── 001_ingest_umapi_departments.log
│   │   ├── 002_ingest_tdx_accounts.log
│   │   ├── 003_ingest_mcommunity_users.log
│   │   └── 010_enrich_tdx_accounts.log
│   └── silver/          # Silver transformation logs
│       ├── 001_transform_silver_departments.log
│       ├── 002_transform_silver_users.log
│       ├── 003_transform_silver_groups.log
│       └── 004_transform_silver_labs.log
```

```python
import os
import sys

# Determine log directory based on script location
# Scripts should be in scripts/database/[layer]/ subdirectories
script_path = os.path.abspath(__file__)
script_name = os.path.basename(__file__).replace('.py', '')

# Detect layer from script path
if '/bronze/' in script_path:
    log_dir = 'logs/bronze'
elif '/silver/' in script_path:
    log_dir = 'logs/silver'
elif '/gold/' in script_path:
    log_dir = 'logs/gold'
else:
    log_dir = 'logs'

# Create log directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{log_dir}/{script_name}.log'),  # Append mode
        logging.StreamHandler(sys.stdout)                     # Also to console
    ]
)
```

---

## Testing & Validation

### Pre-Commit Validation Checklist

Before committing any database script, verify:

- [ ] Script has `--full-sync` and `--dry-run` arguments
- [ ] Incremental processing uses `meta.ingestion_runs` timestamps
- [ ] All consolidated silver tables have `data_quality_score` and `quality_flags`
- [ ] Foreign keys are nullable and documented as logical FKs (no constraints)
- [ ] Logging uses standardized emoji vocabulary
- [ ] Summary statistics follow the standard format
- [ ] Script creates and completes `meta.ingestion_runs` record
- [ ] Content hashing is implemented for sources without timestamps
- [ ] All JSONB columns are indexed with GIN indexes
- [ ] Source-specific silver tables are used for 3+ source entities

### Testing Commands

```bash
# IMPORTANT: Always activate virtual environment first
source venv/bin/activate

# Test dry run mode
python scripts/database/silver/002_transform_silver_users.py --dry-run

# Test full sync
python scripts/database/silver/002_transform_silver_users.py --full-sync --dry-run

# Test incremental (default)
python scripts/database/silver/002_transform_silver_users.py

# Run all bronze scripts in order (useful for cron)
for script in scripts/database/bronze/*.py; do
    echo "Running $script..."
    python "$script" || echo "Warning: $script failed"
done

# Run all silver scripts in order
for script in scripts/database/silver/*.py; do
    echo "Running $script..."
    python "$script" || echo "Warning: $script failed"
done

# Validate ingestion runs were tracked
docker exec -i <container_id> psql -U lsats_user -d lsats_db -c \
  "SELECT * FROM meta.ingestion_runs ORDER BY started_at DESC LIMIT 5;"

# Check data quality distribution
docker exec -i <container_id> psql -U lsats_user -d lsats_db -c \
  "SELECT 
     CASE 
       WHEN data_quality_score = 1.0 THEN 'Perfect'
       WHEN data_quality_score >= 0.9 THEN 'Excellent'
       WHEN data_quality_score >= 0.8 THEN 'Good'
       ELSE 'Needs Review'
     END as quality_tier,
     COUNT(*) 
   FROM silver.users 
   GROUP BY quality_tier 
   ORDER BY MIN(data_quality_score) DESC;"
```

---

## Appendix: Quick Reference

### Bronze → Silver Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                             │
│  bronze.raw_entities (universal JSONB table)                    │
│  - entity_type: 'user', 'group', 'department', etc.            │
│  - source_system: 'tdx', 'mcom', 'ad', 'umapi', etc.           │
│  - raw_data: JSONB (complete unmodified source data)           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├─→ Transform Script (ingest_*.py)
                         │   - Content hash change detection
                         │   - Timestamp-based detection
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                   SILVER LAYER (Tier 1)                         │
│  Source-Specific Tables (typed columns)                         │
│  - silver.tdx_users, silver.mcom_users, silver.ad_users        │
│  - silver.mcom_groups, silver.ad_groups                         │
│  - Links to bronze via raw_id                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├─→ Transform Script (transform_*.py)
                         │   - Merge with priority rules
                         │   - Calculate quality scores
                         │   - Validate logical FKs
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                   SILVER LAYER (Tier 2)                         │
│  Consolidated Tables (business-ready)                           │
│  - silver.users (unified user records)                          │
│  - silver.groups (unified group records)                        │
│  - silver.departments (unified department records)              │
│  - Junction tables: group_members, lab_members, etc.           │
└─────────────────────────────────────────────────────────────────┘
```

### Essential SQL Queries

```sql
-- Check recent ingestion activity
SELECT 
    source_system,
    entity_type,
    status,
    records_processed,
    records_created,
    started_at,
    completed_at
FROM meta.ingestion_runs
ORDER BY started_at DESC
LIMIT 10;

-- Find records with quality issues
SELECT 
    uniqname,
    full_name,
    data_quality_score,
    quality_flags
FROM silver.users
WHERE data_quality_score < 0.8
ORDER BY data_quality_score ASC
LIMIT 20;

-- Identify orphaned FK references
SELECT 
    u.uniqname,
    u.department_id,
    u.quality_flags
FROM silver.users u
WHERE u.department_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.departments d 
      WHERE d.dept_id = u.department_id
  );

-- Track transformation lag (bronze vs silver)
WITH latest_bronze AS (
    SELECT 
        entity_type,
        MAX(ingested_at) as last_bronze_ingest
    FROM bronze.raw_entities
    WHERE source_system = 'tdx'
    GROUP BY entity_type
),
latest_silver_transform AS (
    SELECT 
        entity_type,
        MAX(completed_at) as last_silver_transform
    FROM meta.ingestion_runs
    WHERE source_system = 'silver_transformation'
      AND status = 'completed'
    GROUP BY entity_type
)
SELECT 
    b.entity_type,
    b.last_bronze_ingest,
    s.last_silver_transform,
    AGE(b.last_bronze_ingest, s.last_silver_transform) as lag
FROM latest_bronze b
LEFT JOIN latest_silver_transform s ON b.entity_type = s.entity_type
ORDER BY lag DESC NULLS FIRST;
```

---

**Document Version:** 1.0  
**Last Updated:** 2025-01-21

