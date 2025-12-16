# LSATS Data Hub Medallion Architecture Standards

**Version:** 2.0  
**Last Updated:** 2025-01-24  
**Purpose:** High-level architecture overview and navigation to layer-specific standards

---

## Overview

This document serves as the **hub** for LSATS Data Hub's medallion architecture standards. For detailed layer-specific standards, see:

- **[Bronze Layer Standards](bronze_layer_standards.md)** — Raw data ingestion, change detection, universal table
- **[Silver Layer Standards](silver_layer_standards.md)** — Entity resolution, composite patterns, dbt vs Python
- **[Database Script Standards](database_script_standards.md)** — Script structure, service classes, logging

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Key Design Principles](#key-design-principles)
3. [Technology Stack & Tool Selection](#technology-stack--tool-selection)
4. [Naming Conventions](#naming-conventions)
5. [Cross-Layer Standards](#cross-layer-standards)
6. [Quick Reference & Decision Trees](#quick-reference--decision-trees)

---

## Architecture Overview

### Three-Tier Medallion Architecture

```
┌─────────────────────────────────────────┐
│         Bronze Layer (Raw Data)         │
│                                         │
│  Technology: Python (API adapters)     │
│  Storage: bronze.raw_entities (JSONB)  │
│  Standards: bronze_layer_standards.md  │
└──────────────┬──────────────────────────┘
               │
               │ Python ingestion
               ▼
┌─────────────────────────────────────────┐
│    Silver Layer (Cleaned & Unified)     │
│                                         │
│  Technology: dbt SQL (future)          │
│              Python (current)           │
│  Storage: Typed tables                 │
│  Standards: silver_layer_standards.md  │
└──────────────┬──────────────────────────┘
               │
               │ dbt SQL models
               ▼
┌─────────────────────────────────────────┐
│     Gold Layer (Analytics-Ready)        │
│                                         │
│  Technology: dbt SQL (future)          │
│  Storage: Star schema                  │
│  Standards: (Planned for 2026)         │
└─────────────────────────────────────────┘
```

### Layer Characteristics

| Layer | Purpose | Technology | Storage | Characteristics |
|-------|---------|-----------|---------|-----------------|
| **Bronze** | Raw data landing zone | Python | Single universal table (JSONB) | Append-only, immutable, complete history |
| **Silver** | Cleaned & unified entities | dbt (future) / Python (now) | Typed tables (3-tier) | Entity resolution, quality scoring |
| **Gold** | Analytics-ready aggregates | dbt (future) | Star schema | Denormalized, optimized for BI |
| **Meta** | System metadata | Python | Ingestion tracking | Run logs, statistics, lineage |

---

## Key Design Principles

### Universal Principles (All Layers)

1. **Append-only bronze** — Never delete or modify bronze records
2. **Single universal bronze table** — Modern best practice (streaming-friendly, scalable)
3. **Silver is source of truth** — All business queries use silver layer
4. **ETL-managed integrity** — No database foreign key constraints
5. **Incremental by default** — Full-sync as override option
6. **Observable transformations** — Comprehensive logging with emoji indicators
7. **Data quality scoring** — All silver tables have 0.00-1.00 quality scores

### Layer-Specific Principles

**Bronze:**
- Store complete raw data (zero transformation)
- Use content hashing or timestamps for change detection
- Enrich with `_prefixed` metadata fields

**Silver:**
- Three-tier architecture (source-specific → consolidated → composite)
- SQL for transformations (dbt preferred), Python for algorithms
- Composite entities for business concepts
- Junction tables for many-to-many relationships

**Gold (Future):**
- Star schema dimensional modeling
- Pre-aggregated for dashboard performance
- Denormalized for query simplicity

---

## Technology Stack & Tool Selection

### Current Stack (2025)

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | PostgreSQL 15+ | Data warehouse |
| **Bronze Ingestion** | Python | API calls, change detection, file parsing |
| **Silver Transformation** | Python | Entity resolution, merging (transitioning to dbt) |
| **Complex Algorithms** | Python | Manager scoring, ML models |
| **Orchestration** | Python scripts | Pipeline execution (future: Airflow) |
| **Version Control** | Git | All code version controlled |

### Future Stack (2026+)

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Bronze Ingestion** | Python | *(No change - dbt doesn't do ingestion)* |
| **Silver Transformation** | **dbt Core** | SQL transformations, testing, lineage |
| **Complex Algorithms** | Python | *(No change - algorithms stay in Python)* |
| **Gold Analytics** | **dbt Core** | Star schema, materialized aggregates |
| **Orchestration** | **Airflow/Dagster** | Scheduled pipelines, monitoring |
| **Testing** | **dbt tests + pytest** | Automated data quality validation |

### Technology Decision Matrix

```
┌──────────────────────────────┐
│ What type of work is this?   │
└────────┬─────────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐  ┌───────────┐
│ SQL-   │  │ Algorithm │
│ like   │  │ (complex  │
│ (joins,│  │  logic,   │
│  agg)  │  │  ML, API) │
└───┬────┘  └─────┬─────┘
    │             │
    ▼             ▼
┌────────┐  ┌────────────┐
│  dbt   │  │   Python   │
│  SQL   │  │   Service  │
│ (pref) │  │ (required) │
└────────┘  └────────────┘
```

**Detailed Matrix:**

| Use Case | dbt SQL | Python | Current |
|----------|---------|--------|---------|
| Multi-table joins | ✅ **PREFERRED** | ❌ | Python (migrate) |
| Aggregations (SUM, COUNT) | ✅ **PREFERRED** | ❌ | Python (migrate) |
| Field merge priority | ✅ **PREFERRED** | ❌ | Python (migrate) |
| Data quality tests | ✅ **PREFERRED** | ⚠️ | Python manual |
| Complex scoring algorithm | ❌ | ✅ **REQUIRED** | Python (keep) |
| API calls, ingestion | ❌ | ✅ **REQUIRED** | Python (keep) |
| Content hashing | ❌ | ✅ **REQUIRED** | Python (keep) |
| ML model inference | ⚠️ | ✅ **PREFERRED** | N/A (future) |

**See [silver_layer_standards.md § Technology Decision Matrix](silver_layer_standards.md#technology-decision-matrix) for complete guidance.**

---

## Naming Conventions

### Universal Standards (All Layers)

**Schema Naming:**
- `bronze` — Raw data
- `silver` — Cleaned entities
- `gold` — Analytics (future)
- `meta` — System metadata

**Table Naming (snake_case):**

| Type | Pattern | Examples |
|------|---------|----------|
| Bronze | `raw_entities` | Single universal table |
| Silver Source-Specific | `{source}_{entity}` | `tdx_users`, `ad_computers`, `lab_awards` |
| Silver Consolidated | `{entity}` (plural) | `users`, `groups`, `computers`, `departments` |
| Silver Composite | `{business_concept}` | `labs`, `pi_research_groups` |
| Silver Junction | `{entity1}_{entity2}` | `lab_members`, `group_members`, `computer_labs` |
| Silver Views | `v_{entity}_{purpose}` | `v_lab_summary`, `v_labs_monitored` |
| Silver Materialized Views | `mv_{entity}_{purpose}` | `mv_department_summary` |

**Column Naming:**

| Type | Pattern | Example |
|------|---------|---------|
| Business key | `{entity}_id` or `uniqname` | `dept_id`, `lab_id`, `uniqname` |
| Surrogate key | `silver_id` or `{entity}_key` | `silver_id UUID`, `user_key BIGSERIAL` |
| Foreign key | `{referenced_entity}_id` | `department_id`, `pi_uniqname` |
| External system ID | `{system}_{entity}_uid` | `tdx_user_uid`, `ad_object_guid` |
| Source tracking | `source_system`, `source_entity_id` | VARCHAR, tracks contributors |
| Quality metrics | `data_quality_score`, `quality_flags` | DECIMAL(3,2), JSONB array |
| Timestamps | `created_at`, `updated_at`, `ingested_at` | Always `TIMESTAMP WITH TIME ZONE` |

**Reserved Field Names:**
- `entity_hash` — SHA-256 content hash for change detection
- `ingestion_run_id` — UUID linking to `meta.ingestion_runs`
- `raw_id` — UUID of bronze record (in silver source-specific tables)

---

## Cross-Layer Standards

### Data Quality Framework

**Applies to:** All silver tables

**Quality Score Calculation (0.00 - 1.00):**

```python
score = 1.0
- 0.10 for each missing critical source
- 0.05 for each missing important field
- 0.05 for cross-source inconsistencies
- 0.05 for invalid FK references

# Round to 2 decimal places
score = max(0.0, round(score, 2))
```

**Standard Quality Flags:**

| Category | Flag Examples | Meaning |
|----------|---------------|---------|
| **Missing Sources** | `missing_tdx_source`, `missing_umapi_source` | Expected source data not found |
| **Missing Fields** | `missing_email`, `missing_department` | Critical field is NULL |
| **Invalid References** | `invalid_department_reference`, `pi_not_in_users` | FK points to non-existent record |
| **Data Inconsistencies** | `name_mismatch_across_sources` | Data doesn't pass validation |
| **Structural Issues** | `no_location_data`, `no_award_data` | Expected complex data missing |

**Example:**

```sql
CREATE TABLE silver.users (
    uniqname VARCHAR(50) PRIMARY KEY,
    -- ... other fields
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00),
    quality_flags JSONB DEFAULT '[]'::jsonb
);

-- Query low-quality records
SELECT uniqname, data_quality_score, quality_flags
FROM silver.users
WHERE data_quality_score < 0.80
ORDER BY data_quality_score ASC;
```

### Incremental Processing Requirements

**Applies to:** Bronze ingestion, silver transformation

**Mandatory Features:**

1. **Default behavior: Incremental** — Only process new/changed records
2. **`--full-sync` flag** — Override to process all records
3. **`--dry-run` flag** — Log actions without making changes
4. **Ingestion run tracking** — Log to `meta.ingestion_runs`

**Python Pattern:**

```python
def ingest(self, full_sync: bool = False, dry_run: bool = False):
    # Create run record
    run_id = self.db.create_ingestion_run(
        source_system='tdx',
        entity_type='user',
        metadata={'full_sync': full_sync, 'dry_run': dry_run}
    )
    
    try:
        # Get last timestamp (unless full sync)
        last_run = None if full_sync else self._get_last_successful_run()
        
        # Process records
        # ...
        
        # Complete run
        self.db.complete_ingestion_run(run_id, stats)
    except Exception as e:
        self.db.fail_ingestion_run(run_id, str(e))
        raise
```

**dbt Pattern:**

```sql
{{ config(materialized='incremental', unique_key='id') }}

SELECT * FROM {{ source('bronze', 'raw_entities') }}

{% if is_incremental() %}
  -- Only new records since last run
  WHERE ingested_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

### Logging & Emoji Standards

**Applies to:** All Python scripts

**Emoji Vocabulary:**

| Emoji | Meaning | Usage |
|-------|---------|-------|
| 🔄 | Process start | `logger.info("🔄 Starting user consolidation...")` |
| 📥 | Data fetch | `logger.info("📥 Fetching from API...")` |
| 📚 | Loading caches | `logger.info("📚 Loading department cache...")` |
| ⚙️ | Transformation | `logger.info("⚙️ Transforming records...")` |
| 📊 | Statistics | `logger.info("📊 Total: 1000 records")` |
| ✅ | Success | `logger.info("✅ Completed successfully")` |
| ❌ | Error | `logger.error("❌ Failed: {error}")` |
| ⏭️ | Skipped | `logger.debug("⏭️ Skipped unchanged record")` |
| 🆕 | New record | `logger.info("🆕 New department: Chemistry")` |
| 📝 | Updated record | `logger.info("📝 Updated: user jdoe")` |
| 🧪 | Dry run | `logger.info("🧪 DRY RUN: Would insert 100 records")` |

**Log Level Guidelines:**

```python
logger.debug()   # Detailed debugging (individual records, skipped items)
logger.info()    # Normal operations (progress, statistics, completion)
logger.warning() # Potential issues (missing optional data, fallbacks)
logger.error()   # Errors that don't stop execution (individual record failures)
logger.critical() # Fatal errors (database connection, missing config)
```

**Summary Statistics Format:**

```python
logger.info("📊 Ingestion Summary:")
logger.info(f"   ├─ Fetched: {stats['records_fetched']}")
logger.info(f"   ├─ New: {stats['records_new']}")
logger.info(f"   ├─ Updated: {stats['records_updated']}")
logger.info(f"   └─ Unchanged: {stats['records_skipped_unchanged']}")
```

### Foreign Key & Referential Integrity

**Applies to:** All silver tables

**LSATS Standard:** **Do NOT use PostgreSQL foreign key constraints**

**Rationale:**
1. **Load performance** — FK constraints slow bulk inserts
2. **ETL flexibility** — Parent records may arrive after child records
3. **Partial data** — Not all sources provide complete reference data
4. **Data quality tracking** — Quality flags provide better observability

**Pattern:**

```sql
-- ✅ CORRECT: Nullable FK with quality flag tracking
ALTER TABLE silver.users
    ADD COLUMN department_id VARCHAR(50);  -- No FK constraint

COMMENT ON COLUMN silver.users.department_id IS 
    'Logical FK to silver.departments(dept_id). No constraint enforced; 
     use quality_flags to track missing references.';

-- ❌ WRONG: Enforced FK constraint
ALTER TABLE silver.users
    ADD CONSTRAINT fk_users_department
    FOREIGN KEY (department_id) REFERENCES silver.departments(dept_id);
```

**Validation Pattern (Python):**

```python
def _validate_foreign_keys(self, silver_record: Dict) -> List[str]:
    """Validate FK references and return quality flags."""
    flags = []
    
    dept_id = silver_record.get('department_id')
    if dept_id and dept_id not in self.dept_cache:
        flags.append('invalid_department_reference')
        logger.warning(f"User {silver_record['uniqname']} references non-existent dept: {dept_id}")
    elif not dept_id:
        flags.append('missing_department')
    
    return flags
```

### Testing & Validation

**Applies to:** Silver layer

**Current (Python):**
- `--dry-run` flag for preview
- Manual verification queries
- Script-specific validation logic

**Future (dbt):**
- Automated tests in `schema.yml`
- Test types: `unique`, `not_null`, `relationships`, `accepted_values`
- Custom tests in `tests/` directory

**Example dbt tests:**

```yaml
# models/silver/schema.yml
version: 2

models:
  - name: users
    columns:
      - name: uniqname
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

**Run tests:**

```bash
# Test all models
dbt test

# Test specific model
dbt test --select users
```

---

## Quick Reference & Decision Trees

### Decision Tree: Which Layer?

```
Is this raw data from external source?
├─ YES → Bronze (bronze.raw_entities)
└─ NO
   Is this cleaned data from single source?
   ├─ YES → Silver Tier 1 (source-specific table)
   └─ NO
      Is this merged data from multiple sources of same entity type?
      ├─ YES → Silver Tier 2 (consolidated table)
      └─ NO
         Is this business-level aggregation across entity types?
         ├─ YES → Silver Tier 3 (composite table)
         └─ NO → Gold Layer (future)
```

### Decision Tree: Python vs dbt?

```
Is this an ingestion task (API calls, file parsing)?
├─ YES → Python (dbt doesn't do ingestion)
└─ NO
   Is this complex algorithm (ML, scoring, hashing)?
   ├─ YES → Python (dbt not suitable)
   └─ NO
      Is this SQL transformation (joins, aggregation, filtering)?
      ├─ YES → dbt (preferred) or Python (current)
      └─ NO
         Is this data quality testing?
         └─ YES → dbt tests (preferred) or Python manual queries (current)
```

### Decision Tree: Table vs View?

```
Is this a composite business entity?
├─ YES
│  Does it require aggregation across multiple tables?
│  ├─ YES
│  │  Is query frequency high (>100/day)?
│  │  ├─ YES → CREATE TABLE (materialized)
│  │  └─ NO → CREATE VIEW (compute on demand)
│  └─ NO → CREATE VIEW (simple join)
└─ NO
   Is this just filtering/enriching an existing table?
   └─ YES → CREATE VIEW
```

### Common Queries

**Check bronze ingestion status:**

```sql
SELECT 
    source_system,
    entity_type,
    MAX(ingested_at) as last_ingested,
    COUNT(*) as total_records
FROM bronze.raw_entities
GROUP BY source_system, entity_type
ORDER BY last_ingested DESC;
```

**Check silver data quality:**

```sql
SELECT 
    'users' as table_name,
    AVG(data_quality_score) as avg_quality,
    COUNT(*) FILTER (WHERE data_quality_score < 0.80) as low_quality_count,
    COUNT(*) as total_records
FROM silver.users

UNION ALL

SELECT 
    'labs',
    AVG(data_quality_score),
    COUNT(*) FILTER (WHERE data_quality_score < 0.80),
    COUNT(*)
FROM silver.labs;
```

**Check ingestion run history:**

```sql
SELECT 
    source_system,
    entity_type,
    status,
    started_at,
    completed_at,
    completed_at - started_at as duration,
    records_processed,
    records_created,
    records_updated
FROM meta.ingestion_runs
ORDER BY started_at DESC
LIMIT 20;
```

### Migration Roadmap: Python → dbt

**Phase 1: Foundation (Q1 2025 - Current)**
- ✅ Complete composite table implementations in Python
- ✅ Document desired dbt patterns in standards
- ✅ Establish source-specific silver tables

**Phase 2: Pilot (Q2 2025)**
- Set up dbt project structure
- Migrate `silver.departments` (simple, low-risk)
- Establish patterns and team training

**Phase 3: Core Entities (Q3 2025)**
- Migrate `silver.users` (high-value)
- Migrate `silver.groups`
- Migrate `silver.computers`

**Phase 4: Composite Entities (Q4 2025)**
- Migrate `silver.labs` to dbt SQL
- Keep `lab_managers` scoring in Python
- Hybrid dbt + Python pattern established

**Phase 5: Gold Layer (2026)**
- Implement star schema analytics tables
- Executive dashboards and reporting
- Materialized aggregates for BI tools

---

## Summary

**LSATS Data Hub medallion architecture:**

1. **Bronze Layer** — Raw data in single universal table ([bronze_layer_standards.md](bronze_layer_standards.md))
2. **Silver Layer** — Three-tier architecture with composite entities ([silver_layer_standards.md](silver_layer_standards.md))
3. **Gold Layer** — Analytics-ready (future, 2026)
4. **Technology** — Python for ingestion/algorithms, dbt for transformations (future)
5. **Quality First** — All silver tables have quality scores and flags
6. **ETL-Managed** — No database foreign keys, quality flags instead
7. **Incremental Always** — Full-sync as override

**Key Documents:**
- **[bronze_layer_standards.md](bronze_layer_standards.md)** — Ingestion, change detection, universal table patterns
- **[silver_layer_standards.md](silver_layer_standards.md)** — Composite entities, dbt vs Python, transformation patterns
- **[database_script_standards.md](database_script_standards.md)** — Script structure, service classes, logging standards

**For Questions:**
- Architecture questions → This document
- Bronze ingestion → [bronze_layer_standards.md](bronze_layer_standards.md)
- Silver transformation → [silver_layer_standards.md](silver_layer_standards.md)
- Script structure → [database_script_standards.md](database_script_standards.md)
