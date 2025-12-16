# Gold Layer Standards

**Version:** 1.0  
**Last Updated:** 2025-01-24  
**Purpose:** Complete gold layer coding standards for dimensional modeling and analytics-ready data

---

## Table of Contents

1. [Overview & Philosophy](#overview--philosophy)
2. [Gold vs Silver: Key Differences](#gold-vs-silver-key-differences)
3. [Dimensional Modeling Fundamentals](#dimensional-modeling-fundamentals)
4. [Star Schema Design Patterns](#star-schema-design-patterns)
5. [Dimension Table Standards](#dimension-table-standards)
6. [Fact Table Standards](#fact-table-standards)
7. [Bridge Tables & Many-to-Many Relationships](#bridge-tables--many-to-many-relationships)
8. [Slowly Changing Dimensions (SCD)](#slowly-changing-dimensions-scd)
9. [Surrogate Keys vs Natural Keys](#surrogate-keys-vs-natural-keys)
10. [Denormalization Principles](#denormalization-principles)
11. [Grain Declaration & Management](#grain-declaration--management)
12. [dbt Implementation Patterns](#dbt-implementation-patterns)
13. [Incremental Refresh Strategies](#incremental-refresh-strategies)
14. [Partitioning & Performance Optimization](#partitioning--performance-optimization)
15. [Naming Conventions](#naming-conventions)
16. [Data Quality & Testing](#data-quality--testing)
17. [gold_views Schema Pattern](#gold_views-schema-pattern)
18. [Migration from Silver](#migration-from-silver)
19. [Common Anti-Patterns to Avoid](#common-anti-patterns-to-avoid)
20. [Reference Implementations](#reference-implementations)

---

## Overview & Philosophy

**Purpose:** Business-ready, query-optimized data for dashboards, reports, and analytics

**Key Principles:**
1. **Denormalized for speed** — Minimize joins through strategic denormalization
2. **Business-friendly** — Human-readable names, categories, derived metrics
3. **Star schema preferred** — Central fact tables surrounded by dimension tables
4. **Surrogate keys mandatory** — Integer keys for dimension PKs (performance)
5. **Grain-explicit** — Every fact table must declare its grain
6. **Incremental by default** — Daily/hourly refresh via dbt
7. **Type 1 SCD standard** — Current state only (Type 2 for critical dimensions only)
8. **PostgreSQL-optimized** — Leverage PostgreSQL features (JSONB, GIN indexes, partitioning)

**Current State (2025):** Initial gold layer build with compliance analytics focus  
**Future State (2026+):** Full dimensional model, predictive analytics, ML integration

**Technology Stack:**
- **Transformation:** dbt Core 1.7+ (SQL models)
- **Storage:** PostgreSQL 15+
- **Orchestration:** Python scripts → Airflow (future)
- **Testing:** dbt tests + custom SQL tests
- **Documentation:** dbt docs (auto-generated)

**References:**
- See [medallion_standards.md](medallion_standards.md) for cross-layer standards
- See [silver_layer_standards.md](silver_layer_standards.md) for source data patterns
- See [bronze_layer_standards.md](bronze_layer_standards.md) for raw data ingestion

---

## Gold vs Silver: Key Differences

| Aspect | Silver Layer | Gold Layer |
|--------|-------------|------------|
| **Purpose** | Single source of truth for ETL | Query-optimized for analytics |
| **Audience** | Data engineers, transformation scripts | Business users, analysts, dashboards |
| **Structure** | Normalized (3NF-like) | Denormalized (star schema) |
| **Joins Required** | Many (2-5 typical) | Few or zero (pre-joined) |
| **Foreign Keys** | None (ETL-managed) | Optional (dimension PKs for optimizer) |
| **Primary Keys** | Natural keys (business IDs) | Surrogate keys (integers) |
| **Complex Data** | JSONB arrays common | Simple typed columns preferred |
| **Data Quality Fields** | Required (scores, flags, source tracking) | Minimal (only what users need) |
| **Update Frequency** | Hourly/daily (source changes) | Daily/weekly (batch refresh) |
| **History Tracking** | Append-only source tables | SCD Type 1 (current) or Type 2 (history) |
| **Categories** | Raw values | Derived business categories |
| **Aggregations** | Computed on-demand | Pre-computed and stored |
| **Field Names** | Technical (snake_case IDs) | Business-friendly (descriptive) |
| **Example Query** | Requires 3+ joins | Zero joins (denormalized) |

**Example: Getting lab information**

```sql
-- SILVER: Requires multiple joins
SELECT 
    l.lab_id,
    l.lab_name,
    u.full_name AS pi_name,        -- JOIN to users
    d.department_name,              -- JOIN to departments
    d.college_group                 -- JOIN to departments
FROM silver.labs l
LEFT JOIN silver.users u ON l.pi_uniqname = u.uniqname
LEFT JOIN silver.departments d ON l.primary_department_id = d.dept_id
WHERE l.is_active = true;

-- GOLD: Zero joins (denormalized)
SELECT 
    lab_id,
    lab_name,
    pi_full_name,                   -- Already denormalized
    department_name,                -- Already denormalized
    college_group                   -- Already denormalized
FROM gold.dim_labs
WHERE is_active = true;
```

---

## Dimensional Modeling Fundamentals

### Kimball's Four-Step Design Process

**Official methodology from Kimball Group:**

1. **Select a business process** — What business activity are we analyzing?
   - Examples: compliance checking, ticket resolution, award spending
   - One fact table per business process

2. **Declare the grain** — What does one row in the fact table represent?
   - Examples: "one compliance check per computer per day", "one ticket"
   - Grain must be atomic (lowest level of detail)

3. **Identify the dimensions** — How do we slice/filter/group the facts?
   - Examples: time, lab, user, department, computer
   - Dimensions answer: WHO, WHAT, WHERE, WHEN

4. **Identify the facts** — What are we measuring?
   - Examples: compliance_rate, ticket_count, days_to_resolution
   - Facts are numeric, additive (can be summed) or semi-additive

**LSATS Example: Compliance Analytics**

```
1. Business Process: Daily compliance checking of lab computers
2. Grain: One row per computer per day
3. Dimensions: 
   - When: dim_time (date of check)
   - What computer: dim_computers (which device)
   - What lab: dim_labs (which research group)
   - What department: dim_departments (organizational unit)
4. Facts:
   - is_compliant (boolean measure)
   - compliance_score (0-100 numeric)
   - critical_issues_count (integer count)
```

---

### Star Schema vs Snowflake Schema

**Star Schema (RECOMMENDED):**
```
         ┌─────────┐
         │dim_time │
         └────┬────┘
              │
    ┌─────────┼─────────┐
    │         │         │
┌───▼───┐ ┌──▼────┐ ┌──▼────┐
│dim_   │ │ FACT  │ │dim_   │
│labs   │◄┤       ├►│depts  │
└───────┘ └───┬───┘ └───────┘
              │
         ┌────▼────┐
         │dim_     │
         │computers│
         └─────────┘
```

**Characteristics:**
- ✅ One central fact table
- ✅ Dimensions are denormalized (flat)
- ✅ Simple joins (fact → dimension only)
- ✅ Fast query performance
- ✅ Easy for BI tools to understand
- ✅ **Use this for LSATS**

**Snowflake Schema (NOT RECOMMENDED):**
```
         ┌─────────┐
         │dim_time │
         └────┬────┘
              │
         ┌────▼────┐     ┌───────────┐
         │ FACT    │────►│dim_labs   │
         └────┬────┘     └─────┬─────┘
              │                │
              │           ┌────▼──────┐
              │           │dim_       │
              │           │departments│
              └──────────►└───────────┘
```

**Characteristics:**
- ❌ Dimensions are normalized (hierarchical)
- ❌ Multiple join levels required
- ❌ Harder to query
- ❌ Slower performance
- ✅ Less storage (minimal duplication)
- ❌ **Avoid for LSATS** (storage is cheap, query speed matters)

**Decision Matrix:**

| Use Star Schema When | Use Snowflake When |
|---------------------|-------------------|
| Query performance critical (always) | Storage extremely constrained (rare) |
| Business users self-serve queries | Only data engineers query |
| BI tool integration needed | Legacy data warehouse migration |
| **LSATS use case** ✅ | Not applicable to LSATS ❌ |

---

### Fact vs Dimension Tables

**Dimensions = Descriptive Context (WHO, WHAT, WHERE, WHEN)**

**Characteristics:**
- Mostly text/categorical data
- Wide tables (20-50+ columns)
- Relatively static (changes slowly)
- Fewer rows (hundreds to millions)
- Surrogate key as PK
- Denormalized (no normalization)

**Examples:**
- `dim_time` — WHEN (calendar dates)
- `dim_labs` — WHAT (organizational units)
- `dim_users` — WHO (people)
- `dim_computers` — WHAT (assets)

**Typical Columns:**
```sql
CREATE TABLE gold.dim_{entity} (
    {entity}_key BIGSERIAL PRIMARY KEY,   -- Surrogate key
    {entity}_id VARCHAR UNIQUE,            -- Natural key
    
    -- Descriptive attributes
    name VARCHAR,
    description TEXT,
    type VARCHAR,
    category VARCHAR,
    
    -- Hierarchical attributes
    parent_name VARCHAR,
    level_1 VARCHAR,
    level_2 VARCHAR,
    
    -- Status flags
    is_active BOOLEAN,
    is_current BOOLEAN,
    
    -- Metadata
    last_refreshed TIMESTAMP
);
```

---

**Facts = Numeric Measurements (HOW MANY, HOW MUCH)**

**Characteristics:**
- Mostly numeric data (measures)
- Narrow tables (10-20 columns)
- Changes frequently (new rows constantly)
- Many rows (millions to billions)
- Foreign keys to dimensions
- Immutable once inserted (append-only)

**Examples:**
- `fact_compliance_daily` — Compliance measurements
- `fact_tickets` — Ticket metrics
- `fact_lab_activity_monthly` — Lab operational metrics

**Typical Columns:**
```sql
CREATE TABLE gold.fact_{subject} (
    fact_id BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions
    time_key INTEGER REFERENCES gold.dim_time(time_key),
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),
    
    -- Degenerate dimensions (IDs without dimension tables)
    transaction_id VARCHAR,
    
    -- Additive measures (can SUM)
    quantity INTEGER,
    amount NUMERIC(15,2),
    count_items INTEGER,
    
    -- Semi-additive measures (can AVG, not SUM)
    rate DECIMAL(5,2),
    percentage DECIMAL(5,2),
    
    -- Non-additive measures (ratios, averages)
    average_score DECIMAL(5,2),
    
    -- Flags
    is_successful BOOLEAN,
    
    -- Timestamp
    created_at TIMESTAMP
);
```

---

## Star Schema Design Patterns

### Pattern 1: Standard Star Schema

**Use When:** Most common analytics needs

```sql
-- Central fact table
CREATE TABLE gold.fact_compliance_daily (
    fact_id BIGSERIAL PRIMARY KEY,
    
    -- Dimensional keys (integers for performance)
    time_key INTEGER REFERENCES gold.dim_time(time_key),
    computer_key BIGINT REFERENCES gold.dim_computers(computer_key),
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),
    
    -- Measures
    is_compliant BOOLEAN,
    compliance_score DECIMAL(5,2),
    critical_issues_count INTEGER,
    
    created_at TIMESTAMP
);

-- Surrounding dimensions
-- dim_time, dim_computers, dim_labs (see Dimension Standards section)
```

**Query Pattern:**
```sql
-- Business question: "Show me average compliance by department last month"
SELECT 
    dt.fiscal_year,
    dt.fiscal_month,
    dl.department_name,
    AVG(fc.compliance_score) as avg_compliance,
    COUNT(DISTINCT fc.computer_key) as computers_checked
FROM gold.fact_compliance_daily fc
JOIN gold.dim_time dt ON fc.time_key = dt.time_key
JOIN gold.dim_labs dl ON fc.lab_key = dl.lab_key
WHERE dt.fiscal_month = 1 
  AND dt.fiscal_year = 2025
GROUP BY dt.fiscal_year, dt.fiscal_month, dl.department_name;
```

---

### Pattern 2: Factless Fact Table

**Use When:** Tracking events/relationships without measurements

```sql
-- Example: Lab membership events
CREATE TABLE gold.fact_lab_membership_events (
    event_id BIGSERIAL PRIMARY KEY,
    
    -- Dimensional keys
    user_key BIGINT REFERENCES gold.dim_users(user_key),
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),
    event_date_key INTEGER REFERENCES gold.dim_time(time_key),
    
    -- Event type (no numeric measures)
    event_type VARCHAR(50),  -- 'joined', 'left', 'role_changed'
    
    -- Degenerate dimensions
    previous_role VARCHAR(100),
    new_role VARCHAR(100),
    
    created_at TIMESTAMP
);
```

**Use Cases:**
- "How many people joined labs this month?"
- "Which labs have the highest turnover?"
- Event tracking without numeric measures

---

### Pattern 3: Aggregated Summary Tables

**Use When:** Frequent queries on pre-computed aggregations

```sql
-- Pre-aggregated departmental summary
CREATE TABLE gold.department_summary (
    dept_key BIGSERIAL PRIMARY KEY,
    dept_id VARCHAR(50) UNIQUE,
    
    -- Pre-computed metrics (updated daily)
    lab_count INTEGER,
    total_members INTEGER,
    total_computers INTEGER,
    avg_compliance_rate DECIMAL(5,2),
    
    -- Metadata
    as_of_date DATE,
    last_refreshed TIMESTAMP
);
```

**Benefits:**
- Instant query results (no GROUP BY needed)
- Executive dashboards (high-level metrics)
- Department comparison reports

---

## Dimension Table Standards

### Mandatory Schema Template

```sql
CREATE TABLE gold.dim_{entity} (
    -- ============================================
    -- KEYS (REQUIRED)
    -- ============================================
    {entity}_key BIGSERIAL PRIMARY KEY,        -- Surrogate key (integer)
    {entity}_id VARCHAR(100) UNIQUE NOT NULL,  -- Natural key (business ID)
    
    -- ============================================
    -- DESCRIPTIVE ATTRIBUTES (DENORMALIZED)
    -- ============================================
    {entity}_name VARCHAR(255) NOT NULL,
    {entity}_display_name VARCHAR(255),
    description TEXT,
    
    -- Category/Type fields
    {entity}_type VARCHAR(50),
    {entity}_category VARCHAR(50),
    
    -- ============================================
    -- DENORMALIZED PARENT DATA (if applicable)
    -- ============================================
    parent_{entity}_id VARCHAR(100),
    parent_{entity}_name VARCHAR(255),
    
    -- Organizational hierarchy
    department_name VARCHAR(255),
    college_group VARCHAR(255),
    campus_name VARCHAR(255),
    
    -- ============================================
    -- DERIVED BUSINESS CATEGORIES (if applicable)
    -- ============================================
    size_category VARCHAR(20),        -- 'small', 'medium', 'large'
    tier VARCHAR(20),                 -- 'tier1', 'tier2', 'tier3'
    risk_level VARCHAR(20),           -- 'low', 'medium', 'high'
    
    -- ============================================
    -- STATUS FLAGS (REQUIRED)
    -- ============================================
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_current BOOLEAN DEFAULT true,  -- For SCD Type 2
    
    -- ============================================
    -- SCD TYPE 2 FIELDS (OPTIONAL - only if tracking history)
    -- ============================================
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    version INTEGER DEFAULT 1,
    
    -- ============================================
    -- METADATA (REQUIRED)
    -- ============================================
    last_refreshed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ============================================
    -- CONSTRAINTS
    -- ============================================
    CONSTRAINT chk_{entity}_key_positive CHECK ({entity}_key > 0)
);

-- ============================================
-- STANDARD INDEXES (REQUIRED)
-- ============================================

-- Natural key lookup
CREATE UNIQUE INDEX idx_dim_{entity}_id ON gold.dim_{entity}({entity}_id);

-- Surrogate key (already indexed as PK, but explicit for documentation)
CREATE INDEX idx_dim_{entity}_key ON gold.dim_{entity}({entity}_key);

-- Active records filter
CREATE INDEX idx_dim_{entity}_active ON gold.dim_{entity}(is_active) 
WHERE is_active = true;

-- Current records filter (for SCD Type 2)
CREATE INDEX idx_dim_{entity}_current ON gold.dim_{entity}(is_current) 
WHERE is_current = true;

-- Parent relationship
CREATE INDEX idx_dim_{entity}_parent ON gold.dim_{entity}(parent_{entity}_id);

-- Business category filtering
CREATE INDEX idx_dim_{entity}_category ON gold.dim_{entity}({entity}_category);

-- ============================================
-- COMMENTS (REQUIRED)
-- ============================================

COMMENT ON TABLE gold.dim_{entity} IS 
'{Entity} dimension table. Grain: One row per {entity}. 
Denormalized for query performance. Updated daily via dbt.';

COMMENT ON COLUMN gold.dim_{entity}.{entity}_key IS 
'Surrogate key (integer) for fact table foreign keys.';

COMMENT ON COLUMN gold.dim_{entity}.{entity}_id IS 
'Natural key from source system(s). Use this for lookups by business ID.';
```

---

### Dimension Table Checklist

Before creating a dimension, verify:

- [ ] Surrogate key is `BIGSERIAL` (or `BIGINT` if manually assigned)
- [ ] Natural key is `UNIQUE NOT NULL`
- [ ] Denormalized parent data included (no joins to other dimensions required)
- [ ] Derived business categories calculated (size, tier, risk, etc.)
- [ ] `is_active` flag present for filtering
- [ ] Indexes created on natural key, categories, and flags
- [ ] Table and column comments added
- [ ] dbt model includes tests (unique, not_null, relationships)

---

### Common Dimension Patterns

**Pattern A: Simple Dimension (no hierarchy)**

```sql
-- Example: dim_users
CREATE TABLE gold.dim_users (
    user_key BIGSERIAL PRIMARY KEY,
    uniqname VARCHAR(50) UNIQUE NOT NULL,
    
    full_name VARCHAR(255),
    primary_email VARCHAR(255),
    job_title TEXT,
    
    -- Denormalized department (no join needed)
    department_name VARCHAR(255),
    college_group VARCHAR(255),
    
    is_active BOOLEAN NOT NULL,
    last_refreshed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

**Pattern B: Hierarchical Dimension**

```sql
-- Example: dim_departments
CREATE TABLE gold.dim_departments (
    dept_key BIGSERIAL PRIMARY KEY,
    dept_id VARCHAR(50) UNIQUE NOT NULL,
    
    department_name VARCHAR(255),
    department_code VARCHAR(50),
    
    -- Hierarchy levels (denormalized)
    parent_department_id VARCHAR(50),
    parent_department_name VARCHAR(255),
    college_group VARCHAR(255),
    campus_name VARCHAR(255),
    vp_area VARCHAR(255),
    
    -- Depth in hierarchy
    hierarchy_level INTEGER,
    full_path TEXT,  -- e.g., "LSA / Natural Sciences / Chemistry"
    
    is_active BOOLEAN NOT NULL,
    last_refreshed TIMESTAMP
);
```

---

**Pattern C: Role-Playing Dimension**

**Use When:** Same dimension used multiple times in one fact

```sql
-- dim_time is used as both "created_date" and "closed_date"
CREATE TABLE gold.fact_tickets (
    fact_id BIGSERIAL PRIMARY KEY,
    
    -- Same dimension, different roles
    created_date_key INTEGER REFERENCES gold.dim_time(time_key),
    closed_date_key INTEGER REFERENCES gold.dim_time(time_key),
    due_date_key INTEGER REFERENCES gold.dim_time(time_key),
    
    -- Other dimensions...
);

-- Query using role-playing dimension
SELECT 
    created.fiscal_year as created_year,
    closed.fiscal_year as closed_year,
    COUNT(*) as ticket_count
FROM gold.fact_tickets ft
JOIN gold.dim_time created ON ft.created_date_key = created.time_key
JOIN gold.dim_time closed ON ft.closed_date_key = closed.time_key;
```

**Alternative: Create dimension views for each role**

```sql
-- More readable approach
CREATE VIEW gold.dim_time_created AS SELECT * FROM gold.dim_time;
CREATE VIEW gold.dim_time_closed AS SELECT * FROM gold.dim_time;

-- Now queries are clearer
SELECT 
    tc.fiscal_year as created_year,
    tcl.fiscal_year as closed_year
FROM gold.fact_tickets ft
JOIN gold.dim_time_created tc ON ft.created_date_key = tc.time_key
JOIN gold.dim_time_closed tcl ON ft.closed_date_key = tcl.time_key;
```

---

## Fact Table Standards

### Mandatory Schema Template

```sql
CREATE TABLE gold.fact_{subject} (
    -- ============================================
    -- PRIMARY KEY (REQUIRED)
    -- ============================================
    fact_id BIGSERIAL PRIMARY KEY,
    
    -- ============================================
    -- DIMENSIONAL FOREIGN KEYS (REQUIRED)
    -- ============================================
    time_key INTEGER NOT NULL REFERENCES gold.dim_time(time_key),
    {entity}_key BIGINT NOT NULL REFERENCES gold.dim_{entity}({entity}_key),
    -- Additional dimension FKs...
    
    -- ============================================
    -- DEGENERATE DIMENSIONS (OPTIONAL)
    -- IDs that don't warrant full dimension tables
    -- ============================================
    transaction_id VARCHAR(100),
    confirmation_number VARCHAR(50),
    
    -- ============================================
    -- ADDITIVE MEASURES (can SUM across all dimensions)
    -- ============================================
    quantity INTEGER,
    amount NUMERIC(15,2),
    count_items INTEGER,
    duration_seconds INTEGER,
    
    -- ============================================
    -- SEMI-ADDITIVE MEASURES (can SUM across some dimensions)
    -- Example: balances (additive across accounts, not time)
    -- ============================================
    balance NUMERIC(15,2),
    inventory_count INTEGER,
    
    -- ============================================
    -- NON-ADDITIVE MEASURES (cannot SUM, use AVG/MIN/MAX)
    -- ============================================
    rate DECIMAL(5,2),
    percentage DECIMAL(5,2),
    ratio DECIMAL(10,4),
    temperature DECIMAL(5,2),
    
    -- ============================================
    -- FLAGS (BOOLEAN MEASURES)
    -- ============================================
    is_successful BOOLEAN,
    is_compliant BOOLEAN,
    is_exception BOOLEAN,
    
    -- ============================================
    -- METADATA (REQUIRED)
    -- ============================================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    
    -- ============================================
    -- CONSTRAINTS
    -- ============================================
    -- Grain enforcement (prevents duplicates at declared grain)
    CONSTRAINT unique_{subject}_grain UNIQUE(time_key, {entity}_key, ...)
);

-- ============================================
-- STANDARD INDEXES (REQUIRED)
-- ============================================

-- Dimensional queries (most common pattern)
CREATE INDEX idx_fact_{subject}_time ON gold.fact_{subject}(time_key DESC);
CREATE INDEX idx_fact_{subject}_{entity}_time ON gold.fact_{subject}({entity}_key, time_key DESC);

-- Reverse lookup (dimension to facts)
CREATE INDEX idx_fact_{subject}_{entity} ON gold.fact_{subject}({entity}_key);

-- Filtered queries
CREATE INDEX idx_fact_{subject}_flag ON gold.fact_{subject}(time_key) 
WHERE is_successful = false;

-- ============================================
-- PARTITIONING (RECOMMENDED for large facts)
-- ============================================

-- PostgreSQL 10+ declarative partitioning
-- See Partitioning section for full implementation

-- ============================================
-- COMMENTS (REQUIRED - especially GRAIN)
-- ============================================

COMMENT ON TABLE gold.fact_{subject} IS 
'{Subject} fact table. 
GRAIN: {Explicit grain declaration - e.g., "One row per computer per day"}
Source: {silver table or source}
Updated: {frequency - e.g., "Daily via dbt"}';

COMMENT ON COLUMN gold.fact_{subject}.{measure} IS 
'{Measure description}. Type: {additive/semi-additive/non-additive}';
```

---

### Fact Table Grain Declaration

**CRITICAL:** Every fact table MUST explicitly declare its grain

**Grain = What does one row represent?**

**Examples of Good Grain Declarations:**

```sql
COMMENT ON TABLE gold.fact_compliance_daily IS 
'GRAIN: One row per computer per day.
Each row represents a single compliance check performed on a specific computer on a specific date.';

COMMENT ON TABLE gold.fact_tickets IS
'GRAIN: One row per ticket (ticket lifecycle).
Each row represents a complete ticket from creation to closure.';

COMMENT ON TABLE gold.fact_lab_activity_monthly IS
'GRAIN: One row per lab per month.
Each row represents aggregated activity metrics for a lab during a calendar month.';

COMMENT ON TABLE gold.fact_award_transactions IS
'GRAIN: One row per financial transaction.
Each row represents a single expenditure against an award.';
```

**Grain Determines:**
- Which dimensions are included
- What measures can be calculated
- How to handle duplicates
- Query patterns and performance

---

### Measure Type Classification

**Additive Measures (can SUM across ALL dimensions)**

```sql
-- These are safe to aggregate freely
quantity INTEGER,
amount NUMERIC(15,2),
count_computers INTEGER,
total_tickets INTEGER,
duration_minutes INTEGER
```

**Query:**
```sql
-- Can SUM across time, labs, departments, etc.
SELECT 
    SUM(count_computers),
    SUM(total_tickets)
FROM gold.fact_lab_activity_monthly;
```

---

**Semi-Additive Measures (can SUM across SOME dimensions, not time)**

```sql
-- Can sum across labs/depts, but NOT across time (use AVG)
current_balance NUMERIC(15,2),
inventory_on_hand INTEGER,
headcount INTEGER
```

**Query:**
```sql
-- ✅ CORRECT: Average balance over time
SELECT 
    AVG(current_balance) as avg_balance
FROM gold.fact_monthly_balances
WHERE time_key BETWEEN 20250101 AND 20250131;

-- ❌ WRONG: Don't SUM balances across time
SELECT 
    SUM(current_balance)  -- This makes no sense!
FROM gold.fact_monthly_balances;
```

---

**Non-Additive Measures (cannot SUM, use AVG/MIN/MAX/etc.)**

```sql
-- These are ratios, rates, percentages
compliance_rate DECIMAL(5,2),     -- Already a percentage
temperature DECIMAL(5,2),
average_score DECIMAL(5,2)
```

**Query:**
```sql
-- ✅ CORRECT: Average of rates
SELECT AVG(compliance_rate)
FROM gold.fact_compliance_daily;

-- ❌ WRONG: Cannot SUM rates
SELECT SUM(compliance_rate)  -- Nonsensical result
FROM gold.fact_compliance_daily;
```

---

### Fact Table Checklist

Before creating a fact, verify:

- [ ] Grain explicitly declared in table comment
- [ ] Primary key is `BIGSERIAL` or composite unique constraint
- [ ] All dimension FKs use surrogate keys (integers, not VARCHAR)
- [ ] At least one dimension is `time_key` (almost always required)
- [ ] Measures classified (additive/semi-additive/non-additive)
- [ ] Grain enforcement via UNIQUE constraint
- [ ] Indexes on dimensional FKs created
- [ ] Table comment includes grain, source, update frequency
- [ ] dbt model includes tests (unique, not_null, relationships)
- [ ] Consider partitioning if > 1M rows expected

---

## Bridge Tables & Many-to-Many Relationships

### When to Use Bridge Tables

**Problem:** A fact needs to relate to MULTIPLE dimension records

**Examples:**
- One article has multiple authors
- One diagnosis has multiple procedures
- One lab has multiple members
- One computer belongs to multiple groups

**Without Bridge Table (WRONG):**
```sql
-- ❌ Creates duplicate facts (overcounting)
CREATE TABLE gold.fact_article_views (
    fact_id BIGSERIAL PRIMARY KEY,
    article_id VARCHAR(100),
    author_id VARCHAR(100),  -- Problem: multiple authors!
    view_count INTEGER
);

-- Query problem: Views are counted multiple times
SELECT article_id, SUM(view_count)
FROM gold.fact_article_views
GROUP BY article_id;
-- Wrong result if article has 3 authors!
```

**With Bridge Table (CORRECT):**
```sql
-- Fact table (one row per article)
CREATE TABLE gold.fact_article_views (
    fact_id BIGSERIAL PRIMARY KEY,
    article_key BIGINT REFERENCES gold.dim_articles(article_key),
    author_group_key BIGINT REFERENCES gold.bridge_article_authors(group_key),
    view_count INTEGER
);

-- Bridge table resolves many-to-many
CREATE TABLE gold.bridge_article_authors (
    bridge_id BIGSERIAL PRIMARY KEY,
    group_key BIGINT NOT NULL,        -- Links to fact
    author_key BIGINT NOT NULL REFERENCES gold.dim_authors(author_key),
    
    -- Weighting to prevent overcounting
    allocation_weight DECIMAL(5,4),   -- Sums to 1.0 per group
    
    UNIQUE(group_key, author_key)
);
```

---

### Bridge Table Standard Schema

```sql
CREATE TABLE gold.bridge_{entity1}_{entity2} (
    -- ============================================
    -- KEYS
    -- ============================================
    bridge_id BIGSERIAL PRIMARY KEY,
    
    -- Group key (links to fact table)
    group_key BIGINT NOT NULL,
    
    -- Dimension key (links to dimension)
    {entity}_key BIGINT NOT NULL REFERENCES gold.dim_{entity}({entity}_key),
    
    -- ============================================
    -- WEIGHTING (CRITICAL for preventing overcounting)
    -- ============================================
    allocation_weight DECIMAL(5,4) NOT NULL,  -- Must sum to 1.0 per group
    
    -- Alternative: allocation_percentage
    allocation_percentage DECIMAL(5,2),       -- Must sum to 100.0 per group
    
    -- ============================================
    -- OPTIONAL: DENORMALIZED ATTRIBUTES
    -- (for query performance)
    -- ============================================
    {entity}_name VARCHAR(255),
    {entity}_type VARCHAR(50),
    
    -- ============================================
    -- FLAGS
    -- ============================================
    is_primary BOOLEAN DEFAULT false,  -- Designate one primary member
    
    -- ============================================
    -- METADATA
    -- ============================================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ============================================
    -- CONSTRAINTS
    -- ============================================
    UNIQUE(group_key, {entity}_key),
    CHECK (allocation_weight > 0 AND allocation_weight <= 1.0)
);

-- Indexes
CREATE INDEX idx_bridge_{entity1}_{entity2}_group ON gold.bridge_{entity1}_{entity2}(group_key);
CREATE INDEX idx_bridge_{entity1}_{entity2}_{entity} ON gold.bridge_{entity1}_{entity2}({entity}_key);

COMMENT ON TABLE gold.bridge_{entity1}_{entity2} IS
'Bridge table resolving many-to-many relationship between {entity1} and {entity2}.
WEIGHTING: allocation_weight must sum to 1.0 per group_key to prevent overcounting.';
```

---

### Weighting Example: Article Authors

```sql
-- Article A has 3 authors
INSERT INTO gold.bridge_article_authors (group_key, author_key, allocation_weight) VALUES
(1, 101, 0.3333),  -- Author 1 gets 33.33%
(1, 102, 0.3333),  -- Author 2 gets 33.33%
(1, 103, 0.3334);  -- Author 3 gets 33.34% (total = 1.0)

-- Query with weighting
SELECT 
    da.author_name,
    SUM(fav.view_count * ba.allocation_weight) as weighted_views
FROM gold.fact_article_views fav
JOIN gold.bridge_article_authors ba ON fav.author_group_key = ba.group_key
JOIN gold.dim_authors da ON ba.author_key = da.author_key
GROUP BY da.author_name;

-- Now each view is correctly allocated across authors
```

---

### Alternative: Change Fact Grain

**Sometimes better to change grain instead of bridge table:**

```sql
-- Instead of: One row per article
-- Use: One row per article per author

CREATE TABLE gold.fact_article_author_views (
    fact_id BIGSERIAL PRIMARY KEY,
    article_key BIGINT,
    author_key BIGINT,
    view_count INTEGER,
    
    UNIQUE(article_key, author_key)
);

-- Now query is simpler (but fact table is larger)
SELECT 
    author_key,
    SUM(view_count) as total_views
FROM gold.fact_article_author_views
GROUP BY author_key;
```

**Trade-offs:**
- ✅ Simpler queries (no bridge table)
- ❌ Larger fact table (more rows)
- ❌ Need to be careful when aggregating by article (avoid double-counting)

---

## Slowly Changing Dimensions (SCD)

### SCD Type 1: Overwrite (STANDARD for LSATS)

**Use When:**
- Only current state matters
- Historical changes not needed for analysis
- Simplest to implement and query

**Implementation:**
```sql
CREATE TABLE gold.dim_users (
    user_key BIGSERIAL PRIMARY KEY,
    uniqname VARCHAR(50) UNIQUE,
    
    full_name VARCHAR(255),
    job_title TEXT,
    department_name VARCHAR(255),
    
    -- Type 1: Just update in place
    last_refreshed TIMESTAMP
);

-- Daily update (overwrites previous values)
UPDATE gold.dim_users
SET 
    job_title = 'Associate Professor',
    department_name = 'Chemistry',
    last_refreshed = CURRENT_TIMESTAMP
WHERE uniqname = 'jdoe';
```

**Pro:**
- ✅ Simple to implement
- ✅ Fast queries (no date filtering)
- ✅ Small table size

**Con:**
- ❌ No historical tracking
- ❌ Can't analyze "as of" past date

---

### SCD Type 2: Track History (OPTIONAL for critical dimensions)

**Use When:**
- Historical analysis required ("what was compliance rate when lab had different PI?")
- Regulatory/audit requirements
- Trend analysis needs

**Implementation:**
```sql
CREATE TABLE gold.dim_users (
    user_key BIGSERIAL PRIMARY KEY,      -- Surrogate key (changes with version)
    uniqname VARCHAR(50) NOT NULL,       -- Natural key (same across versions)
    
    full_name VARCHAR(255),
    job_title TEXT,
    department_name VARCHAR(255),
    
    -- SCD Type 2 fields
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,              -- NULL = current version
    is_current BOOLEAN DEFAULT true,
    version INTEGER DEFAULT 1,
    
    -- Optional: reason for change
    change_reason VARCHAR(255),
    
    last_refreshed TIMESTAMP
);

-- Indexes for SCD Type 2
CREATE INDEX idx_dim_users_current ON gold.dim_users(uniqname, is_current) 
WHERE is_current = true;

CREATE INDEX idx_dim_users_effective ON gold.dim_users(effective_from, effective_to);

-- When job title changes:
-- 1. Expire old record
UPDATE gold.dim_users
SET 
    is_current = false,
    effective_to = CURRENT_TIMESTAMP
WHERE uniqname = 'jdoe' AND is_current = true;

-- 2. Insert new record
INSERT INTO gold.dim_users (
    uniqname, full_name, job_title, department_name,
    effective_from, is_current, version
) VALUES (
    'jdoe', 'John Doe', 'Associate Professor', 'Chemistry',
    CURRENT_TIMESTAMP, true, 2
);
```

**Pro:**
- ✅ Complete historical tracking
- ✅ Point-in-time analysis
- ✅ Audit trail

**Con:**
- ❌ More complex queries (need WHERE is_current = true)
- ❌ Larger table size (multiple rows per entity)
- ❌ More complex ETL logic

---

### SCD Type 2 in dbt (Snapshots)

**dbt provides built-in SCD Type 2 via snapshots:**

```sql
-- snapshots/dim_users_snapshot.sql
{% snapshot dim_users_snapshot %}

{{
    config(
      target_schema='gold',
      unique_key='uniqname',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT * FROM {{ ref('users') }}

{% endsnapshot %}
```

**Run snapshot:**
```bash
dbt snapshot
```

**dbt automatically adds:**
- `dbt_valid_from` (effective_from)
- `dbt_valid_to` (effective_to)
- `dbt_updated_at` (last change timestamp)

---

### SCD Type 3: Current + Previous (RARELY USED)

**Use When:**
- Only last value matters (not full history)
- Space constrained

**Implementation:**
```sql
CREATE TABLE gold.dim_users (
    user_key BIGSERIAL PRIMARY KEY,
    uniqname VARCHAR(50) UNIQUE,
    
    full_name VARCHAR(255),
    
    -- Current value
    current_job_title TEXT,
    current_department VARCHAR(255),
    
    -- Previous value (one version back)
    previous_job_title TEXT,
    previous_department VARCHAR(255),
    
    last_changed TIMESTAMP
);
```

**Generally avoid Type 3** - if you need history, use Type 2 properly.

---

### SCD Decision Matrix

| Requirement | Type 1 | Type 2 | Type 3 |
|-------------|--------|--------|--------|
| Only current state needed | ✅ YES | ❌ Overkill | ❌ Overkill |
| Full historical tracking | ❌ No | ✅ YES | ❌ Incomplete |
| Point-in-time analysis | ❌ No | ✅ YES | ❌ No |
| Simple queries | ✅ YES | ❌ Complex | ⚠️ Moderate |
| Small table size | ✅ YES | ❌ Grows | ✅ YES |
| Audit/compliance | ❌ No trail | ✅ YES | ⚠️ Limited |
| **LSATS standard** | ✅ **Default** | ⚠️ **Critical dims only** | ❌ **Avoid** |

---

## Surrogate Keys vs Natural Keys

### Surrogate Keys (REQUIRED for Gold Dimensions)

**Definition:** System-generated integer keys with no business meaning

**Implementation:**
```sql
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,        -- Surrogate key
    lab_id VARCHAR(100) UNIQUE NOT NULL,  -- Natural key
    ...
);
```

**Benefits:**
- ✅ **Performance:** Integer joins 3-5x faster than VARCHAR joins
- ✅ **Stability:** Doesn't change if business key changes
- ✅ **Smaller indexes:** 8 bytes (BIGINT) vs 50+ bytes (VARCHAR)
- ✅ **SCD Type 2 friendly:** Same natural key, multiple surrogate keys
- ✅ **Database agnostic:** No business logic in PK

**Example Performance:**
```sql
-- Slow: VARCHAR foreign key
CREATE TABLE fact_table (
    lab_id VARCHAR(100) REFERENCES dim_labs(lab_id)
);

-- Fast: Integer foreign key
CREATE TABLE fact_table (
    lab_key BIGINT REFERENCES dim_labs(lab_key)
);

-- Query performance difference:
-- Integer join: ~100ms for 1M rows
-- VARCHAR join: ~300-500ms for 1M rows
```

---

### Natural Keys (Keep for Lookups)

**Definition:** Business-meaningful identifiers from source systems

**Usage in Gold:**
- Keep as `UNIQUE NOT NULL` column
- Use for lookups: `WHERE lab_id = 'jdoe'`
- Don't use as PRIMARY KEY
- Don't use as foreign keys

```sql
-- ✅ CORRECT
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,        -- Surrogate PK
    lab_id VARCHAR(100) UNIQUE NOT NULL,  -- Natural key for lookups
    ...
);

CREATE TABLE gold.fact_compliance (
    fact_id BIGSERIAL PRIMARY KEY,
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),  -- Surrogate FK
    ...
);

-- Lookup by natural key when needed
SELECT lab_key FROM gold.dim_labs WHERE lab_id = 'jdoe';

-- ❌ WRONG
CREATE TABLE gold.dim_labs (
    lab_id VARCHAR(100) PRIMARY KEY,  -- Don't use natural key as PK in gold
    ...
);
```

---

### Generating Surrogate Keys in dbt

**Method 1: Database SERIAL (Recommended for PostgreSQL)**

```sql
-- models/gold/dimensions/dim_labs.sql
{{
    config(
        materialized='table',
        indexes=[{'columns': ['lab_key'], 'unique': True}]
    )
}}

-- PostgreSQL generates lab_key automatically via SERIAL
SELECT
    -- No need to generate lab_key, BIGSERIAL handles it
    l.lab_id,
    l.lab_name,
    ...
FROM {{ ref('labs') }} l
```

**Method 2: dbt_utils.generate_surrogate_key() (Cross-database)**

```sql
-- models/gold/dimensions/dim_labs.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['lab_id']) }} AS lab_key,
    lab_id,
    lab_name,
    ...
FROM {{ ref('labs') }}
```

**Method 3: ROW_NUMBER() (For specific ordering)**

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY lab_id) AS lab_key,
    lab_id,
    lab_name,
    ...
FROM {{ ref('labs') }}
```

---

## Denormalization Principles

### What to Denormalize

**✅ ALWAYS Denormalize These:**

1. **Frequently joined parent attributes**
```sql
-- Instead of joining to departments every query
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100),
    
    -- Denormalized from departments (no join needed)
    department_name VARCHAR(255),
    college_group VARCHAR(255),
    campus_name VARCHAR(255)
);
```

2. **Display-ready text fields**
```sql
-- Denormalize human-readable names
pi_full_name VARCHAR(255),           -- Instead of storing just pi_id
department_name VARCHAR(255),        -- Instead of just department_id
status_display_name VARCHAR(50),     -- Instead of status_code
```

3. **Small categorical hierarchies**
```sql
-- Organizational hierarchy
department_name VARCHAR(255),
college_group VARCHAR(255),
vp_area VARCHAR(255),
campus_name VARCHAR(255)
```

4. **Commonly filtered attributes**
```sql
-- If users frequently filter by these, denormalize
is_active BOOLEAN,
tier VARCHAR(20),
category VARCHAR(50)
```

---

### What NOT to Denormalize

**❌ AVOID Denormalizing These:**

1. **Large text blobs**
```sql
-- ❌ Don't denormalize
award_full_description TEXT,  -- Could be 10KB+
lab_notes TEXT,
ticket_full_history TEXT

-- ✅ Keep in separate table or link
award_summary VARCHAR(500),   -- Short summary instead
```

2. **Frequently changing data**
```sql
-- ❌ Don't denormalize (will be out of sync)
computer_last_seen TIMESTAMP,     -- Changes hourly
current_ticket_count INTEGER,     -- Changes constantly

-- ✅ Query from fact table or keep separate
```

3. **Complex nested structures**
```sql
-- ❌ Don't denormalize full JSONB
all_attributes JSONB,
full_member_list JSONB,

-- ✅ Flatten important fields only
primary_attribute_value VARCHAR(100),
member_count INTEGER
```

4. **Rarely accessed fields**
```sql
-- ❌ Don't denormalize if only needed 1% of queries
internal_tracking_code VARCHAR(50),
legacy_system_reference VARCHAR(100)
```

---

### Denormalization Decision Matrix

| Field Type | Denormalize? | Rationale |
|------------|-------------|-----------|
| Parent entity name | ✅ YES | Frequently displayed |
| Parent entity ID | ⚠️ MAYBE | If frequently filtered |
| Grandparent entity name | ⚠️ MAYBE | If frequently used |
| Text description (<500 chars) | ✅ YES | Display-ready |
| Text description (>1000 chars) | ❌ NO | Too large |
| Categorical hierarchy (3-5 levels) | ✅ YES | Common pattern |
| Created/updated timestamps | ⚠️ MAYBE | If frequently filtered |
| Last activity timestamp | ❌ NO | Changes too often |
| Count of children | ✅ YES | Pre-computed aggregation |
| Full list of children | ❌ NO | Use fact/bridge table |
| Status code | ❌ NO | Use lookup |
| Status display name | ✅ YES | Display-ready |

---

### LSATS Examples

**dim_labs Denormalization:**

```sql
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100) UNIQUE,
    
    -- ✅ Denormalized PI data (from users)
    pi_uniqname VARCHAR(50),
    pi_full_name VARCHAR(255),           -- Frequently displayed
    pi_email VARCHAR(255),                -- Frequently used
    pi_job_title TEXT,                    -- Business context
    
    -- ✅ Denormalized department data (from departments)
    primary_department_id VARCHAR(50),    -- For filtering
    department_name VARCHAR(255),         -- Frequently displayed
    college_group VARCHAR(255),           -- Grouping/filtering
    campus_name VARCHAR(255),             -- Hierarchy level
    
    -- ✅ Pre-computed counts
    member_count INTEGER,
    computer_count INTEGER,
    
    -- ✅ Derived categories
    lab_size_category VARCHAR(20),
    funding_tier VARCHAR(20),
    
    -- ❌ NOT denormalized (kept in fact tables)
    -- current_compliance_rate (changes daily)
    -- open_ticket_count (changes constantly)
    -- full_member_list (use lab_members table)
);
```

---

## Grain Declaration & Management

### Why Grain Matters

**Grain = The atomic level of detail represented by one fact table row**

**Critical for:**
- Preventing duplicate rows
- Choosing correct dimensions
- Determining query patterns
- Calculating aggregations correctly

**Bad grain = Bad data = Bad decisions**

---

### Declaring Grain

**Every fact table MUST have explicit grain declaration in table comment:**

```sql
COMMENT ON TABLE gold.fact_{subject} IS 
'GRAIN: {One explicit sentence declaring what one row represents}
Example 1: One row per computer per day
Example 2: One row per ticket (complete lifecycle)
Example 3: One row per lab per month
Example 4: One row per award transaction

Source: {source table}
Updated: {frequency}';
```

---

### Grain Enforcement via Constraints

**Use UNIQUE constraints to enforce grain:**

```sql
-- Grain: One row per computer per day
CREATE TABLE gold.fact_compliance_daily (
    fact_id BIGSERIAL PRIMARY KEY,
    computer_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    ...
    
    -- Enforce grain
    CONSTRAINT unique_compliance_grain UNIQUE(computer_key, time_key)
);

-- Grain: One row per ticket
CREATE TABLE gold.fact_tickets (
    fact_id BIGSERIAL PRIMARY KEY,
    ticket_id INTEGER NOT NULL,
    ...
    
    -- Enforce grain
    CONSTRAINT unique_ticket_grain UNIQUE(ticket_id)
);

-- Grain: One row per lab per month
CREATE TABLE gold.fact_lab_activity_monthly (
    fact_id BIGSERIAL PRIMARY KEY,
    lab_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,  -- Points to first day of month in dim_time
    ...
    
    -- Enforce grain
    CONSTRAINT unique_lab_monthly_grain UNIQUE(lab_key, time_key)
);
```

---

### Common Grain Patterns

**Pattern 1: Transactional Grain**
- One row per business transaction/event
- Examples: ticket, purchase, login event
- Most atomic level

```sql
-- GRAIN: One row per ticket
CREATE TABLE gold.fact_tickets (
    fact_id BIGSERIAL PRIMARY KEY,
    ticket_id INTEGER UNIQUE,  -- Natural grain enforcement
    ...
);
```

---

**Pattern 2: Periodic Snapshot Grain**
- One row per entity per time period
- Examples: daily compliance, monthly activity
- Regular intervals

```sql
-- GRAIN: One row per computer per day
CREATE TABLE gold.fact_compliance_daily (
    fact_id BIGSERIAL PRIMARY KEY,
    computer_key BIGINT,
    time_key INTEGER,  -- Daily granularity
    ...
    UNIQUE(computer_key, time_key)
);
```

---

**Pattern 3: Accumulating Snapshot Grain**
- One row per business process lifecycle
- Multiple date columns (created, assigned, completed, etc.)
- Row updated as process progresses

```sql
-- GRAIN: One row per ticket lifecycle
CREATE TABLE gold.fact_ticket_lifecycle (
    fact_id BIGSERIAL PRIMARY KEY,
    ticket_id INTEGER UNIQUE,
    
    -- Multiple dates (updated as ticket progresses)
    created_date_key INTEGER,
    assigned_date_key INTEGER,
    resolved_date_key INTEGER,
    closed_date_key INTEGER,
    
    -- Status (updated)
    current_status VARCHAR(50),
    
    -- Measures (some updated, some final)
    days_to_assignment INTEGER,  -- Updated when assigned
    days_to_resolution INTEGER,  -- Updated when resolved
    ...
);
```

---

**Pattern 4: Aggregated Grain**
- One row per pre-computed aggregation
- Examples: department summary, monthly rollup
- Summarized from more atomic facts

```sql
-- GRAIN: One row per department (current snapshot)
CREATE TABLE gold.department_summary (
    dept_key BIGSERIAL PRIMARY KEY,
    dept_id VARCHAR(50) UNIQUE,
    
    -- Pre-aggregated metrics
    lab_count INTEGER,
    total_computers INTEGER,
    avg_compliance_rate DECIMAL(5,2),
    
    -- Snapshot metadata
    as_of_date DATE
);
```

---

### Grain Testing in dbt

```yaml
# models/gold/facts/schema.yml
models:
  - name: fact_compliance_daily
    description: |
      GRAIN: One row per computer per day.
      Daily compliance check results.
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - computer_key
            - time_key
    columns:
      - name: computer_key
        tests:
          - not_null
      - name: time_key
        tests:
          - not_null
```

---

## dbt Implementation Patterns

### Project Structure

```
lsats_gold/                          # dbt project root
├── dbt_project.yml
├── profiles.yml (in ~/.dbt/)
│
├── models/
│   ├── sources.yml                  # Define silver as source
│   │
│   ├── gold/
│   │   ├── dimensions/
│   │   │   ├── dim_time.sql
│   │   │   ├── dim_labs.sql
│   │   │   ├── dim_computers.sql
│   │   │   ├── dim_users.sql
│   │   │   ├── dim_departments.sql
│   │   │   └── schema.yml           # Tests & documentation
│   │   │
│   │   ├── facts/
│   │   │   ├── fact_compliance_daily.sql
│   │   │   ├── fact_lab_activity_monthly.sql
│   │   │   └── schema.yml
│   │   │
│   │   └── summaries/
│   │       ├── department_summary.sql
│   │       └── schema.yml
│   │
│   └── gold_views/                  # Display-ready views
│       ├── v_labs_monitored.sql
│       ├── v_compliance_dashboard.sql
│       └── schema.yml
│
├── macros/
│   ├── generate_schema_name.sql     # Custom schema logic
│   └── custom_tests.sql             # Custom dbt tests
│
├── tests/
│   └── compliance_quality_checks.sql
│
└── snapshots/                        # SCD Type 2 snapshots
    └── dim_users_snapshot.sql
```

---

### dbt_project.yml Configuration

```yaml
name: 'lsats_gold'
version: '1.0.0'
config-version: 2

profile: 'lsats_gold'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Model-specific configurations
models:
  lsats_gold:
    # Gold dimensions (materialized as tables)
    gold:
      +schema: gold
      dimensions:
        +materialized: table
        +tags: ['gold', 'dimension']
      
      # Gold facts (incremental for performance)
      facts:
        +materialized: incremental
        +on_schema_change: fail
        +tags: ['gold', 'fact']
      
      # Gold summaries (tables, full refresh)
      summaries:
        +materialized: table
        +tags: ['gold', 'summary']
    
    # Gold views (always views, never tables)
    gold_views:
      +schema: gold_views
      +materialized: view
      +tags: ['gold_views']

# Test configurations
tests:
  lsats_gold:
    +severity: error  # All tests fail builds by default
```

---

### Source Definition (sources.yml)

```yaml
# models/sources.yml
version: 2

sources:
  - name: silver
    description: "Silver layer tables (cleaned and unified data)"
    database: lsats_db
    schema: silver
    
    tables:
      - name: labs
        description: "Composite lab records from awards + OUs"
        columns:
          - name: lab_id
            tests:
              - unique
              - not_null
      
      - name: users
        description: "Consolidated user records from all sources"
        columns:
          - name: uniqname
            tests:
              - unique
              - not_null
      
      - name: departments
        description: "Merged department data from TDX + UMAPI"
      
      - name: computers
        description: "Computer records from TDX + AD + KeyConfigure"
      
      - name: computer_compliance_history
        description: "Historical compliance check results"
        columns:
          - name: computer_id
            tests:
              - not_null
          - name: check_date
            tests:
              - not_null
```

---

### Dimension Model Template (dbt)

```sql
-- models/gold/dimensions/dim_labs.sql
{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['lab_key'], 'unique': True},
            {'columns': ['lab_id'], 'unique': True}
        ],
        tags=['gold', 'dimension', 'labs']
    )
}}

WITH labs AS (
    SELECT * FROM {{ source('silver', 'labs') }}
),

users AS (
    SELECT * FROM {{ source('silver', 'users') }}
),

departments AS (
    SELECT * FROM {{ source('silver', 'departments') }}
)

SELECT
    -- Surrogate key (generated by BIGSERIAL in table definition)
    ROW_NUMBER() OVER (ORDER BY l.lab_id) AS lab_key,
    
    -- Natural key
    l.lab_id,
    l.lab_name,
    l.pi_uniqname,
    
    -- Denormalized PI data
    u.full_name AS pi_full_name,
    u.primary_email AS pi_email,
    u.work_phone AS pi_phone,
    u.job_title AS pi_job_title,
    u.tdx_user_uid AS pi_tdx_uid,
    
    -- Denormalized department data
    l.primary_department_id,
    d.department_name,
    d.college_group,
    d.campus_name,
    
    -- Metrics
    l.total_award_dollars,
    l.member_count,
    l.computer_count,
    
    -- Derived business categories
    CASE
        WHEN l.member_count = 0 THEN 'empty'
        WHEN l.member_count <= 5 THEN 'small'
        WHEN l.member_count <= 15 THEN 'medium'
        WHEN l.member_count <= 30 THEN 'large'
        ELSE 'xl'
    END AS lab_size_category,
    
    CASE
        WHEN l.total_award_dollars = 0 THEN 'unfunded'
        WHEN l.total_award_dollars < 100000 THEN 'low'
        WHEN l.total_award_dollars < 500000 THEN 'medium'
        ELSE 'high'
    END AS funding_tier,
    
    -- Status
    l.is_active,
    (l.computer_count > 0 AND d.tdx_id IS NOT NULL) AS is_monitored,
    
    -- Metadata
    CURRENT_TIMESTAMP AS last_refreshed,
    CURRENT_TIMESTAMP AS created_at

FROM labs l
LEFT JOIN users u ON l.pi_uniqname = u.uniqname
LEFT JOIN departments d ON l.primary_department_id = d.dept_id
```

**Testing (schema.yml):**

```yaml
# models/gold/dimensions/schema.yml
version: 2

models:
  - name: dim_labs
    description: |
      Lab dimension table (denormalized).
      One row per lab with PI and department context.
    columns:
      - name: lab_key
        description: "Surrogate key (integer) for fact table FKs"
        tests:
          - unique
          - not_null
      
      - name: lab_id
        description: "Natural key (PI uniqname)"
        tests:
          - unique
          - not_null
      
      - name: lab_name
        tests:
          - not_null
      
      - name: is_active
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

---

### Fact Model Template (dbt - Incremental)

```sql
-- models/gold/facts/fact_compliance_daily.sql
{{
    config(
        materialized='incremental',
        unique_key=['computer_key', 'time_key'],
        on_schema_change='fail',
        tags=['gold', 'fact', 'compliance']
    )
}}

WITH compliance_history AS (
    SELECT * FROM {{ source('silver', 'computer_compliance_history') }}
    
    {% if is_incremental() %}
    -- Only process new dates since last run
    WHERE check_date > (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}
),

dim_computers AS (
    SELECT * FROM {{ ref('dim_computers') }}
),

dim_labs AS (
    SELECT * FROM {{ ref('dim_labs') }}
),

dim_time AS (
    SELECT * FROM {{ ref('dim_time') }}
)

SELECT
    -- Dimensional keys
    dc.computer_key,
    dl.lab_key,
    dt.time_key,
    
    -- Degenerate dimensions
    ch.check_date AS snapshot_date,
    ch.check_timestamp AS snapshot_timestamp,
    
    -- Measures
    ch.is_compliant,
    ch.compliance_score,
    ch.is_bitlocker_enabled,
    ch.is_av_current,
    ch.is_os_current,
    ch.is_firewall_enabled,
    
    -- Computed measures
    jsonb_array_length(ch.critical_issues) AS critical_issues_count,
    
    -- Metadata
    CURRENT_TIMESTAMP AS created_at

FROM compliance_history ch
JOIN dim_computers dc ON ch.computer_id = dc.computer_id
JOIN dim_time dt ON ch.check_date = dt.full_date
LEFT JOIN dim_labs dl ON dc.current_lab_id = dl.lab_id
```

---

### Running dbt

```bash
# Install dependencies
pip install dbt-core dbt-postgres

# Test connection
dbt debug

# Run all models
dbt run

# Run specific model
dbt run --select dim_labs

# Run facts only
dbt run --select tag:fact

# Run with full refresh (ignore incremental)
dbt run --full-refresh

# Test all models
dbt test

# Test specific model
dbt test --select dim_labs

# Generate documentation
dbt docs generate
dbt docs serve  # Opens browser to http://localhost:8080
```

---

## Incremental Refresh Strategies

### When to Use Incremental Models

**Use incremental for:**
- ✅ Fact tables (append-only, grows continuously)
- ✅ Large tables (> 1M rows)
- ✅ Daily/hourly refresh patterns
- ✅ Time-series data with natural filter (date > last_run)

**Don't use incremental for:**
- ❌ Dimensions (usually small, full refresh is fast)
- ❌ Tables < 100K rows (full refresh is fast enough)
- ❌ Tables without reliable filter column
- ❌ Complex merge logic (use table with full refresh instead)

---

### dbt Incremental Strategies

**Strategy 1: Append (Fastest)**

```sql
-- Default strategy: just INSERT new rows
{{ config(
    materialized='incremental',
    unique_key='fact_id'
) }}

SELECT ...
FROM source

{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

**Pro:** Fastest (no deletes/updates)  
**Con:** Can't handle late-arriving data or updates

---

**Strategy 2: Delete+Insert (Safe)**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='fact_id'
) }}

SELECT ...
FROM source

{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }}) - INTERVAL '2 days'
{% endif %}
```

**How it works:**
1. dbt identifies rows to update (via unique_key)
2. DELETEs existing rows with those keys
3. INSERTs all new/updated rows

**Pro:** Handles updates, safe for late data  
**Con:** Slower than append

---

**Strategy 3: Merge (PostgreSQL 15+)**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='fact_id'
) }}

SELECT ...
FROM source

{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }}) - INTERVAL '2 days'
{% endif %}
```

**How it works:**
- Uses PostgreSQL `MERGE` statement (INSERT ... ON CONFLICT UPDATE)
- Atomically handles inserts and updates

**Pro:** Most efficient for upserts  
**Con:** Requires PostgreSQL 15+ (or use `insert_overwrite`)

---

### Incremental Predicates (Advanced)

**Optimize incremental runs by limiting scan of existing table:**

```sql
{{ config(
    materialized='incremental',
    unique_key='fact_id',
    incremental_predicates=[
        "dbt_internal_dest.snapshot_date >= (current_date - interval '7 days')"
    ]
) }}

SELECT ...
{% if is_incremental() %}
WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}
```

**Performance benefit:**
- Instead of scanning entire existing table, only scans last 7 days
- Critical for tables with 100M+ rows

---

### Handling Late-Arriving Data

**Problem:** Data arrives after initial load (common with external APIs)

**Solution: Look-back window**

```sql
{{ config(materialized='incremental') }}

SELECT ...
FROM source

{% if is_incremental() %}
-- Look back 2 days to catch late data
WHERE created_at > (SELECT MAX(created_at) - INTERVAL '2 days' FROM {{ this }})
{% endif %}
```

**Trade-off:**
- ✅ Catches late data
- ❌ Reprocesses some rows (use `unique_key` to handle duplicates)

---

## Partitioning & Performance Optimization

### When to Partition

**Partition when:**
- ✅ Fact table > 10M rows
- ✅ Queries filter by date range frequently
- ✅ Need to archive old data easily
- ✅ Table grows continuously

**Don't partition when:**
- ❌ Table < 1M rows (overhead not worth it)
- ❌ Queries don't filter by partition key
- ❌ Frequent updates across all partitions

---

### PostgreSQL Declarative Partitioning

**Partition by date (most common):**

```sql
-- Parent table
CREATE TABLE gold.fact_compliance_daily (
    fact_id BIGSERIAL NOT NULL,
    computer_key BIGINT NOT NULL,
    time_key INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    is_compliant BOOLEAN,
    ...
) PARTITION BY RANGE (snapshot_date);

-- Create partitions (monthly)
CREATE TABLE gold.fact_compliance_daily_2025_01 PARTITION OF gold.fact_compliance_daily
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE gold.fact_compliance_daily_2025_02 PARTITION OF gold.fact_compliance_daily
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

-- Auto-create future partitions (PostgreSQL 13+)
-- Or use pg_partman extension for automatic management
```

---

### Partitioning in dbt

**dbt + PostgreSQL partitioning:**

```sql
-- models/gold/facts/fact_compliance_daily.sql
{{
    config(
        materialized='incremental',
        unique_key=['computer_key', 'time_key'],
        partition_by={
            'field': 'snapshot_date',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

SELECT ...
```

**Note:** dbt partitioning support varies by database:
- **BigQuery:** Excellent support
- **Snowflake:** Uses clustering instead
- **PostgreSQL:** Manual partition creation needed (dbt doesn't auto-create partitions)

---

### Index Optimization

**Standard fact table indexes:**

```sql
-- Dimension-to-fact queries (most common)
CREATE INDEX idx_fact_compliance_time 
ON gold.fact_compliance_daily(time_key DESC);

CREATE INDEX idx_fact_compliance_lab_time 
ON gold.fact_compliance_daily(lab_key, time_key DESC);

CREATE INDEX idx_fact_compliance_computer_time 
ON gold.fact_compliance_daily(computer_key, time_key DESC);

-- Filtered queries
CREATE INDEX idx_fact_compliance_non_compliant 
ON gold.fact_compliance_daily(time_key) 
WHERE is_compliant = false;

-- Covering index (include extra columns for index-only scans)
CREATE INDEX idx_fact_compliance_lab_cover 
ON gold.fact_compliance_daily(lab_key, time_key DESC) 
INCLUDE (is_compliant, compliance_score);
```

---

### Query Performance Best Practices

**1. Always filter on time dimension**

```sql
-- ✅ GOOD: Date filter allows partition pruning
SELECT ...
FROM gold.fact_compliance_daily fc
JOIN gold.dim_time dt ON fc.time_key = dt.time_key
WHERE dt.full_date BETWEEN '2025-01-01' AND '2025-01-31';

-- ❌ BAD: No date filter, scans entire table
SELECT ...
FROM gold.fact_compliance_daily fc;
```

---

**2. Use surrogate keys in joins**

```sql
-- ✅ GOOD: Integer join
FROM gold.fact_compliance fc
JOIN gold.dim_labs dl ON fc.lab_key = dl.lab_key;

-- ❌ BAD: VARCHAR join (slower)
FROM gold.fact_compliance fc
JOIN gold.dim_labs dl ON fc.lab_id = dl.lab_id;
```

---

**3. Denormalize frequently filtered dimension attributes into fact**

```sql
-- If you frequently filter by department, add to fact:
CREATE TABLE gold.fact_compliance_daily (
    ...
    lab_key BIGINT,
    department_key BIGINT,  -- Redundant but allows filtered queries without join
    ...
);

-- Now this is fast (no join to dim_labs)
SELECT COUNT(*)
FROM gold.fact_compliance_daily
WHERE department_key = 123
  AND time_key > 20250101;
```

---

**4. Use materialized views for expensive aggregations**

```sql
-- Create materialized view for dashboard
CREATE MATERIALIZED VIEW gold.mv_compliance_by_dept_month AS
SELECT 
    dt.fiscal_year,
    dt.fiscal_month,
    dl.department_name,
    COUNT(DISTINCT fc.computer_key) as computers,
    AVG(fc.compliance_score) as avg_compliance
FROM gold.fact_compliance_daily fc
JOIN gold.dim_time dt ON fc.time_key = dt.time_key
JOIN gold.dim_labs dl ON fc.lab_key = dl.lab_key
GROUP BY dt.fiscal_year, dt.fiscal_month, dl.department_name;

-- Index it
CREATE INDEX idx_mv_compliance_dept_month 
ON gold.mv_compliance_by_dept_month(fiscal_year, fiscal_month, department_name);

-- Refresh daily
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_compliance_by_dept_month;
```

---

## Naming Conventions

### Schema Naming

```
gold         -- Dimensions, facts, summaries
gold_views   -- Display-ready views
```

---

### Table Naming

| Type | Pattern | Examples |
|------|---------|----------|
| Dimension | `dim_{entity}` (singular) | `dim_lab`, `dim_user`, `dim_computer`, `dim_department` |
| Fact | `fact_{subject}` (plural or event) | `fact_compliance_daily`, `fact_tickets`, `fact_lab_activity_monthly` |
| Bridge | `bridge_{entity1}_{entity2}` | `bridge_article_authors`, `bridge_lab_members` |
| Summary | `{entity}_summary` | `department_summary`, `lab_monthly_summary` |
| Materialized View | `mv_{description}` | `mv_compliance_by_dept`, `mv_lab_trends` |
| Display View | `v_{description}` | `v_labs_monitored`, `v_compliance_dashboard` |

---

### Column Naming

| Type | Pattern | Examples |
|------|---------|----------|
| Surrogate key | `{entity}_key` | `lab_key`, `user_key`, `computer_key` |
| Natural key | `{entity}_id` | `lab_id`, `user_id`, `uniqname` |
| Foreign key (to dimension) | `{entity}_key` | `lab_key`, `time_key`, `department_key` |
| Denormalized name | `{entity}_name` | `lab_name`, `department_name`, `pi_full_name` |
| Denormalized parent | `parent_{entity}_{attribute}` | `parent_department_name`, `parent_lab_id` |
| Derived category | `{attribute}_category` or `{attribute}_tier` | `size_category`, `funding_tier`, `risk_level` |
| Boolean flag | `is_{state}` or `has_{attribute}` | `is_active`, `is_compliant`, `has_award_data` |
| Count measure | `{thing}_count` or `total_{thing}` | `computer_count`, `total_tickets` |
| Amount measure | `{type}_amount` or `total_{type}` | `award_amount`, `total_dollars` |
| Rate/percentage | `{type}_rate` or `{type}_percentage` | `compliance_rate`, `success_percentage` |
| Date column | `{event}_date` | `created_date`, `closed_date`, `snapshot_date` |
| Timestamp column | `{event}_at` or `{event}_timestamp` | `created_at`, `last_refreshed` |

---

### Forbidden Names

**Avoid these ambiguous/confusing names:**

❌ `id` (too generic - use `{entity}_id`)  
❌ `key` (too generic - use `{entity}_key`)  
❌ `name` (too generic - use `{entity}_name`)  
❌ `type` (SQL reserved word - use `{entity}_type`)  
❌ `date` (SQL reserved word - use `{event}_date`)  
❌ `timestamp` (SQL reserved word - use `{event}_timestamp`)  
❌ `value` (too generic - use `{attribute}_value`)  
❌ `status` (too generic - use `{entity}_status`)  

---

## Data Quality & Testing

### dbt Tests (Required)

**Every dimension must have:**

```yaml
# models/gold/dimensions/schema.yml
models:
  - name: dim_labs
    tests:
      # Table-level tests
      - dbt_utils.recency:
          datepart: day
          field: last_refreshed
          interval: 1
    
    columns:
      - name: lab_key
        tests:
          - unique
          - not_null
      
      - name: lab_id
        tests:
          - unique
          - not_null
      
      - name: is_active
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

---

**Every fact must have:**

```yaml
# models/gold/facts/schema.yml
models:
  - name: fact_compliance_daily
    description: "GRAIN: One row per computer per day"
    tests:
      # Grain enforcement
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - computer_key
            - time_key
      
      # Recency check
      - dbt_utils.recency:
          datepart: day
          field: snapshot_date
          interval: 2  # Should have data from last 2 days
    
    columns:
      - name: computer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_computers')
              field: computer_key
      
      - name: time_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_time')
              field: time_key
      
      - name: compliance_score
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
```

---

### Custom SQL Tests

```sql
-- tests/fact_compliance_row_count.sql
-- Ensure fact table grows daily

SELECT 
    COUNT(*) as row_count
FROM {{ ref('fact_compliance_daily') }}
WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
HAVING COUNT(*) < 1000  -- Expect at least 1000 compliance checks daily
```

---

### Data Quality Checks

**Manual verification queries:**

```sql
-- Check dimension coverage
SELECT 
    'dim_labs' as dimension,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE is_active = true) as active_rows,
    MAX(last_refreshed) as last_refresh
FROM gold.dim_labs

UNION ALL

SELECT 
    'dim_computers',
    COUNT(*),
    COUNT(*) FILTER (WHERE is_active = true),
    MAX(last_refreshed)
FROM gold.dim_computers;

-- Check fact table grain
SELECT 
    computer_key,
    time_key,
    COUNT(*) as duplicate_count
FROM gold.fact_compliance_daily
GROUP BY computer_key, time_key
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- Check orphaned facts (referential integrity)
SELECT COUNT(*)
FROM gold.fact_compliance_daily fc
LEFT JOIN gold.dim_computers dc ON fc.computer_key = dc.computer_key
WHERE dc.computer_key IS NULL;
-- Should return 0

-- Check for future dates (data quality issue)
SELECT COUNT(*)
FROM gold.fact_compliance_daily
WHERE snapshot_date > CURRENT_DATE;
-- Should return 0
```

---

## gold_views Schema Pattern

### Purpose of gold_views

**gold schema:** Tables (dimensions, facts)  
**gold_views schema:** Views on gold tables (display-ready, BI-friendly)

**Benefits:**
- ✅ Separate presentation layer from storage
- ✅ BI tools can query views directly
- ✅ Add calculated fields without altering tables
- ✅ Format data for specific dashboards
- ✅ Apply business-specific filters

---

### gold_views Patterns

**Pattern 1: Filtered Production View**

```sql
-- gold_views.v_labs_monitored
CREATE OR REPLACE VIEW gold_views.v_labs_monitored AS
SELECT *
FROM gold.dim_labs
WHERE is_active = true
  AND is_monitored = true
ORDER BY lab_name;

COMMENT ON VIEW gold_views.v_labs_monitored IS
'Production labs actively monitored by LSATS.
Filters: is_active = true AND is_monitored = true';
```

---

**Pattern 2: Enriched View (Join Multiple Dimensions)**

```sql
-- gold_views.v_lab_members
CREATE OR REPLACE VIEW gold_views.v_lab_members AS
SELECT
    dl.lab_name,
    dl.department_name,
    du.full_name as member_name,
    du.primary_email as member_email,
    lm.member_role,
    lm.is_pi
FROM silver.lab_members lm
JOIN gold.dim_labs dl ON lm.lab_id = dl.lab_id
JOIN gold.dim_users du ON lm.member_uniqname = du.uniqname
WHERE dl.is_active = true;
```

---

**Pattern 3: Dashboard View (Pre-Aggregated)**

```sql
-- gold_views.v_compliance_dashboard
CREATE OR REPLACE VIEW gold_views.v_compliance_dashboard AS
SELECT 
    dt.full_date,
    dt.fiscal_year,
    dt.fiscal_quarter,
    dl.department_name,
    dl.college_group,
    
    COUNT(DISTINCT fc.computer_key) as computers_checked,
    SUM(CASE WHEN fc.is_compliant THEN 1 ELSE 0 END) as compliant_count,
    AVG(fc.compliance_score) as avg_compliance_score,
    SUM(fc.critical_issues_count) as total_critical_issues

FROM gold.fact_compliance_daily fc
JOIN gold.dim_time dt ON fc.time_key = dt.time_key
JOIN gold.dim_labs dl ON fc.lab_key = dl.lab_key
WHERE dt.days_from_today BETWEEN -90 AND 0  -- Last 90 days
GROUP BY dt.full_date, dt.fiscal_year, dt.fiscal_quarter, 
         dl.department_name, dl.college_group;

COMMENT ON VIEW gold_views.v_compliance_dashboard IS
'Dashboard-ready compliance metrics. 
Pre-aggregated by date and department. Last 90 days only.';
```

---

**Pattern 4: BI Tool Semantic Layer**

```sql
-- gold_views.v_labs_for_tableau
CREATE OR REPLACE VIEW gold_views.v_labs_for_tableau AS
SELECT
    lab_key,
    lab_id,
    lab_name AS "Lab Name",
    pi_full_name AS "Principal Investigator",
    department_name AS "Department",
    college_group AS "College",
    
    -- Formatted metrics
    TO_CHAR(total_award_dollars, 'FM$999,999,999') AS "Total Funding",
    member_count AS "Team Size",
    computer_count AS "Computers",
    
    -- User-friendly categories
    CASE lab_size_category
        WHEN 'small' THEN '1-5 people'
        WHEN 'medium' THEN '6-15 people'
        WHEN 'large' THEN '16-30 people'
        ELSE '30+ people'
    END AS "Lab Size",
    
    -- Status
    CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END AS "Status"

FROM gold.dim_labs;
```

---

### gold_views Best Practices

1. **Always CREATE OR REPLACE** (idempotent)
2. **Add descriptive comments**
3. **Use AS aliases for BI-friendly column names**
4. **Apply business filters** (is_active, date ranges)
5. **Pre-format data** (currency, dates, categories)
6. **Limit to specific use case** (don't try to be everything)
7. **Performance:** Views are computed on query - use materialized views if slow

---

## Migration from Silver

See complete migration plan in `.plan/gold_layer_view_migration_plan.md`.

**Quick Reference:**

```
silver.v_lab_summary          → gold.dim_labs (denormalized table)
silver.v_department_labs      → gold.department_summary (pre-aggregated)
silver.v_labs_monitored       → gold_views.v_labs_monitored (filtered view)
silver.v_lab_members_detailed → gold_views.v_lab_members (enriched view)

KEEP IN SILVER (no migration):
silver.v_*_tdx_reference      → Stay in silver (operational write-back)
silver.v_eligible_lab_members → Stay in silver (algorithm input)
```

---

## Common Anti-Patterns to Avoid

### Anti-Pattern 1: Normalized Gold Layer

```sql
-- ❌ WRONG: Normalized (defeats purpose of gold)
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100),
    lab_name VARCHAR(255),
    pi_key BIGINT,  -- Just FK, no denormalized data
    dept_key BIGINT  -- Just FK, no denormalized data
);

-- ✅ CORRECT: Denormalized
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100),
    lab_name VARCHAR(255),
    
    -- Denormalized PI data
    pi_uniqname VARCHAR(50),
    pi_full_name VARCHAR(255),
    pi_email VARCHAR(255),
    
    -- Denormalized dept data
    department_name VARCHAR(255),
    college_group VARCHAR(255)
);
```

---

### Anti-Pattern 2: Missing Grain Declaration

```sql
-- ❌ WRONG: No grain declared
CREATE TABLE gold.fact_compliance (...);

-- ✅ CORRECT: Explicit grain
CREATE TABLE gold.fact_compliance (...);

COMMENT ON TABLE gold.fact_compliance IS
'GRAIN: One row per computer per day';
```

---

### Anti-Pattern 3: VARCHAR Foreign Keys

```sql
-- ❌ WRONG: VARCHAR FK (slow joins)
CREATE TABLE gold.fact_compliance (
    fact_id BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100),  -- String FK
    ...
);

-- ✅ CORRECT: Integer surrogate key FK
CREATE TABLE gold.fact_compliance (
    fact_id BIGSERIAL PRIMARY KEY,
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),
    ...
);
```

---

### Anti-Pattern 4: Fact Without Time Dimension

```sql
-- ❌ WRONG: No time dimension
CREATE TABLE gold.fact_compliance (
    fact_id BIGSERIAL PRIMARY KEY,
    computer_key BIGINT,
    is_compliant BOOLEAN,
    ...
);

-- ✅ CORRECT: Always include time
CREATE TABLE gold.fact_compliance (
    fact_id BIGSERIAL PRIMARY KEY,
    computer_key BIGINT,
    time_key INTEGER,  -- Always have time dimension
    is_compliant BOOLEAN,
    ...
);
```

---

### Anti-Pattern 5: Mixing Grains in One Fact

```sql
-- ❌ WRONG: Mixed grain (some rows are daily, some monthly)
CREATE TABLE gold.fact_metrics (
    fact_id BIGSERIAL PRIMARY KEY,
    granularity VARCHAR(20),  -- 'daily' or 'monthly' - BAD!
    ...
);

-- ✅ CORRECT: Separate fact tables per grain
CREATE TABLE gold.fact_metrics_daily (...);
CREATE TABLE gold.fact_metrics_monthly (...);
```

---

### Anti-Pattern 6: Storing Calculated Fields Instead of Calculating

```sql
-- ❌ WRONG: Store derived value (gets out of sync)
CREATE TABLE gold.fact_compliance (
    compliant_count INTEGER,
    total_count INTEGER,
    compliance_rate DECIMAL(5,2)  -- Redundant, can be calculated
);

-- ✅ CORRECT: Calculate in query or view
CREATE TABLE gold.fact_compliance (
    compliant_count INTEGER,
    total_count INTEGER
);

-- Calculate rate in view/query
SELECT 
    compliant_count,
    total_count,
    (compliant_count::DECIMAL / total_count * 100) as compliance_rate
FROM gold.fact_compliance;
```

---

### Anti-Pattern 7: Over-Denormalization

```sql
-- ❌ WRONG: Denormalize everything
CREATE TABLE gold.dim_labs (
    ...
    -- Don't store large blobs
    pi_full_cv TEXT,  -- 50KB document!
    lab_full_history JSONB,  -- Massive array
    all_award_details JSONB  -- Another huge object
);

-- ✅ CORRECT: Denormalize strategically
CREATE TABLE gold.dim_labs (
    ...
    -- Small, frequently used fields only
    pi_full_name VARCHAR(255),
    pi_email VARCHAR(255),
    total_award_dollars NUMERIC(15,2),
    award_count INTEGER
);
```

---

## Reference Implementations

### Complete Star Schema: Compliance Analytics

**See Appendices in `.plan/gold_layer_implementation_plan.md` for:**
- gold.dim_time (full DDL + population script)
- gold.dim_labs (full DDL + dbt model)
- gold.dim_computers (full DDL + dbt model)
- gold.fact_compliance_daily (full DDL + dbt incremental model)
- gold.department_summary (full DDL + dbt model)

---

### Quick Reference: dim_time

```sql
CREATE TABLE gold.dim_time (
    time_key INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    fiscal_year INTEGER,
    academic_year VARCHAR(10),
    semester VARCHAR(20),
    is_weekend BOOLEAN,
    ...
);

-- Pre-populate for 2020-2030 (3,653 rows)
-- See: scripts/database/gold/populate_dim_time.py
```

---

### Quick Reference: dim_labs

```sql
CREATE TABLE gold.dim_labs (
    lab_key BIGSERIAL PRIMARY KEY,
    lab_id VARCHAR(100) UNIQUE,
    lab_name VARCHAR(255),
    
    -- Denormalized PI
    pi_full_name VARCHAR(255),
    pi_email VARCHAR(255),
    
    -- Denormalized dept
    department_name VARCHAR(255),
    college_group VARCHAR(255),
    
    -- Derived categories
    lab_size_category VARCHAR(20),
    funding_tier VARCHAR(20),
    
    is_active BOOLEAN,
    last_refreshed TIMESTAMP
);
```

---

### Quick Reference: fact_compliance_daily

```sql
CREATE TABLE gold.fact_compliance_daily (
    fact_id BIGSERIAL PRIMARY KEY,
    
    -- Dimensional FKs
    computer_key BIGINT REFERENCES gold.dim_computers(computer_key),
    lab_key BIGINT REFERENCES gold.dim_labs(lab_key),
    time_key INTEGER REFERENCES gold.dim_time(time_key),
    
    -- Measures
    is_compliant BOOLEAN,
    compliance_score DECIMAL(5,2),
    critical_issues_count INTEGER,
    
    created_at TIMESTAMP,
    
    UNIQUE(computer_key, time_key)  -- Grain enforcement
);

COMMENT ON TABLE gold.fact_compliance_daily IS
'GRAIN: One row per computer per day';
```

---

## Summary

**Gold Layer Coding Standards:**

1. ✅ Use star schema (fact + dimensions)
2. ✅ Surrogate keys for all dimension PKs
3. ✅ Declare grain explicitly in table comments
4. ✅ Denormalize frequently joined attributes
5. ✅ dbt for transformations, Python for complex logic
6. ✅ Incremental refresh for facts, full refresh for dimensions
7. ✅ Type 1 SCD standard, Type 2 for critical dimensions only
8. ✅ Test everything (unique, not_null, relationships, recency)
9. ✅ gold_views for display layer
10. ✅ PostgreSQL-specific optimizations (partitioning, indexes)

**Key Takeaways:**
- **Simplicity over purity** - denormalize for speed
- **Business-friendly** - readable names, derived categories
- **Grain is king** - declare and enforce it
- **Test rigorously** - dbt tests prevent data quality issues
- **Document everything** - table comments, column comments, grain

---

**Document Status:** COMPLETE - Ready for Use  
**Version:** 1.0  
**Last Updated:** 2025-01-24  
**Owner:** Data Engineering Team
