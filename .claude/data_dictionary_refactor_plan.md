# Data Dictionary Refactor - Implementation Plan

**Project:** LSATS Data Hub Silver Layer Column Standardization  
**Timeline:** Q1 2025 (4-6 weeks)  
**Status:** Planning Phase  
**Owner:** TBD

---

## Executive Summary

This plan implements canonical column naming across all silver layer tables to improve code maintainability, simplify merge logic, and prepare for future dbt migration.

**Scope:** 32 column renames across 11 source-specific tables  
**Impact:** High (affects all transformation scripts, queries, and views)  
**Risk Level:** Medium (mitigated by phased rollout and comprehensive testing)  

**Key Changes:**
- LDAP fields → Business-friendly names (`uid` → `uniqname`, `given_name` → `first_name`)
- Name field semantics (`display_name` → `preferred_name`, standardized `full_name`)
- JSONB flattening (MCommunity `sn`, UMAPI `work_location`)
- Department ID mapping (TDX `default_account_id` → `department_id`)

---

## Table of Contents

1. [Phase Overview](#phase-overview)
2. [Phase 1: Users Entity](#phase-1-users-entity)
3. [Phase 2: Departments Entity](#phase-2-departments-entity)
4. [Phase 3: Groups Entity](#phase-3-groups-entity)
5. [Testing Strategy](#testing-strategy)
6. [Rollback Plan](#rollback-plan)
7. [Success Criteria](#success-criteria)
8. [Appendix: Scripts & Queries](#appendix-scripts--queries)

---

## Phase Overview

### Implementation Approach: Incremental Rollout

```
Week 1-2: Phase 1 - Users Entity (HIGH PRIORITY)
  ├─ Day 1-2:   Schema migrations (4 tables)
  ├─ Day 3-5:   Bronze script updates (4 scripts)
  ├─ Day 6-8:   Silver transformation update (1 script)
  ├─ Day 9-10:  Testing & validation
  └─ Day 11-12: Deploy to production

Week 3-4: Phase 2 - Departments Entity (MEDIUM PRIORITY)
  ├─ Day 1-2:   Schema migrations (2 tables)
  ├─ Day 3-4:   Bronze script updates (2 scripts)
  ├─ Day 5-6:   Silver transformation update (1 script)
  ├─ Day 7-8:   Testing & validation
  └─ Day 9-10:  Deploy to production

Week 5-6: Phase 3 - Groups Entity (LOW PRIORITY)
  ├─ Day 1-2:   Schema migrations (2 tables)
  ├─ Day 3-4:   Bronze script updates (2 scripts)
  ├─ Day 5-6:   Silver transformation update (1 script)
  ├─ Day 7-8:   Testing & validation
  └─ Day 9-10:  Deploy to production
```

### Priority Rationale

1. **Users** - Highest impact (23 renames), most complex merge logic, most frequently joined
2. **Departments** - Medium impact (7 renames), simpler logic, well-isolated
3. **Groups** - Lowest impact (2 renames), minimal changes

---

## Phase 1: Users Entity

**Timeline:** Week 1-2 (12 days)  
**Tables Affected:** 5 (4 source-specific + 1 consolidated)  
**Scripts Affected:** 5 bronze + 1 silver transformation  
**Estimated Renames:** 23 columns

### Step 1.1: Schema Migrations (Day 1-2)

#### 1.1.1 MCommunity Users

**File:** `docker/postgres/migrations/001_rename_mcommunity_users_columns.sql`

```sql
-- Migration: Rename MCommunity Users columns to canonical names
-- Author: [Your Name]
-- Date: 2025-01-XX
-- Ticket: [JIRA-XXX]

BEGIN;

-- Backup current schema
CREATE TABLE IF NOT EXISTS silver.mcommunity_users_backup_20250118 AS 
SELECT * FROM silver.mcommunity_users LIMIT 0;

-- Add comment documenting backup
COMMENT ON TABLE silver.mcommunity_users_backup_20250118 IS 
'Schema backup before canonical naming migration on 2025-01-18';

-- 1. Add new columns with canonical names
ALTER TABLE silver.mcommunity_users 
  ADD COLUMN IF NOT EXISTS uniqname VARCHAR(50),
  ADD COLUMN IF NOT EXISTS first_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS last_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS preferred_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS primary_email VARCHAR(255),
  ADD COLUMN IF NOT EXISTS work_phone VARCHAR(50),
  ADD COLUMN IF NOT EXISTS job_title TEXT,
  ADD COLUMN IF NOT EXISTS cn_aliases JSONB,
  ADD COLUMN IF NOT EXISTS ldap_uid_number BIGINT,
  ADD COLUMN IF NOT EXISTS ldap_gid_number BIGINT,
  ADD COLUMN IF NOT EXISTS full_name VARCHAR(255);

-- 2. Copy data to new columns
UPDATE silver.mcommunity_users SET
  uniqname = uid,
  first_name = given_name,
  -- Flatten sn JSONB array to text (take first element)
  last_name = CASE 
    WHEN jsonb_typeof(sn) = 'array' THEN sn->>0
    WHEN jsonb_typeof(sn) = 'string' THEN sn#>>'{}'
    ELSE NULL
  END,
  preferred_name = display_name,
  primary_email = mail,
  work_phone = telephone_number,
  job_title = umich_title,
  cn_aliases = cn,
  ldap_uid_number = uid_number,
  ldap_gid_number = gid_number,
  -- Derive full_name from last_name, first_name
  full_name = CASE
    WHEN (CASE WHEN jsonb_typeof(sn) = 'array' THEN sn->>0 ELSE sn#>>'{}' END) IS NOT NULL 
         AND given_name IS NOT NULL
    THEN (CASE WHEN jsonb_typeof(sn) = 'array' THEN sn->>0 ELSE sn#>>'{}' END) || ', ' || given_name
    ELSE NULL
  END;

-- 3. Create indexes on new columns (before dropping old ones)
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_uniqname ON silver.mcommunity_users(uniqname);
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_primary_email ON silver.mcommunity_users(primary_email);
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_first_name ON silver.mcommunity_users(first_name);
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_last_name ON silver.mcommunity_users(last_name);
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_ldap_uid_number ON silver.mcommunity_users(ldap_uid_number);
CREATE INDEX IF NOT EXISTS idx_mcommunity_users_ldap_gid_number ON silver.mcommunity_users(ldap_gid_number);

-- 4. Validation: Ensure no data loss
DO $$
DECLARE
  original_count INTEGER;
  new_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO original_count FROM silver.mcommunity_users WHERE uid IS NOT NULL;
  SELECT COUNT(*) INTO new_count FROM silver.mcommunity_users WHERE uniqname IS NOT NULL;
  
  IF original_count != new_count THEN
    RAISE EXCEPTION 'Data loss detected: uid count (%) != uniqname count (%)', 
                    original_count, new_count;
  END IF;
  
  RAISE NOTICE 'Validation passed: % records migrated successfully', new_count;
END $$;

-- 5. Drop old columns (AFTER validation and bronze script updates)
-- COMMENTED OUT - Uncomment after bronze scripts updated and tested
-- ALTER TABLE silver.mcommunity_users 
--   DROP COLUMN IF EXISTS uid,
--   DROP COLUMN IF EXISTS given_name,
--   DROP COLUMN IF EXISTS sn,
--   DROP COLUMN IF EXISTS display_name,
--   DROP COLUMN IF EXISTS mail,
--   DROP COLUMN IF EXISTS telephone_number,
--   DROP COLUMN IF EXISTS umich_title,
--   DROP COLUMN IF EXISTS cn,
--   DROP COLUMN IF EXISTS uid_number,
--   DROP COLUMN IF EXISTS gid_number;

-- 6. Drop old indexes
DROP INDEX IF EXISTS idx_mcommunity_users_uid;
DROP INDEX IF EXISTS idx_mcommunity_users_given_name;
DROP INDEX IF EXISTS idx_mcommunity_users_mail;

COMMIT;

-- Post-migration verification queries
-- Run these manually after migration:

-- Verify data integrity
SELECT 
  'Total records' as check_type,
  COUNT(*) as count
FROM silver.mcommunity_users
UNION ALL
SELECT 
  'Has uniqname',
  COUNT(*)
FROM silver.mcommunity_users 
WHERE uniqname IS NOT NULL
UNION ALL
SELECT 
  'Has first_name',
  COUNT(*)
FROM silver.mcommunity_users 
WHERE first_name IS NOT NULL;

-- Check for JSONB flattening issues
SELECT 
  uid as old_uid,
  uniqname as new_uniqname,
  sn as old_sn_jsonb,
  last_name as new_last_name_flattened
FROM silver.mcommunity_users
WHERE sn IS NOT NULL
LIMIT 10;
```

#### 1.1.2 Active Directory Users

**File:** `docker/postgres/migrations/002_rename_ad_users_columns.sql`

```sql
-- Migration: Rename AD Users columns to canonical names
-- Author: [Your Name]
-- Date: 2025-01-XX

BEGIN;

-- 1. Add new columns
ALTER TABLE silver.ad_users
  ADD COLUMN IF NOT EXISTS uniqname TEXT,
  ADD COLUMN IF NOT EXISTS first_name TEXT,
  ADD COLUMN IF NOT EXISTS last_name TEXT,
  ADD COLUMN IF NOT EXISTS full_name TEXT,
  ADD COLUMN IF NOT EXISTS primary_email TEXT,
  ADD COLUMN IF NOT EXISTS work_phone TEXT,
  ADD COLUMN IF NOT EXISTS mobile_phone TEXT,
  ADD COLUMN IF NOT EXISTS job_title TEXT,
  ADD COLUMN IF NOT EXISTS department_name TEXT,
  ADD COLUMN IF NOT EXISTS ad_cn TEXT,
  ADD COLUMN IF NOT EXISTS ad_name TEXT,
  ADD COLUMN IF NOT EXISTS ad_object_sid TEXT,
  ADD COLUMN IF NOT EXISTS ldap_uid_number BIGINT,
  ADD COLUMN IF NOT EXISTS ldap_gid_number BIGINT,
  ADD COLUMN IF NOT EXISTS preferred_name TEXT;

-- 2. Copy data
UPDATE silver.ad_users SET
  uniqname = uid,
  first_name = given_name,
  last_name = sn,
  full_name = display_name,  -- AD display_name is "Last, First" format
  primary_email = mail,
  work_phone = telephone_number,
  mobile_phone = mobile,
  job_title = title,
  department_name = department,
  ad_cn = cn,
  ad_name = name,
  ad_object_sid = object_sid,
  ldap_uid_number = uid_number,
  ldap_gid_number = gid_number,
  -- Derive preferred_name from first_name + last_name
  preferred_name = CASE
    WHEN given_name IS NOT NULL AND sn IS NOT NULL 
    THEN given_name || ' ' || sn
    WHEN given_name IS NOT NULL THEN given_name
    WHEN sn IS NOT NULL THEN sn
    ELSE NULL
  END;

-- 3. Create indexes
CREATE INDEX IF NOT EXISTS idx_ad_users_uniqname ON silver.ad_users(uniqname);
CREATE INDEX IF NOT EXISTS idx_ad_users_primary_email ON silver.ad_users(primary_email);
CREATE INDEX IF NOT EXISTS idx_ad_users_first_name ON silver.ad_users(first_name);
CREATE INDEX IF NOT EXISTS idx_ad_users_last_name ON silver.ad_users(last_name);
CREATE INDEX IF NOT EXISTS idx_ad_users_ldap_uid_number ON silver.ad_users(ldap_uid_number);

-- 4. Validation
DO $$
DECLARE
  original_count INTEGER;
  new_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO original_count FROM silver.ad_users WHERE uid IS NOT NULL;
  SELECT COUNT(*) INTO new_count FROM silver.ad_users WHERE uniqname IS NOT NULL;
  
  IF original_count != new_count THEN
    RAISE EXCEPTION 'Data loss detected: uid count (%) != uniqname count (%)', 
                    original_count, new_count;
  END IF;
  
  RAISE NOTICE 'Validation passed: % records migrated successfully', new_count;
END $$;

-- 5. Drop old columns (AFTER bronze scripts updated)
-- COMMENTED OUT
-- ALTER TABLE silver.ad_users
--   DROP COLUMN IF EXISTS uid,
--   DROP COLUMN IF EXISTS given_name,
--   DROP COLUMN IF EXISTS sn,
--   DROP COLUMN IF EXISTS mail,
--   DROP COLUMN IF EXISTS telephone_number,
--   DROP COLUMN IF EXISTS mobile,
--   DROP COLUMN IF EXISTS title,
--   DROP COLUMN IF EXISTS department,
--   DROP COLUMN IF EXISTS cn,
--   DROP COLUMN IF EXISTS name,
--   DROP COLUMN IF EXISTS object_sid,
--   DROP COLUMN IF EXISTS uid_number,
--   DROP COLUMN IF EXISTS gid_number;

DROP INDEX IF EXISTS idx_ad_users_uid;
DROP INDEX IF EXISTS idx_ad_users_mail;

COMMIT;
```

#### 1.1.3 TDX Users

**File:** `docker/postgres/migrations/003_rename_tdx_users_columns.sql`

```sql
-- Migration: Rename TDX Users columns to canonical names
-- Author: [Your Name]
-- Date: 2025-01-XX

BEGIN;

-- 1. Add new columns
ALTER TABLE silver.tdx_users
  ADD COLUMN IF NOT EXISTS preferred_name VARCHAR(255),
  ADD COLUMN IF NOT EXISTS job_title VARCHAR(255),
  ADD COLUMN IF NOT EXISTS tdx_account_id INTEGER,
  ADD COLUMN IF NOT EXISTS department_id VARCHAR(10),
  ADD COLUMN IF NOT EXISTS full_name VARCHAR(255);

-- 2. Copy data
UPDATE silver.tdx_users SET
  preferred_name = full_name,  -- TDX full_name is user's preferred format
  job_title = title,
  tdx_account_id = default_account_id,
  -- Derive full_name from last_name, first_name
  full_name = CASE
    WHEN last_name IS NOT NULL AND first_name IS NOT NULL
    THEN last_name || ', ' || first_name
    ELSE NULL
  END;

-- 3. Map department_id from tdx_account_id
-- This requires the TDX ID → Dept Code mapping from bronze.raw_entities
UPDATE silver.tdx_users u
SET department_id = (
  SELECT raw_data->>'Code'
  FROM bronze.raw_entities
  WHERE entity_type = 'department'
    AND source_system = 'tdx'
    AND (raw_data->>'ID')::INTEGER = u.default_account_id
  LIMIT 1
)
WHERE default_account_id IS NOT NULL;

-- 4. Create indexes
CREATE INDEX IF NOT EXISTS idx_tdx_users_preferred_name ON silver.tdx_users(preferred_name);
CREATE INDEX IF NOT EXISTS idx_tdx_users_job_title ON silver.tdx_users(job_title);
CREATE INDEX IF NOT EXISTS idx_tdx_users_tdx_account_id ON silver.tdx_users(tdx_account_id);
CREATE INDEX IF NOT EXISTS idx_tdx_users_department_id ON silver.tdx_users(department_id);

-- 5. Validation
DO $$
DECLARE
  original_count INTEGER;
  new_count INTEGER;
  mapped_dept_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO original_count FROM silver.tdx_users WHERE default_account_id IS NOT NULL;
  SELECT COUNT(*) INTO new_count FROM silver.tdx_users WHERE tdx_account_id IS NOT NULL;
  SELECT COUNT(*) INTO mapped_dept_count FROM silver.tdx_users WHERE department_id IS NOT NULL;
  
  IF original_count != new_count THEN
    RAISE EXCEPTION 'Data loss detected: default_account_id count (%) != tdx_account_id count (%)', 
                    original_count, new_count;
  END IF;
  
  RAISE NOTICE 'Validation passed: % records migrated, % departments mapped', 
               new_count, mapped_dept_count;
               
  IF mapped_dept_count < (original_count * 0.9) THEN
    RAISE WARNING 'Less than 90%% of departments mapped (% / %)', mapped_dept_count, original_count;
  END IF;
END $$;

-- 6. Drop old columns (AFTER bronze scripts updated)
-- COMMENTED OUT
-- ALTER TABLE silver.tdx_users
--   DROP COLUMN IF EXISTS default_account_id;

-- Note: Keep 'full_name' column name, but data is now "Last, First" format
-- The old 'full_name' column is now renamed to 'preferred_name'

COMMIT;
```

#### 1.1.4 UMAPI Employees

**File:** `docker/postgres/migrations/004_rename_umapi_employees_columns.sql`

```sql
-- Migration: Rename UMAPI Employees columns and flatten work_location JSONB
-- Author: [Your Name]
-- Date: 2025-01-XX

BEGIN;

-- 1. Add new columns
ALTER TABLE silver.umapi_employees
  ADD COLUMN IF NOT EXISTS preferred_name VARCHAR(60),
  ADD COLUMN IF NOT EXISTS job_title TEXT,
  ADD COLUMN IF NOT EXISTS dept_job_title VARCHAR(50),
  -- Flattened work_location fields
  ADD COLUMN IF NOT EXISTS work_phone VARCHAR(50),
  ADD COLUMN IF NOT EXISTS work_city VARCHAR(100),
  ADD COLUMN IF NOT EXISTS work_state VARCHAR(50),
  ADD COLUMN IF NOT EXISTS work_postal_code VARCHAR(20),
  ADD COLUMN IF NOT EXISTS work_country VARCHAR(100),
  ADD COLUMN IF NOT EXISTS work_address_line1 VARCHAR(255),
  ADD COLUMN IF NOT EXISTS work_address_line2 VARCHAR(255);

-- 2. Copy data
UPDATE silver.umapi_employees SET
  job_title = university_job_title,
  dept_job_title = department_job_title,
  -- Derive preferred_name from first_name + last_name
  preferred_name = CASE
    WHEN first_name IS NOT NULL AND last_name IS NOT NULL
    THEN first_name || ' ' || last_name
    WHEN first_name IS NOT NULL THEN first_name
    WHEN last_name IS NOT NULL THEN last_name
    WHEN full_name IS NOT NULL THEN full_name  -- Fallback
    ELSE NULL
  END,
  -- Flatten work_location JSONB
  work_phone = work_location->>'phone',
  work_city = work_location->>'city',
  work_state = work_location->>'state',
  work_postal_code = work_location->>'postal',
  work_country = work_location->>'country',
  work_address_line1 = work_location->>'address1',
  work_address_line2 = work_location->>'address2';

-- 3. Create indexes
CREATE INDEX IF NOT EXISTS idx_umapi_employees_job_title ON silver.umapi_employees(job_title);
CREATE INDEX IF NOT EXISTS idx_umapi_employees_work_phone ON silver.umapi_employees(work_phone);
CREATE INDEX IF NOT EXISTS idx_umapi_employees_work_city ON silver.umapi_employees(work_city);

-- 4. Validation
DO $$
DECLARE
  total_count INTEGER;
  has_job_title INTEGER;
  has_phone INTEGER;
BEGIN
  SELECT COUNT(*) INTO total_count FROM silver.umapi_employees;
  SELECT COUNT(*) INTO has_job_title FROM silver.umapi_employees WHERE job_title IS NOT NULL;
  SELECT COUNT(*) INTO has_phone FROM silver.umapi_employees WHERE work_phone IS NOT NULL;
  
  RAISE NOTICE 'Total records: %', total_count;
  RAISE NOTICE 'Records with job_title: % (%.1f%%)', has_job_title, (has_job_title::FLOAT / total_count * 100);
  RAISE NOTICE 'Records with work_phone: % (%.1f%%)', has_phone, (has_phone::FLOAT / total_count * 100);
END $$;

-- 5. Drop old columns (AFTER bronze scripts updated)
-- COMMENTED OUT
-- ALTER TABLE silver.umapi_employees
--   DROP COLUMN IF EXISTS university_job_title,
--   DROP COLUMN IF EXISTS department_job_title;

-- Keep work_location JSONB for full data preservation

COMMIT;
```

#### 1.1.5 Consolidated Users Table

**File:** `docker/postgres/migrations/005_rename_users_consolidated.sql`

```sql
-- Migration: Rename consolidated users table columns
-- Author: [Your Name]
-- Date: 2025-01-XX

BEGIN;

-- 1. Add new column
ALTER TABLE silver.users
  ADD COLUMN IF NOT EXISTS preferred_name VARCHAR(255);

-- 2. Copy data
UPDATE silver.users SET
  preferred_name = display_name;

-- 3. Create index
CREATE INDEX IF NOT EXISTS idx_users_preferred_name ON silver.users(preferred_name);

-- 4. Validation
DO $$
DECLARE
  original_count INTEGER;
  new_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO original_count FROM silver.users WHERE display_name IS NOT NULL;
  SELECT COUNT(*) INTO new_count FROM silver.users WHERE preferred_name IS NOT NULL;
  
  IF original_count != new_count THEN
    RAISE EXCEPTION 'Data loss detected: display_name count (%) != preferred_name count (%)', 
                    original_count, new_count;
  END IF;
  
  RAISE NOTICE 'Validation passed: % records migrated successfully', new_count;
END $$;

-- 5. Drop old column (AFTER silver transformation script updated)
-- COMMENTED OUT
-- ALTER TABLE silver.users
--   DROP COLUMN IF EXISTS display_name;

DROP INDEX IF EXISTS idx_users_display_name;

COMMIT;
```

### Step 1.2: Silver Transformation Script Updates (Day 3-5)

**IMPORTANT:** Bronze scripts (`scripts/database/bronze/`) do NOT need changes - they only ingest raw JSON to `bronze.raw_entities`. The column name changes happen in **Silver transformation scripts** that extract from bronze JSONB into typed silver columns.

**Files to Update:**
- `scripts/database/silver/001_transform_tdx_users.py` (Bronze → silver.tdx_users)
- `scripts/database/silver/006_transform_mcommunity_users.py` (Bronze → silver.mcommunity_users)
- `scripts/database/silver/004_transform_ad_users.py` (Bronze → silver.ad_users)
- `scripts/database/silver/002_transform_umapi_employees.py` (Bronze → silver.umapi_employees)

#### 1.2.1 TDX Users Silver Transformation

**File:** `scripts/database/silver/001_transform_tdx_users.py`

**Method to Update:** `_extract_tdx_fields(self, raw_data: Dict[str, Any], raw_id: str)`

**Changes Required:**

```python
# OLD CODE (before migration)
def _extract_tdx_fields(self, raw_data: Dict[str, Any], raw_id: str) -> Dict[str, Any]:
    """Extract and type-cast TDX fields from bronze JSONB to silver columns."""
    
    silver_record = {
        "tdx_user_uid": to_uuid(raw_data.get("UID")),
        "uniqname": raw_data.get("AlternateID", "").lower() or None,
        "external_id": raw_data.get("ExternalID"),
        "username": raw_data.get("UserName"),
        "first_name": raw_data.get("FirstName"),
        "middle_name": raw_data.get("MiddleName"),
        "last_name": raw_data.get("LastName"),
        "full_name": raw_data.get("FullName"),  # ← OLD: TDX user's preferred format
        "nickname": raw_data.get("Nickname"),
        "primary_email": raw_data.get("PrimaryEmail"),
        # ... more fields ...
        "title": raw_data.get("Title"),  # ← OLD: Rename to job_title
        "default_account_id": raw_data.get("DefaultAccountID"),  # ← OLD: Need mapping
        # ... more fields ...
    }
    return silver_record

# NEW CODE (after migration)
def _extract_tdx_fields(self, raw_data: Dict[str, Any], raw_id: str) -> Dict[str, Any]:
    """Extract and type-cast TDX fields from bronze JSONB to silver columns."""
    
    silver_record = {
        "tdx_user_uid": to_uuid(raw_data.get("UID")),
        "uniqname": raw_data.get("AlternateID", "").lower() or None,
        "external_id": raw_data.get("ExternalID"),
        "username": raw_data.get("UserName"),
        "first_name": raw_data.get("FirstName"),
        "middle_name": raw_data.get("MiddleName"),
        "last_name": raw_data.get("LastName"),
        "preferred_name": raw_data.get("FullName"),  # ← NEW: Semantic rename
        "nickname": raw_data.get("Nickname"),
        "primary_email": raw_data.get("PrimaryEmail"),
        # ... more fields ...
        "job_title": raw_data.get("Title"),  # ← NEW: Canonical name
        "tdx_account_id": raw_data.get("DefaultAccountID"),  # ← NEW: Keep internal ID
        "department_id": self._map_tdx_account_to_dept(raw_data.get("DefaultAccountID")),  # ← NEW: Mapped code
        # ... more fields ...
    }
    
    # Derive full_name from components if not explicitly available
    if silver_record.get("last_name") and silver_record.get("first_name"):
        silver_record["full_name"] = f"{silver_record['last_name']}, {silver_record['first_name']}"
    
    return silver_record
        "last_name": raw_data.get("LastName"),
        "preferred_name": raw_data.get("FullName"),  # ← NEW (was full_name)
        "nickname": raw_data.get("Nickname"),
        "primary_email": raw_data.get("PrimaryEmail"),
        # ... more fields ...
        "job_title": raw_data.get("Title"),  # ← NEW (was title)
        "tdx_account_id": raw_data.get("DefaultAccountID"),  # ← NEW (was default_account_id)
        # ... more fields ...
    }
    
    # Derive full_name (Last, First format)
    first = raw_data.get("FirstName")
    last = raw_data.get("LastName")
    if last and first:
        silver_record["full_name"] = f"{last}, {first}"
    elif last:
        silver_record["full_name"] = last
    elif first:
        silver_record["full_name"] = first
    else:
        silver_record["full_name"] = None
    
    # Map department_id from tdx_account_id
    # This requires access to department mapping
    tdx_account_id = raw_data.get("DefaultAccountID")
    if tdx_account_id:
        silver_record["department_id"] = self._map_tdx_dept_id(tdx_account_id)
    else:
        silver_record["department_id"] = None
    
    return silver_record

def _map_tdx_dept_id(self, tdx_account_id: int) -> Optional[str]:
    """
    Map TDX DefaultAccountID to department code.
    
    Args:
        tdx_account_id: TDX internal account ID (e.g., 41)
    
    Returns:
        Department code (e.g., "173500") or None if not found
    """
    try:
        query = """
        SELECT raw_data->>'Code' as dept_code
        FROM bronze.raw_entities
        WHERE entity_type = 'department'
          AND source_system = 'tdx'
          AND (raw_data->>'ID')::INTEGER = :tdx_id
        LIMIT 1
        """
        result = self.db_adapter.query_to_dataframe(query, {"tdx_id": tdx_account_id})
        
        if not result.empty:
            return result.iloc[0]["dept_code"]
        else:
            logger.debug(f"No department mapping found for TDX account ID {tdx_account_id}")
            return None
    except Exception as e:
        logger.warning(f"Error mapping TDX department ID {tdx_account_id}: {e}")
        return None
```

**Testing Script:** `tests/bronze/test_002_tdx_users_migration.py`

```python
#!/usr/bin/env python3
"""
Test TDX Users bronze ingestion after column rename migration.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
from database.adapters.postgres_adapter import PostgresAdapter

load_dotenv()


def test_tdx_users_schema():
    """Verify TDX users table has new canonical column names."""
    db = PostgresAdapter(database_url=os.getenv("DATABASE_URL"))
    
    # Check new columns exist
    query = """
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'silver' 
      AND table_name = 'tdx_users'
      AND column_name IN ('preferred_name', 'job_title', 'tdx_account_id', 'department_id', 'full_name')
    """
    
    result = db.query_to_dataframe(query)
    expected_columns = {'preferred_name', 'job_title', 'tdx_account_id', 'department_id', 'full_name'}
    actual_columns = set(result['column_name'].tolist())
    
    missing = expected_columns - actual_columns
    if missing:
        print(f"❌ Missing columns: {missing}")
        return False
    
    print(f"✅ All new columns exist: {expected_columns}")
    
    # Check data populated
    query = """
    SELECT 
        COUNT(*) as total,
        COUNT(preferred_name) as has_preferred_name,
        COUNT(job_title) as has_job_title,
        COUNT(tdx_account_id) as has_tdx_account_id,
        COUNT(department_id) as has_department_id,
        COUNT(full_name) as has_full_name
    FROM silver.tdx_users
    """
    
    result = db.query_to_dataframe(query)
    row = result.iloc[0]
    
    print(f"\n📊 TDX Users Data Check:")
    print(f"   Total records: {row['total']}")
    print(f"   Has preferred_name: {row['has_preferred_name']} ({row['has_preferred_name']/row['total']*100:.1f}%)")
    print(f"   Has job_title: {row['has_job_title']} ({row['has_job_title']/row['total']*100:.1f}%)")
    print(f"   Has tdx_account_id: {row['has_tdx_account_id']} ({row['has_tdx_account_id']/row['total']*100:.1f}%)")
    print(f"   Has department_id: {row['has_department_id']} ({row['has_department_id']/row['total']*100:.1f}%)")
    print(f"   Has full_name: {row['has_full_name']} ({row['has_full_name']/row['total']*100:.1f}%)")
    
    # Check full_name format (should be "Last, First")
    query = """
    SELECT uniqname, first_name, last_name, preferred_name, full_name
    FROM silver.tdx_users
    WHERE full_name IS NOT NULL
    LIMIT 5
    """
    
    result = db.query_to_dataframe(query)
    print(f"\n📝 Sample full_name formats:")
    for _, row in result.iterrows():
        print(f"   {row['uniqname']}: '{row['full_name']}' (preferred: '{row['preferred_name']}')")
    
    db.close()
    return True


if __name__ == "__main__":
    success = test_tdx_users_schema()
    sys.exit(0 if success else 1)
```

#### 1.2.2 MCommunity Users Bronze Script

**File:** `scripts/database/bronze/007_ingest_mcommunity_users.py`

**Changes Required:**

```python
# OLD CODE
def _transform_to_silver(self, ldap_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Transform LDAP entry to silver schema."""
    
    return {
        "uid": self._extract_single_value(ldap_entry.get("uid")),  # ← OLD
        "display_name": self._extract_single_value(ldap_entry.get("displayName")),
        "given_name": self._extract_single_value(ldap_entry.get("givenName")),  # ← OLD
        "cn": ldap_entry.get("cn"),  # Keep as JSONB array
        "sn": ldap_entry.get("sn"),  # Keep as JSONB array  # ← OLD
        "mail": self._extract_single_value(ldap_entry.get("mail")),  # ← OLD
        "telephone_number": self._extract_single_value(ldap_entry.get("telephoneNumber")),  # ← OLD
        "umich_title": self._extract_single_value(ldap_entry.get("umichTitle")),  # ← OLD
        "uid_number": self._extract_single_value(ldap_entry.get("uidNumber")),  # ← OLD
        "gid_number": self._extract_single_value(ldap_entry.get("gidNumber")),  # ← OLD
        # ... more fields ...
    }

# NEW CODE
def _transform_to_silver(self, ldap_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Transform LDAP entry to silver schema with canonical column names."""
    
    # Extract and flatten sn (surname) from JSONB array
    sn_value = ldap_entry.get("sn")
    if isinstance(sn_value, list) and sn_value:
        last_name = sn_value[0]  # Take first element
    elif isinstance(sn_value, str):
        last_name = sn_value
    else:
        last_name = None
    
    # Build silver record
    first_name = self._extract_single_value(ldap_entry.get("givenName"))
    
    silver_record = {
        "uniqname": self._extract_single_value(ldap_entry.get("uid")),  # ← NEW
        "preferred_name": self._extract_single_value(ldap_entry.get("displayName")),  # ← NEW (was display_name)
        "first_name": first_name,  # ← NEW (was given_name)
        "cn_aliases": ldap_entry.get("cn"),  # ← NEW (was cn, keep JSONB)
        "last_name": last_name,  # ← NEW (was sn, now flattened)
        "primary_email": self._extract_single_value(ldap_entry.get("mail")),  # ← NEW (was mail)
        "work_phone": self._extract_single_value(ldap_entry.get("telephoneNumber")),  # ← NEW
        "job_title": self._extract_single_value(ldap_entry.get("umichTitle")),  # ← NEW
        "ldap_uid_number": self._extract_single_value(ldap_entry.get("uidNumber")),  # ← NEW
        "ldap_gid_number": self._extract_single_value(ldap_entry.get("gidNumber")),  # ← NEW
        # ... more fields ...
    }
    
    # Derive full_name (Last, First format)
    if last_name and first_name:
        silver_record["full_name"] = f"{last_name}, {first_name}"
    elif last_name:
        silver_record["full_name"] = last_name
    elif first_name:
        silver_record["full_name"] = first_name
    else:
        silver_record["full_name"] = None
    
    return silver_record
```

#### 1.2.3 AD Users Bronze Script

**File:** `scripts/database/bronze/004_ingest_ad_users.py`

**Changes (similar pattern):**

```python
# NEW CODE
def _transform_to_silver(self, ad_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Transform AD entry to silver schema with canonical column names."""
    
    first_name = ad_entry.get("givenName")
    last_name = ad_entry.get("sn")
    
    silver_record = {
        "uniqname": ad_entry.get("uid"),  # ← NEW (was uid)
        "first_name": first_name,  # ← NEW (was given_name)
        "last_name": last_name,  # ← NEW (was sn)
        "full_name": ad_entry.get("displayName"),  # Already "Last, First" format
        "primary_email": ad_entry.get("mail"),  # ← NEW (was mail)
        "work_phone": ad_entry.get("telephoneNumber"),  # ← NEW
        "mobile_phone": ad_entry.get("mobile"),  # ← NEW (was mobile)
        "job_title": ad_entry.get("title"),  # ← NEW (was title)
        "department_name": ad_entry.get("department"),  # ← NEW (was department)
        "ad_cn": ad_entry.get("cn"),  # ← NEW (add prefix)
        "ad_name": ad_entry.get("name"),  # ← NEW (add prefix)
        "ad_object_sid": ad_entry.get("objectSid"),  # ← NEW (add prefix)
        "ldap_uid_number": ad_entry.get("uidNumber"),  # ← NEW
        "ldap_gid_number": ad_entry.get("gidNumber"),  # ← NEW
        # ... more fields ...
    }
    
    # Derive preferred_name (First Last format)
    if first_name and last_name:
        silver_record["preferred_name"] = f"{first_name} {last_name}"
    elif first_name:
        silver_record["preferred_name"] = first_name
    elif last_name:
        silver_record["preferred_name"] = last_name
    else:
        silver_record["preferred_name"] = None
    
    return silver_record
```

#### 1.2.4 UMAPI Employees Bronze Script

**File:** `scripts/database/bronze/003_ingest_umapi_employees.py`

**Changes:**

```python
# NEW CODE
def _transform_to_silver(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform UMAPI employee record to silver schema with canonical names."""
    
    raw_data = raw_record.get("raw_data", {})
    
    # Extract work location data
    work_location = {}
    for key in ["Work_Address1", "Work_Address2", "Work_City", "Work_State", 
                "Work_Postal", "Work_Country", "Work_Phone"]:
        if key in raw_data:
            # Map to lowercase keys for JSONB
            json_key = key.lower().replace("work_", "")
            work_location[json_key] = raw_data[key]
    
    first_name = raw_data.get("FirstName")
    last_name = raw_data.get("LastName")
    
    silver_record = {
        "empl_id": raw_data.get("EmplID"),
        "empl_rcd": raw_data.get("EmplRcd"),
        "uniqname": raw_data.get("UniqName"),
        "first_name": first_name,
        "last_name": last_name,
        "full_name": raw_data.get("Name"),  # Already "Last, First Middle" format
        "department_id": raw_data.get("DepartmentId"),
        "dept_description": raw_data.get("DeptDescription"),
        "supervisor_id": raw_data.get("SupervisorID"),
        "jobcode": raw_data.get("Jobcode"),
        "dept_job_title": raw_data.get("DepartmentJobTitle"),  # ← NEW (was department_job_title)
        "job_title": raw_data.get("UniversityJobTitle"),  # ← NEW (was university_job_title)
        "work_location": work_location,  # Keep JSONB
        # Flattened work_location fields
        "work_phone": work_location.get("phone"),
        "work_city": work_location.get("city"),
        "work_state": work_location.get("state"),
        "work_postal_code": work_location.get("postal"),
        "work_country": work_location.get("country"),
        "work_address_line1": work_location.get("address1"),
        "work_address_line2": work_location.get("address2"),
        # ... more fields ...
    }
    
    # Derive preferred_name
    if first_name and last_name:
        silver_record["preferred_name"] = f"{first_name} {last_name}"
    elif first_name:
        silver_record["preferred_name"] = first_name
    elif last_name:
        silver_record["preferred_name"] = last_name
    elif raw_data.get("Name"):  # Fallback to full name
        silver_record["preferred_name"] = raw_data.get("Name")
    else:
        silver_record["preferred_name"] = None
    
    return silver_record
```

### Step 1.3: Silver Transformation Update (Day 6-8)

**File:** `scripts/database/silver/012_transform_users.py`

**Changes Required:**

```python
# OLD CODE (excerpt from _merge_user_records)
def _merge_user_records(
    self,
    uniqname: str,
    tdx_record: Optional[Dict[str, Any]],
    ad_record: Optional[Dict[str, Any]],
    umapi_records: List[Dict[str, Any]],
    mcom_record: Optional[Dict[str, Any]],
    pi_uniqnames: Set[str],
    tdx_dept_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Merge user records from all sources."""
    
    # ... source tracking ...
    
    # Priority: TDX > UMAPI > MCommunity > AD
    first_name = pick_first(
        tdx_record.get("first_name") if tdx_record else None,
        umapi_records[0].get("first_name") if umapi_records else None,
        mcom_record.get("given_name") if mcom_record else None,  # ← OLD
        ad_record.get("given_name") if ad_record else None,  # ← OLD
    )
    
    # ... similar for other fields ...

# NEW CODE
def _merge_user_records(
    self,
    uniqname: str,
    tdx_record: Optional[Dict[str, Any]],
    ad_record: Optional[Dict[str, Any]],
    umapi_records: List[Dict[str, Any]],
    mcom_record: Optional[Dict[str, Any]],
    pi_uniqnames: Set[str],
) -> Dict[str, Any]:
    """
    Merge user records from all sources using canonical column names.
    
    NOTE: No longer needs tdx_dept_map parameter - department_id already mapped
    in bronze ingestion script.
    """
    
    # ... source tracking ...
    
    # --- Core Identity (all sources now use canonical names) ---
    first_name = pick_first(
        tdx_record.get("first_name") if tdx_record else None,
        umapi_records[0].get("first_name") if umapi_records else None,
        mcom_record.get("first_name") if mcom_record else None,  # ← NEW
        ad_record.get("first_name") if ad_record else None,  # ← NEW
    )
    
    last_name = pick_first(
        tdx_record.get("last_name") if tdx_record else None,
        umapi_records[0].get("last_name") if umapi_records else None,
        mcom_record.get("last_name") if mcom_record else None,  # ← NEW (now flattened)
        ad_record.get("last_name") if ad_record else None,  # ← NEW
    )
    
    # Full name (standardized "Last, First" format)
    full_name = pick_first(
        umapi_records[0].get("full_name") if umapi_records else None,  # Already Last, First
        ad_record.get("full_name") if ad_record else None,  # Already Last, First
        tdx_record.get("full_name") if tdx_record else None,  # Now derived as Last, First
        mcom_record.get("full_name") if mcom_record else None,  # Now derived as Last, First
        f"{last_name}, {first_name}" if last_name and first_name else None,  # Final fallback
    )
    
    # Preferred name (user's chosen display format)
    preferred_name = pick_first(
        tdx_record.get("preferred_name") if tdx_record else None,  # ← NEW (was full_name)
        mcom_record.get("preferred_name") if mcom_record else None,  # ← NEW (was display_name)
        ad_record.get("preferred_name") if ad_record else None,  # ← NEW (derived)
        umapi_records[0].get("preferred_name") if umapi_records else None,  # ← NEW (derived)
        f"{first_name} {last_name}" if first_name and last_name else None,  # Fallback
    )
    
    # --- Contact Info ---
    primary_email = pick_first(
        tdx_record.get("primary_email") if tdx_record else None,
        mcom_record.get("primary_email") if mcom_record else None,  # ← NEW (was mail)
        ad_record.get("primary_email") if ad_record else None,  # ← NEW (was mail)
    )
    
    # Work phone (now flattened from UMAPI)
    work_phone = pick_first(
        tdx_record.get("work_phone") if tdx_record else None,
        umapi_records[0].get("work_phone") if umapi_records else None,  # ← NEW (flattened)
        mcom_record.get("work_phone") if mcom_record else None,  # ← NEW (was telephone_number)
        ad_record.get("work_phone") if ad_record else None,  # ← NEW (was telephone_number)
    )
    
    mobile_phone = pick_first(
        tdx_record.get("mobile_phone") if tdx_record else None,
        ad_record.get("mobile_phone") if ad_record else None,  # ← NEW (was mobile)
    )
    
    # --- Employment ---
    # Department ID (no longer needs tdx_dept_map - already mapped!)
    department_id = pick_first(
        umapi_records[0].get("department_id") if umapi_records else None,
        tdx_record.get("department_id") if tdx_record else None,  # ← NEW (pre-mapped)
    )
    
    # Job title
    job_title = pick_first(
        umapi_records[0].get("job_title") if umapi_records else None,  # ← NEW (was university_job_title)
        mcom_record.get("job_title") if mcom_record else None,  # ← NEW (was umich_title)
        tdx_record.get("job_title") if tdx_record else None,  # ← NEW (was title)
        ad_record.get("job_title") if ad_record else None,  # ← NEW (was title)
    )
    
    # Department job title
    department_job_title = umapi_records[0].get("dept_job_title") if umapi_records else None  # ← NEW
    
    # ... continue for remaining fields ...
    
    merged = {
        "uniqname": uniqname,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "preferred_name": preferred_name,  # ← NEW (replaces display_name)
        "primary_email": primary_email,
        "work_phone": work_phone,
        "mobile_phone": mobile_phone,
        "department_id": department_id,
        "department_name": umapi_agg.get("department_name"),
        "job_title": job_title,
        "department_job_title": department_job_title,  # ← NEW field
        # ... all other fields ...
        "source_system": "+".join(sorted(sources)),
    }
    
    return merged
```

**Key Simplifications:**
1. ✅ No more field name mapping (`given_name` → `first_name` done in bronze)
2. ✅ No more `tdx_dept_map` parameter (mapping done in bronze)
3. ✅ Cleaner `pick_first()` calls - all sources use same column name
4. ✅ Clear distinction: `full_name` (formal) vs `preferred_name` (casual)

### Step 1.4: Testing & Validation (Day 9-10)

**File:** `tests/integration/test_users_refactor_integration.py`

```python
#!/usr/bin/env python3
"""
Integration test for users entity refactoring.

Tests the complete pipeline:
1. Bronze ingestion (with new column names)
2. Silver transformation (using canonical names)
3. Data quality validation
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
from database.adapters.postgres_adapter import PostgresAdapter

load_dotenv()


class UsersRefactorIntegrationTest:
    """Integration test suite for users refactoring."""
    
    def __init__(self):
        self.db = PostgresAdapter(database_url=os.getenv("DATABASE_URL"))
        self.tests_passed = 0
        self.tests_failed = 0
    
    def test_source_specific_schemas(self):
        """Test 1: Verify all source-specific tables have canonical columns."""
        print("\n" + "="*80)
        print("TEST 1: Source-Specific Table Schemas")
        print("="*80)
        
        tables_and_columns = {
            "silver.tdx_users": ["preferred_name", "job_title", "tdx_account_id", "department_id", "full_name"],
            "silver.umapi_employees": ["preferred_name", "job_title", "dept_job_title", "work_phone", "work_city"],
            "silver.mcommunity_users": ["uniqname", "first_name", "last_name", "preferred_name", "primary_email", "work_phone", "full_name"],
            "silver.ad_users": ["uniqname", "first_name", "last_name", "full_name", "preferred_name", "primary_email", "work_phone", "mobile_phone"],
        }
        
        for table, expected_columns in tables_and_columns.items():
            query = f"""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'silver' 
              AND table_name = '{table.split('.')[1]}'
              AND column_name = ANY(:columns)
            """
            
            result = self.db.query_to_dataframe(query, {"columns": expected_columns})
            actual = set(result['column_name'].tolist())
            expected = set(expected_columns)
            
            if actual == expected:
                print(f"✅ {table}: All columns present")
                self.tests_passed += 1
            else:
                missing = expected - actual
                print(f"❌ {table}: Missing columns: {missing}")
                self.tests_failed += 1
    
    def test_column_name_consistency(self):
        """Test 2: Verify identical concepts use identical column names."""
        print("\n" + "="*80)
        print("TEST 2: Column Name Consistency Across Tables")
        print("="*80)
        
        # Check that 'uniqname' is used everywhere (not uid)
        query = """
        SELECT 
            table_name,
            column_name
        FROM information_schema.columns
        WHERE table_schema = 'silver'
          AND table_name IN ('tdx_users', 'umapi_employees', 'mcommunity_users', 'ad_users', 'users')
          AND column_name IN ('uid', 'uniqname')
        ORDER BY table_name, column_name
        """
        
        result = self.db.query_to_dataframe(query)
        
        # All tables should have 'uniqname', none should have 'uid' (except maybe during migration)
        for table in ['tdx_users', 'umapi_employees', 'mcommunity_users', 'ad_users', 'users']:
            table_cols = result[result['table_name'] == table]['column_name'].tolist()
            
            if 'uniqname' in table_cols and 'uid' not in table_cols:
                print(f"✅ {table}: Uses canonical 'uniqname' (no 'uid')")
                self.tests_passed += 1
            elif 'uniqname' in table_cols and 'uid' in table_cols:
                print(f"⚠️  {table}: Has both 'uniqname' and 'uid' (migration in progress)")
            else:
                print(f"❌ {table}: Missing 'uniqname' column")
                self.tests_failed += 1
    
    def test_name_field_semantics(self):
        """Test 3: Verify name field semantics (preferred_name vs full_name)."""
        print("\n" + "="*80)
        print("TEST 3: Name Field Semantics")
        print("="*80)
        
        # Sample users and check name formats
        query = """
        SELECT 
            uniqname,
            first_name,
            last_name,
            preferred_name,
            full_name
        FROM silver.users
        WHERE preferred_name IS NOT NULL 
          AND full_name IS NOT NULL
        LIMIT 10
        """
        
        result = self.db.query_to_dataframe(query)
        
        if result.empty:
            print("❌ No users found with both preferred_name and full_name")
            self.tests_failed += 1
            return
        
        # Check formats
        full_name_format_ok = True
        for _, row in result.iterrows():
            # full_name should be "Last, First" format (contains comma)
            if ',' not in str(row['full_name']):
                print(f"⚠️  {row['uniqname']}: full_name not in 'Last, First' format: '{row['full_name']}'")
                full_name_format_ok = False
        
        if full_name_format_ok:
            print(f"✅ All {len(result)} sampled users have full_name in 'Last, First' format")
            self.tests_passed += 1
        else:
            print(f"❌ Some users have incorrect full_name format")
            self.tests_failed += 1
        
        # Display sample
        print("\n📝 Sample name formats:")
        for _, row in result.head(5).iterrows():
            print(f"   {row['uniqname']:12s} | preferred: '{row['preferred_name']:25s}' | full: '{row['full_name']}'")
    
    def test_jsonb_flattening(self):
        """Test 4: Verify JSONB fields properly flattened."""
        print("\n" + "="*80)
        print("TEST 4: JSONB Flattening")
        print("="*80)
        
        # Test MCommunity sn → last_name
        query = """
        SELECT COUNT(*) as count
        FROM silver.mcommunity_users
        WHERE last_name IS NOT NULL
        """
        result = self.db.query_to_dataframe(query)
        mcom_count = result.iloc[0]['count']
        
        if mcom_count > 0:
            print(f"✅ MCommunity: {mcom_count} users have flattened last_name")
            self.tests_passed += 1
        else:
            print(f"❌ MCommunity: No users with flattened last_name")
            self.tests_failed += 1
        
        # Test UMAPI work_location → work_phone
        query = """
        SELECT COUNT(*) as count
        FROM silver.umapi_employees
        WHERE work_phone IS NOT NULL
        """
        result = self.db.query_to_dataframe(query)
        umapi_count = result.iloc[0]['count']
        
        if umapi_count > 0:
            print(f"✅ UMAPI: {umapi_count} employees have flattened work_phone")
            self.tests_passed += 1
        else:
            print(f"⚠️  UMAPI: No employees with flattened work_phone (may be expected)")
    
    def test_tdx_department_mapping(self):
        """Test 5: Verify TDX department_id mapping."""
        print("\n" + "="*80)
        print("TEST 5: TDX Department ID Mapping")
        print("="*80)
        
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(tdx_account_id) as has_account_id,
            COUNT(department_id) as has_dept_id
        FROM silver.tdx_users
        """
        
        result = self.db.query_to_dataframe(query)
        row = result.iloc[0]
        
        mapping_pct = (row['has_dept_id'] / row['has_account_id'] * 100) if row['has_account_id'] > 0 else 0
        
        print(f"   Total TDX users: {row['total']}")
        print(f"   Has tdx_account_id: {row['has_account_id']}")
        print(f"   Has department_id: {row['has_dept_id']} ({mapping_pct:.1f}% mapped)")
        
        if mapping_pct >= 90:
            print(f"✅ Department mapping successful (>90% mapped)")
            self.tests_passed += 1
        elif mapping_pct >= 70:
            print(f"⚠️  Department mapping partial (70-90% mapped)")
        else:
            print(f"❌ Department mapping failed (<70% mapped)")
            self.tests_failed += 1
    
    def test_consolidated_merge_logic(self):
        """Test 6: Verify consolidated users table merge logic."""
        print("\n" + "="*80)
        print("TEST 6: Consolidated Users Merge Logic")
        print("="*80)
        
        # Check consolidated table populated
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(preferred_name) as has_preferred,
            COUNT(full_name) as has_full,
            COUNT(primary_email) as has_email,
            COUNT(department_id) as has_dept,
            AVG(data_quality_score) as avg_quality
        FROM silver.users
        """
        
        result = self.db.query_to_dataframe(query)
        row = result.iloc[0]
        
        print(f"   Total users: {row['total']}")
        print(f"   Has preferred_name: {row['has_preferred']} ({row['has_preferred']/row['total']*100:.1f}%)")
        print(f"   Has full_name: {row['has_full']} ({row['has_full']/row['total']*100:.1f}%)")
        print(f"   Has primary_email: {row['has_email']} ({row['has_email']/row['total']*100:.1f}%)")
        print(f"   Has department_id: {row['has_dept']} ({row['has_dept']/row['total']*100:.1f}%)")
        print(f"   Avg quality score: {row['avg_quality']:.2f}")
        
        if row['total'] > 0 and row['has_preferred'] > 0:
            print(f"✅ Consolidated table populated successfully")
            self.tests_passed += 1
        else:
            print(f"❌ Consolidated table empty or missing data")
            self.tests_failed += 1
    
    def run_all_tests(self):
        """Run complete test suite."""
        print("\n" + "="*80)
        print("USERS ENTITY REFACTOR - INTEGRATION TEST SUITE")
        print("="*80)
        
        self.test_source_specific_schemas()
        self.test_column_name_consistency()
        self.test_name_field_semantics()
        self.test_jsonb_flattening()
        self.test_tdx_department_mapping()
        self.test_consolidated_merge_logic()
        
        # Summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        print(f"✅ Passed: {self.tests_passed}")
        print(f"❌ Failed: {self.tests_failed}")
        print(f"   Total:  {self.tests_passed + self.tests_failed}")
        
        if self.tests_failed == 0:
            print("\n🎉 ALL TESTS PASSED!")
            return True
        else:
            print(f"\n⚠️  {self.tests_failed} TEST(S) FAILED")
            return False
    
    def cleanup(self):
        """Clean up resources."""
        self.db.close()


if __name__ == "__main__":
    tester = UsersRefactorIntegrationTest()
    try:
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)
    finally:
        tester.cleanup()
```

### Step 1.5: Deployment (Day 11-12)

**Deployment Checklist:**

```markdown
## Phase 1 Deployment Checklist - Users Entity

### Pre-Deployment (Day 11 Morning)
- [ ] All tests passing in development environment
- [ ] Code review completed and approved
- [ ] Database backup created
- [ ] Rollback plan documented and tested
- [ ] Team notified of deployment window

### Deployment Steps (Day 11 Afternoon)

#### 1. Database Migrations (30 minutes)
- [ ] Run migration 001_rename_mcommunity_users_columns.sql
- [ ] Run migration 002_rename_ad_users_columns.sql
- [ ] Run migration 003_rename_tdx_users_columns.sql
- [ ] Run migration 004_rename_umapi_employees_columns.sql
- [ ] Run migration 005_rename_users_consolidated.sql
- [ ] Verify all migrations completed without errors
- [ ] Run validation queries from each migration

#### 2. Deploy Bronze Scripts (1 hour)
- [ ] Deploy updated 002_ingest_tdx_users.py
- [ ] Deploy updated 007_ingest_mcommunity_users.py
- [ ] Deploy updated 004_ingest_ad_users.py
- [ ] Deploy updated 003_ingest_umapi_employees.py
- [ ] Run smoke test: Ingest 1 test record from each source
- [ ] Verify records written with new column names

#### 3. Deploy Silver Transformation (30 minutes)
- [ ] Deploy updated 012_transform_users.py
- [ ] Run transformation on small test dataset (--dry-run first)
- [ ] Verify consolidated table populated correctly
- [ ] Check data quality scores

#### 4. Full Pipeline Test (1 hour)
- [ ] Run complete pipeline: Bronze → Silver transformation
- [ ] Run integration test suite
- [ ] Verify all tests pass
- [ ] Check for any unexpected warnings/errors in logs

#### 5. Drop Old Columns (30 minutes) - Optional, can defer
- [ ] Uncomment DROP COLUMN statements in migrations
- [ ] Re-run migrations to drop old columns
- [ ] Verify application still works without old columns

### Post-Deployment Verification (Day 12)
- [ ] Monitor logs for 24 hours
- [ ] Check data quality metrics
- [ ] Verify scheduled jobs run successfully
- [ ] User acceptance testing
- [ ] Update documentation
- [ ] Close deployment ticket

### Rollback Triggers
If ANY of these occur, execute rollback:
- [ ] Integration tests fail
- [ ] Data quality score drops > 10%
- [ ] More than 5% of records show NULL in critical fields
- [ ] Transformation script crashes repeatedly
- [ ] Critical queries fail due to missing columns
```

---

## Phase 2: Departments Entity

**Timeline:** Week 3-4 (10 days)  
**Tables Affected:** 3 (2 source-specific + 1 consolidated)  
**Scripts Affected:** 2 bronze + 1 silver transformation  
**Estimated Renames:** 7 columns

### Abbreviated Plan (Following Same Pattern)

**Step 2.1: Schema Migrations (Day 1-2)**
- `006_rename_tdx_departments_columns.sql`
- `007_rename_umapi_departments_columns.sql`
- `008_rename_departments_consolidated.sql`

**Renames:**
- TDX: `dept_code` → `dept_id`, `dept_name` → `department_name`, `dept_notes` → `description`, `manager_uid` → `tdx_manager_uid`
- UMAPI: `dept_description` → `department_name`, `dept_group` → `college_group`, `vp_area_name` → `vp_area`
- Consolidated: Remove redundant fields, align naming

**Step 2.2: Bronze Script Updates (Day 3-4)**
- `scripts/database/bronze/001_ingest_umapi_departments.py`
- `scripts/database/bronze/005_ingest_tdx_departments.py`

**Step 2.3: Silver Transformation Update (Day 5-6)**
- `scripts/database/silver/010_transform_departments.py`

**Step 2.4: Testing (Day 7-8)**
- Same integration test pattern as users

**Step 2.5: Deployment (Day 9-10)**
- Follow users deployment checklist

---

## Phase 3: Groups Entity

**Timeline:** Week 5-6 (10 days)  
**Tables Affected:** 3 (2 source-specific + 1 consolidated)  
**Scripts Affected:** 2 bronze + 1 silver transformation  
**Estimated Renames:** 2 columns

### Abbreviated Plan

**Step 3.1: Schema Migrations (Day 1-2)**
- `009_rename_ad_groups_columns.sql`
- `010_rename_groups_consolidated.sql`

**Renames (minimal):**
- AD Groups: `name` → `group_name`, `mail` → `group_email`
- MCommunity Groups: Already uses canonical names (no changes needed!)

**Step 3.2-3.5:** Follow same pattern as Phases 1 & 2

---

## Testing Strategy

### Test Levels

**1. Unit Tests** (per script)
- Test individual transformation functions
- Test column mapping logic
- Test JSONB flattening
- Test department ID mapping

**2. Integration Tests** (per entity)
- Test complete pipeline (Bronze → Silver)
- Validate schema changes
- Verify data integrity
- Check for regressions

**3. Data Quality Tests** (continuous)
- Monitor quality scores
- Check for NULL values in critical fields
- Validate FK relationships
- Compare before/after metrics

### Automated Test Suite Structure

```
tests/
├── unit/
│   ├── test_bronze_tdx_users.py
│   ├── test_bronze_mcommunity_users.py
│   ├── test_silver_user_merge.py
│   └── test_name_field_derivation.py
├── integration/
│   ├── test_users_refactor_integration.py
│   ├── test_departments_refactor_integration.py
│   └── test_groups_refactor_integration.py
├── data_quality/
│   ├── test_null_critical_fields.py
│   ├── test_quality_score_regression.py
│   └── test_foreign_key_integrity.py
└── regression/
    ├── test_existing_queries.py
    └── test_view_compatibility.py
```

### Test Execution Schedule

```bash
# During development (after each script update)
python -m pytest tests/unit/test_bronze_tdx_users.py -v

# Before deployment (comprehensive)
python -m pytest tests/ -v --tb=short

# Post-deployment (monitoring)
python tests/integration/test_users_refactor_integration.py
python tests/data_quality/test_quality_score_regression.py
```

---

## Rollback Plan

### Rollback Triggers

Execute rollback if:
1. ❌ Integration tests fail
2. ❌ Data quality score drops > 10%
3. ❌ > 5% of records have NULL in critical fields (uniqname, first_name, last_name)
4. ❌ Critical queries/views fail
5. ❌ Transformation crashes repeatedly

### Rollback Procedure

**Option 1: Schema-Level Rollback** (if old columns still exist)

```sql
-- Revert to old column names
BEGIN;

-- Example: MCommunity users
ALTER TABLE silver.mcommunity_users RENAME COLUMN uniqname TO uid;
ALTER TABLE silver.mcommunity_users RENAME COLUMN first_name TO given_name;
-- ... etc for all columns ...

-- Revert indexes
DROP INDEX idx_mcommunity_users_uniqname;
CREATE INDEX idx_mcommunity_users_uid ON silver.mcommunity_users(uid);

COMMIT;
```

**Option 2: Table-Level Rollback** (restore from backup)

```sql
-- If backup tables created
BEGIN;

-- Drop current table
DROP TABLE silver.mcommunity_users;

-- Restore from backup
ALTER TABLE silver.mcommunity_users_backup_20250118 
RENAME TO mcommunity_users;

-- Recreate indexes
-- ... (from original schema) ...

COMMIT;
```

**Option 3: Code Rollback** (revert scripts)

```bash
# Revert to previous git commit
git revert <commit-hash>

# Redeploy old bronze/silver scripts
# ... deployment commands ...
```

### Post-Rollback Actions

1. ✅ Verify system operational with old column names
2. ✅ Document failure cause
3. ✅ Fix identified issues
4. ✅ Re-plan deployment with fixes
5. ✅ Notify stakeholders

---

## Success Criteria

### Phase 1: Users Entity

**Functional Criteria:**
- ✅ All 4 source-specific tables renamed successfully
- ✅ Bronze ingestion scripts write to new columns
- ✅ Silver transformation reads from new columns
- ✅ Consolidated table populated with canonical names
- ✅ No errors in transformation logs

**Data Quality Criteria:**
- ✅ Zero data loss (row counts match before/after)
- ✅ Data quality score maintained or improved
- ✅ < 1% NULL values in critical fields (uniqname, first_name, last_name)
- ✅ Department mapping > 90% success rate
- ✅ Name field formats correct (full_name = "Last, First", preferred_name = user's choice)

**Performance Criteria:**
- ✅ Transformation runtime within 10% of baseline
- ✅ Query performance maintained or improved
- ✅ Index usage optimal

### Phase 2: Departments Entity

- ✅ 2 source-specific tables renamed
- ✅ Merge logic simplified (no field mapping needed)
- ✅ All tests passing

### Phase 3: Groups Entity

- ✅ 1 source-specific table renamed (AD Groups)
- ✅ Minimal impact (only 2 columns)
- ✅ All tests passing

### Overall Project Success

- ✅ All 32 column renames completed
- ✅ All transformation scripts using canonical names
- ✅ Data dictionary fully implemented
- ✅ Zero production incidents
- ✅ Team trained on new naming conventions
- ✅ Documentation updated
- ✅ Ready for dbt migration (clean foundation)

---

## Appendix: Scripts & Queries

### A. Validation Queries

```sql
-- Check column rename completeness
SELECT 
    table_name,
    column_name,
    CASE 
        WHEN column_name IN ('uid', 'given_name', 'sn', 'mail', 'telephone_number') THEN 'OLD'
        WHEN column_name IN ('uniqname', 'first_name', 'last_name', 'primary_email', 'work_phone') THEN 'NEW'
        ELSE 'OTHER'
    END as column_category
FROM information_schema.columns
WHERE table_schema = 'silver'
  AND table_name IN ('tdx_users', 'umapi_employees', 'mcommunity_users', 'ad_users', 'users')
  AND column_name IN ('uid', 'uniqname', 'given_name', 'first_name', 'sn', 'last_name', 
                       'mail', 'primary_email', 'telephone_number', 'work_phone')
ORDER BY table_name, column_category, column_name;
```

```sql
-- Data quality comparison (before/after)
SELECT 
    'Before' as phase,
    COUNT(*) as total_users,
    AVG(data_quality_score) as avg_quality,
    COUNT(*) FILTER (WHERE data_quality_score < 0.70) as poor_quality
FROM silver.users_backup_20250118

UNION ALL

SELECT 
    'After',
    COUNT(*),
    AVG(data_quality_score),
    COUNT(*) FILTER (WHERE data_quality_score < 0.70)
FROM silver.users;
```

### B. Performance Benchmarks

```sql
-- Benchmark query performance
EXPLAIN ANALYZE
SELECT u.uniqname, u.first_name, u.last_name, u.primary_email, d.department_name
FROM silver.users u
LEFT JOIN silver.departments d ON u.department_id = d.dept_id
WHERE u.is_active = true
  AND u.primary_email LIKE '%@umich.edu'
ORDER BY u.last_name, u.first_name
LIMIT 100;
```

### C. Monitoring Queries

```sql
-- Daily health check
SELECT 
    DATE(updated_at) as date,
    COUNT(*) as records_updated,
    AVG(data_quality_score) as avg_quality
FROM silver.users
WHERE updated_at > CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(updated_at)
ORDER BY date DESC;
```

---

**Document Version:** 1.0  
**Last Updated:** 2025-01-18  
**Status:** Ready for Implementation  
**Next Steps:** Begin Phase 1, Step 1.1 (Schema Migrations)
