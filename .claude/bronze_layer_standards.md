# Bronze Layer Standards

**Version:** 1.0  
**Last Updated:** 2025-01-24  
**Purpose:** Complete bronze layer reference for ingestion, change detection, and raw storage

---

## Table of Contents

1. [Overview & Philosophy](#overview--philosophy)
2. [Universal Raw Entities Table](#universal-raw-entities-table)
3. [Change Detection Strategies](#change-detection-strategies)
4. [Metadata Enrichment Patterns](#metadata-enrichment-patterns)
5. [Bronze Ingestion Script Standards](#bronze-ingestion-script-standards)
6. [Performance Optimization](#performance-optimization)
7. [Common Patterns & Examples](#common-patterns--examples)
8. [Troubleshooting](#troubleshooting)

---

## Overview & Philosophy

**Purpose:** Store raw data exactly as received with complete history

**Key Principles:**
1. **Append-only** — Never delete or modify bronze records
2. **Single universal table** — All entity types in `bronze.raw_entities`
3. **Zero transformation** — Preserve original structure (except metadata enrichment)
4. **Complete audit trail** — Track every version of every record

**Technology:** Python (API adapters, file parsers, change detection)

**Reference:** See [medallion_standards.md](medallion_standards.md) for cross-layer standards

---

## Universal Raw Entities Table

### Schema

```sql
CREATE TABLE bronze.raw_entities (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type VARCHAR(50) NOT NULL,        -- 'user', 'group', 'computer', 'department', etc.
    source_system VARCHAR(50) NOT NULL,      -- 'tdx', 'umapi', 'ad', 'mcommunity_ldap'
    external_id VARCHAR(255) NOT NULL,       -- ID from source system
    raw_data JSONB NOT NULL,                 -- Complete unmodified data
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    entity_hash VARCHAR(64),                 -- SHA-256 hash for change detection
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    ingestion_metadata JSONB DEFAULT '{}'::jsonb
);
```

### Why Single Universal Table?

**Modern best practice** (2024+):

✅ **Advantages:**
- **Simplified management** — One ACL, one retention policy, one backup strategy
- **Streaming-friendly** — Single Kafka/Kinesis sink instead of dozens
- **Better scalability** — PostgreSQL optimizes large partitioned tables better than many small tables
- **Operational simplicity** — Universal policies like "delete records older than 90 days"
- **Easier schema evolution** — Add new entity types without migrations

❌ **Avoid:** One table per source/entity combination (creates hundreds of tables)

**Example of what NOT to do:**
```sql
-- ❌ BAD: Creates table explosion
bronze.tdx_users
bronze.tdx_assets
bronze.tdx_tickets
bronze.umapi_users
bronze.umapi_departments
bronze.ad_users
bronze.ad_groups
bronze.ad_computers
-- ... 50+ tables
```

### Column Definitions

| Column | Type | Purpose | Example |
|--------|------|---------|---------|
| `raw_id` | UUID | Unique identifier for this bronze record | `550e8400-e29b-41d4-a716-446655440000` |
| `entity_type` | VARCHAR(50) | Type of entity | `user`, `group`, `computer`, `department`, `lab_award` |
| `source_system` | VARCHAR(50) | Source system identifier | `tdx`, `umapi`, `ad`, `mcommunity_ldap` |
| `external_id` | VARCHAR(255) | ID from source system | TDX UID, LDAP DN, API ID |
| `raw_data` | JSONB | Complete unmodified source data | Full JSON response from API |
| `ingested_at` | TIMESTAMP | When record was ingested | `2025-01-24 10:30:00+00` |
| `entity_hash` | VARCHAR(64) | SHA-256 content hash | For change detection |
| `ingestion_run_id` | UUID | Links to meta.ingestion_runs | For tracking batch runs |
| `ingestion_metadata` | JSONB | Optional metadata | `{"api_version": "v2", "batch_id": "123"}` |

### Standard Indexes

```sql
-- Query by entity type and source
CREATE INDEX idx_bronze_entity_source 
ON bronze.raw_entities (entity_type, source_system);

-- Query recent ingestions
CREATE INDEX idx_bronze_ingested_at 
ON bronze.raw_entities (ingested_at DESC);

-- Query by external ID (for lookups)
CREATE INDEX idx_bronze_external_id 
ON bronze.raw_entities (source_system, entity_type, external_id);

-- Change detection by hash
CREATE INDEX idx_bronze_entity_hash 
ON bronze.raw_entities (entity_hash);

-- Composite index for incremental queries
CREATE INDEX idx_bronze_source_type_ingested 
ON bronze.raw_entities (source_system, entity_type, ingested_at DESC);
```

---

## Change Detection Strategies

Bronze ingestion should **avoid duplicate records** by detecting when data hasn't changed. Two strategies:

### Strategy 1: Content Hashing (No Timestamps)

**Use when:** Source system has no `modified_date` or `updated_at` field

**Examples:**
- UMich API departments (no modification timestamp)
- Some REST APIs that don't track changes
- CSV files without change tracking

**Pattern:**

```python
import hashlib
import json
from typing import Dict, Any

def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
    """
    Calculate SHA-256 hash of significant fields.
    
    CRITICAL: Exclude metadata that changes without meaningful updates:
    - Active Directory: uSNChanged, uSNCreated, dSCorePropagationData
    - LDAP: modifyTimestamp, entryCSN, sequence numbers
    - API: last_synced, api_request_id, internal tracking fields
    - Auto-increment IDs, sync counters, audit timestamps
    
    Args:
        data: Raw data dictionary from source system
        
    Returns:
        SHA-256 hex string (64 characters)
    
    Example:
        >>> data = {'DeptId': '185500', 'DeptDescription': 'Chemistry', 'uSNChanged': '12345'}
        >>> hash = self._calculate_content_hash(data)
        >>> len(hash)
        64
    """
    # Extract only business-relevant fields
    significant_fields = {
        'DeptId': data.get('DeptId', '').strip(),
        'DeptDescription': data.get('DeptDescription', '').strip(),
        'IsActive': data.get('IsActive'),
        'ParentDeptId': data.get('ParentDeptId'),
        # Include all business fields
        # EXCLUDE: uSNChanged, modifyTimestamp, _sync_date, etc.
    }
    
    # Normalize JSON (sorted keys, no whitespace)
    normalized_json = json.dumps(
        significant_fields,
        sort_keys=True,
        separators=(',', ':')  # No spaces
    )
    
    # Calculate SHA-256 hash
    return hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()
```

**Usage in ingestion:**

```python
def ingest_departments(self):
    """Ingest departments using content hashing for change detection."""
    
    # 1. Fetch existing hashes from database
    existing_hashes = self._get_existing_department_hashes()
    # Returns: {'185500': 'abc123...', '211000': 'def456...'}
    
    # 2. Fetch departments from source API
    departments = self.api.get_all_departments()
    
    stats = {
        'fetched': len(departments),
        'new': 0,
        'updated': 0,
        'unchanged': 0
    }
    
    for dept in departments:
        dept_id = dept.get('DeptId')
        current_hash = self._calculate_content_hash(dept)
        
        if existing_hash := existing_hashes.get(dept_id):
            if existing_hash == current_hash:
                logger.debug(f"⏭️  Unchanged, skipping: {dept.get('DeptDescription')}")
                stats['unchanged'] += 1
                continue
            else:
                logger.info(f"📝 Changed: {dept.get('DeptDescription')}")
                stats['updated'] += 1
        else:
            logger.info(f"🆕 New department: {dept.get('DeptDescription')}")
            stats['new'] += 1
        
        # Insert new version
        self._insert_bronze_entity(
            entity_type='department',
            source_system='umapi',
            external_id=dept_id,
            raw_data=dept,
            entity_hash=current_hash
        )
    
    logger.info(f"📊 Statistics: {stats['new']} new, {stats['updated']} updated, {stats['unchanged']} unchanged")
    return stats
```

**Helper method:**

```python
def _get_existing_department_hashes(self) -> Dict[str, str]:
    """
    Get most recent hash for each department.
    
    Returns:
        Dict mapping dept_id to entity_hash
    """
    query = """
        WITH ranked_depts AS (
            SELECT 
                external_id,
                entity_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY external_id 
                    ORDER BY ingested_at DESC
                ) as rn
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
              AND source_system = 'umapi'
        )
        SELECT external_id, entity_hash
        FROM ranked_depts
        WHERE rn = 1
    """
    
    result = self.db.query(query)
    return {row['external_id']: row['entity_hash'] for row in result}
```

### Strategy 2: Timestamp-Based (Has Modified Date)

**Use when:** Source provides reliable `modified_date`, `whenChanged`, or `updated_at` field

**Examples:**
- Active Directory (has `whenChanged` field)
- LDAP (has `modifyTimestamp` field)
- Many modern REST APIs (have `updated_at` field)

**Pattern:**

```python
from datetime import datetime, timezone

def ingest_users_incremental(self, last_sync: datetime):
    """
    Ingest only users modified since last sync.
    
    Args:
        last_sync: Timestamp of last successful ingestion
    """
    # 1. Query only modified records from source
    modified_users = self._fetch_modified_since(last_sync)
    
    # 2. Insert all (they've all changed by definition)
    for user in modified_users:
        self._insert_bronze_entity(
            entity_type='user',
            source_system='ad',
            external_id=user.get('distinguishedName'),
            raw_data=user,
            entity_hash=None  # Not needed with timestamp-based
        )
    
    logger.info(f"📥 Ingested {len(modified_users)} modified users since {last_sync}")
```

**LDAP Example:**

```python
def _fetch_modified_since(self, last_sync: datetime) -> List[Dict]:
    """
    Query LDAP for records modified since timestamp.
    
    Args:
        last_sync: Datetime to query from
        
    Returns:
        List of user records
    """
    import ldap
    
    # Format timestamp for LDAP (GeneralizedTime format)
    timestamp_str = last_sync.strftime('%Y%m%d%H%M%S.0Z')
    
    # LDAP filter with timestamp
    search_filter = (
        f"(&"
        f"(objectClass=user)"
        f"(whenChanged>={timestamp_str})"
        f")"
    )
    
    results = self.ldap_conn.search_s(
        base_dn='OU=Users,DC=example,DC=com',
        scope=ldap.SCOPE_SUBTREE,
        filterstr=search_filter,
        attrlist=['*']  # All attributes
    )
    
    return [self._ldap_entry_to_dict(entry) for dn, entry in results]
```

**Active Directory Example:**

```python
def _fetch_ad_users_modified_since(self, last_sync: datetime) -> List[Dict]:
    """
    Query Active Directory for users modified since timestamp.
    
    Uses whenChanged attribute for change detection.
    """
    from datetime import timezone
    
    # AD expects UTC
    timestamp_str = last_sync.astimezone(timezone.utc).strftime('%Y%m%d%H%M%S.0Z')
    
    search_filter = f"(&(objectClass=user)(whenChanged>={timestamp_str}))"
    
    # Execute AD query
    results = self.ad_adapter.search(
        filter=search_filter,
        attributes=['*']
    )
    
    logger.info(f"📥 Found {len(results)} AD users modified since {last_sync}")
    return results
```

### Strategy Comparison

| Aspect | Content Hashing | Timestamp-Based |
|--------|----------------|-----------------|
| **Source requirement** | None | Must have modification timestamp |
| **Detection accuracy** | Exact (byte-level) | Depends on timestamp granularity |
| **Performance** | Requires hash calculation | Faster (database does filtering) |
| **Query complexity** | Must fetch all records | Can filter at source |
| **Best for** | Small datasets, APIs without timestamps | Large datasets, LDAP/AD |
| **Example** | UMich API departments | Active Directory users |

### Decision Matrix

```
Does source provide modification timestamp?
├─ YES → Use timestamp-based (faster)
└─ NO
   Is dataset small (<10k records)?
   ├─ YES → Use content hashing (acceptable performance)
   └─ NO → Request timestamp field from API or implement full-sync only
```

---

## Metadata Enrichment Patterns

### Prefix Convention: `_` for Computed Fields

All metadata fields added during ingestion use underscore prefix to distinguish from original data:

```python
enriched_data = {
    **raw_data,  # Original data preserved (spread operator)
    
    # Computed metadata (underscore prefix):
    '_content_hash': content_hash,
    '_extracted_uniqname': extract_uniqname(raw_data.get('ou')),
    '_ou_hierarchy': parse_ou_hierarchy(raw_data.get('dn')),
    '_direct_computer_count': count_computers(ou_dn),
    '_depth_category': categorize_depth(ou_depth),
    '_source_file': 'lab_awards_2025_q1.csv',
}
```

**Why underscore prefix?**
- Clearly distinguishes computed vs original fields
- Prevents naming collisions with source fields
- Makes it obvious what was added during ingestion
- Industry convention (similar to Python private attributes)

### Common Enrichment Patterns

| Enrichment Field | Purpose | Example Value | When to Use |
|-----------------|---------|---------------|-------------|
| `_content_hash` | Change detection | `a7f3b2c1...` (SHA-256) | Sources without timestamps |
| `_extracted_uniqname` | Parsed identifier from DN/path | `jdoe` from `OU=jdoe,OU=labs` | Organizational units, LDAP |
| `_ou_hierarchy` | Parsed organizational path | `["LSA", "Chemistry", "jdoe"]` | Active Directory OUs |
| `_direct_computer_count` | Infrastructure count | `47` | OUs with computer children |
| `_depth_category` | Classification based on depth | `potential_lab`, `department` | OU depth analysis |
| `_source_file` | Batch file tracking | `awards_2025_q1.csv` | CSV/file imports |
| `_ingestion_timestamp` | When ingestion occurred | `2025-01-24T10:30:00Z` | All sources |
| `_api_version` | API version used | `v2.1` | REST APIs with versioning |

### Example: OU Hierarchy Enrichment

```python
def _enrich_organizational_unit(self, ou_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich OU data with computed metadata.
    
    Args:
        ou_data: Raw OU data from Active Directory
        
    Returns:
        Enriched data with _prefixed metadata fields
    """
    dn = ou_data.get('distinguishedName', '')
    ou_name = ou_data.get('ou', '')
    
    # Parse hierarchy from DN
    ou_hierarchy = self._parse_ou_hierarchy(dn)
    
    # Extract potential uniqname (if OU looks like a lab)
    extracted_uniqname = self._extract_uniqname_from_ou(ou_name)
    
    # Count direct computer children
    computer_count = self._count_direct_computers(dn)
    
    # Categorize based on depth
    depth = len(ou_hierarchy)
    depth_category = self._categorize_ou_depth(depth, ou_hierarchy)
    
    # Return enriched data
    return {
        **ou_data,  # Original fields preserved
        '_ou_hierarchy': ou_hierarchy,
        '_extracted_uniqname': extracted_uniqname,
        '_direct_computer_count': computer_count,
        '_ou_depth': depth,
        '_depth_category': depth_category,
        '_ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
    }

def _parse_ou_hierarchy(self, dn: str) -> List[str]:
    """
    Parse DN into hierarchical list.
    
    Args:
        dn: Distinguished Name like "OU=jdoe,OU=Chemistry,OU=LSA,DC=umich,DC=edu"
        
    Returns:
        List like ["LSA", "Chemistry", "jdoe"]
    """
    import re
    
    # Extract OU components (ignore DC)
    ou_parts = re.findall(r'OU=([^,]+)', dn)
    
    # Reverse (DN is bottom-up, we want top-down)
    return list(reversed(ou_parts))

def _extract_uniqname_from_ou(self, ou_name: str) -> Optional[str]:
    """
    Extract uniqname if OU name looks like a uniqname.
    
    Args:
        ou_name: OU name like "jdoe" or "Chemistry Department"
        
    Returns:
        Uniqname if detected, None otherwise
    """
    # Simple heuristic: lowercase, no spaces, 2-8 chars
    if ou_name and ou_name.islower() and ' ' not in ou_name and 2 <= len(ou_name) <= 8:
        return ou_name
    return None

def _categorize_ou_depth(self, depth: int, hierarchy: List[str]) -> str:
    """
    Categorize OU based on depth in hierarchy.
    
    Args:
        depth: Number of OU levels
        hierarchy: OU path like ["LSA", "Chemistry", "jdoe"]
        
    Returns:
        Category: 'department', 'potential_lab', 'deep_nested'
    """
    if depth <= 2:
        return 'department'  # Top-level or one level down
    elif depth == 3:
        return 'potential_lab'  # Three levels suggests lab
    else:
        return 'deep_nested'  # Unusual depth
```

### Example: CSV File Enrichment

```python
def ingest_from_csv(self, file_path: Path) -> Dict[str, int]:
    """
    Ingest lab awards from CSV file with enrichment.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        Statistics dict
    """
    import csv
    from pathlib import Path
    
    stats = {'records_ingested': 0}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        entities = []
        for row_num, row in enumerate(reader, start=1):
            # Enrich with file metadata
            enriched = {
                **row,  # Original CSV columns
                '_source_file': file_path.name,
                '_row_number': row_num,
                '_ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
                '_file_size_bytes': file_path.stat().st_size,
                '_file_modified': datetime.fromtimestamp(
                    file_path.stat().st_mtime, 
                    tz=timezone.utc
                ).isoformat(),
            }
            
            # Calculate content hash
            content_hash = self._calculate_content_hash(row)
            
            entities.append({
                'entity_type': 'lab_award',
                'source_system': 'umich_orsp',
                'external_id': row.get('Award Id'),
                'raw_data': enriched,
                'entity_hash': content_hash,
                'ingestion_run_id': self.run_id,
            })
        
        # Bulk insert
        self._bulk_insert_entities(entities)
        stats['records_ingested'] = len(entities)
    
    logger.info(f"📄 Ingested {stats['records_ingested']} awards from {file_path.name}")
    return stats
```

---

## Bronze Ingestion Script Standards

### Required Components

Every bronze ingestion script MUST have:

1. **Service Class Pattern**
2. **Ingestion Run Tracking**
3. **Statistics Logging**
4. **Error Handling**
5. **Command-Line Arguments**

### Standard Script Template

```python
#!/usr/bin/env python3
"""
Ingest {entity_type} from {source_system} into bronze.raw_entities.

Usage:
    python ingest_{source}_{entity}.py [--full-sync] [--dry-run]
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from database.adapters.postgres_adapter import PostgresAdapter
from {source}.api.{entity}_api import {Entity}API

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class {Entity}IngestionService:
    """Service for ingesting {entity_type} from {source_system}."""
    
    def __init__(self, db_adapter: PostgresAdapter, api_adapter: {Entity}API):
        """
        Initialize ingestion service.
        
        Args:
            db_adapter: PostgreSQL database adapter
            api_adapter: API adapter for fetching data
        """
        self.db = db_adapter
        self.api = api_adapter
        self.run_id: Optional[str] = None
    
    def _get_existing_hashes(self) -> Dict[str, str]:
        """
        Get most recent hash for each entity.
        
        Returns:
            Dict mapping external_id to entity_hash
        """
        query = """
            WITH ranked_entities AS (
                SELECT 
                    external_id,
                    entity_hash,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id 
                        ORDER BY ingested_at DESC
                    ) as rn
                FROM bronze.raw_entities
                WHERE entity_type = %s
                  AND source_system = %s
            )
            SELECT external_id, entity_hash
            FROM ranked_entities
            WHERE rn = 1
        """
        
        result = self.db.query(query, ('{entity_type}', '{source_system}'))
        return {row['external_id']: row['entity_hash'] for row in result}
    
    def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 hash of significant fields.
        
        Args:
            data: Raw entity data
            
        Returns:
            SHA-256 hex string
        """
        # Extract significant fields (exclude volatile metadata)
        significant_fields = {
            'id': data.get('id'),
            'name': data.get('name'),
            # Add all business-relevant fields
            # EXCLUDE: timestamps, sync counters, internal IDs
        }
        
        normalized_json = json.dumps(
            significant_fields,
            sort_keys=True,
            separators=(',', ':')
        )
        
        return hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()
    
    def _enrich_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich data with computed metadata.
        
        Args:
            data: Raw entity data
            
        Returns:
            Enriched data with _prefixed fields
        """
        return {
            **data,
            '_ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
            # Add other enrichments as needed
        }
    
    def _fetch_source_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from source API.
        
        Returns:
            List of entity dictionaries
        """
        logger.info("📥 Fetching {entity_type} from {source_system}...")
        
        # Fetch from API (handle pagination if needed)
        entities = self.api.get_all_{entities}()
        
        logger.info(f"   Fetched {len(entities)} {entity_type} records")
        return entities
    
    def _bulk_insert_entities(
        self, 
        entities: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> int:
        """
        Bulk insert entities into bronze.raw_entities.
        
        Args:
            entities: List of entity dicts to insert
            batch_size: Number of records per batch
            
        Returns:
            Number of records inserted
        """
        from psycopg2.extras import execute_values
        
        if not entities:
            return 0
        
        query = """
            INSERT INTO bronze.raw_entities 
            (entity_type, source_system, external_id, raw_data, entity_hash, ingestion_run_id)
            VALUES %s
        """
        
        # Prepare tuples for bulk insert
        values = [
            (
                entity['entity_type'],
                entity['source_system'],
                entity['external_id'],
                json.dumps(entity['raw_data']),
                entity['entity_hash'],
                entity['ingestion_run_id']
            )
            for entity in entities
        ]
        
        # Execute in batches
        with self.db.conn.cursor() as cursor:
            execute_values(cursor, query, values, page_size=batch_size)
        
        self.db.conn.commit()
        return len(entities)
    
    def ingest(
        self, 
        full_sync: bool = False,
        dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Main ingestion orchestration.
        
        Args:
            full_sync: If True, ingest all records (ignore change detection)
            dry_run: If True, log but don't insert records
            
        Returns:
            Statistics dict
        """
        logger.info("🔄 Starting {entity_type} ingestion from {source_system}")
        
        # Initialize statistics
        stats = {
            'records_fetched': 0,
            'records_new': 0,
            'records_updated': 0,
            'records_skipped_unchanged': 0,
            'records_failed': 0,
        }
        
        # Create ingestion run
        self.run_id = self.db.create_ingestion_run(
            source_system='{source_system}',
            entity_type='{entity_type}',
            metadata={'full_sync': full_sync, 'dry_run': dry_run}
        )
        
        try:
            # 1. Fetch existing hashes (unless full sync)
            existing_hashes = {} if full_sync else self._get_existing_hashes()
            logger.info(f"📚 Loaded {len(existing_hashes)} existing hashes")
            
            # 2. Fetch source data
            source_data = self._fetch_source_data()
            stats['records_fetched'] = len(source_data)
            
            # 3. Process each record
            entities_to_insert = []
            
            for data in source_data:
                external_id = str(data.get('id'))  # Adjust field name as needed
                
                # Calculate hash
                current_hash = self._calculate_content_hash(data)
                
                # Check if changed (unless full sync)
                if not full_sync and external_id in existing_hashes:
                    if existing_hashes[external_id] == current_hash:
                        logger.debug(f"⏭️  Unchanged: {external_id}")
                        stats['records_skipped_unchanged'] += 1
                        continue
                    else:
                        logger.info(f"📝 Updated: {external_id}")
                        stats['records_updated'] += 1
                else:
                    logger.info(f"🆕 New: {external_id}")
                    stats['records_new'] += 1
                
                # Enrich with metadata
                enriched_data = self._enrich_metadata(data)
                
                # Prepare for insertion
                entities_to_insert.append({
                    'entity_type': '{entity_type}',
                    'source_system': '{source_system}',
                    'external_id': external_id,
                    'raw_data': enriched_data,
                    'entity_hash': current_hash,
                    'ingestion_run_id': self.run_id,
                })
            
            # 4. Bulk insert (unless dry run)
            if entities_to_insert and not dry_run:
                inserted_count = self._bulk_insert_entities(entities_to_insert)
                logger.info(f"✅ Inserted {inserted_count} records")
            elif dry_run:
                logger.info(f"🧪 DRY RUN: Would insert {len(entities_to_insert)} records")
            
            # 5. Complete ingestion run
            if not dry_run:
                self.db.complete_ingestion_run(self.run_id, stats)
            
            # Log summary
            logger.info("📊 Ingestion Summary:")
            logger.info(f"   ├─ Fetched: {stats['records_fetched']}")
            logger.info(f"   ├─ New: {stats['records_new']}")
            logger.info(f"   ├─ Updated: {stats['records_updated']}")
            logger.info(f"   └─ Unchanged: {stats['records_skipped_unchanged']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Ingestion failed: {str(e)}")
            if self.run_id and not dry_run:
                self.db.fail_ingestion_run(self.run_id, str(e))
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Ingest {entity_type} from {source_system} into bronze layer'
    )
    parser.add_argument(
        '--full-sync',
        action='store_true',
        help='Ingest all records (ignore change detection)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Log actions without inserting records'
    )
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    
    # Initialize adapters
    db_adapter = PostgresAdapter(
        database_url=os.getenv('DATABASE_URL'),
        pool_size=5
    )
    
    api_adapter = {Entity}API(
        base_url=os.getenv('{SOURCE}_BASE_URL'),
        api_token=os.getenv('{SOURCE}_API_TOKEN')
    )
    
    # Run ingestion
    service = {Entity}IngestionService(db_adapter, api_adapter)
    try:
        stats = service.ingest(
            full_sync=args.full_sync,
            dry_run=args.dry_run
        )
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        db_adapter.close()


if __name__ == '__main__':
    main()
```

---

## Performance Optimization

### 1. Batching Inserts

**❌ BAD: Individual inserts (very slow)**

```python
for entity in entities:
    cursor.execute(
        "INSERT INTO bronze.raw_entities (...) VALUES (...)",
        (entity['type'], entity['system'], ...)
    )
    conn.commit()  # Commit after each insert
# Result: 10,000 records = 10,000 round trips = ~10 minutes
```

**✅ GOOD: Batch insert with execute_values (1000x faster)**

```python
from psycopg2.extras import execute_values

query = """
    INSERT INTO bronze.raw_entities 
    (entity_type, source_system, external_id, raw_data, entity_hash, ingestion_run_id)
    VALUES %s
"""

# Prepare all values at once
values = [
    (e['entity_type'], e['source_system'], e['external_id'], 
     json.dumps(e['raw_data']), e['entity_hash'], e['ingestion_run_id'])
    for e in entities
]

# Single bulk insert
with conn.cursor() as cursor:
    execute_values(cursor, query, values, page_size=1000)
conn.commit()  # Single commit
# Result: 10,000 records = 10 batches = ~5 seconds
```

### 2. Connection Pooling

```python
# ✅ Use connection pooling for multiple scripts
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=database_url
)

# Get connection from pool
conn = connection_pool.getconn()
try:
    # Use connection
    cursor.execute(query)
finally:
    # Return to pool (don't close)
    connection_pool.putconn(conn)
```

### 3. Essential Indexes

```sql
-- ✅ Create indexes AFTER bulk insert for better performance

-- Core query indexes
CREATE INDEX CONCURRENTLY idx_bronze_entity_source 
ON bronze.raw_entities (entity_type, source_system);

CREATE INDEX CONCURRENTLY idx_bronze_ingested_at 
ON bronze.raw_entities (ingested_at DESC);

-- Change detection index
CREATE INDEX CONCURRENTLY idx_bronze_entity_hash 
ON bronze.raw_entities (entity_hash);

-- Source-specific JSONB indexes (only for frequently queried fields)
CREATE INDEX CONCURRENTLY idx_bronze_lab_award_uniqname
ON bronze.raw_entities (LOWER(raw_data->>'Person Uniqname'))
WHERE entity_type = 'lab_award';

CREATE INDEX CONCURRENTLY idx_bronze_ou_extracted_uniqname
ON bronze.raw_entities (LOWER(raw_data->>'_extracted_uniqname'))
WHERE entity_type = 'organizational_unit';

-- GIN index for full JSONB search (use sparingly - expensive)
CREATE INDEX CONCURRENTLY idx_bronze_raw_data_gin
ON bronze.raw_entities USING gin(raw_data jsonb_path_ops);
```

**Note:** Use `CONCURRENTLY` to avoid locking table during index creation.

### 4. Partitioning (Future)

For very large bronze tables (millions of records):

```sql
-- Partition by ingested_at (monthly)
CREATE TABLE bronze.raw_entities (
    ...
) PARTITION BY RANGE (ingested_at);

CREATE TABLE bronze.raw_entities_2025_01 
PARTITION OF bronze.raw_entities
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE bronze.raw_entities_2025_02 
PARTITION OF bronze.raw_entities
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
```

---

## Common Patterns & Examples

### Pattern 1: API Ingestion with Pagination

```python
def _fetch_all_pages(self, endpoint: str) -> List[Dict]:
    """
    Fetch all pages from paginated API.
    
    Args:
        endpoint: API endpoint path
        
    Returns:
        List of all records across all pages
    """
    all_records = []
    page = 1
    
    while True:
        logger.info(f"📥 Fetching page {page}...")
        
        response = self.api.get(
            endpoint, 
            params={'page': page, 'size': 1000}
        )
        
        data = response.json()
        records = data.get('data', [])
        
        if not records:
            logger.info(f"   No more records (last page: {page - 1})")
            break
        
        all_records.extend(records)
        logger.info(f"   ├─ Page {page}: {len(records)} records")
        logger.info(f"   └─ Total so far: {len(all_records)} records")
        
        # Check if there are more pages
        if 'next' not in data or data['next'] is None:
            break
        
        page += 1
    
    logger.info(f"📊 Total fetched: {len(all_records)} records from {page} pages")
    return all_records
```

### Pattern 2: CSV File Ingestion

```python
def ingest_from_csv(self, file_path: Path) -> Dict[str, int]:
    """
    Ingest records from CSV file.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        Statistics dict
    """
    import csv
    from pathlib import Path
    
    logger.info(f"📄 Ingesting from {file_path.name}")
    
    entities = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=1):
            # Enrich with file metadata
            enriched = {
                **row,
                '_source_file': file_path.name,
                '_row_number': row_num,
                '_ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
            }
            
            # Calculate hash
            content_hash = self._calculate_content_hash(row)
            
            entities.append({
                'entity_type': 'lab_award',
                'source_system': 'umich_orsp',
                'external_id': row.get('Award Id'),
                'raw_data': enriched,
                'entity_hash': content_hash,
                'ingestion_run_id': self.run_id,
            })
    
    # Bulk insert
    inserted = self._bulk_insert_entities(entities)
    
    logger.info(f"✅ Ingested {inserted} records from {file_path.name}")
    return {'records_ingested': inserted}
```

### Pattern 3: LDAP/Active Directory Ingestion

```python
def ingest_ldap_users(self, last_sync: Optional[datetime] = None):
    """
    Ingest users from LDAP with timestamp-based change detection.
    
    Args:
        last_sync: Only fetch records modified after this timestamp
    """
    import ldap
    
    # Build filter
    base_filter = "(objectClass=user)"
    
    if last_sync:
        timestamp_str = last_sync.strftime('%Y%m%d%H%M%S.0Z')
        search_filter = f"(&{base_filter}(modifyTimestamp>={timestamp_str}))"
        logger.info(f"📥 Fetching users modified since {last_sync}")
    else:
        search_filter = base_filter
        logger.info("📥 Fetching all users (full sync)")
    
    # Query LDAP
    results = self.ldap_conn.search_s(
        base_dn='OU=Users,DC=umich,DC=edu',
        scope=ldap.SCOPE_SUBTREE,
        filterstr=search_filter,
        attrlist=['*']
    )
    
    logger.info(f"   Found {len(results)} users")
    
    # Convert LDAP entries to dicts and insert
    entities = []
    for dn, entry in results:
        # Convert LDAP entry to dict
        user_data = self._ldap_entry_to_dict(entry)
        user_data['distinguishedName'] = dn
        
        entities.append({
            'entity_type': 'user',
            'source_system': 'mcommunity_ldap',
            'external_id': dn,
            'raw_data': user_data,
            'entity_hash': None,  # Not needed with timestamp-based
            'ingestion_run_id': self.run_id,
        })
    
    # Bulk insert
    self._bulk_insert_entities(entities)
```

---

## Troubleshooting

### Issue: Duplicate Records

**Symptom:** Same record appears multiple times with different raw_id

**Cause:** Change detection not working (hash changes on every run)

**Diagnosis:**
```sql
-- Check for duplicates
SELECT 
    external_id,
    COUNT(*) as version_count
FROM bronze.raw_entities
WHERE entity_type = 'department'
  AND source_system = 'umapi'
GROUP BY external_id
HAVING COUNT(*) > 10  -- Suspiciously many versions
ORDER BY version_count DESC;
```

**Fix:** Exclude volatile fields from content hash:

```python
# ❌ Bad: Includes timestamp that always changes
def _calculate_content_hash(self, data):
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

# ✅ Good: Excludes metadata fields
def _calculate_content_hash(self, data):
    significant_fields = {
        k: v for k, v in data.items()
        if k not in ['uSNChanged', 'modifyTimestamp', 'whenChanged', '_sync_date']
    }
    return hashlib.sha256(
        json.dumps(significant_fields, sort_keys=True).encode()
    ).hexdigest()
```

### Issue: Missing Records

**Symptom:** Expected records not in bronze layer

**Cause:** API pagination not handled correctly

**Diagnosis:**

```python
# Add pagination logging
def _fetch_all_pages(self, endpoint):
    all_records = []
    page = 1
    
    while True:
        response = self.api.get(endpoint, params={'page': page, 'size': 1000})
        records = response.json()['data']
        
        # Log each page
        logger.info(f"📥 Page {page}: {len(records)} records")
        
        if not records:
            break
        
        all_records.extend(records)
        page += 1
    
    # Log total
    logger.info(f"📊 Total records fetched: {len(all_records)}")
    logger.info(f"📊 Total pages: {page - 1}")
    
    return all_records
```

**Common pagination issues:**
- Not checking for `next` field in API response
- Assuming fixed number of pages
- Page size too large (API limits)

### Issue: Slow Ingestion

**Symptom:** Ingestion takes hours for thousands of records

**Cause:** Individual inserts instead of batching

**Diagnosis:**

```python
import time

# Time individual inserts
start = time.time()
for entity in entities[:100]:  # Test with 100 records
    cursor.execute("INSERT INTO bronze.raw_entities (...) VALUES (...)", (...))
    conn.commit()
elapsed = time.time() - start
logger.info(f"100 records took {elapsed:.2f} seconds")
logger.info(f"Projected time for {len(entities)} records: {elapsed * len(entities) / 100:.2f} seconds")
```

**Fix:** Use bulk insert with execute_values (see Performance Optimization section)

### Issue: Memory Overflow

**Symptom:** Python process crashes with MemoryError

**Cause:** Loading entire dataset into memory before inserting

**Fix:** Process in batches

```python
# ✅ Good: Process in batches
def ingest_large_dataset(self):
    batch_size = 1000
    entities_batch = []
    
    for entity in self.api.fetch_streaming():  # Generator, not list
        entities_batch.append(self._prepare_entity(entity))
        
        if len(entities_batch) >= batch_size:
            self._bulk_insert_entities(entities_batch)
            logger.info(f"✅ Inserted batch of {len(entities_batch)}")
            entities_batch = []  # Clear batch
    
    # Insert remaining
    if entities_batch:
        self._bulk_insert_entities(entities_batch)
```

### Issue: JSONB Encoding Errors

**Symptom:** `TypeError: Object of type datetime is not JSON serializable`

**Cause:** JSONB column contains Python objects that can't be serialized

**Fix:** Convert objects before JSON encoding

```python
def _prepare_for_jsonb(self, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Python objects to JSON-serializable types.
    
    Args:
        data: Dictionary with mixed types
        
    Returns:
        JSON-serializable dictionary
    """
    from datetime import datetime, date
    from decimal import Decimal
    
    def convert_value(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        elif isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [convert_value(item) for item in value]
        else:
            return value
    
    return {k: convert_value(v) for k, v in data.items()}
```

---

## Summary

**Bronze layer ingestion follows these principles:**

1. **Append-only storage** in single universal `bronze.raw_entities` table
2. **Change detection** via content hashing or timestamps
3. **Metadata enrichment** with `_prefixed` computed fields
4. **Bulk operations** for performance (execute_values)
5. **Service class pattern** with standard structure
6. **Ingestion run tracking** in `meta.ingestion_runs`
7. **Comprehensive logging** with emoji indicators

**Key takeaways:**
- Never transform data in bronze (except metadata enrichment)
- Use content hashing when source has no timestamps
- Use timestamp-based when available (faster)
- Batch inserts for performance (1000x speedup)
- Log comprehensive statistics
- Handle errors gracefully

**Next steps:**
- See [silver_layer_standards.md](silver_layer_standards.md) for transformation patterns
- See [medallion_standards.md](medallion_standards.md) for cross-layer standards
