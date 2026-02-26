# Database Script Patterns and Standards

This document defines the required patterns and standards for all database scripts in the LSATS Data Hub medallion architecture.

## Script Categories

1. **Ingest Scripts** (`ingest_*.py`): Load raw data from sources → bronze layer
   - Examples: `ingest_umapi_departments.py`, `ingest_mcommunity_users.py`, `ingest_ad_groups.py`
   - Purpose: Fetch data from external APIs/LDAP and store in `bronze.raw_entities`
   
2. **Enrich Scripts** (`enrich_*.py`): Progressive enrichment of bronze records
   - Examples: `enrich_tdx_accounts.py` (adds complete data to basic records)
   - Purpose: Fill in missing fields by making additional API calls for complete data
   
3. **Transform Scripts** (`transform_*.py`): Bronze → silver transformations
   - Examples: `transform_silver_departments.py`, `transform_silver_users_optimized.py`
   - Purpose: Merge data from multiple bronze sources into unified silver records

## Standard Script Structure

All scripts follow this pattern:

```python
#!/usr/bin/env python3
"""
Script Description

Clear explanation of:
- What data source(s) it ingests/transforms
- Key features (change detection method, merging strategy, etc.)
- Any important caveats or requirements
"""

import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

# Standard imports
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# LSATS imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from database.adapters.postgres_adapter import PostgresAdapter
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/script_name.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataService:
    """
    Service class with clear docstring explaining purpose.
    
    All services follow this structure:
    - __init__: Initialize adapters and connections
    - _private_methods: Internal logic (prefixed with _)
    - public_methods: Main entry points
    - create_run / complete_run: Tracking methods
    """
    
    def __init__(self, database_url: str, **kwargs):
        """Initialize with typed parameters."""
        self.db_adapter = PostgresAdapter(database_url=database_url)
        logger.info("Service initialized")
    
    def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 hash for change detection.
        
        Pattern: Extract significant fields → normalize → JSON → hash
        """
        significant_fields = {...}  # Only fields that matter for changes
        normalized = json.dumps(significant_fields, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create tracking record in meta.ingestion_runs."""
        run_id = str(uuid.uuid4())
        metadata = {'key': 'value'}  # Run-specific config
        
        with self.db_adapter.engine.connect() as conn:
            insert_query = text("""
                INSERT INTO meta.ingestion_runs (
                    run_id, source_system, entity_type, started_at, status, metadata
                ) VALUES (
                    :run_id, :source_system, :entity_type, :started_at, 'running', :metadata
                )
            """)
            conn.execute(insert_query, {
                'run_id': run_id,
                'source_system': source_system,
                'entity_type': entity_type,
                'started_at': datetime.now(timezone.utc),
                'metadata': json.dumps(metadata)
            })
            conn.commit()
        
        return run_id
    
    def complete_ingestion_run(self, run_id: str, records_processed: int,
                             records_created: int, records_updated: int = 0,
                             error_message: Optional[str] = None):
        """Update run record with results."""
        status = 'failed' if error_message else 'completed'
        
        with self.db_adapter.engine.connect() as conn:
            update_query = text("""
                UPDATE meta.ingestion_runs
                SET completed_at = :completed_at,
                    status = :status,
                    records_processed = :records_processed,
                    records_created = :records_created,
                    records_updated = :records_updated,
                    error_message = :error_message
                WHERE run_id = :run_id
            """)
            conn.execute(update_query, {
                'run_id': run_id,
                'completed_at': datetime.now(timezone.utc),
                'status': status,
                'records_processed': records_processed,
                'records_created': records_created,
                'records_updated': records_updated,
                'error_message': error_message
            })
            conn.commit()
    
    def run_main_process(self) -> Dict[str, Any]:
        """
        Main entry point - always returns Dict with statistics.
        
        Standard pattern:
        1. Create ingestion run
        2. Initialize stats dict
        3. Process records (with progress logging)
        4. Complete ingestion run
        5. Return stats
        """
        run_id = self.create_ingestion_run('source', 'entity')
        
        stats = {
            'run_id': run_id,
            'records_processed': 0,
            'records_created': 0,
            'records_updated': 0,
            'errors': [],
            'started_at': datetime.now(timezone.utc)
        }
        
        try:
            # Main processing logic here
            # Log progress every N records (see Logging Standards below)
            
            # Complete with success
            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=stats['records_processed'],
                records_created=stats['records_created'],
                records_updated=stats['records_updated']
            )
            
            stats['completed_at'] = datetime.now(timezone.utc)
            return stats
            
        except Exception as e:
            # Complete with failure
            error_msg = f"Processing failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=stats['records_processed'],
                records_created=stats['records_created'],
                records_updated=stats['records_updated'],
                error_message=error_msg
            )
            raise
    
    def close(self):
        """Clean up connections."""
        if self.db_adapter:
            self.db_adapter.close()


def main():
    """
    Command-line entry point.
    
    Standard pattern:
    1. Load .env
    2. Validate required config
    3. Initialize service
    4. Run process
    5. Display results
    6. Clean up
    """
    try:
        load_dotenv()
        
        # Get and validate config
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL required")
        
        # Initialize and run
        service = DataService(database_url)
        results = service.run_main_process()
        
        # Display results with emoji indicators
        print(f"\n📊 Results Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Records Processed: {results['records_processed']}")
        print(f"   Records Created: {results['records_created']}")
        print(f"   Records Updated: {results['records_updated']}")
        print(f"   Errors: {len(results['errors'])}")
        
        # Show duration
        duration = (results['completed_at'] - results['started_at']).total_seconds()
        print(f"   Duration: {duration:.2f} seconds")
        
        # Clean up
        service.close()
        print("✅ Completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        print(f"❌ Failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
```

## Change Detection Patterns

### 1. Content Hashing (for sources without timestamps)

Use for sources that don't provide reliable modification timestamps (UMich API, MCommunity LDAP):

```python
def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
    """
    Calculate SHA-256 hash for change detection.
    
    Only include fields that represent meaningful changes.
    Exclude volatile fields like access timestamps, session IDs, etc.
    
    Args:
        data: Raw data from source system
    
    Returns:
        SHA-256 hash string
    """
    # Extract ONLY significant fields (those that matter for changes)
    significant_fields = {
        'id': data.get('id'),
        'name': data.get('name'),
        'email': data.get('email'),
        # Include stable fields only
        # EXCLUDE: lastAccessedAt, sessionToken, etc.
    }
    
    # Normalize and hash
    normalized = json.dumps(significant_fields, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def _get_existing_hashes(self) -> Dict[str, str]:
    """
    Retrieve latest content hash for each entity from bronze layer.
    
    Uses window function to get only most recent record per entity.
    """
    query = """
    WITH latest_records AS (
        SELECT
            external_id,
            raw_data,
            ROW_NUMBER() OVER (
                PARTITION BY external_id
                ORDER BY ingested_at DESC
            ) as row_num
        FROM bronze.raw_entities
        WHERE entity_type = :entity_type
        AND source_system = :source_system
    )
    SELECT external_id, raw_data
    FROM latest_records
    WHERE row_num = 1
    """
    
    results_df = self.db_adapter.query_to_dataframe(query, {
        'entity_type': 'user',
        'source_system': 'mcommunity_ldap'
    })
    
    # Calculate hashes for existing records
    existing_hashes = {}
    for _, row in results_df.iterrows():
        entity_id = row['external_id']
        content_hash = self._calculate_content_hash(row['raw_data'])
        existing_hashes[entity_id] = content_hash
    
    return existing_hashes

# In main processing loop:
existing_hashes = self._get_existing_hashes()

for record in source_records:
    external_id = record['id']
    current_hash = self._calculate_content_hash(record)
    
    if external_id not in existing_hashes:
        # New record
        logger.info(f"🆕 New record: {external_id}")
        self.db_adapter.insert_raw_entity(...)
        stats['records_created'] += 1
    elif existing_hashes[external_id] != current_hash:
        # Changed record
        logger.info(f"📝 Record changed: {external_id}")
        self.db_adapter.insert_raw_entity(...)
        stats['records_created'] += 1
    else:
        # Unchanged - skip
        logger.debug(f"⏭️  Record unchanged: {external_id}")
        stats['records_skipped'] += 1
```

### 2. Timestamp-Based (for sources with ModifiedDate)

Use for sources that provide modification timestamps (TeamDynamix):

```python
def _get_last_sync_timestamp(self) -> Optional[datetime]:
    """
    Get timestamp of last successful ingestion.
    
    Returns None for first run (full sync).
    """
    query = """
    SELECT MAX(completed_at) as last_sync
    FROM meta.ingestion_runs
    WHERE source_system = :source_system
    AND entity_type = :entity_type
    AND status = 'completed'
    """
    
    result_df = self.db_adapter.query_to_dataframe(query, {
        'source_system': 'tdx',
        'entity_type': 'user'
    })
    
    if not result_df.empty and result_df.iloc[0]['last_sync'] is not None:
        return result_df.iloc[0]['last_sync']
    return None

# In main processing:
last_sync = self._get_last_sync_timestamp()

if last_sync:
    # Incremental sync - only get changed records
    logger.info(f"Incremental sync since {last_sync}")
    query_params = {'ModifiedDateFrom': last_sync.isoformat()}
else:
    # Full sync - get everything
    logger.info("Full sync (first run)")
    query_params = {}

records = self.api.get_records(**query_params)
```

## Incremental Processing Pattern

All transform scripts must support incremental updates:

```python
def _get_last_transformation_timestamp(self) -> Optional[datetime]:
    """
    Get timestamp of last successful silver transformation.
    
    Enables incremental processing - only transform entities with new bronze data.
    
    Returns:
        datetime of last transformation, or None for first run
    """
    query = """
    SELECT MAX(completed_at) as last_completed
    FROM meta.ingestion_runs
    WHERE source_system = 'silver_transformation'
    AND entity_type = :entity_type
    AND status = 'completed'
    """
    
    result_df = self.db_adapter.query_to_dataframe(query, {
        'entity_type': 'user'
    })
    
    if not result_df.empty and result_df.iloc[0]['last_completed'] is not None:
        last_timestamp = result_df.iloc[0]['last_completed']
        logger.info(f"Last successful transformation: {last_timestamp}")
        return last_timestamp
    
    logger.info("No previous transformation found - processing all records")
    return None

def _get_entities_needing_transformation(
    self, since_timestamp: Optional[datetime] = None
) -> Set[str]:
    """
    Find entity IDs that have new/updated bronze records.
    
    Args:
        since_timestamp: Only include entities with bronze records after this time
    
    Returns:
        Set of entity IDs to process
    """
    time_filter = ""
    params = {}
    
    if since_timestamp:
        time_filter = "AND ingested_at > :since_timestamp"
        params['since_timestamp'] = since_timestamp
    
    query = f"""
    SELECT DISTINCT external_id
    FROM bronze.raw_entities
    WHERE entity_type = :entity_type
    AND source_system IN :source_systems
    {time_filter}
    """
    
    result_df = self.db_adapter.query_to_dataframe(query, {
        'entity_type': 'user',
        'source_systems': tuple(['tdx', 'umich_api', 'mcommunity_ldap']),
        **params
    })
    
    entity_ids = set(result_df['external_id'].tolist())
    logger.info(f"Found {len(entity_ids)} entities needing transformation")
    
    return entity_ids

# In main transformation:
last_transformation = self._get_last_transformation_timestamp()
entity_ids = self._get_entities_needing_transformation(last_transformation)

for entity_id in entity_ids:
    # Transform this entity
    pass
```

## Data Quality Scoring Pattern

All silver transformations must calculate quality scores:

```python
def _calculate_data_quality(
    self,
    silver_record: Dict[str, Any],
    source_records: Dict[str, Optional[Dict]]
) -> Tuple[float, List[str]]:
    """
    Calculate data quality score and identify quality flags.
    
    Scoring criteria:
    - Start at 1.0 (perfect quality)
    - Deduct 0.1 if missing a source
    - Deduct 0.05 per missing key field
    - Deduct 0.05 for data mismatches between sources
    - Add flags but don't always deduct for minor issues
    
    Args:
        silver_record: The merged silver record
        source_records: Dict of source_name -> raw_data (may be None if source missing)
    
    Returns:
        Tuple of (quality_score, quality_flags_list)
        - quality_score: 0.0 to 1.0
        - quality_flags: List of strings describing issues
    """
    score = 1.0
    flags = []
    
    # Check for missing sources
    for source_name, source_data in source_records.items():
        if not source_data:
            score -= 0.1
            flags.append(f'missing_{source_name}_source')
    
    # Check for missing key fields
    required_fields = ['name', 'email', 'department_id']
    for field in required_fields:
        if not silver_record.get(field):
            score -= 0.05
            flags.append(f'missing_{field}')
    
    # Check for data mismatches between sources
    if source_records.get('tdx') and source_records.get('umich_api'):
        tdx_email = source_records['tdx'].get('email', '').lower()
        api_email = source_records['umich_api'].get('email', '').lower()
        
        if tdx_email and api_email and tdx_email != api_email:
            score -= 0.05
            flags.append('email_mismatch')
    
    # Add informational flags (don't deduct points)
    if not silver_record.get('phone_number'):
        flags.append('no_phone_number')
    
    # Ensure score doesn't go below 0
    score = max(0.0, score)
    
    return round(score, 2), flags

# In transformation:
silver_record = self._merge_sources(entity_id, source_records)
quality_score, quality_flags = self._calculate_data_quality(
    silver_record, 
    source_records
)

silver_record['data_quality_score'] = quality_score
silver_record['quality_flags'] = quality_flags

# Track quality issues
if quality_flags:
    stats['quality_issues'].append({
        'entity_id': entity_id,
        'score': quality_score,
        'flags': quality_flags
    })
```

## Logging Standards

### Progress Logging

```python
# Log progress every 10-50 records (depending on total volume)
if stats['records_processed'] % 50 == 0 and stats['records_processed'] > 0:
    logger.info(
        f"Progress: {stats['records_processed']}/{total_records} processed "
        f"({stats['records_created']} created, {stats['records_skipped']} skipped)"
    )
```

### Emoji Indicators

Use emojis for visual scanning of logs:

```python
# New records
logger.info(f"🆕 New record detected: {entity_id}")

# Changed records
logger.info(f"📝 Record changed: {entity_id}")

# Skipped (unchanged)
logger.debug(f"⏭️  Record unchanged, skipping: {entity_id}")

# Success
logger.info(f"✅ Successfully processed: {entity_id}")

# Failures
logger.error(f"❌ Failed to process: {entity_id}")

# Warnings
logger.warning(f"⚠️  Quality issue detected: {entity_id}")

# Stats summary
logger.info("📊 Results Summary:")
logger.info("🔄 Starting processing...")
logger.info("🎉 Completed successfully!")
```

### Final Summary

Always log a comprehensive summary at the end:

```python
# Log final results
logger.info(f"{'='*60}")
logger.info(f"📊 FINAL RESULTS SUMMARY")
logger.info(f"{'='*60}")
logger.info(f"Duration: {duration:.2f} seconds")
logger.info(f"Total Processed: {stats['records_processed']}")
logger.info(f"├─ New Records: {stats['records_created']}")
logger.info(f"├─ Updated Records: {stats['records_updated']}")
logger.info(f"└─ Skipped (Unchanged): {stats['records_skipped']}")
logger.info(f"Quality Issues: {len(stats['quality_issues'])}")
logger.info(f"Errors: {len(stats['errors'])}")
logger.info(f"{'='*60}")
```

## Error Handling Pattern

### Individual Record Errors

Individual record failures should NOT stop processing:

```python
for record in records:
    try:
        # Process record
        result = self._process_record(record)
        stats['records_processed'] += 1
        stats['records_created'] += 1
        
    except Exception as record_error:
        # Log error but continue
        error_msg = f"Failed to process {record.get('id', 'unknown')}: {record_error}"
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        stats['records_processed'] += 1  # Still count as processed
        # Continue to next record
```

### Fatal Service Errors

Fatal errors should be caught at the service level:

```python
def run_main_process(self) -> Dict[str, Any]:
    run_id = self.create_ingestion_run('source', 'entity')
    
    stats = {...}
    
    try:
        # Main processing logic
        for record in self.fetch_records():
            # Process with individual error handling
            pass
        
        # Mark as completed
        self.complete_ingestion_run(
            run_id=run_id,
            records_processed=stats['records_processed'],
            records_created=stats['records_created']
        )
        
        return stats
        
    except Exception as e:
        # Handle fatal errors
        error_msg = f"Service failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # Mark run as failed
        self.complete_ingestion_run(
            run_id=run_id,
            records_processed=stats['records_processed'],
            records_created=stats['records_created'],
            error_message=error_msg
        )
        
        # Re-raise to stop execution
        raise
```

## Environment Configuration

All scripts must load configuration from `.env`:

```python
# At top of main()
load_dotenv()

# Required variables
database_url = os.getenv('DATABASE_URL')
if not database_url:
    raise ValueError("DATABASE_URL environment variable is required")

# Optional variables with defaults
max_records = int(os.getenv('MAX_RECORDS', '0'))  # 0 = unlimited
batch_size = int(os.getenv('BATCH_SIZE', '1000'))

# Validate required variables
required_vars = {
    'DATABASE_URL': database_url,
    'TDX_API_TOKEN': os.getenv('TDX_API_TOKEN'),
    # ... more as needed
}

missing_vars = [name for name, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {missing_vars}")
```

## Performance Optimization

### Batch Processing

Use batch inserts for large datasets:

```python
# Accumulate records for batch insert
batch = []
batch_size = 1000

for record in source_records:
    entity = {
        'entity_type': 'user',
        'source_system': 'tdx',
        'external_id': record['id'],
        'raw_data': record,
        'ingestion_run_id': run_id
    }
    batch.append(entity)
    
    # Insert when batch is full
    if len(batch) >= batch_size:
        count = self.db_adapter.bulk_insert_raw_entities(batch)
        stats['records_created'] += count
        batch = []

# Insert remaining records
if batch:
    count = self.db_adapter.bulk_insert_raw_entities(batch)
    stats['records_created'] += count
```

### Connection Pooling

Always initialize PostgresAdapter with connection pooling:

```python
self.db_adapter = PostgresAdapter(
    database_url=database_url,
    pool_size=5,        # Connections to keep in pool
    max_overflow=10     # Additional connections if needed
)
```

### Query Optimization

Use indexes and avoid N+1 queries:

```python
# BAD: N+1 query pattern
for user_id in user_ids:
    user = self.db_adapter.query_to_dataframe(
        "SELECT * FROM silver.users WHERE uniqname = :id",
        {'id': user_id}
    )

# GOOD: Single query with IN clause
users_df = self.db_adapter.query_to_dataframe(
    "SELECT * FROM silver.users WHERE uniqname = ANY(:ids)",
    {'ids': list(user_ids)}
)
```

## Testing and Validation

### Dry Run Support (Optional)

Consider adding dry-run mode for transform scripts:

```python
def __init__(self, database_url: str, dry_run: bool = False):
    self.db_adapter = PostgresAdapter(database_url=database_url)
    self.dry_run = dry_run
    
    if dry_run:
        logger.info("🧪 DRY RUN MODE - No database changes will be made")

def _upsert_silver_record(self, record: Dict):
    if self.dry_run:
        logger.info(f"[DRY RUN] Would upsert: {record['id']}")
        return
    
    # Actual database operation
    with self.db_adapter.engine.connect() as conn:
        # ... upsert logic
        pass
```

### Validation Checks

Add validation for transformed data:

```python
def _validate_silver_record(self, record: Dict) -> List[str]:
    """
    Validate silver record before insertion.
    
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check required fields
    if not record.get('primary_key'):
        errors.append("Missing primary key")
    
    # Check data types
    if record.get('email') and '@' not in record['email']:
        errors.append("Invalid email format")
    
    # Check foreign keys
    if record.get('department_id'):
        exists = self._department_exists(record['department_id'])
        if not exists:
            errors.append(f"Department {record['department_id']} not found")
    
    return errors

# In transformation:
validation_errors = self._validate_silver_record(silver_record)
if validation_errors:
    logger.error(f"Validation failed for {entity_id}: {validation_errors}")
    stats['errors'].append(f"{entity_id}: {', '.join(validation_errors)}")
    continue
```

## Summary Checklist

When creating a new database script, ensure it includes:

- [ ] Comprehensive docstring explaining purpose and features
- [ ] Standard imports and logging setup
- [ ] Service class with typed parameters
- [ ] `create_ingestion_run()` method
- [ ] `complete_ingestion_run()` method  
- [ ] Main processing method returning stats Dict
- [ ] Change detection (content hashing or timestamp-based)
- [ ] Incremental processing support (for transforms)
- [ ] Data quality scoring (for transforms)
- [ ] Progress logging every 10-50 records
- [ ] Emoji indicators for visual log scanning
- [ ] Individual record error handling (continue processing)
- [ ] Fatal error handling (mark run failed, raise)
- [ ] Final comprehensive summary logging
- [ ] `close()` method for cleanup
- [ ] `main()` function with .env loading
- [ ] Environment variable validation
- [ ] Results display with emoji indicators
- [ ] Exit code 1 on failure
