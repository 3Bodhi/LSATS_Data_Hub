# TeamDynamix Lab CI Management Scripts

This directory contains scripts for managing TeamDynamix Lab Configuration Items (CIs) and their relationships.

## Script Overview

### 001_create_lab_cis.py
**Purpose**: Create TeamDynamix Configuration Items for research labs

**What it does**:
- Reads lab data from `silver.v_labs_monitored`, `silver.v_lab_managers_tdx_reference`, and `silver.v_lab_locations_tdx_reference`
- Creates TDX Lab CIs with proper attributes (managers, locations, ownership)
- Can update existing CIs with `--full-sync` flag

**Usage**:
```bash
# Dry run for all labs
python scripts/tdx/001_create_lab_cis.py --dry-run

# Create CI for specific lab
python scripts/tdx/001_create_lab_cis.py --lab-id aabol --no-dry-run

# Update existing CIs
python scripts/tdx/001_create_lab_cis.py --full-sync --no-dry-run
```

**Key fields set**:
- `FormID`: 3830 (Research Lab form)
- `TypeID`: 10132 (Lab type)
- `OwnerUID`: PI's TDX UID
- `OwningDepartmentID`: Lab's primary department TDX ID
- `LocationID` / `LocationRoomID`: Primary location
- Attributes: Lab managers (up to 3), secondary locations (up to 2)

---

### 002_sync_tdx_lab_ci_ids.py
**Purpose**: Write TDX CI IDs back to the database

**What it does**:
- Fetches all Lab CIs from TeamDynamix
- Extracts lab_id from CI name (format: "{lab_id} Lab")
- Matches CIs to labs in `silver.labs`
- Writes `tdx_ci_id` back to database
- Tracks sync in `meta.ingestion_runs`

**Usage**:
```bash
# Dry run
python scripts/tdx/002_sync_tdx_lab_ci_ids.py --dry-run

# Live sync
python scripts/tdx/002_sync_tdx_lab_ci_ids.py --no-dry-run
```

**Database changes**:
- Updates `silver.labs.tdx_ci_id` column
- Creates ingestion run record in `meta.ingestion_runs`

---

### 003_sync_lab_computers.py
**Purpose**: Add computer relationships to Lab CIs

**What it does**:
- Reads labs with CI IDs from `silver.v_labs_monitored`
- Reads lab computers from `silver.v_lab_computers_tdx_reference`
- Uses bulk API to add "Located in" relationships
- Skips existing relationships to avoid duplicates
- Processes in batches for performance

**Usage**:
```bash
# Dry run for all labs
python scripts/tdx/003_sync_lab_computers.py --dry-run

# Sync specific lab
python scripts/tdx/003_sync_lab_computers.py --lab-id aabol --no-dry-run

# Live sync with custom batch size
python scripts/tdx/003_sync_lab_computers.py --no-dry-run --batch-size 50
```

**Relationship structure**:
- `ParentItemID`: Lab CI ID (from `silver.v_labs_monitored.tdx_ci_id`)
- `ChildItemID`: Computer CI ID (from `silver.v_lab_computers_tdx_reference.tdx_configuration_item_id`)
- `RelationshipTypeID`: 10016 ("Place" / "Located in")

**Performance**:
- Default batch size: 100 relationships per API call
- Checks existing relationships before adding (no duplicates)
- Logs progress with batch numbers

---

## Execution Order

To set up lab CIs from scratch:

1. **Create Lab CIs**
   ```bash
   python scripts/tdx/001_create_lab_cis.py --no-dry-run
   ```

2. **Sync CI IDs back to database**
   ```bash
   python scripts/tdx/002_sync_tdx_lab_ci_ids.py --no-dry-run
   ```

3. **Add computer relationships**
   ```bash
   python scripts/tdx/003_sync_lab_computers.py --no-dry-run
   ```

## Database Views Used

### Input Views (Read from)
- **`silver.v_labs_monitored`**: Labs eligible for TDX CI creation
  - Must have: `ad_ou_dn`, `primary_department_id`, `computer_count > 0`
  - Now includes: `tdx_ci_id` (populated by script 002)

- **`silver.v_lab_managers_tdx_reference`**: Lab manager TDX UIDs
  - Fields: `lab_id`, `pi_tdx_uid`, `manager_tdx_uid`, `lab_department_tdx_id`

- **`silver.v_lab_locations_tdx_reference`**: Lab computer locations
  - Fields: `lab_id`, `room_id`, `location_id`, `location_description`
  - Sorted by computer count (descending)

- **`silver.v_lab_computers_tdx_reference`**: Lab computers with TDX IDs
  - Fields: `lab_id`, `computer_id`, `tdx_configuration_item_id`
  - Filters: `confidence_score >= 0.65`

### Output Tables (Write to)
- **`silver.labs.tdx_ci_id`**: TDX CI ID for each lab (script 002)
- **`meta.ingestion_runs`**: Sync tracking and audit trail

## API Enhancements

### ConfigurationItemAPI.bulk_add_relationships()
Added to `teamdynamix/api/configuration_item_api.py`:

```python
def bulk_add_relationships(
    self,
    relationships: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Adds multiple relationships in bulk using the BulkAdd endpoint.
    
    Args:
        relationships: List of relationship mappings with:
            - ParentItemID: int
            - ChildItemID: int
            - RelationshipTypeID: int
    
    Returns:
        Dictionary with AddedCount, NotAddedCount, and ErrorMessages.
    """
```

**Benefits**:
- Reduces API calls by 100x (batch of 100 vs 100 individual calls)
- Faster execution
- Better error handling with bulk responses

## Environment Variables

Required in `.env`:

```bash
# Database
DATABASE_URL=postgresql://lsats_user:password@localhost:5432/lsats_db

# TeamDynamix
TDX_BASE_URL=https://teamdynamix.umich.edu/TDWebApi
TDX_API_TOKEN=your_token_here
```

## Common Options

All scripts support:
- `--dry-run` (default): Preview actions without making changes
- `--no-dry-run`: Execute actual API calls
- `--verbose`: Enable debug logging
- `--lab-id <id>`: Process a specific lab only

Script 003 additionally supports:
- `--batch-size <n>`: Number of relationships per batch (default: 100)

## Troubleshooting

### "No labs found with TDX CI IDs"
**Solution**: Run script 002 first to sync CI IDs back to the database

### "No computers to sync for lab X"
**Possible causes**:
- Lab has no computers in `silver.lab_computers`
- Computers don't have TDX asset IDs
- Confidence score < 0.65 (filtered out)

### Relationship already exists errors
**Normal behavior**: Script checks existing relationships and skips them automatically

### Rate limiting
**Solution**: Reduce `--batch-size` (default: 100)

## Monitoring

Check ingestion runs:
```sql
SELECT * FROM meta.ingestion_runs 
WHERE entity_type IN ('lab_cis', 'lab_computer_relationships')
ORDER BY started_at DESC;
```

Check lab CI status:
```sql
SELECT 
    COUNT(*) as total_labs,
    COUNT(tdx_ci_id) as labs_with_ci_ids,
    COUNT(*) - COUNT(tdx_ci_id) as labs_without_ci_ids
FROM silver.v_labs_monitored;
```

Check computer relationships:
```sql
-- Get relationship count per lab
SELECT 
    lm.lab_id,
    COUNT(lc.computer_id) as computers_in_db,
    (SELECT COUNT(*) FROM <tdx_relationships_api>) as relationships_in_tdx
FROM silver.v_labs_monitored lm
LEFT JOIN silver.v_lab_computers_tdx_reference lc ON lm.lab_id = lc.lab_id
WHERE lm.tdx_ci_id IS NOT NULL
GROUP BY lm.lab_id
ORDER BY lm.lab_id;
```

## Next Steps

Future enhancements could include:
- Remove relationships for computers no longer in labs
- Update CI attributes when lab data changes
- Sync other relationship types (tickets, assets)
- Scheduled runs via cron/scheduler
