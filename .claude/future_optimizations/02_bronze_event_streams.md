# 2. Bronze Reconceptualization — Raw Event Streams, Not Entity Snapshots

**Affects:** All bronze ingestion scripts, `scripts/database/bronze/tdx/010_enrich_tdx_users.py`, `scripts/database/silver/001_transform_tdx_users.py`

## The Problem

The current bronze layer models **entity state** (a user record as it exists right now) rather than **API interaction events** (what a specific endpoint returned at a specific moment). This creates two structural tensions:

1. **The enrichment ambiguity**: `010_enrich_tdx_users.py` exists because the TDX search endpoint (`GET /api/users?search=...`) returns basic data, while the detail endpoint (`GET /api/users/{uid}`) returns comprehensive data (`OrgApplications`, `Attributes`, `Permissions`, `GroupIDs`). Currently, enrichment creates a new bronze record that merges or supersedes the search record. This implies bronze records are mutable/completable — which conflicts with the append-only philosophy.

2. **Meta fields bleeding into `raw_data`**: Some scripts inject fields like `_enriched_at`, `_source_script`, etc. directly into `raw_data`. This means `raw_data` no longer represents exactly what the source API returned. If you ever want to replay or verify a response, you must strip your own injected keys first.

## The Preferred Model — Separate Event Streams

**Bronze should be an immutable log of upstream API interactions.** Each record represents one API call, not one entity.

```
entity_type = 'tdx_user_search'   → raw response from GET /api/users?search=...
entity_type = 'tdx_user_detail'   → raw response from GET /api/users/{uid}
```

No merging. No enrichment mutation. No "needs enrichment" detection in bronze.

**Silver becomes the assembly layer** where event streams are joined into a canonical entity view.

## What Changes

### Bronze Layer

| Current | Under stream model |
|---|---|
| `entity_type='user'`, `source_system='tdx'` (mixed search + detail) | `entity_type='tdx_user_search'` for search responses |
| `010_enrich_tdx_users.py` — "enrich" existing records | `010_ingest_tdx_user_details.py` — ingest a second stream |
| Hash-driven skip in enrich script (complex staleness logic) | Hash-driven skip in detail ingestion (same logic, cleaner intent) |
| `_enriched_at` injected into `raw_data` | All meta moved to `ingestion_metadata` column (already exists in schema) |

The staleness/skip logic in enrichment does **not** disappear — it moves to the detail ingestion script. The question "has this uid been detail-fetched recently?" is answered by querying `entity_type='tdx_user_detail'` records, not by checking for enrichment flags in `raw_data`.

### `raw_data` Cleanup

Under this model, `raw_data` must be byte-for-byte what the API returned. All provenance fields move to `ingestion_metadata` (already a first-class JSONB column in `bronze.raw_entities`):

```python
# ❌ Current: meta injected into source data
raw_data = {
    "UID": "abc123",
    "FirstName": "Jane",
    "_enriched_at": "2026-03-01T02:00:00Z",  # ← not from API
    "_source_script": "010_enrich_tdx_users.py",  # ← not from API
}

# ✅ Target: raw_data is pure API response
raw_data = {"UID": "abc123", "FirstName": "Jane", ...}  # exactly what TDX returned

ingestion_metadata = {
    "source_script": "010_ingest_tdx_user_details.py",
    "api_endpoint": "/api/users/{uid}",
    "fetched_at": "2026-03-01T02:00:00Z",
}
```

### Silver Transform — `scripts/database/silver/001_transform_tdx_users.py`

This is the most significant change. Currently the transform assumes one bronze record per user that already contains both search and detail fields. Under the stream model, it must JOIN two streams.

**`_get_users_needing_transformation`** — currently queries:
```sql
WHERE entity_type = 'user' AND source_system = 'tdx'
```
Becomes: union of UIDs with new `tdx_user_search` OR `tdx_user_detail` records since last run.

**`_fetch_latest_bronze_records_batch`** — currently fetches one record per UID. Becomes: fetch latest `tdx_user_search` + latest `tdx_user_detail` per UID in a single query using conditional aggregation or two CTEs:

```sql
WITH latest_search AS (
    SELECT
        raw_data->>'UID' AS tdx_user_uid,
        raw_data AS search_data,
        raw_id AS search_raw_id,
        ROW_NUMBER() OVER (PARTITION BY raw_data->>'UID' ORDER BY ingested_at DESC) AS rn
    FROM bronze.raw_entities
    WHERE entity_type = 'tdx_user_search' AND source_system = 'tdx'
      AND raw_data->>'UID' = ANY(:uids)
),
latest_detail AS (
    SELECT
        raw_data->>'UID' AS tdx_user_uid,
        raw_data AS detail_data,
        raw_id AS detail_raw_id,
        ROW_NUMBER() OVER (PARTITION BY raw_data->>'UID' ORDER BY ingested_at DESC) AS rn
    FROM bronze.raw_entities
    WHERE entity_type = 'tdx_user_detail' AND source_system = 'tdx'
      AND raw_data->>'UID' = ANY(:uids)
)
SELECT
    s.tdx_user_uid,
    s.search_data,
    s.search_raw_id,
    d.detail_data,     -- NULL if no detail record yet
    d.detail_raw_id
FROM latest_search s LEFT JOIN latest_detail d USING (tdx_user_uid)
WHERE s.rn = 1 AND (d.rn = 1 OR d.rn IS NULL)
```

**`_extract_tdx_fields`** — currently receives a single `raw_data` dict. Becomes: receives `search_data` + `detail_data` (nullable). Basic identity/contact/employment fields come from `search_data`; enrichment-only fields (`OrgApplications`, `Attributes`, `Permissions`, `GroupIDs`) come from `detail_data` with safe fallbacks:

```python
def _extract_tdx_fields(self, search_data, detail_data, raw_id):
    detail = detail_data or {}
    return {
        # Basic fields from search stream
        "first_name": search_data.get("FirstName"),
        "uniqname": search_data.get("AlternateID", "").lower() or None,
        # ...

        # Detail-only fields — gracefully absent until detail is ingested
        "org_applications": detail.get("OrgApplications", []),
        "attributes": detail.get("Attributes", []),
        "permissions": detail.get("Permissions", {}),
        "group_ids": detail.get("GroupIDs", []),

        # Traceability — reference whichever stream drove this transform
        "raw_id": raw_id,  # search record's raw_id as primary reference
    }
```

## Migration Steps

1. **Add new `entity_type` values** to bronze ingestion (no schema change needed — `entity_type` is VARCHAR)
2. **Rename `002_ingest_tdx_users.py`** to write `entity_type='tdx_user_search'`
3. **Rename `010_enrich_tdx_users.py`** → `010_ingest_tdx_user_details.py`, write `entity_type='tdx_user_detail'`, strip all `_enriched_at` / meta key injection from `raw_data`
4. **Update `001_transform_tdx_users.py`** with the two-stream query and merged field extraction
5. **Backfill or reclassify** existing bronze records: old `entity_type='user'` records can be left as-is (they are valid history) while new ingestion writes to the new stream types
6. **Verify silver output** after first run with new transform — especially `org_applications`, `attributes`, `permissions` which will be empty for users not yet covered by the detail stream

## Prerequisites / Things to Verify Before Implementing

- Confirm which fields appear exclusively in the detail endpoint (`GET /api/users/{uid}`) vs. the search endpoint — map these before splitting `_extract_tdx_fields`
- The detail ingestion staleness check must tolerate users that exist in search but have no detail record yet (silver must handle `LEFT JOIN` gracefully, not skip the user)
- The `ingestion_metadata` column is present in the production `bronze.raw_entities` schema (confirmed in Docker init; verify on prod before deploying)
- Other bronze enrich scripts (`010_enrich_tdx_departments.py`, `011_enrich_tdx_assets.py`) follow the same pattern and should be migrated consistently
