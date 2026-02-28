# Future Optimizations & Refactoring Ideas

A running log of known inefficiencies, the pragmatic workaround currently in place, and the preferred fix when time allows.

---

## 1. MCommunity LDAP Ingestion — Streaming vs. Full In-Memory Load

**File:** `scripts/database/bronze/mcommunity/007_ingest_mcommunity_users.py`
**Relevant adapter:** `ldap/adapters/ldap_adapter.py`

### The Problem

MCommunity (ldap.umich.edu) enforces a **hard cumulative result limit per connection** (~350 records) even when LDAP Simple Paged Results (RFC 2696) is used. This means:

- Standard cookie-based pagination breaks at the cumulative cap with `sizeLimitExceeded` (result code 4)
- `search_paged_generator` cannot be used safely — it would silently truncate results at the server cap and return only a fraction of the directory (~350 records instead of hundreds of thousands)

### Current Fix

`007_ingest_mcommunity_users.py` calls `search_as_dicts(use_pagination=True)`, which routes through:

```
search_as_dicts()
  → search()
    → _execute_intelligent_search()
      → _execute_paged_search()        # paged_search(generator=False) — tries ldap3 helper
        → _execute_cookie_based_pagination()  # hits sizeLimitExceeded after ~350 records
          → _execute_filter_based_chunking()  # the MCommunity workaround
```

`_execute_filter_based_chunking` sidesteps the server cap by making many independent searches
with range filters (`(&(uid=*)(uid>lastValue))`), advancing the window after each chunk.
This works reliably but is inherently accumulative — all chunks are collected into a single
`all_results` list before returning.

**Consequence:** The entire result set is held in memory before any DB writes occur.
For `ou=People,dc=umich,dc=edu` this is likely 200K–400K records, causing a
**peak RAM spike of ~600MB–1.2GB** (Python dict list + ldap3 Entry objects coexist
briefly during the `search_as_dicts` conversion loop).

By contrast, `004_ingest_ad_users.py` uses `search_paged_generator` correctly for AD
(which supports RFC 2696 properly), keeping memory flat at ~1 page × record size
throughout the entire run.

### Preferred Refactor (When Time Allows)

Convert `_execute_filter_based_chunking` into a **generator** that yields each chunk,
then update `007` to consume it incrementally (write to DB between chunks, then discard).

Sketch of the adapter change:

```python
def _filter_based_chunking_generator(self, conn, chunk_size, **search_kwargs):
    """Generator version of filter-based chunking for MCommunity."""
    last_sort_value = None
    sort_attr = self._detect_sort_attribute(search_kwargs.get("search_filter", ""))

    while True:
        # Build range filter
        if last_sort_value is not None:
            modified_filter = self._add_range_filter(
                search_kwargs["search_filter"], sort_attr, last_sort_value
            )
        else:
            modified_filter = search_kwargs["search_filter"]

        kw = {**search_kwargs, "search_filter": modified_filter, "size_limit": chunk_size}
        conn.search(**kw)

        chunk = list(conn.entries)
        if not chunk:
            break

        yield chunk   # ← caller writes to DB here, chunk can be GC'd

        if len(chunk) < chunk_size:
            break

        last_sort_value = getattr(chunk[-1], sort_attr).value
```

And the ingestion script would then mirror the pattern in `004`:

```python
for user_batch in self.ldap_adapter.mcommunity_paged_generator(...):
    for user_data in user_batch:
        # process and insert to DB
    db.commit()  # write after each chunk, batch is then discardable
```

**Prerequisites / things to verify before doing this:**
- MCommunity `uid` values have a stable, consistent sort order (almost certain since `uid` is the primary key, but worth confirming)
- The `uid>lastValue` boundary doesn't miss records where multiple users share the same `uid` prefix at a chunk boundary (shouldn't happen — `uid` is unique)
- Connection stability across many sequential searches (reconnect logic may be needed for very large directories)
- Safety limit still needed (currently 2500 chunks) to guard against infinite loops if the boundary stops advancing
