# Future Optimizations & Refactoring Ideas

A running log of known inefficiencies and preferred fixes when time allows. Each item has its own document in `.claude/future_optimizations/`.

---

## Index

| # | Title | Scope | Priority |
|---|---|---|---|
| [1](future_optimizations/01_mcommunity_ldap_streaming.md) | MCommunity LDAP Ingestion — Streaming vs. Full In-Memory Load | `ldap/adapters/ldap_adapter.py`, `007_ingest_mcommunity_users.py` | Medium — current workaround is stable, RAM spike is the only risk |
| [2](future_optimizations/02_bronze_event_streams.md) | Bronze Reconceptualization — Raw Event Streams, Not Entity Snapshots | All bronze ingestion + silver TDX transform | High — architectural correctness; enrichment ambiguity grows over time |
| [3](future_optimizations/03_pydantic_repository_pattern.md) | Typed Entity Models — Pydantic + Repository Pattern for Downstream Scripts | `scripts/tdx/`, `scripts/ticket_queue/`, future automation scripts | Medium — clarity and safety win for all new downstream automation |
