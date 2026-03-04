# 3. Typed Entity Models — Pydantic + Repository Pattern for Downstream Scripts

**Affects:** `scripts/tdx/001_create_lab_cis.py`, `scripts/ticket_queue/ticket_queue_daemon.py`, and future automation scripts that read from silver views

## The Problem

Downstream scripts that consume silver views currently operate on raw dicts from `query_to_dataframe(...).to_dict("records")`. This has several friction points:

- **No IDE support**: accessing `row["pi_tdx_uid"]` gives no autocomplete, no type inference, and silently returns `None` on typos instead of raising an error at definition time
- **Schema drift is silent**: if a silver view renames or drops a column, the script fails at runtime (often mid-run) with a `KeyError` rather than at startup with a clear validation error
- **Repeated boilerplate**: every script re-implements the same defensive `row.get("field") or default` patterns
- **Opaque payloads**: building TDX API payloads from dicts means the shape of the data is only clear by reading the construction code, not the type signature

## The Preferred Approach — Pydantic Models + Repository Classes

This is **not** a full ORM. No SQLAlchemy sessions, no schema migrations, no query generation. The pattern has two layers:

**Layer 1: Pydantic entity models** (`database/models/`)

Define the shape of each silver entity once, using Python type hints. Pydantic validates on instantiation and coerces compatible types (e.g. Postgres `Decimal` → `float`, `int` → `str`).

```python
# database/models/lab.py
from pydantic import BaseModel
from typing import Optional

class Lab(BaseModel):
    lab_id: str
    lab_name: str
    pi_uniqname: str
    pi_tdx_uid: Optional[str] = None
    department_id: Optional[str] = None
    is_active: bool = True

class LabManager(BaseModel):
    lab_id: str
    manager_tdx_uid: str
    pi_tdx_uid: str
    lab_department_tdx_id: Optional[str] = None

class LabLocation(BaseModel):
    lab_id: str
    location_id: Optional[int] = None
    room_id: Optional[int] = None
    computers_with_location_description: int = 0
```

**Layer 2: Repository classes** (`database/repositories/`)

Thin wrappers that execute specific silver view queries and return typed model lists. The `PostgresAdapter` stays unchanged as the connection layer.

```python
# database/repositories/lab_repository.py
from typing import List, Optional
from database.adapters.postgres_adapter import PostgresAdapter
from database.models.lab import Lab, LabManager, LabLocation

class LabRepository:
    def __init__(self, db: PostgresAdapter):
        self.db = db

    def get_monitored_labs(self, lab_id: Optional[str] = None) -> List[Lab]:
        query = "SELECT * FROM silver.v_labs_monitored"
        params = {}
        if lab_id:
            query += " WHERE lab_id = :lab_id"
            params["lab_id"] = lab_id
        rows = self.db.query_to_dataframe(query, params).to_dict("records")
        return [Lab(**row) for row in rows]

    def get_lab_managers(self, lab_id: Optional[str] = None) -> List[LabManager]:
        query = "SELECT * FROM silver.v_lab_managers_tdx_reference"
        params = {}
        if lab_id:
            query += " WHERE lab_id = :lab_id"
            params["lab_id"] = lab_id
        rows = self.db.query_to_dataframe(query, params).to_dict("records")
        return [LabManager(**row) for row in rows]

    def get_lab_locations(self, lab_id: Optional[str] = None) -> List[LabLocation]:
        query = "SELECT * FROM silver.v_lab_locations_tdx_reference"
        params = {}
        if lab_id:
            query += " WHERE lab_id = :lab_id"
            params["lab_id"] = lab_id
        rows = self.db.query_to_dataframe(query, params).to_dict("records")
        return [LabLocation(**row) for row in rows]
```

**Resulting downstream script** (`001_create_lab_cis.py` after refactor):

```python
# Before: dict-heavy, error-prone
lab_managers = managers_by_lab.get(lab_id, [])
pi_tdx_uid = lab_managers[0].get("pi_tdx_uid")
lab_dept_id = lab_managers[0].get("lab_department_tdx_id")

# After: typed, IDE-supported, schema-drift-detected at startup
lab_repo = LabRepository(db)
labs = lab_repo.get_monitored_labs(args.lab_id)
managers_by_lab = defaultdict(list)
for m in lab_repo.get_lab_managers(args.lab_id):
    managers_by_lab[m.lab_id].append(m)

# Access is clean and type-checked
pi_tdx_uid = managers[0].pi_tdx_uid        # Optional[str], not dict.get()
dept_id = managers[0].lab_department_tdx_id
```

## What Changes and What Doesn't

| Unchanged | Changes |
|---|---|
| `PostgresAdapter` (connection, pooling, `query_to_dataframe`) | New `database/models/` directory with Pydantic entity models |
| Silver views (the abstraction boundary stays at SQL) | New `database/repositories/` directory with repository classes |
| Bronze ingestion and silver transform scripts | Downstream scripts (`scripts/tdx/`, `scripts/ticket_queue/`) updated to use repositories |
| All existing ORM-free SQL patterns | — |

## Scope of Entities to Model

Priority order based on downstream usage:

1. **Lab** — used by `001_create_lab_cis.py`, ticket queue `AddLabAction`
2. **LabManager** — used by `001_create_lab_cis.py`
3. **LabLocation** — used by `001_create_lab_cis.py`
4. **User** — used by ticket queue `AddAssetAction`, compliance scripts
5. **Computer / Asset** — used by ticket queue `AddAssetAction`
6. **Department** — used by lab CI creation, compliance context

## Prerequisites / Things to Verify Before Implementing

- Confirm `pydantic` is already a transitive dependency (likely via FastAPI or other packages); add explicitly to `setup.py` `[database]` or `[all]` extras if not
- Map each silver view's actual column names to model field names before writing models (avoid silent `None` from mismatched names)
- Decide on model location: `database/models/` aligns with existing `database/` module structure and the planned SQLAlchemy models path noted in CLAUDE.md
- Pydantic v2 is the target (v1 uses different validator syntax — check installed version first with `pip show pydantic`)
