# Ubuntu Server Deployment Plan
## LSATS Data Hub — Production Migration

**Created:** 2026-02-24
**Last revised:** 2026-02-24 (v3 — confirmed Ubuntu 24.04 / Python 3.12 / .pgpass)
**Branch context:** `feature/lab-notes/data` → target: `main`
**Target:** Ubuntu 24.04 LTS server (15 GB RAM, 49 GB root, 59 GB /var) with native PostgreSQL 16 (no Docker), Python 3.12.3

---

## Table of Contents

1. [Pre-Implementation Decisions](#1-pre-implementation-decisions)
2. [Linux and PostgreSQL User Accounts](#2-linux-and-postgresql-user-accounts)
3. [Clean Production Schema Strategy](#3-clean-production-schema-strategy)
4. [Sandbox vs Production Environment Design](#4-sandbox-vs-production-environment-design)
5. [Phase-by-Phase Migration Steps](#5-phase-by-phase-migration-steps)
6. [Post-Deployment Validation](#6-post-deployment-validation)
7. [Operational Checklist](#7-operational-checklist)
8. [Log Rotation and Monitoring](#8-log-rotation-and-monitoring)
9. [Backup Strategy](#9-backup-strategy)

---

## 1. Pre-Implementation Decisions

### 1.1 Branch Strategy

**Recommendation: Merge `feature/lab-notes/data` → `main` before deploying.**

Rationale:
- Production servers should track `main`, not a feature branch. Deploying from a feature branch means future hot-fixes to `main` won't automatically apply.
- The current branch name (`feature/lab-notes/data`) implies in-progress work. Evaluate whether it is feature-complete before merging.
- If the branch contains experimental code not ready for production, split it: merge the stable parts to `main` now and keep the experimental remainder on its branch.

Steps before deployment:
```bash
# 1. Review what's different from main
git log main..feature/lab-notes/data --oneline

# 2. Open a PR (or merge directly if no review process)
git checkout main
git merge --no-ff feature/lab-notes/data
git push origin main

# 3. Tag the release
git tag -a v1.0.0-production -m "Initial production server deployment"
git push origin v1.0.0-production
```

### 1.2 Clean Schema Decision

**Recommendation: Do NOT carry migrations forward. Use a clean consolidated schema for the new server.**

The Docker-based dev database accumulated 71 migrations during iterative development, including:
- Multiple `_legacy` table renames (`users_legacy`, `groups_legacy`, `lab_awards_legacy`)
- A complete `silver.computers` table rebuild (migration 025)
- A three-phase silver layer column-name refactor (completed 2026-01-12, renaming LDAP-style names to business-friendly canonical names)
- Tables that were created, renamed, and partially dropped across separate migrations

The new server gets a clean start:
- **`pg_dump --schema-only`** from the dev Docker database provides the authoritative starting point
- Legacy tables (`users_legacy`, `groups_legacy`, `lab_awards_legacy`, `keyconfigure_computers_backup`, `mcommunity_users_backup_20250118`) are stripped from the dump
- The stale foreign key `lab_managers_manager_uniqname_fkey_legacy` (pointing to `users_legacy` instead of `users`) is corrected
- Migrations folder is **archived** — it is not run on the new server

### 1.3 Fix setup.py Extras (Pre-Deployment Code Change)

The current `[all]` extra is **missing critical packages** (`sqlalchemy`, `psycopg2-binary`, `ldap3`, `python-dateutil`, `keyring`) and includes several unnecessary transitive dependencies. Additionally, a `[compliance]` extra does not exist but should.

**Updated extras based on import audit:**

| Extra | Purpose | Packages |
|---|---|---|
| `[database]` | All postgres/database scripts (bronze, silver, transforms) | `sqlalchemy>=1.4.0`, `psycopg2-binary>=2.9.0`, `pandas>=1.3.0`, `python-dotenv>=0.15.0`, `python-dateutil>=2.8.0`, `ldap3>=2.9.0`, `keyring>=23.0.0`, `requests>=2.25.0` |
| `[compliance]` | Compliance ticket automation scripts | `python-dotenv>=0.15.0`, `pandas>=1.3.0`, `google-api-python-client>=2.0.0`, `google-auth>=2.38.0`, `google-auth-httplib2>=0.1.0`, `google-auth-oauthlib>=0.4.0`, `requests>=2.25.0` |
| `[google]` | Google Workspace integration | *(keep as-is, remove meta-packages)* |
| `[ai]` | AI/lab notes features | `openai>=1.0.0`, `beautifulsoup4>=4.0.0`, `html2text>=2020.1.16`, `readability-lxml>=0.8.1`, `tldextract>=3.0.0` |
| `[all]` | Everything | Union of `[database]` + `[compliance]` + `[google]` + `[ai]` |

**Packages to remove from `[all]`** (transitive deps, not directly imported):
`google` (meta-package), `googleapis-common-protos`, `google-api-core`, `httplib2`, `oauthlib`, `proto-plus`, `uritemplate`, `cachetools`, `anthropic`

**Also update:** `python_requires=">=3.11"` (was `>=3.6`, deploying with 3.12.3 on Ubuntu 24.04).

---

## 2. Linux and PostgreSQL User Accounts

### 2.1 Linux System User

Create a dedicated service account to run all Python scripts and scheduled jobs. **Never run data pipelines as root or as a personal user account.**

```bash
# On the Ubuntu server
sudo useradd --system \
  --home-dir /opt/lsats-data-hub \
  --shell /usr/sbin/nologin \
  --comment "LSATS Data Hub service account" \
  lsats

# Set ownership of the application directory
sudo mkdir -p /opt/lsats-data-hub
sudo chown -R lsats:lsats /opt/lsats-data-hub
```

The `lsats` user:
- Owns `/opt/lsats-data-hub/` (the production repo clone)
- Owns `/opt/lsats-data-hub/venv/` (the Python virtual environment)
- Runs all cron jobs and systemd services
- Has read access to `.env` (mode 600, owned by `lsats`)
- Shell is `/usr/sbin/nologin` — scripts run via `sudo -u lsats /bin/bash -c "..."` or systemd `User=lsats`

### 2.2 PostgreSQL Roles

Create two PostgreSQL roles with appropriate privilege separation:

```sql
-- Role 1: Application role (read/write, used by Python scripts)
CREATE USER lsats_user WITH
  PASSWORD 'choose_a_strong_password'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  LOGIN;

-- Role 2: Read-only role (for reporting, pgAdmin browsing, analysts)
CREATE USER lsats_readonly WITH
  PASSWORD 'choose_a_strong_password'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  LOGIN;

-- Create the database
CREATE DATABASE lsats_db
  OWNER lsats_user
  ENCODING 'UTF8'
  LC_COLLATE 'en_US.UTF-8'
  LC_CTYPE 'en_US.UTF-8';

-- After schema creation, grant read-only access
-- (Run after all tables and views are created)
GRANT CONNECT ON DATABASE lsats_db TO lsats_readonly;
GRANT USAGE ON SCHEMA bronze, silver, gold, meta TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO lsats_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO lsats_readonly;  -- Gold deferred
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO lsats_readonly;

-- Make future tables automatically accessible to readonly
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA bronze
  GRANT SELECT ON TABLES TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA silver
  GRANT SELECT ON TABLES TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA gold
  GRANT SELECT ON TABLES TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA meta
  GRANT SELECT ON TABLES TO lsats_readonly;
```

### 2.3 PostgreSQL Configuration

```bash
# Edit pg_hba.conf to allow local socket connections for the lsats user
# Ubuntu 24.04 default: /etc/postgresql/16/main/pg_hba.conf
# Add:
#   local   lsats_db    lsats_user                  scram-sha-256
#   local   lsats_db    lsats_readonly              scram-sha-256
#   host    lsats_db    lsats_user    127.0.0.1/32  scram-sha-256

# Create .pgpass for passwordless pg_dump/pg_restore in shell scripts
# Python scripts use DATABASE_URL from .env — this is only for CLI tools
# in orchestrate_silver.sh, backup_database.sh, and manual pg_restore
sudo -u lsats /bin/bash -c "
cat > /opt/lsats-data-hub/.pgpass << 'PGPASS'
localhost:5432:lsats_db:lsats_user:choose_strong_password
PGPASS
chmod 600 /opt/lsats-data-hub/.pgpass
"
# Set PGPASSFILE in orchestrator scripts or export in .bashrc:
# export PGPASSFILE=/opt/lsats-data-hub/.pgpass

# Tune postgresql.conf for 15 GB RAM server (Ubuntu 24.04: /etc/postgresql/16/main/postgresql.conf):
# shared_buffers = 4GB             (25% of 15 GB RAM)
# effective_cache_size = 11GB      (75% of RAM — OS page cache estimate)
# work_mem = 64MB                  (for JSONB/sort operations)
# maintenance_work_mem = 512MB     (for index builds during ingestion)
# max_connections = 50             (data hub doesn't need hundreds)
# wal_buffers = 64MB               (auto-tuned from shared_buffers)
# checkpoint_completion_target = 0.9
# random_page_cost = 1.1           (SSDs — adjust to 4.0 for spinning disk)
```

---

## 3. Clean Production Schema Strategy

### 3.1 The Problem With Running Migrations on the New Server

The current `docker/postgres/migrations/` directory contains 71 migration files designed for *incremental changes to an existing database*. Running them sequentially on a fresh DB would:
1. Create tables in an old form, then rename them (e.g., `silver.users` → `silver.users_legacy`)
2. Leave `_legacy` tables in place that are only referenced by transitional views
3. Create and then drop 18 columns that were renamed during the silver layer refactor
4. Not produce the same state as the current running database

**Do not run migrations on the new server.**

### 3.2 Approach: pg_dump the Current Schema and Clean It

Rather than manually reconciling `schemas.sql` with 71 migrations, dump the authoritative schema from the running Docker database and strip the artifacts:

```bash
# On your Mac — dump structure only (no data)
docker exec lsats-database pg_dump -U lsats_user -d lsats_db \
  --schema-only --no-owner --no-privileges \
  > docker/postgres/production_schema.sql

# Then manually edit to:
# 1. Remove legacy tables: users_legacy, groups_legacy, lab_awards_legacy
# 2. Remove backup tables: keyconfigure_computers_backup, mcommunity_users_backup_20250118
# 3. Fix FK: lab_managers_manager_uniqname_fkey_legacy → reference silver.users(uniqname)
# 4. Remove any temp indexes or artifacts
# 5. Remove v_lab_active_awards_legacy view (references legacy table)
```

**Tables to include in production schema (29 silver tables):**

| Tier | Tables |
|---|---|
| Source-specific | `tdx_users`, `tdx_departments`, `tdx_assets`, `tdx_labs` |
| Source-specific | `ad_users`, `ad_groups`, `ad_computers`, `ad_organizational_units`, `ad_labs` |
| Source-specific | `mcommunity_users`, `mcommunity_groups` |
| Source-specific | `umapi_departments`, `umapi_employees` |
| Source-specific | `keyconfigure_computers` |
| Consolidated | `users`, `groups`, `departments`, `computers` |
| Relationship | `group_members`, `group_owners`, `computer_attributes`, `computer_groups` |
| Composite | `labs`, `lab_managers`, `lab_members`, `lab_computers`, `lab_awards`, `award_labs` |

**Tables to EXCLUDE** (legacy/backup artifacts):
- `silver.users_legacy`
- `silver.groups_legacy`
- `silver.lab_awards_legacy`
- `silver.keyconfigure_computers_backup`
- `silver.mcommunity_users_backup_20250118`

**Gold layer** — NOT deployed in this phase. The gold schema will be created (empty) but gold tables are deferred until KPIs and OLAP design are finalized. Note: the silver consolidated tables (`users`, `computers`, `labs`) currently serve both analytical reads and daemon ticket-write lookups (OLTP pattern). A future decision is needed on whether to separate OLTP (daemon-facing) from OLAP (reporting/gold) workloads, potentially with materialized views or a read replica.

### 3.3 Required: Fix silver_views.sql Legacy References

Several views in `docker/postgres/views/silver_views.sql` still reference `_legacy` tables:

| View | Legacy Reference | Replace With |
|---|---|---|
| `v_lab_summary` | `silver.users_legacy` | `silver.users` |
| `v_lab_groups` | `silver.groups_legacy` | `silver.groups` |
| `v_lab_members_detailed` | `silver.users_legacy` | `silver.users` |
| `v_lab_active_awards_legacy` | `silver.lab_awards_legacy` | Rename to `v_lab_active_awards` referencing `silver.lab_awards` |
| `v_eligible_lab_members` | `silver.users_legacy` | `silver.users` |

**Action:** Update `silver_views.sql` to use canonical table names before deployment. This is a pre-deployment code change committed to `main`.

### 3.4 Production Schema Initialization

The initialization approach uses a single consolidated SQL file (`production_schema.sql`) that works both with Docker Compose and bare PostgreSQL:

```bash
# production_init.sql — entrypoint for both Docker and bare installs
# Contains: extensions, schemas, helper functions, meta tables, meta views
# Does NOT contain: \i directives or Docker-specific paths

# production_schema.sql — all table definitions (dumped and cleaned)
# Contains: CREATE TABLE for all bronze, silver, gold, meta tables
# Does NOT contain: legacy tables, backup tables

# silver_views.sql — all silver views (already idempotent)
# gold_views.sql — all gold views (if separated)
```

**On the production server, run in order:**
```bash
cd /opt/lsats-data-hub

# Step 1: Extensions, schemas, functions, meta tables
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/production_init.sql

# Step 2: All table definitions (cleaned dump)
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/production_schema.sql

# Step 3: Views
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/views/silver_views.sql

# Step 4: Read-only grants
sudo -u postgres psql -d lsats_db <<'EOF'
GRANT CONNECT ON DATABASE lsats_db TO lsats_readonly;
GRANT USAGE ON SCHEMA bronze, silver, gold, meta TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO lsats_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO lsats_readonly;  -- Gold deferred
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA bronze GRANT SELECT ON TABLES TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA silver GRANT SELECT ON TABLES TO lsats_readonly;
-- ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA gold GRANT SELECT ON TABLES TO lsats_readonly;  -- Gold deferred
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA meta GRANT SELECT ON TABLES TO lsats_readonly;
EOF
```

**For Docker Compose testing**, update `docker/postgres/init.sql` to source the same files:
```sql
-- init.sql (Docker entrypoint)
\i /docker-entrypoint-initdb.d/production_init.sql
\i /docker-entrypoint-initdb.d/production_schema.sql
\i /docker-entrypoint-initdb.d/views/silver_views.sql
```

This way one set of SQL files serves both environments.

---

## 4. Sandbox vs Production Environment Design

### 4.1 Environment Overview

TDX Sandbox operates via a **cutoff date** — it is a point-in-time copy of production data. This means:
- **Production DB data will work in sandbox** (TDX IDs, user UIDs, asset IDs are valid up to the cutoff)
- **Sandbox will not have records created after the cutoff date**
- Both environments share the same source systems (LDAP, MCommunity, UMICH API), only TDX differs
- **Sharing a single `lsats_db` is safe** — bronze `source_system` tagging distinguishes TDX environment, and non-TDX sources (LDAP, AD, UMICH API) are identical across both

### 4.2 Directory Isolation Strategy

**Use two separate directory trees** — production stays untouched while sandbox can diverge freely.

```
/opt/lsats-data-hub/             # PRODUCTION — always stable
├── .env                         # TDX_BASE_URL → TDWebApi (production)
├── venv/
├── scripts/
└── ...

/opt/lsats-sandbox/              # SANDBOX — for testing
├── .env                         # TDX_BASE_URL → SBTDWebApi (sandbox)
├── venv/
├── scripts/
└── ...
```

**Setup:**
```bash
# Production (tracks main)
sudo -u lsats git clone <repo-url> /opt/lsats-data-hub
cd /opt/lsats-data-hub && git checkout main

# Sandbox (can track a feature branch for testing)
sudo -u lsats git clone <repo-url> /opt/lsats-sandbox
cd /opt/lsats-sandbox && git checkout main  # or a feature branch
```

**Key differences between the two:**
- `.env` files differ only in `TDX_BASE_URL` (initially)
- Both share the same database (`lsats_db`) — this is intentional since TDX sandbox is a cutoff-date copy of production, and non-TDX sources are identical
- Bronze `source_system` tagging distinguishes TDX environments where needed
- Sandbox can be on a different git branch for testing pre-merge code
- Production systemd timers always point to `/opt/lsats-data-hub/`

**If full database isolation is later needed** (e.g., destructive schema testing), use a copy-on-write approach:
```bash
# Create a snapshot database for isolated testing
sudo -u postgres createdb lsats_db_test --template=lsats_db
# Update /opt/lsats-sandbox/.env: DATABASE_URL=...lsats_db_test
# Drop when done: sudo -u postgres dropdb lsats_db_test
```

### 4.3 Compliance Scripts: Sandbox-First Policy

All compliance scripts support `--dry-run`. Enforce this workflow:

```
sandbox + --dry-run  →  sandbox (no --dry-run)  →  production + --dry-run  →  production
```

Never skip a step. The TDX sandbox uses a backup of production, so sandbox tickets created during testing look real but don't affect actual users.

---

## 5. Phase-by-Phase Migration Steps

### Phase 0: Pre-Deployment (on your Mac, before touching the server)

**These steps happen on the development machine and result in commits to `main`.**

- [ ] **0.1** Review `feature/lab-notes/data` — confirm it's ready to merge
- [ ] **0.2** Fix `setup.py` extras: add `[compliance]`, fix `[database]` and `[all]` per Section 1.3
- [ ] **0.3** Dump current schema from Docker: `pg_dump --schema-only` → `production_schema.sql`
- [ ] **0.4** Clean `production_schema.sql`: remove 5 legacy/backup tables, fix `lab_managers` FK
- [ ] **0.5** Create `production_init.sql`: extract extensions, schemas, functions, meta tables from `init.sql` — no Docker-specific paths, no `\i` directives
- [ ] **0.6** Update `docker/postgres/views/silver_views.sql` — replace all `_legacy` table references with canonical table names
- [ ] **0.7** Update `docker/postgres/init.sql` to source the new files (maintains Docker Compose compatibility)
- [ ] **0.8** Test clean schema locally: `docker-compose down -v && docker-compose up -d` — verify all views load without errors
- [ ] **0.9** Merge `feature/lab-notes/data` → `main`, tag as `v1.0.0-production`
- [ ] **0.10** Push `main` and tag to remote

### Phase 1: Server Preparation

```bash
# Install system dependencies
sudo apt update && sudo apt upgrade -y
sudo apt install -y \
  postgresql postgresql-contrib \
  python3 python3-venv python3-pip \
  gcc build-essential libpq-dev python3-dev \
  libldap2-dev libsasl2-dev \
  git curl logrotate
# Ubuntu 24.04 ships Python 3.12.3 as system python3 — no PPA needed
# PostgreSQL 16 is the default version

# Create service account
sudo useradd --system --home-dir /opt/lsats-data-hub \
  --shell /usr/sbin/nologin --comment "LSATS Data Hub service account" lsats

# Create directories
sudo mkdir -p /opt/lsats-data-hub /opt/lsats-sandbox /var/log/lsats /var/backups/lsats
sudo chown lsats:lsats /opt/lsats-data-hub /opt/lsats-sandbox /var/log/lsats /var/backups/lsats
```

### Phase 2: Clone Repository

```bash
# Production clone (tracks main)
sudo -u lsats /bin/bash -c "git clone <your-repo-url> /opt/lsats-data-hub"
sudo -u lsats /bin/bash -c "cd /opt/lsats-data-hub && git checkout main"

# Sandbox clone
sudo -u lsats /bin/bash -c "git clone <your-repo-url> /opt/lsats-sandbox"
sudo -u lsats /bin/bash -c "cd /opt/lsats-sandbox && git checkout main"
```

### Phase 3: Transfer Credentials

**From your Mac:**
```bash
# .env file (never in git)
scp "/Users/myodhes/Projects/LSATS Data Hub/.env" \
  user@your-server:/tmp/lsats_env_transfer

# Google credentials (for future use — not required for database deployment)
scp "/Users/myodhes/Projects/LSATS Data Hub/credentials.json" \
  user@your-server:/tmp/lsats_creds_transfer

scp "/Users/myodhes/Projects/LSATS Data Hub/token.json" \
  user@your-server:/tmp/lsats_token_transfer
```

**On the server:**
```bash
# Production .env
sudo cp /tmp/lsats_env_transfer /opt/lsats-data-hub/.env
sudo chown lsats:lsats /opt/lsats-data-hub/.env
sudo chmod 600 /opt/lsats-data-hub/.env

# Sandbox .env (copy, then modify TDX_BASE_URL)
sudo cp /tmp/lsats_env_transfer /opt/lsats-sandbox/.env
sudo chown lsats:lsats /opt/lsats-sandbox/.env
sudo chmod 600 /opt/lsats-sandbox/.env

# Google credentials (place in production dir)
sudo mv /tmp/lsats_creds_transfer /opt/lsats-data-hub/credentials.json
sudo mv /tmp/lsats_token_transfer /opt/lsats-data-hub/token.json
sudo chown lsats:lsats /opt/lsats-data-hub/credentials.json /opt/lsats-data-hub/token.json
sudo chmod 600 /opt/lsats-data-hub/credentials.json /opt/lsats-data-hub/token.json

# Clean up
sudo rm -f /tmp/lsats_env_transfer

# Edit PRODUCTION .env for the new server
sudo -u lsats /bin/bash -c "nano /opt/lsats-data-hub/.env"
# Key changes:
#   DATABASE_URL=postgresql://lsats_user:<password>@localhost:5432/lsats_db
#   TDX_BASE_URL="https://teamdynamix.umich.edu/TDWebApi/api"    # production
#   LOG_FILE=/var/log/lsats/lsats_database.log
#   DATA_PATH=/opt/lsats-data-hub/data

# Edit SANDBOX .env
sudo -u lsats /bin/bash -c "nano /opt/lsats-sandbox/.env"
# Key changes:
#   DATABASE_URL=postgresql://lsats_user:<password>@localhost:5432/lsats_db
#   TDX_BASE_URL="https://teamdynamix.umich.edu/SBTDWebApi/api"  # sandbox
#   LOG_FILE=/var/log/lsats/sandbox.log
#   DATA_PATH=/opt/lsats-sandbox/data
```

### Phase 4: PostgreSQL Setup

```bash
# Create user and database
sudo -u postgres psql <<'EOF'
CREATE USER lsats_user WITH PASSWORD 'choose_strong_password' NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN;
CREATE USER lsats_readonly WITH PASSWORD 'choose_strong_password' NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN;
CREATE DATABASE lsats_db OWNER lsats_user ENCODING 'UTF8';
EOF

# Apply the clean schema
cd /opt/lsats-data-hub

# Step 1: Foundation (extensions, schemas, functions, meta tables)
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/production_init.sql

# Step 2: All table definitions (cleaned pg_dump output)
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/production_schema.sql

# Step 3: Silver views (updated, no legacy refs)
psql -U lsats_user -d lsats_db -h localhost \
  -f docker/postgres/views/silver_views.sql

# Step 4: Read-only grants
sudo -u postgres psql -d lsats_db <<'EOF'
GRANT CONNECT ON DATABASE lsats_db TO lsats_readonly;
GRANT USAGE ON SCHEMA bronze, silver, gold, meta TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO lsats_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO lsats_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO lsats_readonly;  -- Gold deferred
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA bronze GRANT SELECT ON TABLES TO lsats_readonly;
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA silver GRANT SELECT ON TABLES TO lsats_readonly;
-- ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA gold GRANT SELECT ON TABLES TO lsats_readonly;  -- Gold deferred
ALTER DEFAULT PRIVILEGES FOR USER lsats_user IN SCHEMA meta GRANT SELECT ON TABLES TO lsats_readonly;
EOF

# Step 5: Apply PostgreSQL tuning
sudo nano /etc/postgresql/<version>/main/postgresql.conf
# Set: shared_buffers=4GB, effective_cache_size=11GB, work_mem=64MB,
#       maintenance_work_mem=512MB, max_connections=50, random_page_cost=1.1
sudo systemctl restart postgresql

# Step 6: Verify
psql -U lsats_user -d lsats_db -h localhost <<'EOF'
\dn
\dt silver.*
\dv silver.*
-- Expect: 29 silver tables, ~19 silver views, 0 legacy tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'silver' AND table_name LIKE '%legacy%';
EOF
```

### Phase 5: Python Environment

```bash
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-data-hub
  python3 -m venv venv       # Ubuntu 24.04: python3 = Python 3.12.3
  source venv/bin/activate
  pip install --upgrade pip
  pip install -e '.[all]'
"

# Repeat for sandbox
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-sandbox
  python3 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -e '.[all]'
"
```

### Phase 6: Validate Connectivity

```bash
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-data-hub
  source venv/bin/activate

  # Test 1: Database connection
  python -c \"
from database.adapters.postgres_adapter import PostgresAdapter
import os
from dotenv import load_dotenv
load_dotenv()
db = PostgresAdapter(os.environ['DATABASE_URL'])
print('DB connection OK')
db.close()
\"

  # Test 2: TDX connection
  python -c \"
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
import os
from dotenv import load_dotenv
load_dotenv()
tdx = TeamDynamixFacade()
print('TDX connection OK — environment:', 'SANDBOX' if 'SB' in os.environ.get('TDX_BASE_URL','') else 'PRODUCTION')
\"

  # Test 3: LDAP connectivity (port 636 for LDAPS)
  python -c \"
import ldap3
server = ldap3.Server('ldap.umich.edu', port=636, use_ssl=True, get_info=ldap3.DSA)
conn = ldap3.Connection(server, auto_bind=True)
print('LDAP connection OK')
conn.unbind()
\" 2>&1 || echo 'LDAP connection FAILED — check firewall rules for port 636'

  # Test 4: UMich API
  python -c \"
import requests, os
from dotenv import load_dotenv
load_dotenv()
resp = requests.post('https://apigw.it.umich.edu/um/oauth2/token',
  data={'grant_type': 'client_credentials', 'scope': 'umscheduleofclasses'},
  auth=(os.environ['UM_CLIENT_KEY'], os.environ['UM_CLIENT_SECRET']))
print('UMich API OK' if resp.status_code == 200 else f'UMich API FAILED: {resp.status_code}')
\"
"
```

### Phase 7: Bronze Ingestion (All Sources)

Run from sandbox first to validate, then switch to production `.env` and re-run TDX sources.

```bash
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-sandbox && source venv/bin/activate

  echo '=== UMich API Sources ==='
  python scripts/database/bronze/umapi/001_ingest_umapi_departments.py
  python scripts/database/bronze/umapi/009_ingest_umapi_employees.py

  echo '=== MCommunity LDAP Sources ==='
  python scripts/database/bronze/mcommunity/007_ingest_mcommunity_users.py
  python scripts/database/bronze/mcommunity/005_ingest_mcommunity_groups.py

  echo '=== Active Directory Sources ==='
  python scripts/database/bronze/ad/004_ingest_ad_users.py
  python scripts/database/bronze/ad/005_ingest_ad_groups.py
  python scripts/database/bronze/ad/006_ingest_ad_organizational_units.py
  python scripts/database/bronze/ad/007_ingest_ad_computers.py

  echo '=== Document Sources ==='
  python scripts/database/bronze/document/008_ingest_lab_awards.py
  python scripts/database/bronze/document/009_ingest_keyconfigure_computers.py

  echo '=== TDX Sources (SANDBOX) ==='
  python scripts/database/bronze/tdx/001_ingest_tdx_departments.py
  python scripts/database/bronze/tdx/002_ingest_tdx_users.py
  python scripts/database/bronze/tdx/003_ingest_tdx_assets.py

  echo '=== TDX Enrichment ==='
  python scripts/database/bronze/tdx/010_enrich_tdx_departments.py
  python scripts/database/bronze/tdx/010_enrich_tdx_users.py
  python scripts/database/bronze/tdx/011_enrich_tdx_assets.py

  echo '=== Verify bronze counts ==='
  psql -U lsats_user -d lsats_db -h localhost -c \"
    SELECT entity_type, source_system, COUNT(*) as records
    FROM bronze.raw_entities
    GROUP BY 1, 2
    ORDER BY 1, 2;
  \"
"
```

### Phase 8: Silver Transformations

```bash
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-sandbox && source venv/bin/activate

  echo '=== Source-Specific Transforms (Tier 1) ==='
  python scripts/database/silver/001_transform_tdx_users.py
  python scripts/database/silver/002_transform_tdx_departments.py
  python scripts/database/silver/003_transform_ad_groups.py
  python scripts/database/silver/004_transform_tdx_assets.py
  python scripts/database/silver/004_transform_ad_users.py
  python scripts/database/silver/006_transform_mcommunity_users.py
  python scripts/database/silver/006_transform_keyconfigure_computers.py
  python scripts/database/silver/007_transform_ad_computers.py
  python scripts/database/silver/002_transform_umapi_employees.py
  python scripts/database/silver/008_transform_lab_awards.py

  echo '=== Consolidated Transforms (Tier 2) ==='
  python scripts/database/silver/010_transform_departments.py
  python scripts/database/silver/011_transform_groups.py
  python scripts/database/silver/012_transform_users.py
  python scripts/database/silver/013_transform_computers.py

  echo '=== Composite Transforms (Tier 3) ==='
  python scripts/database/silver/013_transform_lab_members.py
  python scripts/database/silver/014_transform_lab_computers.py
  python scripts/database/silver/014_transform_lab_managers.py
  python scripts/database/silver/014_aggregate_tdx_labs.py
  python scripts/database/silver/015_aggregate_award_labs.py
  python scripts/database/silver/016_aggregate_ad_labs.py

  echo '=== Verify silver quality ==='
  psql -U lsats_user -d lsats_db -h localhost -c \"
    SELECT 'departments' as entity, COUNT(*) as records, AVG(data_quality_score)::decimal(3,2) as avg_quality FROM silver.departments
    UNION ALL SELECT 'users', COUNT(*), AVG(data_quality_score)::decimal(3,2) FROM silver.users
    UNION ALL SELECT 'groups', COUNT(*), AVG(data_quality_score)::decimal(3,2) FROM silver.groups
    UNION ALL SELECT 'computers', COUNT(*), AVG(data_quality_score)::decimal(3,2) FROM silver.computers
    UNION ALL SELECT 'labs', COUNT(*), AVG(data_quality_score)::decimal(3,2) FROM silver.labs
    ORDER BY 1;
  \"
"
```

### Phase 9: Switch to Production TDX

After sandbox validation passes:

```bash
# Re-run TDX-specific bronze + silver from the PRODUCTION directory
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-data-hub && source venv/bin/activate

  # Non-TDX sources were already ingested (shared database)
  # Only need to re-run TDX bronze with production endpoint

  echo '=== TDX Sources (PRODUCTION) ==='
  python scripts/database/bronze/tdx/001_ingest_tdx_departments.py
  python scripts/database/bronze/tdx/002_ingest_tdx_users.py
  python scripts/database/bronze/tdx/003_ingest_tdx_assets.py
  python scripts/database/bronze/tdx/010_enrich_tdx_departments.py
  python scripts/database/bronze/tdx/010_enrich_tdx_users.py
  python scripts/database/bronze/tdx/011_enrich_tdx_assets.py

  echo '=== Re-run Silver Transforms (picks up production TDX data) ==='
  python scripts/database/silver/001_transform_tdx_users.py
  python scripts/database/silver/002_transform_tdx_departments.py
  python scripts/database/silver/004_transform_tdx_assets.py
  python scripts/database/silver/014_aggregate_tdx_labs.py

  echo '=== Re-run Consolidated Transforms ==='
  python scripts/database/silver/010_transform_departments.py
  python scripts/database/silver/012_transform_users.py
  python scripts/database/silver/013_transform_computers.py
"
```

---

## 6. Post-Deployment Validation

### 6.1 Schema Validation Queries

```sql
-- All expected schemas exist
SELECT schema_name FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold', 'meta')
ORDER BY schema_name;

-- All silver tables exist (expected: 29 tables)
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- All silver views load without error (expected: ~19 views)
SELECT viewname FROM pg_views WHERE schemaname = 'silver'
ORDER BY viewname;

-- No legacy tables (should return 0 rows)
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'silver' AND table_name LIKE '%legacy%';

-- No legacy foreign keys
SELECT tc.constraint_name, ccu.table_name AS references_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name LIKE '%legacy%';

-- Gold schema exists (empty, tables deferred)
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold';

-- Recent ingestion status
SELECT * FROM meta.current_ingestion_status ORDER BY last_run DESC;
```

### 6.2 Data Quality Validation

```sql
-- Bronze completeness
SELECT entity_type, source_system,
  COUNT(*) as records,
  MAX(ingested_at) as latest_ingestion
FROM bronze.raw_entities
GROUP BY 1, 2
ORDER BY 1, 2;

-- Silver quality scores (all consolidated entities)
SELECT 'departments' as entity, COUNT(*) as total,
  COUNT(*) FILTER (WHERE data_quality_score >= 0.8) as high_quality
FROM silver.departments
UNION ALL
SELECT 'users', COUNT(*), COUNT(*) FILTER (WHERE data_quality_score >= 0.8)
FROM silver.users
UNION ALL
SELECT 'computers', COUNT(*), COUNT(*) FILTER (WHERE data_quality_score >= 0.8)
FROM silver.computers
UNION ALL
SELECT 'labs', COUNT(*), COUNT(*) FILTER (WHERE data_quality_score >= 0.8)
FROM silver.labs;

-- Views return data (spot-check)
SELECT COUNT(*) as lab_summary FROM silver.v_lab_summary;
SELECT COUNT(*) as labs_monitored FROM silver.v_labs_monitored;
SELECT COUNT(*) as legitimate_labs FROM silver.v_legitimate_labs;
SELECT COUNT(*) as lab_managers FROM silver.v_lab_managers_detailed;

-- Verify preferred_name column is populated
SELECT COUNT(*) FILTER (WHERE preferred_name IS NOT NULL) as has_preferred_name,
       COUNT(*) as total
FROM silver.users;
```

### 6.3 Network Connectivity Validation

```bash
# HTTPS endpoints (should all work from secure VLAN)
curl -sf https://teamdynamix.umich.edu/TDWebApi/api/ -o /dev/null && echo "TDX: OK" || echo "TDX: FAIL"
curl -sf https://apigw.it.umich.edu/ -o /dev/null && echo "UMich API: OK" || echo "UMich API: FAIL"

# LDAP (port 636 for LDAPS — this is NOT HTTP, needs direct TCP access)
python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
result = s.connect_ex(('ldap.umich.edu', 636))
print('LDAPS port 636: OK' if result == 0 else 'LDAPS port 636: BLOCKED')
s.close()
"
```

---

## 7. Operational Checklist

### 7.1 Orchestrator Scripts

Instead of individual cron entries per script, use orchestrator scripts that handle execution order and error reporting:

```bash
#!/bin/bash
# /opt/lsats-data-hub/scripts/orchestrate_bronze.sh
# Runs all bronze ingestion scripts in correct order
set -euo pipefail

VENV="/opt/lsats-data-hub/venv/bin/python"
LOG="/var/log/lsats/bronze_$(date +%Y%m%d_%H%M%S).log"

exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze Ingestion Started: $(date) ==="

# Non-TDX sources (independent, could parallelize later)
$VENV /opt/lsats-data-hub/scripts/database/bronze/umapi/001_ingest_umapi_departments.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/umapi/009_ingest_umapi_employees.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/mcommunity/007_ingest_mcommunity_users.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/mcommunity/005_ingest_mcommunity_groups.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/ad/004_ingest_ad_users.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/ad/005_ingest_ad_groups.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/ad/006_ingest_ad_organizational_units.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/ad/007_ingest_ad_computers.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/document/008_ingest_lab_awards.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/document/009_ingest_keyconfigure_computers.py

# TDX sources
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/001_ingest_tdx_departments.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/002_ingest_tdx_users.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/003_ingest_tdx_assets.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/010_enrich_tdx_departments.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/010_enrich_tdx_users.py
$VENV /opt/lsats-data-hub/scripts/database/bronze/tdx/011_enrich_tdx_assets.py

echo "=== Bronze Ingestion Complete: $(date) ==="
```

```bash
#!/bin/bash
# /opt/lsats-data-hub/scripts/orchestrate_silver.sh
# Runs all silver transforms in correct tier order
set -euo pipefail

export PGPASSFILE="/opt/lsats-data-hub/.pgpass"
VENV="/opt/lsats-data-hub/venv/bin/python"
LOG="/var/log/lsats/silver_$(date +%Y%m%d_%H%M%S).log"
SNAPSHOT_DIR="/var/backups/lsats"

exec > >(tee -a "$LOG") 2>&1

echo "=== Silver Transformation Started: $(date) ==="

# --- Pre-silver snapshot ---
# Capture database state AFTER bronze ingestion, BEFORE silver transforms.
# If any silver transform fails: restore this snapshot, fix the script, re-run silver only.
# This avoids re-running the full bronze ingestion pipeline.
SNAPSHOT_FILE="${SNAPSHOT_DIR}/pre_silver_$(date +%Y%m%d_%H%M%S).dump"
echo "Taking pre-silver snapshot: ${SNAPSHOT_FILE}"
pg_dump -U lsats_user -h localhost lsats_db \
  --format=custom --compress=9 \
  --file="${SNAPSHOT_FILE}"
echo "Snapshot complete ($(du -h "${SNAPSHOT_FILE}" | cut -f1))"

# Keep only the 2 most recent pre-silver snapshots (they're replaced daily)
ls -t "${SNAPSHOT_DIR}"/pre_silver_*.dump 2>/dev/null | tail -n +3 | xargs -r rm --

# Tier 1: Source-specific transforms
$VENV /opt/lsats-data-hub/scripts/database/silver/001_transform_tdx_users.py
$VENV /opt/lsats-data-hub/scripts/database/silver/002_transform_tdx_departments.py
$VENV /opt/lsats-data-hub/scripts/database/silver/002_transform_umapi_employees.py
$VENV /opt/lsats-data-hub/scripts/database/silver/003_transform_ad_groups.py
$VENV /opt/lsats-data-hub/scripts/database/silver/004_transform_tdx_assets.py
$VENV /opt/lsats-data-hub/scripts/database/silver/004_transform_ad_users.py
$VENV /opt/lsats-data-hub/scripts/database/silver/006_transform_mcommunity_users.py
$VENV /opt/lsats-data-hub/scripts/database/silver/006_transform_keyconfigure_computers.py
$VENV /opt/lsats-data-hub/scripts/database/silver/007_transform_ad_computers.py
$VENV /opt/lsats-data-hub/scripts/database/silver/008_transform_lab_awards.py

# Tier 2: Consolidated transforms (depend on Tier 1)
$VENV /opt/lsats-data-hub/scripts/database/silver/010_transform_departments.py
$VENV /opt/lsats-data-hub/scripts/database/silver/011_transform_groups.py
$VENV /opt/lsats-data-hub/scripts/database/silver/012_transform_users.py
$VENV /opt/lsats-data-hub/scripts/database/silver/013_transform_computers.py

# Tier 3: Composite transforms (depend on Tier 2)
$VENV /opt/lsats-data-hub/scripts/database/silver/013_transform_lab_members.py
$VENV /opt/lsats-data-hub/scripts/database/silver/014_transform_lab_computers.py
$VENV /opt/lsats-data-hub/scripts/database/silver/014_transform_lab_managers.py
$VENV /opt/lsats-data-hub/scripts/database/silver/014_aggregate_tdx_labs.py
$VENV /opt/lsats-data-hub/scripts/database/silver/015_aggregate_award_labs.py
$VENV /opt/lsats-data-hub/scripts/database/silver/016_aggregate_ad_labs.py

echo "=== Silver Transformation Complete: $(date) ==="
```

### 7.2 Scheduled Jobs (systemd timers)

systemd timers are preferred over cron for this deployment because they support:
- **Dependency chaining** (`After=` ensures silver waits for bronze to complete)
- **Built-in retry logic** (`Restart=on-failure` with backoff)
- **Unified logging** via journald (no separate log piping)
- **Status visibility** (`systemctl list-timers`, `systemctl status`)

#### Backup Timer (runs first, daily at 1am)

```ini
# /etc/systemd/system/lsats-backup.service
[Unit]
Description=LSATS Database Backup
After=postgresql.service

[Service]
Type=oneshot
User=lsats
WorkingDirectory=/opt/lsats-data-hub
ExecStart=/bin/bash /opt/lsats-data-hub/scripts/backup_database.sh
```

```ini
# /etc/systemd/system/lsats-backup.timer
[Unit]
Description=Daily LSATS database backup

[Timer]
OnCalendar=*-*-* 01:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

#### Bronze Ingestion Timer (daily at 2am)

```ini
# /etc/systemd/system/lsats-bronze.service
[Unit]
Description=LSATS Bronze Ingestion
After=postgresql.service lsats-backup.service
# Note: no Restart=on-failure for oneshot — a failed orchestrator would re-run
# ALL scripts from scratch, not just the failed one. Failures are logged to
# journald and detected by the health check script instead.

[Service]
Type=oneshot
User=lsats
WorkingDirectory=/opt/lsats-data-hub
ExecStart=/bin/bash /opt/lsats-data-hub/scripts/orchestrate_bronze.sh
```

```ini
# /etc/systemd/system/lsats-bronze.timer
[Unit]
Description=Daily LSATS bronze ingestion

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

#### Silver Transformation Timer (runs after bronze completes)

```ini
# /etc/systemd/system/lsats-silver.service
[Unit]
Description=LSATS Silver Transformation
After=postgresql.service lsats-bronze.service

[Service]
Type=oneshot
User=lsats
WorkingDirectory=/opt/lsats-data-hub
ExecStart=/bin/bash /opt/lsats-data-hub/scripts/orchestrate_silver.sh
```

```ini
# /etc/systemd/system/lsats-silver.timer
[Unit]
Description=Daily LSATS silver transformation

[Timer]
OnCalendar=*-*-* 04:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

#### Enable All Timers

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lsats-backup.timer
sudo systemctl enable --now lsats-bronze.timer
sudo systemctl enable --now lsats-silver.timer

# Verify timers are scheduled
sudo systemctl list-timers --all | grep lsats

# Check status of last run
sudo systemctl status lsats-bronze.service
sudo journalctl -u lsats-bronze.service --since "1 day ago"
```

### 7.3 Ticket Queue Daemon as systemd Service

systemd is the appropriate choice for a long-running daemon — it handles process supervision, restart-on-failure, and log management. The Python script handles its own `.env` loading via `dotenv`, so `EnvironmentFile` is only used for daemon-specific variables that control the service itself:

```ini
# /etc/systemd/system/lsats-queue-daemon.service
[Unit]
Description=LSATS Ticket Queue Daemon
After=network.target postgresql.service

[Service]
Type=simple
User=lsats
WorkingDirectory=/opt/lsats-data-hub
Environment=DAEMON_REPORT_ID=12345
Environment=DAEMON_INTERVAL=300
ExecStart=/opt/lsats-data-hub/venv/bin/python \
  scripts/ticket_queue/ticket_queue_daemon.py \
  --report-id ${DAEMON_REPORT_ID} \
  --daemon \
  --interval ${DAEMON_INTERVAL} \
  --log /var/log/lsats/queue_daemon.log
Restart=on-failure
RestartSec=30
StartLimitBurst=5
StartLimitIntervalSec=300

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable lsats-queue-daemon
sudo systemctl start lsats-queue-daemon
sudo systemctl status lsats-queue-daemon
```

### 7.4 Updating the Server After Code Changes

```bash
# Standard update procedure
sudo -u lsats /bin/bash -c "
  cd /opt/lsats-data-hub
  git pull origin main
  source venv/bin/activate
  pip install -e '.[all]'  # Only needed if setup.py dependencies changed
"

# If views changed, re-apply:
psql -U lsats_user -d lsats_db -h localhost \
  -f /opt/lsats-data-hub/docker/postgres/views/silver_views.sql

# If tables changed (new column), write a migration:
# psql -U lsats_user -d lsats_db -h localhost -f docker/postgres/migrations/<new>.sql

# Restart daemon if code changed
sudo systemctl restart lsats-queue-daemon
```

### 7.5 Going Forward: Migrations Policy

Now that we have a clean baseline:
- **`production_init.sql`** = canonical truth for extensions, schemas, functions, meta layer
- **`production_schema.sql`** = canonical truth for table structure. Update it when adding tables/columns.
- **`silver_views.sql`** = canonical truth for all views. Re-runnable with `CREATE OR REPLACE`.
- **`migrations/`** = reserved for changes to an *existing production database* (ALTER TABLE, etc.). Write a numbered migration file, run it once on production, then update `production_schema.sql` to match.
- **Never run the old 001–036 migrations** on the new server or any future fresh install.

---

## 8. Log Rotation and Monitoring

### 8.1 Log Rotation

```bash
# /etc/logrotate.d/lsats
/var/log/lsats/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 640 lsats lsats
    dateext
    dateformat -%Y%m%d
    maxsize 50M
}
```

This configuration:
- Rotates daily, keeps 30 days of history
- Compresses old logs (gzip)
- Names rotated files with date suffix for easy parsing
- Caps individual log files at 50 MB (rotates early if exceeded)
- Old logs auto-deleted after 30 days

### 8.2 Monitoring and Alerting

**Orchestrator failure detection** — the `set -euo pipefail` in orchestrator scripts causes them to exit non-zero on any script failure. Cron captures this via `logger`:

```bash
# Check syslog for failures
journalctl -t lsats-bronze --since "1 day ago" --no-pager
journalctl -t lsats-silver --since "1 day ago" --no-pager
```

**Daemon crash detection** — systemd limits restarts and logs failures:

```bash
# Check daemon health
sudo systemctl status lsats-queue-daemon
journalctl -u lsats-queue-daemon --since "1 hour ago" --no-pager

# Alert on repeated crashes (5 failures in 5 minutes triggers stop)
# StartLimitBurst=5 + StartLimitIntervalSec=300 in the service file
```

**Database health check script** (add to cron, run hourly):

```bash
#!/bin/bash
# /opt/lsats-data-hub/scripts/health_check.sh
VENV="/opt/lsats-data-hub/venv/bin/python"

# Check last ingestion was within 36 hours
STALE=$($VENV -c "
from database.adapters.postgres_adapter import PostgresAdapter
import os
from dotenv import load_dotenv
load_dotenv('/opt/lsats-data-hub/.env')
db = PostgresAdapter(os.environ['DATABASE_URL'])
df = db.query_to_dataframe(\"\"\"
  SELECT source_system, MAX(ingested_at) as last_run
  FROM bronze.raw_entities
  GROUP BY source_system
  HAVING MAX(ingested_at) < NOW() - INTERVAL '36 hours'
\"\"\")
db.close()
if len(df) > 0:
    print(df.to_string(index=False))
")

if [ -n "$STALE" ]; then
    echo "STALE INGESTION DETECTED:" | logger -t lsats-health -p user.warning
    echo "$STALE" | logger -t lsats-health -p user.warning
fi
```

---

## 9. Backup Strategy

The backup strategy has **two layers**:

1. **Daily backup** (1am, before bronze): Full database snapshot for disaster recovery. Retained 7 days locally, transferred to remote storage.
2. **Pre-silver snapshot** (inside `orchestrate_silver.sh`, before any transforms): Captures post-bronze state so silver failures can be reverted without re-running bronze ingestion. Only the 2 most recent are kept (they're lightweight safety nets, not long-term backups).

**Recovery scenarios:**
- Silver transform fails → Restore pre-silver snapshot, fix script, re-run `orchestrate_silver.sh` only
- Bronze ingestion corrupts data → Restore daily backup from before bronze ran
- Catastrophic failure → Restore from remote backup

### 9.1 Daily pg_dump with Compression

```bash
#!/bin/bash
# /opt/lsats-data-hub/scripts/backup_database.sh
set -euo pipefail

export PGPASSFILE="/opt/lsats-data-hub/.pgpass"
BACKUP_DIR="/var/backups/lsats"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Custom format dump (supports selective restore, built-in compression)
# ~1-2 GB compressed for current dataset size
pg_dump -U lsats_user -h localhost lsats_db \
  --format=custom --compress=9 \
  --file="${BACKUP_DIR}/lsats_db_${TIMESTAMP}.dump"

# Remove local backups older than 7 days (remote has longer retention)
find "$BACKUP_DIR" -name "lsats_db_*.dump" -mtime +7 -delete

echo "Backup complete: ${BACKUP_DIR}/lsats_db_${TIMESTAMP}.dump"
```

### 9.2 Remote Backup Transfer

Choose one approach based on your infrastructure:

**Option A — NFS mount:**
```bash
# Mount in /etc/fstab
# nfs-server:/backups/lsats  /mnt/backup-lsats  nfs  defaults,noatime  0  0

# Add to backup script:
cp "${BACKUP_DIR}/lsats_db_${TIMESTAMP}.dump" /mnt/backup-lsats/
```

**Option B — rsync over SSH:**
```bash
# Add to cron after backup completes
rsync -avz --remove-source-files \
  "${BACKUP_DIR}/lsats_db_${TIMESTAMP}.dump" \
  backup-user@remote-server:/backups/lsats/
```

**Option C — rclone to cloud/remote:**
```bash
# Configure rclone target once: rclone config
rclone copy "${BACKUP_DIR}/lsats_db_${TIMESTAMP}.dump" \
  remote:lsats-backups/ --max-age 24h
```

### 9.3 Restore Procedure

```bash
# Restore from daily backup or pre-silver snapshot (same command, different file)
pg_restore -U lsats_user -d lsats_db -h localhost \
  --clean --if-exists \
  /var/backups/lsats/lsats_db_YYYYMMDD.dump

# Quick restore from pre-silver snapshot (most common recovery)
# 1. Find the most recent pre-silver snapshot
ls -lt /var/backups/lsats/pre_silver_*.dump | head -1

# 2. Restore it
pg_restore -U lsats_user -d lsats_db -h localhost \
  --clean --if-exists \
  /var/backups/lsats/pre_silver_YYYYMMDD_HHMMSS.dump

# 3. Fix the failing transform script, then re-run silver only
bash /opt/lsats-data-hub/scripts/orchestrate_silver.sh
```

---

## Key Files Reference

| File | Purpose |
|---|---|
| `docker/postgres/production_init.sql` | **[TO CREATE]** Extensions, schemas, functions, meta tables — works with both Docker and bare PG |
| `docker/postgres/production_schema.sql` | **[TO CREATE]** Cleaned pg_dump of all table definitions (no legacy) |
| `docker/postgres/views/silver_views.sql` | **[TO UPDATE]** Remove all `_legacy` table references |
| `setup.py` | **[TO UPDATE]** Fix extras: add `[compliance]`, fix `[all]` and `[database]` |
| `scripts/orchestrate_bronze.sh` | **[TO CREATE]** Bronze ingestion orchestrator |
| `scripts/orchestrate_silver.sh` | **[TO CREATE]** Silver transformation orchestrator |
| `scripts/backup_database.sh` | **[TO CREATE]** Daily pg_dump backup |
| `scripts/health_check.sh` | **[TO CREATE]** Ingestion staleness monitor |
| `/opt/lsats-data-hub/.env` | Runtime config — never in git, mode 600 |
| `/opt/lsats-sandbox/.env` | Sandbox config — never in git, mode 600 |
| `/opt/lsats-data-hub/credentials.json` | Google OAuth — never in git, mode 600 |
| `/opt/lsats-data-hub/token.json` | Google token — never in git, mode 600 |
| `/var/log/lsats/` | Log directory owned by `lsats` user |
| `/var/backups/lsats/` | Local backup directory (7-day retention) |
| `/etc/systemd/system/lsats-queue-daemon.service` | Ticket queue daemon service |
| `/etc/systemd/system/lsats-backup.{service,timer}` | Daily backup service + timer |
| `/etc/systemd/system/lsats-bronze.{service,timer}` | Bronze ingestion service + timer |
| `/etc/systemd/system/lsats-silver.{service,timer}` | Silver transformation service + timer |
| `/etc/logrotate.d/lsats` | Log rotation configuration |

---

## Appendix A: Pre-Mortem Risk Analysis

### High Risk (likely to cause delays)

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| 1 | Silver transforms fail on empty database (edge cases with empty DataFrames, FK ordering) | Blocks Phase 8 | **Two layers:** (1) Pre-test full pipeline on fresh Docker before deploying (Phase 0.8). (2) `orchestrate_silver.sh` takes a pre-silver pg_dump snapshot after bronze completes — if transforms fail in production, restore snapshot, fix, re-run silver only (no bronze re-run needed) |
| 2 | `pg_dump` cleanup misses a legacy reference (index, trigger, FK) causing `production_schema.sql` import failure | Blocks Phase 4 | After cleanup, test in Docker. The `lab_managers` FK needs retargeting, not deletion |
| 3 | LDAP port 636 blocked by VLAN firewall | Blocks AD + MCommunity ingestion | Test in Phase 6 before ingestion. File firewall request immediately if blocked — deploy non-LDAP sources while waiting |
| 4 | First full bronze ingestion takes hours (TDX enrichment scripts are per-record API calls) | Delays Phase 7 | Budget a full day. Non-TDX sources can run in parallel |

### Medium Risk (recoverable)

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| 5 | Backup disk space: two pg_dump formats at ~5 GB each, 7-day retention = ~70 GB (exceeds `/var` free space) | Backups fill disk | Keep only custom format dump (skip gzip duplicate). Store in `/home` if `/var` is tight |
| 6 | Google OAuth token not portable to headless server | Blocks future compliance scripts | Low priority — not needed for database deployment. Test token transfer; if it fails, use `--no-browser` flow via SSH tunnel |
| 7 | ~~Python 3.11 not in default Ubuntu repos~~ | ~~Blocks Phase 5~~ | **RESOLVED** — Ubuntu 24.04 confirmed, ships Python 3.12.3 as system default. No PPA needed. |

### Low Risk (unlikely but noted)

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| 8 | `nologin` shell + cron compatibility | Cron jobs may not run | Plan already uses systemd timers (don't care about user shell). Cron is not used. |
| 9 | Shared database between sandbox and production directories | Sandbox transforms could overwrite production silver data | **Accepted risk** — TDX sandbox is a cutoff-date copy, non-TDX sources are identical across both. Bronze `source_system` tagging distinguishes TDX records. Use `createdb --template` for isolated testing if needed. |

---

## Appendix B: Current Database State (from dev Docker, 2026-02-24)

### Silver Table Inventory (33 tables, 29 canonical + 4 to exclude)

| Table | Size | Status |
|---|---|---|
| `ad_computers` | 65 MB | Keep |
| `ad_groups` | 42 MB | Keep |
| `ad_labs` | 216 kB | Keep |
| `ad_organizational_units` | 3 MB | Keep |
| `ad_users` | 1.9 GB | Keep |
| `award_labs` | 576 kB | Keep |
| `computer_attributes` | 121 MB | Keep |
| `computer_groups` | 21 MB | Keep |
| `computers` | 440 MB | Keep |
| `departments` | 34 MB | Keep |
| `group_members` | 339 MB | Keep |
| `group_owners` | 28 MB | Keep |
| `groups` | 130 MB | Keep |
| `groups_legacy` | 64 MB | **EXCLUDE** |
| `keyconfigure_computers` | 13 MB | Keep |
| `keyconfigure_computers_backup` | 3 kB | **EXCLUDE** |
| `lab_awards` | 1.4 MB | Keep |
| `lab_awards_legacy` | 2.7 MB | **EXCLUDE** |
| `lab_computers` | 13 MB | Keep |
| `lab_managers` | 448 kB | Keep (fix FK) |
| `lab_members` | 20 MB | Keep |
| `labs` | 2 MB | Keep |
| `mcommunity_groups` | 109 MB | Keep |
| `mcommunity_users` | 1.5 GB | Keep |
| `mcommunity_users_backup_20250118` | 8 kB | **EXCLUDE** |
| `tdx_assets` | 215 MB | Keep |
| `tdx_departments` | 30 MB | Keep |
| `tdx_labs` | 376 kB | Keep |
| `tdx_users` | 850 MB | Keep |
| `umapi_departments` | 6 MB | Keep |
| `umapi_employees` | 197 MB | Keep |
| `users` | 2 GB | Keep |
| `users_legacy` | 169 MB | **EXCLUDE** |

### Foreign Key to Fix

| Table | Constraint | Currently References | Should Reference |
|---|---|---|---|
| `silver.lab_managers` | `lab_managers_manager_uniqname_fkey_legacy` | `silver.users_legacy(uniqname)` | `silver.users(uniqname)` |

### Silver Layer Refactor Status (Completed 2026-01-12)

The three-phase silver layer refactor normalized LDAP-style field names to business-friendly canonical names. All 18 old columns have been dropped. All transformation scripts write to canonical columns. No code changes are needed for deployment — only the schema and views need to be cleaned of legacy artifacts.

| Phase | Entity | Columns Dropped | Status |
|---|---|---|---|
| Phase 1 | Users | 13 | Complete |
| Phase 2 | Departments | 3 | Complete |
| Phase 3 | Groups | 2 | Complete |
| Phase 4 | Computers/OUs | 0 (already canonical) | Assessment only |
