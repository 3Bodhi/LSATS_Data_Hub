# LSATS Data Hub — Deployment Checklist

**Companion to:** `ubuntu_server_deployment_plan.md` (v2)
**Created:** 2026-02-24

Use this checklist to track progress across the multi-phase deployment. Check off items as they are completed. Each item references the relevant plan section for details.

> **Directory name change (2026-02-25):** The actual server directories differ from the plan's original names.
> - Production: `/opt/LSATS_Data_Hub` (plan said `/opt/lsats-data-hub`)
> - Sandbox/testing: `/opt/LSATS_testing` (plan said `/opt/lsats-sandbox`)
> All checklist items below use the actual directory names.

---

## Phase 0: Pre-Deployment (on Mac, before touching the server)

*These produce commits to `main`. See §5 Phase 0 and §1.1–1.3.*

### 0A — Code Changes

- [x] **0.1** Review `feature/lab-notes/data` branch — confirm feature-complete for merge
- [x] **0.2** Fix `setup.py` extras (§1.3)
  - [x] Add `[compliance]` extra
  - [x] Fix `[database]` — add `sqlalchemy`, `psycopg2-binary`, `ldap3`, `python-dateutil`, `keyring`
  - [x] Fix `[all]` — union of all extras, remove transitive deps
  - [x] Update `python_requires=">=3.11"`
- [x] **0.3** Dump current schema: `pg_dump --schema-only` from Docker → `production_schema.sql`
- [x] **0.4** Clean `production_schema.sql` (§3.2)
  - [x] Remove `silver.users_legacy`
  - [x] Remove `silver.groups_legacy`
  - [x] Remove `silver.lab_awards_legacy`
  - [x] Remove `silver.keyconfigure_computers_backup`
  - [x] Remove `silver.mcommunity_users_backup_20250118`
  - [x] Retarget FK `lab_managers_manager_uniqname_fkey_legacy` → `silver.users(uniqname)`
  - [x] Remove temp indexes or artifacts
  - [x] Remove `v_lab_active_awards_legacy` view definition
- [x] **0.5** Create `production_init.sql` (§3.4)
  - [x] Extract extensions, schemas, helper functions, meta tables from `init.sql`
  - [x] No `\i` directives, no Docker-specific paths
- [x] **0.6** Update `silver_views.sql` — replace legacy references (§3.3)
  - [x] `v_lab_summary`: `users_legacy` → `users`
  - [x] `v_lab_groups`: `groups_legacy` → `groups`
  - [x] `v_lab_members_detailed`: `users_legacy` → `users`
  - [x] `v_lab_active_awards_legacy` → rename to `v_lab_active_awards`, reference `lab_awards`
  - [x] `v_eligible_lab_members`: `users_legacy` → `users`
- [ ] **0.7** Update `docker/postgres/init.sql` to source new files (§3.4) *(Docker-only, deferred — not needed for server deployment)*

### 0B — Local Validation

- [x] **0.8** Test clean schema in Docker
  - [x] `docker-compose down -v && docker-compose up -d`
  - [x] Verify all schemas created (bronze, silver, gold, meta)
  - [x] Verify all 28 silver tables created (no legacy)
  - [x] Verify all views load without errors
  - [x] Verify zero `%legacy%` tables or FKs

### 0C — Merge and Tag

- [x] **0.9** Merge `feature/lab-notes/data` → `main`
- [x] **0.10** Tag as `v1.0.0-production`
- [x] **0.10** Push `main` and tag to remote

---

## Phase 1: Server Preparation

*SSH into Ubuntu 24.04 server. See §5 Phase 1 and §2.*

- [x] **1.1** System update: `sudo apt update && sudo apt upgrade -y`
- [x] **1.2** Install system packages
  - [x] `postgresql postgresql-contrib` (Ubuntu 24.04 ships PostgreSQL 16)
  - [x] `python3 python3-venv python3-pip` (Ubuntu 24.04 ships Python 3.12.3 — no PPA needed)
  - [x] `gcc build-essential libpq-dev python3-dev`
  - [x] `libldap2-dev libsasl2-dev`
  - [x] `git curl logrotate`
- [x] **1.3** Create `lsats` service account (uid=996, gid=1002)
- [x] **1.4** Create directories with correct ownership
  - [x] `/opt/LSATS_Data_Hub` (production)
  - [x] `/opt/LSATS_testing` (sandbox/testing)
  - [x] `/var/log/lsats` (logs)
  - [x] `/var/backups/lsats` (backups)

---

## Phase 2: Clone Repository

*See §5 Phase 2.*

- [x] **2.1** Clone production: `/opt/LSATS_Data_Hub` → `main` branch
- [x] **2.2** Clone sandbox/testing: `/opt/LSATS_testing` → `main` branch

---

## Phase 3: Transfer Credentials ✓

*See §5 Phase 3.*

> **Path changes from original plan:**
> - Config: `/etc/LSATS_Data_Hub/hub.conf` (production), `hub_sandbox.conf` (sandbox)
> - Data: `DATA_PATH=/var/lib/lsats/data` (not `/opt`)
> - Token: `TOKEN_FILE=/var/lib/lsats/token.json` (runtime state, not `/opt`)
> - `/opt/LSATS_Data_Hub` is `root:root 755` — not owned by `lsats`
>
> **Permission scheme:**
> | Path | Owner | Mode |
> |---|---|---|
> | `/etc/LSATS_Data_Hub/` | `root:lsats` | `750` |
> | `/etc/LSATS_Data_Hub/hub.conf` | `root:lsats` | `640` |
> | `/etc/LSATS_Data_Hub/hub_sandbox.conf` | `root:lsats` | `640` |
> | `/opt/LSATS_Data_Hub/` | `root:root` | `755` |
> | `/opt/LSATS_Data_Hub/.pgpass` | `lsats:lsats` | `600` |
> | `/opt/LSATS_Data_Hub/credentials.json` | `root:lsats` | `640` |
> | `/var/lib/lsats/token.json` | `lsats:lsats` | `600` |
> | `/var/lib/lsats/data/` | `lsats:lsats` | `755` |
> | `/var/log/lsats/` | `lsats:lsats` | `755` |
> | `/var/backups/lsats/` | `lsats:lsats` | `750` |

- [x] **3.1** Create `/etc/LSATS_Data_Hub/` directory (`root:lsats 750`)
- [x] **3.2** SCP `.env` to server and place as `hub.conf` (`root:lsats 640`)
- [x] **3.3** Create `hub_sandbox.conf` from production copy (`root:lsats 640`)
- [x] **3.4** SCP `credentials.json` → `/opt/LSATS_Data_Hub/credentials.json` (`root:lsats 640`)
- [x] **3.5** Edit production `hub.conf`
  - [x] `DATABASE_URL` → `postgresql://lsats_user@localhost:5432/lsats_db` (no password — `.pgpass` supplies it)
  - [x] `TDX_BASE_URL` → `https://teamdynamix.umich.edu/TDWebApi/api`
  - [x] `LOG_FILE` → `/var/log/lsats/lsats_database.log`
  - [x] `DATA_PATH` → `/var/lib/lsats/data`
  - [x] `TOKEN_FILE` → `/var/lib/lsats/token.json`
  - [x] `CREDENTIALS_FILE` → `/opt/LSATS_Data_Hub/credentials.json`
- [x] **3.6** Edit sandbox `hub_sandbox.conf`
  - [x] `TDX_BASE_URL` → `https://teamdynamix.umich.edu/SBTDWebApi/api`
  - [x] `LOG_FILE` → `/var/log/lsats/sandbox.log`
  - [x] `DATA_PATH` → `/var/lib/lsats/data`
- [x] **3.7** Clean up `/tmp` transfer files
- [x] **3.8** Verified `lsats` user can read `hub.conf` (`sudo -u lsats cat /etc/LSATS_Data_Hub/hub.conf`)

---

## Phase 4: PostgreSQL Setup

*See §5 Phase 4 and §2.2–2.3.*

> **Password strategy:** No plaintext passwords anywhere. `.pgpass` is the sole credential store for PostgreSQL. `hub.conf` contains only a passwordless `DATABASE_URL`. systemd unit files set `PGPASSFILE=/opt/LSATS_Data_Hub/.pgpass` so all scripts and `psql` calls pick it up automatically.

### 4A — Users, Database, and `.pgpass`

- [ ] **4.1** Create roles and database (peer auth as `postgres` — no password needed)
  ```bash
  sudo -u postgres psql <<'EOF'
  CREATE USER lsats_user WITH PASSWORD 'choose_strong_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN;
  CREATE USER lsats_readonly WITH PASSWORD 'choose_strong_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN;
  CREATE DATABASE lsats_db OWNER lsats_user ENCODING 'UTF8';
  \du
  \l
  EOF
  ```
- [ ] **4.2** Create `/var/lib/lsats/data/` directory
  ```bash
  sudo mkdir -p /var/lib/lsats/data
  sudo chown -R lsats:lsats /var/lib/lsats
  sudo chmod 755 /var/lib/lsats /var/lib/lsats/data
  ```
- [ ] **4.3** Write `.pgpass` for `lsats` service account (password stored here only)
  ```bash
  sudo -u lsats bash -c \
    'echo "localhost:5432:lsats_db:lsats_user:choose_strong_password" \
    > /opt/LSATS_Data_Hub/.pgpass'
  sudo chmod 600 /opt/LSATS_Data_Hub/.pgpass
  sudo chown lsats:lsats /opt/LSATS_Data_Hub/.pgpass
  ```
- [ ] **4.4** Verify `.pgpass` works — must connect without password prompt
  ```bash
  sudo -u lsats PGPASSFILE=/opt/LSATS_Data_Hub/.pgpass \
    psql -U lsats_user -d lsats_db -h localhost -c '\conninfo'
  # Expected: "You are connected to database 'lsats_db' as user 'lsats_user'..."
  # If prompted for password: check pg_hba.conf (step 4.8) and .pgpass contents/permissions
  ```

### 4B — Schema Import

- [ ] **4.5** Run `production_init.sql` (extensions, schemas, helper functions, meta tables)
  ```bash
  sudo -u lsats PGPASSFILE=/opt/LSATS_Data_Hub/.pgpass \
    psql -U lsats_user -d lsats_db -h localhost \
    -f /opt/LSATS_Data_Hub/docker/postgres/production_init.sql
  ```
- [ ] **4.6** Run `production_schema.sql` (all table definitions)
  ```bash
  sudo -u lsats PGPASSFILE=/opt/LSATS_Data_Hub/.pgpass \
    psql -U lsats_user -d lsats_db -h localhost \
    -f /opt/LSATS_Data_Hub/docker/postgres/production_schema.sql
  ```
- [ ] **4.7** Run `silver_views.sql` (all views)
  ```bash
  sudo -u lsats PGPASSFILE=/opt/LSATS_Data_Hub/.pgpass \
    psql -U lsats_user -d lsats_db -h localhost \
    -f /opt/LSATS_Data_Hub/docker/postgres/views/silver_views.sql
  ```
- [x] **4.8** Grant read-only access to `lsats_readonly` + set default privileges

### 4C — Authentication and Tuning

- [x] **4.9** Confirmed `pg_hba.conf` — `scram-sha-256` for `127.0.0.1/32` and `::1/128` (Ubuntu 24.04 default, no changes needed)
- [x] **4.10** Tuned `postgresql.conf` for 15GB RAM
  - [x] `shared_buffers = 4GB`
  - [x] `effective_cache_size = 11GB`
  - [x] `work_mem = 64MB`
  - [x] `maintenance_work_mem = 512MB`
  - [x] `max_connections = 50`
  - [x] `random_page_cost = 1.1`
- [x] **4.11** Restarted PostgreSQL — connection verified (PostgreSQL 16.11, TLSv1.3)

### 4D — Verification

- [x] **4.12** 5 schemas present: bronze, silver, gold, meta, public (`lsats_user` owns all except public)
- [x] **4.13** 28 silver tables confirmed (checklist corrected from 29 — schema has 28)
- [x] **4.14** 19 silver views confirmed
- [x] **4.15** Zero legacy tables
- [x] **4.16** Zero legacy foreign keys

---

## Phase 5: Python Environment ✓

*See §5 Phase 5.*

> **Sandbox deferred:** `/opt/LSATS_testing` venv not created — sandbox setup postponed pending decision on CI/CD and development workflow. Only production deployment in scope for now.

- [x] **5.1** Production venv created: Python 3.12.3 at `/opt/LSATS_Data_Hub/venv`
- [x] **5.2** Production deps installed: `pip install -e '.[all]'` — `lsats-data-hub 0.1.0` confirmed
  - [x] SQLAlchemy 2.0.47 ✓
  - [x] psycopg2-binary 2.9.11 ✓
  - [x] ldap3 2.9.1 ✓
  - [x] google-api-python-client 2.190.0 ✓
  - [x] requests 2.32.5 ✓
- [ ] **5.3** *(deferred)* Create sandbox venv: `/opt/LSATS_testing/venv`
- [ ] **5.4** *(deferred)* Install sandbox deps

---

## Phase 6: Validate Connectivity ✓

*See §5 Phase 6 and §6.3. CRITICAL — do this before any ingestion.*

> **Note:** `load_dotenv` must be called before importing facades so `os.environ` is populated before facade `__init__` reads credentials. Production scripts handle this via systemd `EnvironmentFile=` — not `load_dotenv` — so this is only relevant for manual one-off tests.

- [x] **6.1** Database connection via PostgresAdapter — OK (`PGPASSFILE` honored by `libpq` transparently)
- [x] **6.2** TDX connection — OK, connected to `https://teamdynamix.umich.edu/TDWebApi/api` (production) ✓
- [x] **6.3** LDAP port 636 — `ldap.umich.edu:636` succeeded (141.211.243.129) ✓
- [x] **6.4** UMich API OAuth endpoint — `gw.api.it.umich.edu` returns 200 ✓
- [x] **6.5** HTTPS to `teamdynamix.umich.edu` — 200 ✓
- [x] **6.6** ~~`apigw.it.umich.edu`~~ Corrected: actual endpoint is `gw.api.it.umich.edu` (matches `UM_BASE_URL` in `hub.conf`)

---

## Phase 7: Bronze Ingestion (Sandbox First)

*See §5 Phase 7. Budget a full day — TDX enrichment is slow.*

### 7A — Non-TDX Sources (from `/opt/LSATS_testing/`)

- [ ] **7.1** UMich API departments (`001_ingest_umapi_departments.py`)
- [ ] **7.2** UMich API employees (`009_ingest_umapi_employees.py`)
- [ ] **7.3** MCommunity users (`007_ingest_mcommunity_users.py`)
- [ ] **7.4** MCommunity groups (`005_ingest_mcommunity_groups.py`)
- [ ] **7.5** AD users (`004_ingest_ad_users.py`)
- [ ] **7.6** AD groups (`005_ingest_ad_groups.py`)
- [ ] **7.7** AD organizational units (`006_ingest_ad_organizational_units.py`)
- [ ] **7.8** AD computers (`007_ingest_ad_computers.py`)
- [ ] **7.9** Document — lab awards (`008_ingest_lab_awards.py`)
- [ ] **7.10** Document — keyconfigure computers (`009_ingest_keyconfigure_computers.py`)

### 7B — TDX Sources (from `/opt/LSATS_testing/` — sandbox TDX endpoint)

- [ ] **7.11** TDX departments (`001_ingest_tdx_departments.py`)
- [ ] **7.12** TDX users (`002_ingest_tdx_users.py`)
- [ ] **7.13** TDX assets (`003_ingest_tdx_assets.py`)

### 7C — TDX Enrichment (slow — per-record API calls)

- [ ] **7.14** Enrich TDX departments (`010_enrich_tdx_departments.py`)
- [ ] **7.15** Enrich TDX users (`010_enrich_tdx_users.py`)
- [ ] **7.16** Enrich TDX assets (`011_enrich_tdx_assets.py`)

### 7D — Verification

- [ ] **7.17** Verify bronze counts by `entity_type` and `source_system`
- [ ] **7.18** Check `meta.ingestion_runs` for all sources showing `completed`

---

## Phase 8: Silver Transformations

*See §5 Phase 8. Pre-silver snapshot is taken automatically by the orchestrator.*

### 8A — Tier 1: Source-Specific

- [ ] **8.1** TDX users (`001_transform_tdx_users.py`)
- [ ] **8.2** TDX departments (`002_transform_tdx_departments.py`)
- [ ] **8.3** UMich API employees (`002_transform_umapi_employees.py`)
- [ ] **8.4** AD groups (`003_transform_ad_groups.py`)
- [ ] **8.5** TDX assets (`004_transform_tdx_assets.py`)
- [ ] **8.6** AD users (`004_transform_ad_users.py`)
- [ ] **8.7** MCommunity users (`006_transform_mcommunity_users.py`)
- [ ] **8.8** KeyConfigure computers (`006_transform_keyconfigure_computers.py`)
- [ ] **8.9** AD computers (`007_transform_ad_computers.py`)
- [ ] **8.10** Lab awards (`008_transform_lab_awards.py`)

### 8B — Tier 2: Consolidated (depends on Tier 1)

- [ ] **8.11** Departments (`010_transform_departments.py`)
- [ ] **8.12** Groups (`011_transform_groups.py`)
- [ ] **8.13** Users (`012_transform_users.py`)
- [ ] **8.14** Computers (`013_transform_computers.py`)

### 8C — Tier 3: Composite (depends on Tier 2)

- [ ] **8.15** Lab members (`013_transform_lab_members.py`)
- [ ] **8.16** Lab computers (`014_transform_lab_computers.py`)
- [ ] **8.17** Lab managers (`014_transform_lab_managers.py`)
- [ ] **8.18** TDX labs aggregate (`014_aggregate_tdx_labs.py`)
- [ ] **8.19** Award labs aggregate (`015_aggregate_award_labs.py`)
- [ ] **8.20** AD labs aggregate (`016_aggregate_ad_labs.py`)

### 8D — Verification

- [ ] **8.21** Check silver quality scores (departments, users, groups, computers, labs)
- [ ] **8.22** Spot-check views return data (`v_lab_summary`, `v_labs_monitored`, etc.)
- [ ] **8.23** Verify `preferred_name` column populated in `silver.users`

---

## Phase 9: Switch to Production TDX

*See §5 Phase 9. Run from `/opt/LSATS_Data_Hub/` (production directory).*

- [ ] **9.1** Re-run TDX bronze with production endpoint
  - [ ] `001_ingest_tdx_departments.py`
  - [ ] `002_ingest_tdx_users.py`
  - [ ] `003_ingest_tdx_assets.py`
  - [ ] `010_enrich_tdx_departments.py`
  - [ ] `010_enrich_tdx_users.py`
  - [ ] `011_enrich_tdx_assets.py`
- [ ] **9.2** Re-run TDX silver transforms
  - [ ] `001_transform_tdx_users.py`
  - [ ] `002_transform_tdx_departments.py`
  - [ ] `004_transform_tdx_assets.py`
  - [ ] `014_aggregate_tdx_labs.py`
- [ ] **9.3** Re-run consolidated transforms (picks up production TDX data)
  - [ ] `010_transform_departments.py`
  - [ ] `012_transform_users.py`
  - [ ] `013_transform_computers.py`

---

## Phase 10: Post-Deployment Validation

*See §6.*

- [ ] **10.1** Run schema validation queries (§6.1)
  - [ ] All 4 schemas exist
  - [ ] 28 silver tables, ~19 views
  - [ ] Zero legacy tables or FKs
  - [ ] Gold schema exists (empty)
- [ ] **10.2** Run data quality validation (§6.2)
  - [ ] Bronze completeness by source
  - [ ] Silver quality scores ≥ 0.8 for majority of records
  - [ ] Views return non-zero counts
- [ ] **10.3** Run network validation (§6.3)
  - [ ] TDX HTTPS
  - [ ] UMich API HTTPS
  - [ ] LDAPS port 636

---

## Phase 11: Operational Setup

*See §7, §8, §9.*

### 11A — Create Scripts

- [ ] **11.1** Create `scripts/orchestrate_bronze.sh` (§7.1)
- [ ] **11.2** Create `scripts/orchestrate_silver.sh` (§7.1 — includes pre-silver snapshot)
- [ ] **11.3** Create `scripts/backup_database.sh` (§9.1)
- [ ] **11.4** Create `scripts/health_check.sh` (§8.2)
- [ ] **11.5** Make all scripts executable: `chmod +x scripts/*.sh`

### 11B — systemd Timers

- [ ] **11.6** Create `lsats-backup.service` + `lsats-backup.timer` (1am daily)
- [ ] **11.7** Create `lsats-bronze.service` + `lsats-bronze.timer` (2am daily)
- [ ] **11.8** Create `lsats-silver.service` + `lsats-silver.timer` (4am daily)
- [ ] **11.9** `sudo systemctl daemon-reload`
- [ ] **11.10** Enable all timers: `sudo systemctl enable --now lsats-{backup,bronze,silver}.timer`
- [ ] **11.11** Verify timers: `sudo systemctl list-timers --all | grep lsats`

### 11C — Ticket Queue Daemon

- [ ] **11.12** Create `lsats-queue-daemon.service` (§7.3)
- [ ] **11.13** Set `DAEMON_REPORT_ID` in service file
- [ ] **11.14** Enable and start: `sudo systemctl enable --now lsats-queue-daemon`
- [ ] **11.15** Verify running: `sudo systemctl status lsats-queue-daemon`

### 11D — Log Rotation

- [ ] **11.16** Create `/etc/logrotate.d/lsats` (§8.1)
- [ ] **11.17** Test: `sudo logrotate --debug /etc/logrotate.d/lsats`

### 11E — Backups

- [ ] **11.18** Run `backup_database.sh` manually — verify dump created
- [ ] **11.19** Configure remote backup transfer (NFS/rsync/rclone — pick one) (§9.2)
- [ ] **11.20** Test restore procedure on a throwaway database (§9.3)

---

## Phase 12: First Automated Run Validation

*Wait for the first full automated cycle (backup → bronze → silver).*

- [ ] **12.1** After 1am: verify backup dump exists in `/var/backups/lsats/`
- [ ] **12.2** After 2am: check `journalctl -u lsats-bronze` for success
- [ ] **12.3** After 4am: check `journalctl -u lsats-silver` for success
- [ ] **12.4** Verify pre-silver snapshot created in `/var/backups/lsats/pre_silver_*.dump`
- [ ] **12.5** Check `meta.ingestion_runs` for today's entries
- [ ] **12.6** Check `meta.daemon_action_log` for queue daemon activity
- [ ] **12.7** Run `health_check.sh` — no stale ingestion warnings

---

## Quick Reference: Recovery Procedures

If something goes wrong, refer to these sections:

| Problem | Action | Plan Reference |
|---|---|---|
| Silver transform fails | Restore pre-silver snapshot, fix, re-run silver | §9.3 |
| Bronze corrupts data | Restore daily backup (1am) | §9.3 |
| Queue daemon crashes | Check `journalctl -u lsats-queue-daemon`, restart | §7.3 |
| Timer didn't fire | `systemctl list-timers`, check `Persistent=true` | §7.2 |
| LDAP blocked | File firewall request for port 636, deploy non-LDAP first | Appendix A, Risk #3 |
| Disk space low | Check `/var/backups/lsats/`, prune old dumps | §9.1 |
| Need to update code | `git -C /opt/LSATS_Data_Hub pull`, `pip install -e '.[all]'`, restart daemon | §7.4 |
| New migration needed | Write migration, run once, update `production_schema.sql` | §7.5 |
