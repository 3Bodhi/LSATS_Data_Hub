# LSATS Data Hub — Deployment Checklist

**Companion to:** `ubuntu_server_deployment_plan.md` (v2)
**Created:** 2026-02-24

Use this checklist to track progress across the multi-phase deployment. Check off items as they are completed. Each item references the relevant plan section for details.

---

## Phase 0: Pre-Deployment (on Mac, before touching the server)

*These produce commits to `main`. See §5 Phase 0 and §1.1–1.3.*

### 0A — Code Changes

- [ ] **0.1** Review `feature/lab-notes/data` branch — confirm feature-complete for merge
- [ ] **0.2** Fix `setup.py` extras (§1.3)
  - [ ] Add `[compliance]` extra
  - [ ] Fix `[database]` — add `sqlalchemy`, `psycopg2-binary`, `ldap3`, `python-dateutil`, `keyring`
  - [ ] Fix `[all]` — union of all extras, remove transitive deps
  - [ ] Update `python_requires=">=3.11"`
- [ ] **0.3** Dump current schema: `pg_dump --schema-only` from Docker → `production_schema.sql`
- [ ] **0.4** Clean `production_schema.sql` (§3.2)
  - [ ] Remove `silver.users_legacy`
  - [ ] Remove `silver.groups_legacy`
  - [ ] Remove `silver.lab_awards_legacy`
  - [ ] Remove `silver.keyconfigure_computers_backup`
  - [ ] Remove `silver.mcommunity_users_backup_20250118`
  - [ ] Retarget FK `lab_managers_manager_uniqname_fkey_legacy` → `silver.users(uniqname)`
  - [ ] Remove temp indexes or artifacts
  - [ ] Remove `v_lab_active_awards_legacy` view definition
- [ ] **0.5** Create `production_init.sql` (§3.4)
  - [ ] Extract extensions, schemas, helper functions, meta tables from `init.sql`
  - [ ] No `\i` directives, no Docker-specific paths
- [ ] **0.6** Update `silver_views.sql` — replace legacy references (§3.3)
  - [ ] `v_lab_summary`: `users_legacy` → `users`
  - [ ] `v_lab_groups`: `groups_legacy` → `groups`
  - [ ] `v_lab_members_detailed`: `users_legacy` → `users`
  - [ ] `v_lab_active_awards_legacy` → rename to `v_lab_active_awards`, reference `lab_awards`
  - [ ] `v_eligible_lab_members`: `users_legacy` → `users`
- [ ] **0.7** Update `docker/postgres/init.sql` to source new files (§3.4)

### 0B — Local Validation

- [ ] **0.8** Test clean schema in Docker
  - [ ] `docker-compose down -v && docker-compose up -d`
  - [ ] Verify all schemas created (bronze, silver, gold, meta)
  - [ ] Verify all 29 silver tables created (no legacy)
  - [ ] Verify all views load without errors
  - [ ] Verify zero `%legacy%` tables or FKs

### 0C — Merge and Tag

- [ ] **0.9** Merge `feature/lab-notes/data` → `main`
- [ ] **0.10** Tag as `v1.0.0-production`
- [ ] **0.10** Push `main` and tag to remote

---

## Phase 1: Server Preparation

*SSH into Ubuntu 24.04 server. See §5 Phase 1 and §2.*

- [ ] **1.1** System update: `sudo apt update && sudo apt upgrade -y`
- [ ] **1.2** Install system packages
  - [ ] `postgresql postgresql-contrib` (Ubuntu 24.04 ships PostgreSQL 16)
  - [ ] `python3 python3-venv python3-pip` (Ubuntu 24.04 ships Python 3.12.3 — no PPA needed)
  - [ ] `gcc build-essential libpq-dev python3-dev`
  - [ ] `libldap2-dev libsasl2-dev`
  - [ ] `git curl logrotate`
- [ ] **1.3** Create `lsats` service account
- [ ] **1.4** Create directories with correct ownership
  - [ ] `/opt/lsats-data-hub` (production)
  - [ ] `/opt/lsats-sandbox` (sandbox)
  - [ ] `/var/log/lsats` (logs)
  - [ ] `/var/backups/lsats` (backups)

---

## Phase 2: Clone Repository

*See §5 Phase 2.*

- [ ] **2.1** Clone production: `/opt/lsats-data-hub` → `main` branch
- [ ] **2.2** Clone sandbox: `/opt/lsats-sandbox` → `main` branch

---

## Phase 3: Transfer Credentials

*See §5 Phase 3.*

- [ ] **3.1** SCP `.env` file from Mac to server
- [ ] **3.2** SCP `credentials.json` and `token.json` (optional — Google OAuth)
- [ ] **3.3** Place and permission production `.env` (mode 600, owner `lsats`)
- [ ] **3.4** Create sandbox `.env` (copy production, change `TDX_BASE_URL` to sandbox)
- [ ] **3.5** Edit production `.env` for server
  - [ ] `DATABASE_URL` → localhost with new password
  - [ ] `TDX_BASE_URL` → production TDWebApi
  - [ ] `LOG_FILE` → `/var/log/lsats/lsats_database.log`
  - [ ] `DATA_PATH` → `/opt/lsats-data-hub/data`
- [ ] **3.6** Edit sandbox `.env`
  - [ ] `TDX_BASE_URL` → sandbox SBTDWebApi
  - [ ] `LOG_FILE` → `/var/log/lsats/sandbox.log`
- [ ] **3.7** Clean up `/tmp` transfer files
- [ ] **3.8** Place and permission Google credentials (mode 600, owner `lsats`)

---

## Phase 4: PostgreSQL Setup

*See §5 Phase 4 and §2.2–2.3.*

### 4A — Users and Database

- [ ] **4.1** Create `lsats_user` role (read/write, login)
- [ ] **4.2** Create `lsats_readonly` role (read-only, login)
- [ ] **4.3** Create `lsats_db` database (owner `lsats_user`)

### 4B — Schema Import

- [ ] **4.4** Run `production_init.sql` (extensions, schemas, functions, meta)
- [ ] **4.5** Run `production_schema.sql` (all table definitions)
- [ ] **4.6** Run `silver_views.sql` (all views)
- [ ] **4.7** Grant read-only access to `lsats_readonly` + set default privileges

### 4C — Authentication and Tuning

- [ ] **4.8** Configure `pg_hba.conf` for local connections (`/etc/postgresql/16/main/pg_hba.conf`)
- [ ] **4.8a** Create `.pgpass` file for `lsats` user (§2.3)
  - [ ] Write `/opt/lsats-data-hub/.pgpass` with `localhost:5432:lsats_db:lsats_user:<password>`
  - [ ] `chmod 600`, owned by `lsats`
  - [ ] Verify: `sudo -u lsats PGPASSFILE=/opt/lsats-data-hub/.pgpass psql -U lsats_user -d lsats_db -h localhost -c '\conninfo'`
- [ ] **4.9** Tune `postgresql.conf` for 15GB RAM (`/etc/postgresql/16/main/postgresql.conf`)
  - [ ] `shared_buffers = 4GB`
  - [ ] `effective_cache_size = 11GB`
  - [ ] `work_mem = 64MB`
  - [ ] `maintenance_work_mem = 512MB`
  - [ ] `max_connections = 50`
  - [ ] `random_page_cost = 1.1` (SSD) or `4.0` (HDD)
- [ ] **4.10** Restart PostgreSQL

### 4D — Verification

- [ ] **4.11** Verify 4 schemas exist (bronze, silver, gold, meta)
- [ ] **4.12** Verify 29 silver tables created
- [ ] **4.13** Verify silver views load (expect ~19)
- [ ] **4.14** Verify zero `%legacy%` tables
- [ ] **4.15** Verify zero legacy foreign keys

---

## Phase 5: Python Environment

*See §5 Phase 5.*

- [ ] **5.1** Create production venv: `python3 -m venv venv` (Python 3.12.3)
- [ ] **5.2** Install production deps: `pip install -e '.[all]'`
- [ ] **5.3** Create sandbox venv: `python3 -m venv venv`
- [ ] **5.4** Install sandbox deps: `pip install -e '.[all]'`

---

## Phase 6: Validate Connectivity

*See §5 Phase 6 and §6.3. CRITICAL — do this before any ingestion.*

- [ ] **6.1** Test database connection (PostgresAdapter)
- [ ] **6.2** Test TDX connection (TeamDynamixFacade — check environment label)
- [ ] **6.3** Test LDAP connectivity (port 636 LDAPS)
  - [ ] If blocked: file firewall request immediately, proceed with non-LDAP sources
- [ ] **6.4** Test UMich API (OAuth token grant)
- [ ] **6.5** Test HTTPS to `teamdynamix.umich.edu`
- [ ] **6.6** Test HTTPS to `apigw.it.umich.edu`

---

## Phase 7: Bronze Ingestion (Sandbox First)

*See §5 Phase 7. Budget a full day — TDX enrichment is slow.*

### 7A — Non-TDX Sources (from sandbox directory)

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

### 7B — TDX Sources (from sandbox directory — sandbox TDX endpoint)

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

*See §5 Phase 9. Run from `/opt/lsats-data-hub/` (production directory).*

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
  - [ ] 29 silver tables, ~19 views
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
| Need to update code | `git pull`, `pip install -e '.[all]'`, restart daemon | §7.4 |
| New migration needed | Write migration, run once, update `production_schema.sql` | §7.5 |
