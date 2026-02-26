# LSATS Data Hub - Database Views

This directory contains consolidated view definitions for the LSATS Data Hub silver layer.

## Organization

All silver schema views are consolidated into a single file for easy maintenance:

- **`silver_views.sql`** - All `silver.v_*` views organized by domain

## Why Consolidate Views?

**Before**: Views were scattered across 9+ migration files and schemas.sql
**After**: Single source of truth for all view definitions

### Benefits

✅ **Easy to find** - One file to search for any view  
✅ **Easy to modify** - Update view logic without creating migrations  
✅ **Clear dependencies** - See related views together  
✅ **Version controlled** - Track view logic changes in git  
✅ **Idempotent** - Can be re-run safely (CREATE OR REPLACE)  

## View Categories

### Lab-Related Views (9 views)

| View Name | Purpose | Key Dependencies |
|-----------|---------|------------------|
| `v_lab_summary` | Comprehensive lab metrics | silver.labs |
| `v_lab_groups` | Lab-to-group associations | silver.labs, silver.groups_legacy |
| `v_lab_members_detailed` | Lab membership with user info | silver.lab_members, silver.users_legacy |
| `v_department_labs` | Department-level lab aggregation | silver.departments, silver.labs |
| `v_labs_monitored` | Labs actively monitored (have computers) | silver.labs |
| `v_labs_refined` | Quality-filtered lab list | silver.labs |
| `v_lab_active_awards_legacy` | Active awards for labs | silver.labs, silver.lab_awards_legacy |
| `v_legitimate_labs` | Labs eligible for manager identification | silver.labs, silver.departments |
| `v_eligible_lab_members` | Members eligible for manager role | silver.lab_members, silver.users_legacy |

### Computer-Related Views (4 views)

| View Name | Purpose | Key Dependencies |
|-----------|---------|------------------|
| `v_computer_summary` | Comprehensive computer info | silver.computers |
| `v_lab_computers` | Lab-computer associations | silver.computers, silver.lab_computers |
| `v_department_computers` | Department computer grouping | silver.computers, silver.departments |
| `v_computer_group_memberships` | Computer AD group memberships | silver.computers, silver.group_members |

## Usage

### Initial Database Setup

Views are automatically created during database initialization via `init.sql`:

```sql
\i docker/postgres/views/silver_views.sql
```

### Updating View Logic

1. Edit the view definition in `silver_views.sql`
2. Re-run the file (all views use `CREATE OR REPLACE`):
   ```bash
   docker exec -i $(docker ps -qf "name=lsats-database") \
     psql -U lsats_user -d lsats_db -f /docker-entrypoint-initdb.d/views/silver_views.sql
   ```
3. Commit changes to git

### Testing Views

```bash
# Test all views
psql -U lsats_user -d lsats_db -f docker/postgres/views/silver_views.sql

# Test a specific view
psql -U lsats_user -d lsats_db -c "SELECT * FROM silver.v_lab_summary LIMIT 5;"
```

## When to Use Migrations vs Views

| Use Case | Location | Rationale |
|----------|----------|-----------|
| **View definitions** | `views/silver_views.sql` | Idempotent, can be re-run |
| **Table schema changes** | `migrations/*.sql` | One-time structural change |
| **Adding columns** | `migrations/*.sql` | Alters existing data |
| **Indexes** | `migrations/*.sql` | Performance optimization |
| **Data migrations** | `migrations/*.sql` | One-time data transformation |

## Migration History

These views were consolidated from the following migrations:

- `006_add_silver_labs.sql` → Created v_lab_summary, v_lab_groups, v_lab_members_detailed, v_department_labs
- `007_add_labs_monitored_view.sql` → Created v_labs_monitored
- `008_fix_lab_groups_matching.sql` → Modified v_lab_groups
- `010_create_v_labs_refined.sql` → Created v_labs_refined
- `011_add_lab_managers.sql` → Created v_legitimate_labs, v_eligible_lab_members
- `019a_rename_lab_awards_to_legacy.sql` → Created v_lab_active_awards_legacy
- `schemas.sql` → Contained outdated versions + computer views

**Consolidation Date**: 2025-01-24  
**Migration**: 033_consolidate_views.sql

## View Naming Conventions

- **Prefix**: `v_` indicates a view (vs table)
- **Domain**: `lab_`, `computer_`, `department_` indicates primary domain
- **Suffix**: 
  - `_summary` = Aggregated metrics
  - `_detailed` = Expanded with joins
  - `_monitored` = Filtered subset
  - `_refined` = Quality-filtered subset
  - `_eligible` = Filtered for specific criteria

## Dependencies

Views depend on silver layer tables. Key relationships:

```
silver.labs ──┐
              ├──> v_lab_summary
              ├──> v_lab_groups ──> silver.groups_legacy
              ├──> v_department_labs ──> silver.departments
              ├──> v_labs_monitored
              ├──> v_labs_refined
              └──> v_legitimate_labs ──> silver.departments

silver.lab_members ──┐
                     ├──> v_lab_members_detailed ──> silver.users_legacy
                     └──> v_eligible_lab_members ──> silver.users_legacy

silver.computers ──┐
                   ├──> v_computer_summary
                   ├──> v_lab_computers ──> silver.lab_computers
                   ├──> v_department_computers ──> silver.departments
                   └──> v_computer_group_memberships ──> silver.group_members
```

## Maintenance

### Adding a New View

1. Add the view definition to `silver_views.sql` in appropriate section
2. Add documentation comments
3. Update this README with the new view details
4. Re-run the views file to test
5. Commit changes

### Modifying Existing View

1. Update the view SQL in `silver_views.sql`
2. Update comments if logic changed significantly
3. Re-run the views file to test
4. Commit changes (migrations not needed for views)

### Removing a View

1. Remove or comment out the view in `silver_views.sql`
2. Create a migration to DROP the view from existing databases
3. Update this README
4. Commit both changes

## Troubleshooting

### View Creation Fails

```bash
# Check for dependency issues
psql -U lsats_user -d lsats_db -c "\d+ silver.labs"
psql -U lsats_user -d lsats_db -c "\d+ silver.departments"

# Check for syntax errors
psql -U lsats_user -d lsats_db -f docker/postgres/views/silver_views.sql
```

### View Returns Unexpected Results

```bash
# Check source tables
psql -U lsats_user -d lsats_db -c "SELECT COUNT(*) FROM silver.labs;"

# Check view definition
psql -U lsats_user -d lsats_db -c "\d+ silver.v_lab_summary"
```

## See Also

- [Database Script Standards](../../../.claude/database_script_standards.md) - Patterns for database scripts
- [Silver Layer Standards](../../../.claude/silver_layer_standards.md) - Standards for silver tables/views
- [Lab Modernization Plan](../../../docs/lab_modernization_plan.md) - Context for lab views
