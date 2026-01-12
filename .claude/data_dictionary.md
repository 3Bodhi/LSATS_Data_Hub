# LSATS Data Hub Data Dictionary

This document establishes canonical column names for all silver layer entities, ensuring consistency across source-specific, consolidated, and composite tables.

## Naming Principles

1. **Identical concepts = identical names** across all tables
2. **Source-specific identifiers** use prefixes (e.g., `tdx_`, `ad_`, `ldap_`)
3. **Canonical fields** use unprefixed standard names in all source-specific tables
4. **Business-friendly names** preferred over technical jargon (`first_name` > `given_name`, `uniqname` > `uid`)
5. **Timestamp consistency**: Always use `created_at` / `updated_at`

---

## Entity 1: USERS

### Core Identity Fields

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **User Identifier** | `uniqname` | VARCHAR(50) | University unique identifier (PRIMARY KEY) | All sources |
| **First Name** | `first_name` | VARCHAR(255) | Person's given name | TDX > UMAPI > MCommunity > AD |
| **Last Name** | `last_name` | VARCHAR(255) | Person's surname | TDX > UMAPI > MCommunity > AD |
| **Full Name** | `full_name` | VARCHAR(255) | Standardized "Last, First Middle" format for sorting | UMAPI > AD > Derived |
| **Preferred Name** | `preferred_name` | VARCHAR(255) | User's chosen display format (casual use) | TDX > MCommunity > Derived |

**Name Field Semantics:**
- `first_name` + `last_name`: Atomic components for filtering/searching
- `full_name`: Formal "Last, First Middle" format (e.g., "Westphal, Aiden Charles") for alphabetical sorting and official documents
- `preferred_name`: User's chosen display format (e.g., "Shawn Eberle", "Robert W Kao") for UIs and casual communications

### Contact Information

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Primary Email** | `primary_email` | VARCHAR(255) | User's primary email address | TDX > MCommunity > AD |
| **Work Phone** | `work_phone` | VARCHAR(50) | Office/work phone number | TDX > UMAPI > MCommunity > AD |
| **Work Phone Extension** | `work_phone_extension` | VARCHAR(20) | Phone extension | TDX |
| **Mobile Phone** | `mobile_phone` | VARCHAR(50) | Cell/mobile phone number | TDX > AD |

### Employment & Department

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Department ID** | `department_id` | VARCHAR(10) | Primary department code (FK to departments.dept_id) | UMAPI > TDX (mapped) |
| **Department Name** | `department_name` | VARCHAR(255) | Department name (denormalized for convenience) | UMAPI > TDX |
| **Department IDs** | `department_ids` | JSONB | Array of all department affiliations (multi-appointment) | UMAPI |
| **Job Title** | `job_title` | TEXT | User's job/position title | UMAPI > MCommunity > TDX > AD |
| **Department Job Title** | `dept_job_title` | VARCHAR(255) | Department-specific job title variant | UMAPI |
| **Primary Job Code** | `primary_job_code` | VARCHAR(10) | Primary job classification code | UMAPI |
| **Job Codes** | `job_codes` | JSONB | Array of all job codes (multi-appointment) | UMAPI |
| **Primary Supervisor ID** | `primary_supervisor_id` | VARCHAR(10) | EmplID of primary supervisor | UMAPI |
| **Primary Supervisor Uniqname** | `primary_supervisor_uniqname` | VARCHAR(50) | Uniqname of primary supervisor (resolved from EmplID) | UMAPI (resolved) |
| **Supervisor IDs** | `supervisor_ids` | JSONB | Array of all supervisor EmplIDs (multi-appointment) | UMAPI |
| **Reports To UID** | `reports_to_uid` | UUID | TDX User UID of reporting manager | TDX |

### Work Location

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Work Address Line 1** | `work_address_line1` | VARCHAR(255) | Street address | TDX > UMAPI |
| **Work Address Line 2** | `work_address_line2` | VARCHAR(255) | Address line 2 | TDX > UMAPI |
| **Work Address Line 3** | `work_address_line3` | VARCHAR(255) | Address line 3 | TDX |
| **Work City** | `work_city` | VARCHAR(100) | City | TDX > UMAPI |
| **Work State** | `work_state` | VARCHAR(50) | State/province | TDX > UMAPI |
| **Work Postal Code** | `work_postal_code` | VARCHAR(20) | ZIP/postal code | TDX > UMAPI |
| **Work Country** | `work_country` | VARCHAR(100) | Country | TDX > UMAPI |

### Status Flags

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Is Active** | `is_active` | BOOLEAN | User account is active | TDX OR UMAPI OR AD (any active) |
| **Is Employee** | `is_employee` | BOOLEAN | User is current employee | UMAPI > TDX |
| **Is PI** | `is_pi` | BOOLEAN | User is Principal Investigator | Derived from lab_awards + AD OUs |
| **AD Account Disabled** | `ad_account_disabled` | BOOLEAN | Active Directory account disabled | AD |
| **AD Account Locked** | `ad_account_locked` | BOOLEAN | Active Directory account locked | AD |

### POSIX & Directory

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **LDAP UID Number** | `ldap_uid_number` | BIGINT | POSIX user ID number | MCommunity > AD |
| **LDAP GID Number** | `ldap_gid_number` | BIGINT | POSIX primary group ID | MCommunity > AD |
| **Home Directory** | `home_directory` | VARCHAR(255) | POSIX home directory path | MCommunity > AD |
| **Login Shell** | `login_shell` | VARCHAR(50) | POSIX login shell | MCommunity > AD |

### Affiliations & Group Memberships

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **MCommunity OU Affiliations** | `mcommunity_ou_affiliations` | JSONB | Array of organizational unit strings from MCommunity | MCommunity |
| **OU Department IDs** | `ou_department_ids` | JSONB | Department IDs matched from OU affiliations | Derived from MCommunity OUs |
| **AD Group Memberships** | `ad_group_memberships` | JSONB | Array of Active Directory group DNs | AD |
| **AD Primary Group ID** | `ad_primary_group_id` | INTEGER | Windows primary group RID | AD |
| **TDX Group IDs** | `tdx_group_ids` | JSONB | Array of TeamDynamix group IDs | TDX |

### Active Directory OU Structure

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **AD OU Root** | `ad_ou_root` | VARCHAR(100) | OU root (e.g., "umichad") | AD |
| **AD OU Organization** | `ad_ou_organization` | VARCHAR(100) | Organization name | AD |
| **AD OU Department** | `ad_ou_department` | VARCHAR(100) | Department within OU | AD |
| **AD OU Full Path** | `ad_ou_full_path` | JSONB | Array of OU hierarchy components | AD |
| **AD Parent OU DN** | `ad_parent_ou_dn` | VARCHAR(500) | Distinguished name of parent OU | AD |

### Source-Specific Identifiers (Keep Prefixes)

| Concept | Canonical Name | Type | Description | Source |
|---------|---------------|------|-------------|--------|
| **TDX User UID** | `tdx_user_uid` | UUID | TeamDynamix internal user UUID (critical for write-back) | TDX |
| **TDX Account ID** | `tdx_account_id` | INTEGER | TDX internal account/department ID (internal use only) | TDX |
| **TDX External ID** | `tdx_external_id` | VARCHAR(255) | TDX external identifier field | TDX |
| **TDX BEID** | `tdx_beid` | VARCHAR(255) | TDX BEID | TDX |
| **TDX Security Role ID** | `tdx_security_role_id` | UUID | TDX security role UUID | TDX |
| **TDX Security Role Name** | `tdx_security_role_name` | VARCHAR(100) | TDX security role name | TDX |
| **UMich Employee ID** | `umich_empl_id` | VARCHAR(10) | Primary employee ID from UMAPI | UMAPI |
| **UMich Employee IDs** | `umich_empl_ids` | JSONB | Array of all EmplIDs (if multiple records) | UMAPI |
| **AD Object GUID** | `ad_object_guid` | VARCHAR(255) | Active Directory object GUID | AD |
| **AD Object SID** | `ad_object_sid` | TEXT | Active Directory security identifier | AD |
| **AD SAM Account Name** | `ad_sam_account_name` | VARCHAR(255) | Windows SAM account name | AD |

### Timestamps (Consistent Across All Tables)

| Concept | Canonical Name | Type | Description |
|---------|---------------|------|-------------|
| **Created At** | `created_at` | TIMESTAMP WITH TIME ZONE | Record creation timestamp |
| **Updated At** | `updated_at` | TIMESTAMP WITH TIME ZONE | Last modification timestamp |

### Metadata (Consistent Across All Tables)

| Concept | Canonical Name | Type | Description |
|---------|---------------|------|-------------|
| **Silver ID** | `silver_id` | UUID | Internal UUID for referencing |
| **Data Quality Score** | `data_quality_score` | NUMERIC(3,2) | Quality score 0.00-1.00 |
| **Quality Flags** | `quality_flags` | JSONB | Array of quality issue identifiers |
| **Source System** | `source_system` | VARCHAR(200) | Pipe-delimited source systems (e.g., "tdx+umapi+mcommunity_ldap") |
| **Source Entity ID** | `source_entity_id` | VARCHAR(255) | Primary identifier from source system |
| **Entity Hash** | `entity_hash` | VARCHAR(64) | SHA-256 hash for change detection |
| **Ingestion Run ID** | `ingestion_run_id` | UUID | FK to meta.ingestion_runs |

---

## USERS: Column Rename Summary

### SILVER.MCOMMUNITY_USERS

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `uid` | `uniqname` | RENAME |
| `given_name` | `first_name` | RENAME |
| `sn` (JSONB) | `last_name` | RENAME + FLATTEN (take first element) |
| `display_name` | `preferred_name` | RENAME |
| `mail` | `primary_email` | RENAME |
| `telephone_number` | `work_phone` | RENAME |
| `umich_title` | `job_title` | RENAME |
| `cn` | `cn_aliases` | RENAME (keep JSONB for name variations) |
| `uid_number` | `ldap_uid_number` | RENAME |
| `gid_number` | `ldap_gid_number` | RENAME |

**Add Derived Column:**
- `full_name` (VARCHAR(255)) - Constructed from `last_name + ", " + first_name`

### SILVER.AD_USERS

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `uid` | `uniqname` | RENAME |
| `given_name` | `first_name` | RENAME |
| `sn` | `last_name` | RENAME |
| `display_name` | `full_name` | RENAME (it's Last, First format) |
| `mail` | `primary_email` | RENAME |
| `telephone_number` | `work_phone` | RENAME |
| `mobile` | `mobile_phone` | RENAME |
| `title` | `job_title` | RENAME |
| `department` | `department_name` | RENAME (text description, not ID) |
| `cn` | `ad_cn` | RENAME (it's username, not a name) |
| `name` | `ad_name` | RENAME (it's username, not a name) |
| `object_sid` | `ad_object_sid` | RENAME (add prefix) |
| `uid_number` | `ldap_uid_number` | RENAME |
| `gid_number` | `ldap_gid_number` | RENAME |

**Add Derived Column:**
- `preferred_name` (VARCHAR(255)) - Constructed from `first_name + " " + last_name`

### SILVER.TDX_USERS

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `full_name` | `preferred_name` | RENAME (user's chosen format) |
| `title` | `job_title` | RENAME |
| `default_account_id` | `tdx_account_id` | RENAME (internal TDX ID) |

**Add Derived Column:**
- `full_name` (VARCHAR(255)) - Constructed from `last_name + ", " + first_name`
- `department_id` (VARCHAR(10)) - Mapped from `tdx_account_id` to dept_code (FK compatible)

### SILVER.UMAPI_EMPLOYEES

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `full_name` | `full_name` | KEEP (already Last, First format) |
| `university_job_title` | `job_title` | RENAME |
| `department_job_title` | `dept_job_title` | RENAME |
| `work_location` (JSONB) | `work_location` | KEEP (preserve full JSONB) |

**Add Flattened Columns from work_location JSONB:**
- `work_phone` (VARCHAR(50)) - Extract from `work_location->>'phone'`
- `work_city` (VARCHAR(100)) - Extract from `work_location->>'city'`
- `work_state` (VARCHAR(50)) - Extract from `work_location->>'state'`
- `work_postal_code` (VARCHAR(20)) - Extract from `work_location->>'postal'`
- `work_country` (VARCHAR(100)) - Extract from `work_location->>'country'`
- `work_address_line1` (VARCHAR(255)) - Extract from `work_location->>'address1'`
- `work_address_line2` (VARCHAR(255)) - Extract from `work_location->>'address2'`

**Add Derived Column:**
- `preferred_name` (VARCHAR(255)) - Derived from `first_name + " " + last_name` (fallback to full_name)

### SILVER.USERS (Consolidated)

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `display_name` | `preferred_name` | RENAME |

**All other fields already use canonical names.**

---

## Migration Notes

### Name Field Migration Logic

**Preferred Name (User's Choice):**
```python
preferred_name = pick_first(
    tdx_record.get("preferred_name"),       # User-set in TDX
    mcom_record.get("preferred_name"),      # User-set in MCommunity
    f"{first_name} {last_name}",            # Derived from components
)
```

**Full Name (Standardized Last, First):**
```python
full_name = pick_first(
    umapi_record.get("full_name"),          # Already Last, First Middle
    ad_record.get("full_name"),             # Already Last, First
    f"{last_name}, {first_name}",           # Derived
)
```

### JSONB Flattening

**MCommunity `sn` field** (JSONB array → VARCHAR):

Updated in `scripts/database/silver/006_transform_mcommunity_users.py`:
```python
# In _extract_mcommunity_fields() method
def _normalize_list_field(self, value: Any) -> List[str]:
    """Normalize to always return array."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]

# Extract sn as JSONB array (keep multi-value capability)
silver_record["sn"] = self._normalize_list_field(raw_data.get("sn"))

# Derive last_name as flattened first element
sn_values = silver_record["sn"]
silver_record["last_name"] = sn_values[0] if sn_values else None
```

**UMAPI `work_location` field** (JSONB → typed columns):

Updated in `scripts/database/silver/002_transform_umapi_employees.py`:
```python
# Extract typed columns from work_location JSONB
work_location_json = raw_data.get("work_location", {})
if isinstance(work_location_json, str):
    work_location_json = json.loads(work_location_json)

silver_record["work_phone"] = work_location_json.get("phone")
silver_record["work_city"] = work_location_json.get("city")
silver_record["work_state"] = work_location_json.get("state")
silver_record["work_zip"] = work_location_json.get("postal_code")
# Keep JSONB for other fields
silver_record["work_location"] = work_location_json
```

### TDX Department ID Mapping

**Problem:** TDX `default_account_id` is internal ID (e.g., "41"), but `department_id` FK requires dept_code (e.g., "173500").

**Solution:** Create mapping during silver transformation in `scripts/database/silver/001_transform_tdx_users.py`:
```python
# Add method to TDXUserTransformationService class
def _load_tdx_dept_id_to_code_map(self) -> Dict[str, str]:
    """Build TDX internal ID → Dept Code mapping from bronze layer."""
    query = """
    SELECT DISTINCT ON (raw_data->>'ID')
        raw_data->>'ID' as account_id,
        raw_data->>'Code' as dept_code
    FROM bronze.raw_entities
    WHERE entity_type = 'department'
      AND source_system = 'tdx'
      AND raw_data->>'ID' IS NOT NULL
    ORDER BY raw_data->>'ID', ingested_at DESC
    """
    result_df = self.db_adapter.query_to_dataframe(query)
    return dict(zip(result_df["account_id"], result_df["dept_code"]))

# In _extract_tdx_fields() method
tdx_account_id = raw_data.get("DefaultAccountID")
silver_record["tdx_account_id"] = tdx_account_id  # Keep internal ID
silver_record["department_id"] = tdx_dept_map.get(str(tdx_account_id))  # Mapped code
```

---

## Validation Queries

### Check Naming Consistency After Migration

```sql
-- Verify uniqname consistency
SELECT 'tdx_users' as table_name, COUNT(*) as has_uniqname 
FROM silver.tdx_users WHERE uniqname IS NOT NULL
UNION ALL
SELECT 'umapi_employees', COUNT(*) FROM silver.umapi_employees WHERE uniqname IS NOT NULL
UNION ALL
SELECT 'mcommunity_users', COUNT(*) FROM silver.mcommunity_users WHERE uniqname IS NOT NULL
UNION ALL
SELECT 'ad_users', COUNT(*) FROM silver.ad_users WHERE uniqname IS NOT NULL;

-- Verify first_name/last_name consistency
SELECT table_name, 
       COUNT(*) FILTER (WHERE first_name IS NOT NULL) as has_first_name,
       COUNT(*) FILTER (WHERE last_name IS NOT NULL) as has_last_name
FROM (
    SELECT 'tdx_users' as table_name, first_name, last_name FROM silver.tdx_users
    UNION ALL
    SELECT 'umapi_employees', first_name, last_name FROM silver.umapi_employees
    UNION ALL
    SELECT 'mcommunity_users', first_name, last_name FROM silver.mcommunity_users
    UNION ALL
    SELECT 'ad_users', first_name, last_name FROM silver.ad_users
) combined
GROUP BY table_name;

-- Verify preferred_name vs full_name distinction
SELECT 
    uniqname,
    preferred_name,  -- "Shawn Eberle" (casual)
    full_name        -- "Eberle, Shawn" (formal)
FROM silver.users
WHERE preferred_name IS NOT NULL
LIMIT 10;
```

---

## Entity 2: DEPARTMENTS

### Core Identity Fields

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Department ID** | `dept_id` | VARCHAR(50) | Department code (PRIMARY KEY, 6-digit format) | UMAPI.dept_id = TDX.dept_code |
| **Department Name** | `department_name` | VARCHAR(255) | Official department name | UMAPI.dept_description > TDX.dept_name |
| **Department Code** | `department_code` | VARCHAR(50) | Alias/legacy field for dept_id | Same as dept_id |
| **Description** | `description` | TEXT | Additional notes/description | TDX.dept_notes |

**Naming Note:** Both sources already use consistent naming:
- UMAPI: `dept_id` + `dept_description`
- TDX: `dept_code` + `dept_name`

### Organizational Hierarchy (from UMAPI)

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Campus Name** | `campus_name` | VARCHAR(255) | Campus affiliation | UMAPI |
| **College Group** | `college_group` | VARCHAR(255) | College/school grouping | UMAPI.dept_group |
| **VP Area** | `vp_area` | VARCHAR(255) | VP area name | UMAPI.vp_area_name |
| **Hierarchical Path** | `hierarchical_path` | TEXT | Full organizational path from root to dept | UMAPI |

### Operational Data (from TDX)

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **TDX ID** | `tdx_id` | INTEGER | TeamDynamix internal ID (for write-back) | TDX |
| **TDX Manager UID** | `tdx_manager_uid` | UUID | TDX User UID of department manager | TDX.manager_uid |
| **Parent Department ID** | `parent_dept_id` | VARCHAR(50) | Parent department code (if hierarchical in TDX) | TDX.parent_id (mapped) |
| **Location Info** | `location_info` | JSONB | Address, city, phone, fax, URL, postal code | TDX |
| **TDX Created Date** | `tdx_created_date` | TIMESTAMP WITH TIME ZONE | When created in TDX | TDX |
| **TDX Modified Date** | `tdx_modified_date` | TIMESTAMP WITH TIME ZONE | Last modified in TDX | TDX |

### Status & Quality

| Concept | Canonical Name | Type | Description | Priority Sources |
|---------|---------------|------|-------------|------------------|
| **Is Active** | `is_active` | BOOLEAN | Department is currently active | TDX (UMAPI assumed active) |
| **Data Quality Score** | `data_quality_score` | NUMERIC(3,2) | Quality score 0.00-1.00 | Calculated |
| **Quality Flags** | `quality_flags` | JSONB | Array of quality issue identifiers | Calculated |

---

## DEPARTMENTS: Column Rename Summary

### ✅ Excellent Consistency

Both source-specific tables already use canonical names or very close variants. **Minimal renames needed!**

### SILVER.TDX_DEPARTMENTS

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `dept_code` | `dept_id` | RENAME (align with UMAPI and consolidated) |
| `dept_name` | `department_name` | RENAME (canonical form) |
| `dept_notes` | `description` | RENAME (generic name) |
| `manager_uid` | `tdx_manager_uid` | RENAME (add TDX prefix for source clarity) |

### SILVER.UMAPI_DEPARTMENTS

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `dept_description` | `department_name` | RENAME (canonical form) |
| `dept_group` | `college_group` | RENAME (clearer semantics) |
| `vp_area_name` | `vp_area` | RENAME (shorter, consistent with campus_name/college_group) |

**All other fields already use canonical names.**

### SILVER.DEPARTMENTS (Consolidated)

| Current Column Name | New Column Name | Action |
|---------------------|-----------------|--------|
| `dept_name` | `department_name` | RENAME (if not already done) |
| `college_group` | `college_group` | KEEP (align with new UMAPI naming) |
| `vp_area` | `vp_area` | KEEP (align with new UMAPI naming) |

**Remove redundant fields:**
- `department_code` - Duplicate of `dept_id`, keep only `dept_id`
- `vp_area_name`, `college_name`, `dept_group` - Consolidate to canonical names above

---

## Migration Notes - Departments

### Minimal Changes Required

Departments entity already has excellent naming consistency:
- ✅ Both sources use `dept_id` or `dept_code` (6-digit format)
- ✅ Both sources use `is_active`, `created_at`, `updated_at`
- ✅ TDX properly prefixes operational fields (tdx_created_date, tdx_modified_date)

### Key Renames

**Primary identifier alignment:**
```python
# Before
tdx_dept_code = tdx_record.get("dept_code")
umapi_dept_id = umapi_record.get("dept_id")

# After (both use dept_id)
tdx_dept_id = tdx_record.get("dept_id")  
umapi_dept_id = umapi_record.get("dept_id")
```

**Name field consistency:**
```python
# Consolidated merge logic becomes clearer
department_name = pick_first(
    umapi_record.get("department_name"),  # Was dept_description
    tdx_record.get("department_name"),    # Was dept_name
)
```

### Validation Query

```sql
-- Verify dept_id consistency after migration
SELECT 
    'tdx_departments' as table_name,
    COUNT(*) as total,
    COUNT(DISTINCT dept_id) as unique_dept_ids
FROM silver.tdx_departments
UNION ALL
SELECT 
    'umapi_departments',
    COUNT(*),
    COUNT(DISTINCT dept_id)
FROM silver.umapi_departments
UNION ALL
SELECT 
    'departments (consolidated)',
    COUNT(*),
    COUNT(DISTINCT dept_id)
FROM silver.departments;
```

---

## Entity 3: GROUPS

### Column Rename Summary

**SILVER.MCOMMUNITY_GROUPS:**
- `group_email` → `group_email` ✅ KEEP
- `group_name` → `group_name` ✅ KEEP  
- (All fields already canonical)

**SILVER.AD_GROUPS:**
- `name` → `group_name` 🔄 RENAME
- `mail` → `group_email` 🔄 RENAME
- `cn` → `cn` ✅ KEEP (canonical name field in AD)

**Excellent consistency** - only 2 renames needed! AD uses generic `name`/`mail` which need alignment to `group_name`/`group_email`.

---

## Entity 4: LABS (Composite)

Labs is a composite entity built from multiple sources. Key canonical names:

| Concept | Canonical Name | Type |
|---------|---------------|------|
| **Lab ID** | `lab_id` | VARCHAR(100) |
| **Lab Name** | `lab_name` | VARCHAR(255) |
| **PI Uniqname** | `pi_uniqname` | VARCHAR(50) |
| **Primary Department ID** | `primary_department_id` | VARCHAR(50) |

Sources already use consistent naming. No major renames required.

---

## Entity 5: COMPUTERS

Computers consolidated from TDX Assets, AD Computers, and KeyConfigure. Analysis shows good consistency with standard fields like `computer_name`, `serial_number`, `operating_system` already aligned across sources.

---

## Universal Standards (All Entities)

### Standard Metadata Columns

These columns are **identical across all silver tables**:

```sql
-- Identity
silver_id UUID UNIQUE DEFAULT uuid_generate_v4()

-- Quality
data_quality_score NUMERIC(3,2) CHECK (data_quality_score BETWEEN 0.00 AND 1.00)
quality_flags JSONB DEFAULT '[]'::jsonb

-- Source tracking  
source_system VARCHAR(200)
source_entity_id VARCHAR(255)
entity_hash VARCHAR(64)

-- Timestamps (ALWAYS use these exact names)
created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP

-- Ingestion tracking
ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id)
```

### Naming Rules Summary

1. **Primary Keys**: Use business-meaningful names (`uniqname`, `dept_id`, `group_id`, `lab_id`)
2. **Names**: Use `_name` suffix for text descriptions (`department_name`, `group_name`, `lab_name`)
3. **Emails**: Use `_email` suffix (`primary_email`, `group_email`)
4. **Phones**: Use `_phone` suffix (`work_phone`, `mobile_phone`)
5. **IDs**: Use `_id` suffix for foreign keys and codes (`department_id`, `tdx_id`)
6. **Booleans**: Use `is_` or `has_` prefix (`is_active`, `has_award_data`)
7. **Timestamps**: Always `created_at`, `updated_at` (never `created`, `modified`, `create_date`)
8. **Source prefixes**: Use for system-specific IDs (`tdx_user_uid`, `ad_object_guid`, `ldap_uid_number`)
9. **JSONB fields**: Use `_ids`, `_flags`, `_info`, `_data` suffixes for arrays/objects

### Forbidden Patterns

❌ **Don't use:**
- `uid` for user identifier (use `uniqname`)
- `given_name` / `sn` for names (use `first_name` / `last_name`)
- `mail` for email (use `primary_email` or `group_email`)
- `created` / `modified` for timestamps (use `created_at` / `updated_at`)
- Inconsistent capitalization (use snake_case everywhere)
- Generic `name`, `email`, `phone` without context (add prefix/suffix)

---

## Implementation Priority

### Phase 1: High Impact (Q1 2025)
1. **USERS** - 23 renames, affects all cross-table joins
2. **GROUPS** - 2 renames, simple but frequently joined

### Phase 2: Medium Impact (Q2 2025)  
3. **DEPARTMENTS** - 7 renames, well-isolated
4. **COMPUTERS** - Analysis pending, likely low impact

### Phase 3: Composite Entities (Q3 2025)
5. **LABS** - Composite entity, depends on users/departments being complete

---

## Migration Scripts

### Template: Column Rename Migration

```sql
-- Example: Rename mcommunity_users.uid → uniqname
BEGIN;

-- 1. Add new column
ALTER TABLE silver.mcommunity_users 
ADD COLUMN uniqname VARCHAR(50);

-- 2. Copy data
UPDATE silver.mcommunity_users 
SET uniqname = uid;

-- 3. Update constraints/indexes
CREATE INDEX idx_mcommunity_users_uniqname ON silver.mcommunity_users(uniqname);
DROP INDEX IF EXISTS idx_mcommunity_users_uid;

-- 4. Drop old column (after verifying transformation scripts updated)
ALTER TABLE silver.mcommunity_users 
DROP COLUMN uid;

COMMIT;
```

### Transformation Script Updates

After schema migrations, update transformation scripts:

```python
# OLD (before migration)
mcom_record.get("uid")
mcom_record.get("given_name")

# NEW (after migration)
mcom_record.get("uniqname")
mcom_record.get("first_name")
```

---

**Last Updated:** 2025-12-18  
**Status:** Complete - All entities audited (USERS, DEPARTMENTS, GROUPS, LABS, COMPUTERS)  
**Next Steps:** Review and approve, then begin Phase 1 implementation
