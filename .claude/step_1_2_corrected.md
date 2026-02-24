### Step 1.2: Silver Transformation Script Updates (Day 3-5)

**IMPORTANT ARCHITECTURE NOTE:**  
Bronze scripts (`scripts/database/bronze/`) do **NOT** need changes. They only ingest raw JSON into `bronze.raw_entities`. The column name transformations happen in **Silver transformation scripts** that read from bronze JSONB and write typed columns to source-specific silver tables (`silver.tdx_users`, `silver.mcommunity_users`, etc.).

**Files to Update:**
- `scripts/database/silver/001_transform_tdx_users.py` (reads bronze → writes silver.tdx_users)
- `scripts/database/silver/006_transform_mcommunity_users.py` (reads bronze → writes silver.mcommunity_users)
- `scripts/database/silver/004_transform_ad_users.py` (reads bronze → writes silver.ad_users)
- `scripts/database/silver/002_transform_umapi_employees.py` (reads bronze → writes silver.umapi_employees)

#### 1.2.1 TDX Users Silver Transformation

**File:** `scripts/database/silver/001_transform_tdx_users.py`

**Method to Update:** `_extract_tdx_fields(self, raw_data: Dict[str, Any], raw_id: str) → Dict[str, Any]`

**Changes:**
1. Rename `full_name` → `preferred_name` (semantic change)
2. Rename `title` → `job_title`
3. Add `department_id` mapping from `default_account_id` → `tdx_account_id`
4. Derive new `full_name` as "Last, First" format

**Code Changes:**

```python
# OLD CODE (before migration) - line ~380
silver_record = {
    "tdx_user_uid": to_uuid(raw_data.get("UID")),
    "uniqname": raw_data.get("AlternateID", "").lower() or None,
    # ... other fields ...
    "full_name": raw_data.get("FullName"),  # ← User's preferred format
    "title": raw_data.get("Title"),  # ← OLD name
    "default_account_id": raw_data.get("DefaultAccountID"),  # ← No mapping
    # ... more fields ...
}

# NEW CODE (after migration)
silver_record = {
    "tdx_user_uid": to_uuid(raw_data.get("UID")),
    "uniqname": raw_data.get("AlternateID", "").lower() or None,
    # ... other fields ...
    "preferred_name": raw_data.get("FullName"),  # ← NEW: Semantic rename
    "job_title": raw_data.get("Title"),  # ← NEW: Canonical name
    "tdx_account_id": raw_data.get("DefaultAccountID"),  # ← NEW: Keep internal ID
    # ... more fields ...
}

# Derive full_name (Last, First format)
if silver_record.get("last_name") and silver_record.get("first_name"):
    silver_record["full_name"] = f"{silver_record['last_name']}, {silver_record['first_name']}"
else:
    silver_record["full_name"] = None

# Map department_id from TDX account ID
tdx_account_id = raw_data.get("DefaultAccountID")
if tdx_account_id:
    silver_record["department_id"] = self._map_tdx_account_to_dept(tdx_account_id)
else:
    silver_record["department_id"] = None

return silver_record
```

**Add New Method to Class:**

```python
def _map_tdx_account_to_dept(self, tdx_account_id: int) -> Optional[str]:
    """
    Map TDX DefaultAccountID to department code.
    
    Args:
        tdx_account_id: TDX internal account ID (e.g., 41)
    
    Returns:
        Department code (e.g., "173500") or None if not found
    """
    if not tdx_account_id:
        return None
        
    try:
        query = """
        SELECT raw_data->>'Code' as dept_code
        FROM bronze.raw_entities
        WHERE entity_type = 'department'
          AND source_system = 'tdx'
          AND (raw_data->>'ID')::INTEGER = :tdx_id
        ORDER BY ingested_at DESC
        LIMIT 1
        """
        result = self.db_adapter.query_to_dataframe(query, {"tdx_id": tdx_account_id})
        
        if not result.empty:
            return result.iloc[0]["dept_code"]
        else:
            logger.debug(f"No department mapping found for TDX account ID {tdx_account_id}")
            return None
    except Exception as e:
        logger.warning(f"Error mapping TDX department ID {tdx_account_id}: {e}")
        return None
```

#### 1.2.2 MCommunity Users Silver Transformation

**File:** `scripts/database/silver/006_transform_mcommunity_users.py`

**Method to Update:** `_extract_mcommunity_fields(self, raw_data: Dict[str, Any], raw_id: str) → Dict[str, Any]`

**Changes:**
1. Rename `uid` → `uniqname`
2. Rename `given_name` → `first_name`
3. Rename `display_name` → `preferred_name`
4. Rename `mail` → `primary_email`
5. Rename `telephone_number` → `work_phone`
6. Rename `umich_title` → `job_title`
7. Rename `cn` → `cn_aliases`
8. Rename `uid_number` → `ldap_uid_number`
9. Rename `gid_number` → `ldap_gid_number`
10. Add `last_name` (flatten from `sn` JSONB array)
11. Add `full_name` (derived)

**Code Changes:**

```python
# OLD CODE (before migration) - line ~220
silver_record = {
    "uid": raw_data.get("uid"),  # ← OLD
    "display_name": raw_data.get("displayName"),  # ← OLD
    "given_name": raw_data.get("givenName"),  # ← OLD
    "cn": self._normalize_list_field(raw_data.get("cn")),  # ← OLD name
    "sn": self._normalize_list_field(raw_data.get("sn")),  # ← JSONB array
    "mail": raw_data.get("mail"),  # ← OLD
    "telephone_number": raw_data.get("telephoneNumber"),  # ← OLD
    "umich_title": raw_data.get("umichTitle"),  # ← OLD
    "uid_number": to_bigint(raw_data.get("uidNumber")),  # ← OLD
    "gid_number": to_bigint(raw_data.get("gidNumber")),  # ← OLD
    # ... more fields ...
}

# NEW CODE (after migration)
silver_record = {
    "uniqname": raw_data.get("uid"),  # ← NEW
    "preferred_name": raw_data.get("displayName"),  # ← NEW
    "first_name": raw_data.get("givenName"),  # ← NEW
    "cn_aliases": self._normalize_list_field(raw_data.get("cn")),  # ← NEW name
    "primary_email": raw_data.get("mail"),  # ← NEW
    "work_phone": raw_data.get("telephoneNumber"),  # ← NEW
    "job_title": raw_data.get("umichTitle"),  # ← NEW
    "ldap_uid_number": to_bigint(raw_data.get("uidNumber")),  # ← NEW
    "ldap_gid_number": to_bigint(raw_data.get("gidNumber")),  # ← NEW
    # ... more fields ...
}

# Flatten sn JSONB array to last_name
sn_values = self._normalize_list_field(raw_data.get("sn"))
silver_record["last_name"] = sn_values[0] if sn_values else None

# Derive full_name (Last, First format)
if silver_record.get("last_name") and silver_record.get("first_name"):
    silver_record["full_name"] = f"{silver_record['last_name']}, {silver_record['first_name']}"
else:
    silver_record["full_name"] = None

return silver_record
```

#### 1.2.3 AD Users Silver Transformation

**File:** `scripts/database/silver/004_transform_ad_users.py`

**Method to Update:** `_extract_ad_fields(self, raw_data: Dict[str, Any], raw_id: str) → Dict[str, Any]`

**Changes:**
1. Rename `uid` → `uniqname`
2. Rename `given_name` → `first_name`
3. Rename `sn` → `last_name`
4. Rename `display_name` → `full_name` (AD display_name is already "Last, First")
5. Rename `mail` → `primary_email`
6. Rename `telephone_number` → `work_phone`
7. Rename `mobile` → `mobile_phone`
8. Rename `title` → `job_title`
9. Rename `department` → `department_name`
10. Rename `cn` → `ad_cn`, `name` → `ad_name`, `object_sid` → `ad_object_sid`
11. Rename `uid_number` → `ldap_uid_number`, `gid_number` → `ldap_gid_number`
12. Add `preferred_name` (derived)

**Code Changes:**

```python
# OLD CODE (before migration)
silver_record = {
    "uid": raw_data.get("uid"),  # ← OLD
    "given_name": raw_data.get("givenName"),  # ← OLD
    "sn": raw_data.get("sn"),  # ← OLD
    "display_name": raw_data.get("displayName"),  # ← OLD (already Last, First)
    "mail": raw_data.get("mail"),  # ← OLD
    "telephone_number": raw_data.get("telephoneNumber"),  # ← OLD
    "mobile": raw_data.get("mobile"),  # ← OLD
    "title": raw_data.get("title"),  # ← OLD
    "department": raw_data.get("department"),  # ← OLD
    "cn": raw_data.get("cn"),  # ← OLD
    "name": raw_data.get("name"),  # ← OLD
    "object_sid": raw_data.get("objectSid"),  # ← OLD
    "uid_number": to_bigint(raw_data.get("uidNumber")),  # ← OLD
    "gid_number": to_bigint(raw_data.get("gidNumber")),  # ← OLD
    # ... more fields ...
}

# NEW CODE (after migration)
silver_record = {
    "uniqname": raw_data.get("uid"),  # ← NEW
    "first_name": raw_data.get("givenName"),  # ← NEW
    "last_name": raw_data.get("sn"),  # ← NEW
    "full_name": raw_data.get("displayName"),  # ← NEW (already Last, First)
    "primary_email": raw_data.get("mail"),  # ← NEW
    "work_phone": raw_data.get("telephoneNumber"),  # ← NEW
    "mobile_phone": raw_data.get("mobile"),  # ← NEW
    "job_title": raw_data.get("title"),  # ← NEW
    "department_name": raw_data.get("department"),  # ← NEW
    "ad_cn": raw_data.get("cn"),  # ← NEW (prefixed)
    "ad_name": raw_data.get("name"),  # ← NEW (prefixed)
    "ad_object_sid": raw_data.get("objectSid"),  # ← NEW (prefixed)
    "ldap_uid_number": to_bigint(raw_data.get("uidNumber")),  # ← NEW
    "ldap_gid_number": to_bigint(raw_data.get("gidNumber")),  # ← NEW
    # ... more fields ...
}

# Derive preferred_name (First Last format)
if silver_record.get("first_name") and silver_record.get("last_name"):
    silver_record["preferred_name"] = f"{silver_record['first_name']} {silver_record['last_name']}"
elif silver_record.get("first_name"):
    silver_record["preferred_name"] = silver_record["first_name"]
elif silver_record.get("last_name"):
    silver_record["preferred_name"] = silver_record["last_name"]
else:
    silver_record["preferred_name"] = None

return silver_record
```

#### 1.2.4 UMAPI Employees Silver Transformation

**File:** `scripts/database/silver/002_transform_umapi_employees.py`

**Method to Update:** `_extract_umapi_fields(self, raw_data: Dict[str, Any], raw_id: str) → Dict[str, Any]`

**Changes:**
1. Rename `university_job_title` → `job_title`
2. Rename `department_job_title` → `dept_job_title`
3. Add `preferred_name` (derived)
4. Flatten `work_location` JSONB to typed columns (keep JSONB too)

**Code Changes:**

```python
# OLD CODE (before migration)
silver_record = {
    "empl_id": raw_data.get("EmplID"),
    "empl_rcd": raw_data.get("EmplRcd"),
    "uniqname": raw_data.get("UniqName"),
    "first_name": raw_data.get("FirstName"),
    "last_name": raw_data.get("LastName"),
    "full_name": raw_data.get("Name"),  # Already "Last, First Middle"
    "department_id": raw_data.get("DepartmentId"),
    "dept_description": raw_data.get("DeptDescription"),
    "supervisor_id": raw_data.get("SupervisorID"),
    "jobcode": raw_data.get("Jobcode"),
    "university_job_title": raw_data.get("UniversityJobTitle"),  # ← OLD
    "department_job_title": raw_data.get("DepartmentJobTitle"),  # ← OLD
    # work_location as JSONB blob
    "work_location": {
        "phone": raw_data.get("Work_Phone"),
        "address1": raw_data.get("Work_Address1"),
        # ... etc
    },
    # ... more fields ...
}

# NEW CODE (after migration)
# Build work_location JSONB
work_location = {}
for key in ["Work_Phone", "Work_Address1", "Work_Address2", "Work_City", 
            "Work_State", "Work_Postal", "Work_Country"]:
    if key in raw_data:
        json_key = key.lower().replace("work_", "")
        work_location[json_key] = raw_data[key]

silver_record = {
    "empl_id": raw_data.get("EmplID"),
    "empl_rcd": raw_data.get("EmplRcd"),
    "uniqname": raw_data.get("UniqName"),
    "first_name": raw_data.get("FirstName"),
    "last_name": raw_data.get("LastName"),
    "full_name": raw_data.get("Name"),  # Already "Last, First Middle"
    "department_id": raw_data.get("DepartmentId"),
    "dept_description": raw_data.get("DeptDescription"),
    "supervisor_id": raw_data.get("SupervisorID"),
    "jobcode": raw_data.get("Jobcode"),
    "job_title": raw_data.get("UniversityJobTitle"),  # ← NEW
    "dept_job_title": raw_data.get("DepartmentJobTitle"),  # ← NEW
    # Keep JSONB for full data
    "work_location": work_location,
    # Flatten to typed columns for performance
    "work_phone": work_location.get("phone"),
    "work_city": work_location.get("city"),
    "work_state": work_location.get("state"),
    "work_postal_code": work_location.get("postal"),
    "work_country": work_location.get("country"),
    "work_address_line1": work_location.get("address1"),
    "work_address_line2": work_location.get("address2"),
    # ... more fields ...
}

# Derive preferred_name (First Last format)
first_name = raw_data.get("FirstName")
last_name = raw_data.get("LastName")
if first_name and last_name:
    silver_record["preferred_name"] = f"{first_name} {last_name}"
elif first_name:
    silver_record["preferred_name"] = first_name
elif last_name:
    silver_record["preferred_name"] = last_name
elif raw_data.get("Name"):
    silver_record["preferred_name"] = raw_data.get("Name")  # Fallback
else:
    silver_record["preferred_name"] = None

return silver_record
```
