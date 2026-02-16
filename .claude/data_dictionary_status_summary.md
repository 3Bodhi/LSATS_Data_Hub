# Data Dictionary Refactor - Status Summary

**Last Updated:** 2026-01-12  
**Overall Completion:** ~60-70%

---

## Quick Status

### ✅ Completed (60-70%)

- **TDX Users**: Fully migrated to canonical names
- **UMAPI Employees**: Fully migrated, work_location flattened
- **MCommunity Groups**: Already using canonical names
- **Consolidated Users**: 8/9 fields canonical
- **All metadata fields**: Standard across all tables

### ⚠️ In Progress (Needs Cleanup)

- **MCommunity Users**: New columns added, old columns need dropping
- **AD Users**: New columns added, old columns need dropping
- **Consolidated Users**: Needs display_name → preferred_name cleanup
- **Consolidated Departments**: Needs dept_name cleanup

### ❌ Not Started

- **TDX Departments**: Still using dept_code, dept_name
- **AD Groups**: Still using name, mail

---

## Priority Actions

### 🔴 HIGH PRIORITY - Complete Phase 1 Cleanup

**What:** Drop old columns from MCommunity Users, AD Users, and Consolidated Users

**Why:** Schema migration is complete but cleanup was skipped, causing:
- Confusion about which columns to use
- Increased storage
- Potential for bugs if code uses wrong columns

**Estimated Time:** 2-3 days

**Steps:**
1. Verify transformation scripts use new column names
2. Run validation queries to ensure no data loss
3. Drop old columns via migration scripts
4. Test thoroughly

**Old Columns to Drop:**
- `mcommunity_users`: uid, given_name, sn, display_name, mail, telephone_number (6 columns)
- `ad_users`: uid, given_name, sn, mail, telephone_number (5 columns)
- `users`: display_name (1 column)
- `departments`: dept_name (1 column)

### 🟡 MEDIUM PRIORITY - Complete Phase 2 (Departments)

**What:** Rename TDX Departments columns to canonical names

**Why:** Only 2 columns need renaming, straightforward work

**Estimated Time:** 2-3 days

**Changes Needed:**
- `dept_code` → `dept_id`
- `dept_name` → `department_name`

### 🟢 LOW PRIORITY - Complete Phase 3 (Groups)

**What:** Rename AD Groups columns to canonical names

**Why:** Only 2 columns, minimal impact

**Estimated Time:** 1 day

**Changes Needed:**
- `name` → `group_name`
- `mail` → `group_email`

---

## Key Findings

### ✅ Good News

1. **Significant progress made**: All new canonical columns have been added
2. **Complex work complete**: JSONB flattening, department mapping, name derivation all done
3. **Transformation scripts likely updated**: TDX and UMAPI scripts confirmed working with new columns
4. **Minimal remaining work**: Mostly cleanup tasks, not new development

### ⚠️ Concerns

1. **Incomplete cleanup**: Old and new columns coexisting in 3 tables
2. **Transformation script status uncertain**: Need to verify MCommunity/AD scripts updated
3. **No integration tests run yet**: Unknown if data quality maintained
4. **Phases 2-3 not started**: 40% of original scope remaining

### 🎯 Recommended Focus

**Immediate (This Week):**
1. Verify all transformation scripts use new columns
2. Run data validation queries
3. Drop old columns (HIGH PRIORITY)

**Next 2 Weeks:**
4. Complete Phase 2 - Departments
5. Complete Phase 3 - Groups

**Timeline to Completion:** 3-4 weeks if following original plan

---

## Files Generated

1. **`.claude/lsats_data_dictionary_official.csv`**
   - 87 rows documenting all canonical fields
   - Tracks status of each field (COMPLETED, PARTIAL, NOT_STARTED)
   - Reference for all database work

2. **`.claude/data_dictionary_refactor_assessment.md`**
   - Complete 20-page assessment
   - Detailed analysis of each table
   - Transformation script status
   - Recommended actions with timelines
   - Risk assessment

3. **`.claude/data_dictionary_status_summary.md`** (this file)
   - Quick reference for status
   - Priority actions
   - Key findings

---

## Critical Next Steps

1. **Review this assessment** with team
2. **Decide on cleanup timeline** - can old columns be dropped now?
3. **Verify transformation scripts** - are they using new columns?
4. **Run validation queries** - check for data quality issues
5. **Execute Phase 1 cleanup** - drop old columns
6. **Plan Phases 2-3** - departments and groups

---

## Contact

For questions about this assessment or the data dictionary refactor, refer to:
- Original plan: `.claude/data_dictionary_refactor_plan.md`
- Data dictionary: `.claude/data_dictionary.md`
- This assessment: `.claude/data_dictionary_refactor_assessment.md`
