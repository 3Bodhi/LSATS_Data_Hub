#!/usr/bin/env python3
"""
Silver Layer Transformation: Computers

Consolidates computer records from three source-specific silver tables:
- silver.tdx_assets (Computer Form assets from TeamDynamix)
- silver.keyconfigure_computers (KeyConfigure inventory data)
- silver.ad_computers (Active Directory computer accounts)

Produces:
- silver.computers (consolidated computer records)
- silver.computer_groups (computer AD group memberships)

Matching Strategy:
1. Serial Number (primary - most reliable)
2. MAC Address (secondary - handles multi-NIC)
3. Computer Name (tertiary - handles renames)

Standards Compliance:
- Follows medallion architecture patterns (.claude/medallion_standards.md)
- Incremental processing with hash-based change detection
- Foreign key validation for users and departments
- Comprehensive data quality scoring with quality_flags
- JSONB consolidation for source-specific details
- Hard columns for frequently queried fields
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# ============================================================================
# LOGGING SETUP
# ============================================================================

script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def clean_nan_for_json(obj):
    """
    Recursively clean NaN, NaT, and inf values from nested dicts/lists for JSON serialization.
    Replaces pandas NaN/NaT with None (null in JSON).
    """
    if isinstance(obj, dict):
        return {k: clean_nan_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_for_json(item) for item in obj]
    elif pd.isna(obj):
        return None
    elif isinstance(obj, float) and (obj == float("inf") or obj == float("-inf")):
        return None
    else:
        return obj


# ============================================================================
# CONSOLIDATION SERVICE
# ============================================================================


class ComputerConsolidationService:
    """
    Service for consolidating computer records from TDX, KeyConfigure, and AD.

    Implements medallion standards:
    - Incremental processing with hash-based change detection
    - Foreign key validation and resolution
    - Data quality scoring with quality_flags array
    - JSONB consolidation for source-specific data
    - Comprehensive audit trail via source_raw_ids
    """

    def __init__(self, database_url: str):
        """Initialize service with database connection."""
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ Computer consolidation service initialized")

    # ========================================================================
    # INCREMENTAL PROCESSING
    # ========================================================================

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """Get timestamp of last successful consolidation run."""
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'computers_consolidated'
              AND status = 'completed'
            """
            result_df = self.db_adapter.query_to_dataframe(query)
            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last consolidation completed at: {timestamp}")
                return timestamp
            logger.info("📅 No previous consolidation runs found")
            return None
        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last consolidation timestamp: {e}")
            return None

    # ========================================================================
    # DATA FETCHING
    # ========================================================================

    def _fetch_source_records(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch records from source-specific silver tables.

        For matching to work correctly, we need ALL records from all sources.
        However, we only process changed records in silver.computers based on hash.
        """
        try:
            # TDX Assets (Computer Form only)
            logger.info("📥 Fetching TDX computer assets...")
            tdx_query = """
            SELECT * FROM silver.tdx_assets
            WHERE form_name = 'Computer Form'
            """
            if since_timestamp and not full_sync:
                tdx_query += f" AND updated_at > '{since_timestamp}'"

            tdx_records = self.db_adapter.query_to_dataframe(tdx_query).to_dict(
                "records"
            )
            logger.info(f"   📦 Fetched {len(tdx_records)} TDX computer assets")

            # KeyConfigure Computers (parse JSONB fields)
            logger.info("📥 Fetching KeyConfigure computers...")
            kc_query = "SELECT * FROM silver.keyconfigure_computers"
            if since_timestamp and not full_sync:
                kc_query += f" WHERE updated_at > '{since_timestamp}'"

            kc_df = self.db_adapter.query_to_dataframe(kc_query)

            # Parse JSONB fields that come as strings from pandas
            jsonb_fields = ["mac_addresses", "ip_addresses", "consolidated_raw_ids"]
            for field in jsonb_fields:
                if field in kc_df.columns:
                    kc_df[field] = kc_df[field].apply(
                        lambda x: json.loads(x)
                        if isinstance(x, str) and x
                        else (x if isinstance(x, list) else None)
                    )

            kc_records = kc_df.to_dict("records")
            logger.info(f"   📦 Fetched {len(kc_records)} KeyConfigure computers")

            # AD Computers
            logger.info("📥 Fetching AD computers...")
            ad_query = "SELECT * FROM silver.ad_computers"
            if since_timestamp and not full_sync:
                ad_query += f" WHERE updated_at > '{since_timestamp}'"

            ad_records = self.db_adapter.query_to_dataframe(ad_query).to_dict("records")
            logger.info(f"   📦 Fetched {len(ad_records)} AD computers")

            return {"tdx": tdx_records, "kc": kc_records, "ad": ad_records}
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch source records: {e}")
            raise

    # ========================================================================
    # MATCHING LOGIC
    # ========================================================================

    def _normalize_serial(self, serial: Any) -> Optional[str]:
        """Normalize serial number for matching."""
        if not serial or pd.isna(serial):
            return None
        s = str(serial).strip().upper()
        # Filter out junk serials (too short, all zeros, placeholders)
        if len(s) < 4 or s in ("0000", "NONE", "N/A", "UNKNOWN"):
            return None
        return s

    def _normalize_mac(self, mac: Any) -> Optional[str]:
        """Normalize MAC address for matching (removes separators)."""
        if not mac or pd.isna(mac):
            return None
        # Remove colons, dashes, dots, make uppercase
        m = str(mac).strip().upper()
        m = m.replace(":", "").replace("-", "").replace(".", "").replace(" ", "")
        # Valid MAC is 12 hex characters
        if len(m) == 12 and all(c in "0123456789ABCDEF" for c in m):
            return m
        return None

    def _normalize_name(self, name: Any) -> Optional[str]:
        """Normalize computer name for matching."""
        if not name or pd.isna(name):
            return None
        return str(name).strip().upper()

    def _extract_mac_list(self, mac_field: Any) -> List[str]:
        """
        Extract list of MAC addresses from various formats.

        Handles:
        - JSONB arrays from consolidated KeyConfigure: ["MAC1", "MAC2"]
        - Comma-separated strings from TDX: "MAC1,MAC2"
        - Single MAC strings: "MAC1"
        """
        # Check for None first
        if mac_field is None:
            return []

        # Handle JSONB arrays (from consolidated KeyConfigure)
        if isinstance(mac_field, list):
            if not mac_field:
                return []
            macs = [self._normalize_mac(m) for m in mac_field]
            return [m for m in macs if m]

        # Handle scalar values (check for NaN)
        try:
            if pd.isna(mac_field):
                return []
        except (ValueError, TypeError):
            pass

        if not mac_field:
            return []

        # Handle strings (comma-separated or single)
        mac_str = str(mac_field)
        macs = [self._normalize_mac(m) for m in re.split(r"[,;]", mac_str)]
        return [m for m in macs if m]

    def _match_records(
        self, source_data: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        Match records across sources using serial, MAC, and name.

        Strategy:
        1. Build indexes for Serial, MAC (multi-value), Name
        2. Process TDX first (most complete), find matches in KC and AD
        3. Process unmatched KC, find matches in AD
        4. Process unmatched AD

        Returns list of matched groups: [{"tdx": record, "kc": record, "ad": record}, ...]
        """
        logger.info("🧩 Matching records across sources...")

        # Helper to build index
        def create_index(records, key_func, allow_multiple=False):
            """Create index mapping normalized key -> record(s)."""
            idx = {}
            for r in records:
                keys = key_func(r) if allow_multiple else [key_func(r)]
                for k in keys:
                    if k:
                        if k not in idx:
                            idx[k] = []
                        idx[k].append(r)
            return idx

        # Index KeyConfigure by serial, MAC (now using mac_addresses array), name
        kc_by_serial = create_index(
            source_data["kc"],
            lambda r: self._normalize_serial(r.get("oem_serial_number")),
        )
        # NEW: Index by all MACs in the mac_addresses JSONB array
        kc_by_mac = {}
        for kc_rec in source_data["kc"]:
            # mac_addresses comes from JSONB, could be list or None
            mac_field = kc_rec.get("mac_addresses")
            if mac_field is not None:
                mac_list = self._extract_mac_list(mac_field)
                for mac in mac_list:
                    if mac:
                        if mac not in kc_by_mac:
                            kc_by_mac[mac] = []
                        kc_by_mac[mac].append(kc_rec)

        kc_by_name = create_index(
            source_data["kc"], lambda r: self._normalize_name(r.get("computer_name"))
        )

        # Index AD by name (primary), serial not available, MAC not reliable
        ad_by_name = create_index(
            source_data["ad"], lambda r: self._normalize_name(r.get("computer_name"))
        )

        matched_groups = []
        processed_kc_ids = set()
        processed_ad_ids = set()

        # PHASE 1: Process TDX records (most complete source)
        logger.info("   🔗 Phase 1: Matching TDX records...")
        for tdx in source_data["tdx"]:
            group = {"tdx": tdx, "kc": None, "ad": None}

            # PRIORITY 1: Try match KC by MAC Address (most reliable after KC consolidation)
            tdx_macs = self._extract_mac_list(tdx.get("attr_mac_address"))
            for tdx_mac in tdx_macs:
                if tdx_mac in kc_by_mac:
                    # Multiple KC records may have same MAC (though rare after consolidation)
                    for kc_match in kc_by_mac[tdx_mac]:
                        if kc_match["raw_id"] not in processed_kc_ids:
                            group["kc"] = kc_match
                            processed_kc_ids.add(kc_match["raw_id"])
                            logger.debug(
                                f"✅ Matched TDX {tdx.get('name')} to KC by MAC {tdx_mac}"
                            )
                            break
                if group["kc"]:
                    break

            # PRIORITY 2: Try match KC by Serial Number (fallback)
            if not group["kc"]:
                serial = self._normalize_serial(tdx.get("serial_number"))
                if serial and serial in kc_by_serial:
                    for kc_match in kc_by_serial[serial]:
                        if kc_match["raw_id"] not in processed_kc_ids:
                            # Verify MAC overlap if possible
                            kc_mac_field = kc_match.get("mac_addresses")
                            if kc_mac_field is not None and tdx_macs:
                                kc_macs = self._extract_mac_list(kc_mac_field)
                                has_overlap = kc_macs and any(
                                    mac in kc_macs for mac in tdx_macs
                                )
                            else:
                                has_overlap = False

                            if has_overlap:
                                group["kc"] = kc_match
                                processed_kc_ids.add(kc_match["raw_id"])
                                logger.debug(
                                    f"✅ Matched TDX {tdx.get('name')} to KC by serial {serial} (MAC verified)"
                                )
                                break

                    # If no MAC verification possible or no overlap, take first unprocessed
                    if not group["kc"]:
                        for kc_match in kc_by_serial[serial]:
                            if kc_match["raw_id"] not in processed_kc_ids:
                                group["kc"] = kc_match
                                processed_kc_ids.add(kc_match["raw_id"])
                                logger.debug(
                                    f"✅ Matched TDX {tdx.get('name')} to KC by serial {serial}"
                                )
                                break

            # PRIORITY 3: Try match KC by Computer Name (least reliable)
            if not group["kc"]:
                name = self._normalize_name(tdx.get("name"))
                if name and name in kc_by_name:
                    for kc_match in kc_by_name[name]:
                        if kc_match["raw_id"] not in processed_kc_ids:
                            group["kc"] = kc_match
                            processed_kc_ids.add(kc_match["raw_id"])
                            logger.debug(
                                f"✅ Matched TDX {tdx.get('name')} to KC by name"
                            )
                            break

            # Try match AD by Computer Name
            name = self._normalize_name(tdx.get("name"))
            if name and name in ad_by_name:
                # Take first AD match (most likely)
                ad_match = ad_by_name[name][0]
                if ad_match["raw_id"] not in processed_ad_ids:
                    group["ad"] = ad_match
                    processed_ad_ids.add(ad_match["raw_id"])

            matched_groups.append(group)

        logger.info(f"   ✅ Phase 1 complete: {len(matched_groups)} TDX-based groups")

        # PHASE 2: Process remaining KC records
        logger.info("   🔗 Phase 2: Matching unmatched KeyConfigure records...")
        unmatched_kc = 0
        for kc in source_data["kc"]:
            if kc["raw_id"] in processed_kc_ids:
                continue

            unmatched_kc += 1
            group = {"tdx": None, "kc": kc, "ad": None}

            # Try match AD by Computer Name
            name = self._normalize_name(kc.get("computer_name"))
            if name and name in ad_by_name:
                ad_match = ad_by_name[name][0]
                if ad_match["raw_id"] not in processed_ad_ids:
                    group["ad"] = ad_match
                    processed_ad_ids.add(ad_match["raw_id"])

            matched_groups.append(group)

        logger.info(f"   ✅ Phase 2 complete: {unmatched_kc} KC-only groups added")

        # PHASE 3: Process remaining AD records
        logger.info("   🔗 Phase 3: Adding unmatched AD records...")
        unmatched_ad = 0
        for ad in source_data["ad"]:
            if ad["raw_id"] in processed_ad_ids:
                continue
            unmatched_ad += 1
            matched_groups.append({"tdx": None, "kc": None, "ad": ad})

        logger.info(f"   ✅ Phase 3 complete: {unmatched_ad} AD-only groups added")
        logger.info(f"🧩 Total matched groups: {len(matched_groups)}")

        return matched_groups

    # ========================================================================
    # FIELD MERGING & CONSOLIDATION
    # ========================================================================

    def _derive_os_family(self, os_name: str) -> str:
        """Derive OS family from OS name string."""
        if not os_name:
            return "Unknown"

        os_upper = os_name.upper()
        if (
            "WINDOWS" in os_upper
            or "WIN " in os_upper
            or "WIN10" in os_upper
            or "WIN11" in os_upper
        ):
            return "Windows"
        elif "MAC" in os_upper or "MACOS" in os_upper or "OSX" in os_upper:
            return "macOS"
        elif any(
            x in os_upper
            for x in ["LINUX", "UBUNTU", "CENTOS", "RHEL", "REDHAT", "DEBIAN", "ALMA"]
        ):
            return "Linux"
        elif "UNIX" in os_upper or "SOLARIS" in os_upper or "AIX" in os_upper:
            return "Unix"
        elif "IOS" in os_upper or "IPAD" in os_upper:
            return "iOS"
        elif "ANDROID" in os_upper:
            return "Android"
        else:
            return "Other"

    def _resolve_owner_uniqname(
        self, tdx: Optional[Dict], kc: Optional[Dict], ad: Optional[Dict]
    ) -> Tuple[Optional[str], List[str]]:
        """
        Resolve operational owner_uniqname with FK validation.
        Priority: TDX Owning Customer → KC Owner → AD Managed By

        Returns: (uniqname, quality_flags)
        """
        quality_flags = []

        # Priority 1: TDX Owning Customer (operational owner)
        if tdx and tdx.get("owning_customer_id"):
            uid = tdx["owning_customer_id"]
            query = "SELECT uniqname FROM silver.users WHERE tdx_user_uid = :uid"
            try:
                result = self.db_adapter.query_to_dataframe(query, {"uid": uid})
                if not result.empty:
                    return result.iloc[0]["uniqname"], quality_flags
                else:
                    quality_flags.append("invalid_owner_tdx_uid")
            except Exception as e:
                logger.warning(
                    f"⚠️  Failed to resolve TDX owning customer UID {uid}: {e}"
                )
                quality_flags.append("owner_lookup_error")

        # Priority 2: KeyConfigure Owner (usually dept code or uniqname)
        if kc and kc.get("owner"):
            owner = kc["owner"]
            # Check if it looks like a uniqname (lowercase, no spaces)
            if (
                owner
                and isinstance(owner, str)
                and owner.islower()
                and " " not in owner
            ):
                # Validate it exists in silver.users
                query = "SELECT uniqname FROM silver.users WHERE uniqname = :uniqname"
                try:
                    result = self.db_adapter.query_to_dataframe(
                        query, {"uniqname": owner}
                    )
                    if not result.empty:
                        return owner, quality_flags
                except Exception:
                    pass

        # Priority 3: AD Managed By (extract CN from DN)
        if ad and ad.get("managed_by"):
            dn = ad["managed_by"]
            # Extract CN=uniqname from DN
            match = re.match(r"CN=([^,]+)", dn)
            if match:
                cn = match.group(1)
                # Validate in silver.users
                query = "SELECT uniqname FROM silver.users WHERE uniqname = :uniqname"
                try:
                    result = self.db_adapter.query_to_dataframe(
                        query, {"uniqname": cn.lower()}
                    )
                    if not result.empty:
                        return cn.lower(), quality_flags
                except Exception:
                    pass

        quality_flags.append("no_valid_owner")
        return None, quality_flags

    def _resolve_financial_owner_uniqname(
        self, tdx: Optional[Dict]
    ) -> Tuple[Optional[str], List[str]]:
        """
        Resolve financial_owner_uniqname from TDX Financial Owner UID only.
        No fallback - this is TDX-specific financial responsibility tracking.

        Returns: (uniqname, quality_flags)
        """
        quality_flags = []

        if tdx and tdx.get("attr_financial_owner_uid"):
            uid = tdx["attr_financial_owner_uid"]
            query = "SELECT uniqname FROM silver.users WHERE tdx_user_uid = :uid"
            try:
                result = self.db_adapter.query_to_dataframe(query, {"uid": uid})
                if not result.empty:
                    return result.iloc[0]["uniqname"], quality_flags
                else:
                    quality_flags.append("invalid_financial_owner_tdx_uid")
            except Exception as e:
                logger.warning(
                    f"⚠️  Failed to resolve TDX financial owner UID {uid}: {e}"
                )
                quality_flags.append("financial_owner_lookup_error")

        # No fallback - financial owner is TDX-specific
        return None, quality_flags

    def _resolve_department_id(
        self, tdx: Optional[Dict], ad: Optional[Dict], kc: Optional[Dict]
    ) -> Tuple[Optional[str], List[str]]:
        """
        Resolve owner_department_id with FK validation.
        Returns: (dept_id, quality_flags)
        """
        quality_flags = []

        # Priority 1: TDX Owning Department ID (map to silver.departments.dept_id)
        if tdx and tdx.get("owning_department_id"):
            tdx_dept_id = tdx["owning_department_id"]
            # Lookup in silver.departments: tdx_id -> dept_id
            query = "SELECT dept_id FROM silver.departments WHERE tdx_id = :tdx_id"
            try:
                result = self.db_adapter.query_to_dataframe(
                    query, {"tdx_id": tdx_dept_id}
                )
                if not result.empty:
                    return result.iloc[0]["dept_id"], quality_flags
                else:
                    quality_flags.append("invalid_department_tdx_id")
            except Exception as e:
                logger.warning(
                    f"⚠️  Failed to resolve TDX department ID {tdx_dept_id}: {e}"
                )
                quality_flags.append("department_lookup_error")

        # Priority 2: AD OU Department (map to silver.departments by name)
        if ad and ad.get("ou_department"):
            ou_dept = ad["ou_department"]
            # Try to match by department name
            query = "SELECT dept_id FROM silver.departments WHERE department_name ILIKE :name"
            try:
                result = self.db_adapter.query_to_dataframe(
                    query, {"name": f"%{ou_dept}%"}
                )
                if not result.empty:
                    return result.iloc[0]["dept_id"], quality_flags
            except Exception:
                pass

        # Priority 3: KeyConfigure Owner (if it's a dept code)
        if kc and kc.get("owner"):
            owner = kc["owner"]
            # Check if it matches dept_id pattern (e.g., "189100", "LSA-PSYC")
            if owner:
                query = "SELECT dept_id FROM silver.departments WHERE dept_id = :dept_id OR department_code = :dept_id"
                try:
                    result = self.db_adapter.query_to_dataframe(
                        query, {"dept_id": owner}
                    )
                    if not result.empty:
                        return result.iloc[0]["dept_id"], quality_flags
                except Exception:
                    pass

        quality_flags.append("no_valid_department")
        return None, quality_flags

    def _merge_computer_group(self, group: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge a matched group into a consolidated computer record.

        Implements field priority rules:
        - Identity: Serial (TDX > KC), MAC (TDX > KC), Name (TDX > KC > AD)
        - Hardware: Manufacturer/Model (TDX), Specs (KC), OS (KC > AD > TDX)
        - Activity: Last Seen = max(all timestamps)
        - Ownership: Resolve FKs during merge
        """
        tdx = group["tdx"]
        kc = group["kc"]
        ad = group["ad"]

        sources = []
        if tdx:
            sources.append("tdx")
        if kc:
            sources.append("keyconfigure")
        if ad:
            sources.append("ad")
        source_system = ",".join(sources)

        # ====================================================================
        # IDENTITY FIELDS
        # ====================================================================

        # Serial Number (TDX primary, KC secondary)
        serial_number = self._normalize_serial(
            tdx.get("serial_number") if tdx else None
        )
        if not serial_number:
            serial_number = self._normalize_serial(
                kc.get("oem_serial_number") if kc else None
            )

        # Collect all serials for serial_numbers array
        all_serials = []
        if tdx and tdx.get("serial_number"):
            all_serials.append(self._normalize_serial(tdx["serial_number"]))
        if kc and kc.get("oem_serial_number"):
            s = self._normalize_serial(kc["oem_serial_number"])
            if s and s not in all_serials:
                all_serials.append(s)

        # MAC Address (TDX primary, KC secondary from JSONB array)
        mac_address = None
        all_macs = []
        if tdx and tdx.get("attr_mac_address"):
            tdx_macs = self._extract_mac_list(tdx["attr_mac_address"])
            all_macs.extend(tdx_macs)
            if tdx_macs:
                mac_address = tdx_macs[0]  # Primary MAC
        if kc:
            kc_mac_field = kc.get("mac_addresses")
            if kc_mac_field is not None:
                # KC now has JSONB array of all MACs (after consolidation)
                kc_macs = self._extract_mac_list(kc_mac_field)
                for kc_mac in kc_macs:
                    if kc_mac and kc_mac not in all_macs:
                        all_macs.append(kc_mac)
            # Use KC primary_mac_address if no TDX MAC
            if not mac_address and kc.get("primary_mac_address"):
                mac_address = self._normalize_mac(kc["primary_mac_address"])

        # Computer Name (TDX > KC > AD)
        computer_name = None
        all_names = []
        if tdx and tdx.get("name"):
            computer_name = tdx["name"]
            all_names.append(tdx["name"])
        if kc and kc.get("computer_name"):
            if kc["computer_name"] not in all_names:
                all_names.append(kc["computer_name"])
            if not computer_name:
                computer_name = kc["computer_name"]
        if ad and ad.get("computer_name"):
            if ad["computer_name"] not in all_names:
                all_names.append(ad["computer_name"])
            if not computer_name:
                computer_name = ad["computer_name"]

        # Fallback: If still no name, use TDX tag, serial, or generate ID
        if not computer_name:
            if tdx and tdx.get("tag"):
                computer_name = f"TDX-{tdx['tag']}"
            elif serial_number:
                computer_name = f"SERIAL-{serial_number}"
            elif tdx and tdx.get("tdx_asset_id"):
                computer_name = f"TDX-ASSET-{tdx['tdx_asset_id']}"
            elif ad and ad.get("object_guid"):
                computer_name = f"AD-{ad['object_guid']}"
            else:
                # Last resort: generate a unique ID
                computer_name = f"UNKNOWN-{uuid.uuid4().hex[:8].upper()}"

            if computer_name:
                all_names.append(computer_name)

        # Computed computer_id (stable identifier)
        computer_id = self._normalize_name(computer_name)

        # ====================================================================
        # SOURCE SYSTEM IDENTIFIERS
        # ====================================================================

        # TDX identifiers
        tdx_asset_id = tdx.get("tdx_asset_id") if tdx else None
        tdx_tag = tdx.get("tag") if tdx else None
        tdx_status_id = tdx.get("status_id") if tdx else None
        tdx_status_name = tdx.get("status_name") if tdx else None
        tdx_form_id = tdx.get("form_id") if tdx else None
        tdx_form_name = tdx.get("form_name") if tdx else None
        tdx_configuration_item_id = tdx.get("configuration_item_id") if tdx else None
        tdx_external_id = tdx.get("external_id") if tdx else None
        tdx_uri = tdx.get("uri") if tdx else None

        # AD identifiers
        ad_object_guid = ad.get("object_guid") if ad else None
        ad_object_sid = ad.get("object_sid") if ad else None
        ad_sam_account_name = ad.get("sam_account_name") if ad else None
        ad_dns_hostname = ad.get("dns_hostname") if ad else None
        ad_distinguished_name = ad.get("distinguished_name") if ad else None

        # KC identifiers (updated for new schema)
        kc_computer_id = kc.get("computer_id") if kc else None
        kc_primary_mac = kc.get("primary_mac_address") if kc else None
        kc_nic_count = kc.get("nic_count") if kc else None

        # ====================================================================
        # OWNERSHIP & ASSIGNMENT (with FK resolution)
        # ====================================================================

        # Operational owner (TDX Owning Customer → KC Owner → AD Managed By)
        owner_uniqname, owner_flags = self._resolve_owner_uniqname(tdx, kc, ad)

        # Financial owner (TDX Financial Owner only, no fallback)
        financial_owner_uniqname, financial_owner_flags = (
            self._resolve_financial_owner_uniqname(tdx)
        )

        # Department resolution
        owner_department_id, dept_flags = self._resolve_department_id(tdx, ad, kc)

        quality_flags = owner_flags + financial_owner_flags + dept_flags

        # ====================================================================
        # HARDWARE & SOFTWARE SPECIFICATIONS
        # ====================================================================

        # Manufacturer and Model (from TDX - most complete)
        manufacturer = tdx.get("manufacturer_name") if tdx else None
        product_model = tdx.get("product_model_name") if tdx else None

        # Operating System (KC > AD > TDX for normalized name)
        os_name = None
        if kc and kc.get("os"):
            os_name = kc["os"]
        elif ad and ad.get("operating_system"):
            os_name = ad["operating_system"]
        elif tdx and tdx.get("attr_operating_system_name"):
            os_name = tdx["attr_operating_system_name"]

        os_family = self._derive_os_family(os_name) if os_name else None

        # OS Version (KC > AD)
        os_version = None
        if kc and kc.get("os_version"):
            os_version = kc["os_version"]
        elif ad and ad.get("operating_system_version"):
            os_version = ad["operating_system_version"]

        os_install_date = kc.get("os_install_date") if kc else None

        # Hardware specs (from KC - most accurate)
        cpu = kc.get("cpu") if kc else None
        cpu_cores = kc.get("cpu_cores") if kc else None
        cpu_sockets = kc.get("cpu_sockets") if kc else None
        cpu_speed_mhz = kc.get("clock_speed_mhz") if kc else None
        ram_mb = kc.get("ram_mb") if kc else None
        disk_gb = kc.get("disk_gb") if kc else None
        disk_free_gb = kc.get("disk_free_gb") if kc else None

        # ====================================================================
        # ACTIVITY & STATUS
        # ====================================================================

        # Status (from TDX)
        is_active = True
        if tdx and tdx.get("status_name"):
            # Consider "Active" status as active, others as inactive
            is_active = "active" in tdx["status_name"].lower()

        # AD enabled status
        is_ad_enabled = ad.get("is_enabled") if ad else None

        # Last seen (max of all activity timestamps)
        timestamps = []
        if tdx and tdx.get("modified_date"):
            timestamps.append(tdx["modified_date"])
        if kc and kc.get("last_audit"):
            timestamps.append(kc["last_audit"])
        if kc and kc.get("last_session"):
            timestamps.append(kc["last_session"])
        if ad and ad.get("last_logon_timestamp"):
            timestamps.append(ad["last_logon_timestamp"])

        last_seen = None
        if timestamps:
            valid_ts = [t for t in timestamps if t and not pd.isna(t)]
            if valid_ts:
                last_seen = max(valid_ts)

        # Recent activity flag (within last 90 days)
        has_recent_activity = False
        if last_seen:
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            has_recent_activity = last_seen > cutoff

        # Last user (from KC)
        last_user = kc.get("last_user") if kc else None

        # ====================================================================
        # JSONB CONSOLIDATED DATA
        # ====================================================================

        # Location info (from TDX)
        location_info = {}
        if tdx:
            location_info = {
                "location_id": tdx.get("location_id"),
                "location_name": tdx.get("location_name"),
                "room_id": tdx.get("location_room_id"),
                "room_name": tdx.get("location_room_name"),
            }

        # Ownership info (from all sources)
        ownership_info = {}
        if tdx:
            ownership_info["tdx_owning"] = {
                "customer_uid": str(tdx["owning_customer_id"])
                if tdx.get("owning_customer_id")
                else None,
                "customer_name": tdx.get("owning_customer_name"),
                "department_id": tdx.get("owning_department_id"),
                "department_name": tdx.get("owning_department_name"),
            }
            ownership_info["tdx_requesting"] = {
                "customer_uid": str(tdx["requesting_customer_id"])
                if tdx.get("requesting_customer_id")
                else None,
                "customer_name": tdx.get("requesting_customer_name"),
                "department_id": tdx.get("requesting_department_id"),
                "department_name": tdx.get("requesting_department_name"),
            }
            ownership_info["tdx_financial_owner"] = {
                "uid": str(tdx["attr_financial_owner_uid"])
                if tdx.get("attr_financial_owner_uid")
                else None,
                "name": tdx.get("attr_financial_owner_name"),
            }
        if kc:
            ownership_info["kc_owner"] = kc.get("owner")
        if ad:
            ownership_info["ad_managed_by"] = ad.get("managed_by")

        # Hardware specs (detailed from all sources)
        hardware_specs = {}
        if tdx:
            hardware_specs["tdx"] = {
                "memory": tdx.get("attr_memory"),
                "storage": tdx.get("attr_storage"),
                "processor": tdx.get("attr_processor_count"),
            }
        if kc:
            hardware_specs["kc"] = {
                "cpu": kc.get("cpu"),
                "cpu_cores": kc.get("cpu_cores"),
                "cpu_sockets": kc.get("cpu_sockets"),
                "clock_speed_mhz": kc.get("clock_speed_mhz"),
                "ram_mb": kc.get("ram_mb"),
                "disk_gb": float(kc["disk_gb"])
                if kc.get("disk_gb") and not pd.isna(kc.get("disk_gb"))
                else None,
                "disk_free_gb": float(kc["disk_free_gb"])
                if kc.get("disk_free_gb") and not pd.isna(kc.get("disk_free_gb"))
                else None,
            }

        # OS details (from all sources)
        os_details = {}
        if tdx:
            os_details["tdx"] = {"name": tdx.get("attr_operating_system_name")}
        if kc:
            os_details["kc"] = {
                "os": kc.get("os"),
                "os_family": kc.get("os_family"),
                "os_version": kc.get("os_version"),
                "os_serial_number": kc.get("os_serial_number"),
                "os_install_date": kc["os_install_date"].isoformat()
                if kc.get("os_install_date") and not pd.isna(kc.get("os_install_date"))
                else None,
            }
        if ad:
            os_details["ad"] = {
                "operating_system": ad.get("operating_system"),
                "operating_system_version": ad.get("operating_system_version"),
                "operating_system_service_pack": ad.get(
                    "operating_system_service_pack"
                ),
            }

        # Network info
        network_info = {"mac_addresses": all_macs, "ip_addresses": []}
        if tdx and tdx.get("attr_ip_address"):
            # TDX can have comma-separated IPs
            ips = [ip.strip() for ip in str(tdx["attr_ip_address"]).split(",")]
            network_info["ip_addresses"].extend(ips)
        if kc and kc.get("last_ip_address"):
            ip = kc["last_ip_address"]
            if ip not in network_info["ip_addresses"]:
                network_info["ip_addresses"].append(ip)
        if ad and ad.get("dns_hostname"):
            network_info["dns_hostname"] = ad["dns_hostname"]

        # AD security info
        ad_security_info = {}
        if ad:
            ad_security_info = {
                "service_principal_names": ad.get("service_principal_names"),
                "ms_laps_password_expiration_time": ad.get(
                    "ms_laps_password_expiration_time"
                ),
                "user_account_control": ad.get("user_account_control"),
                "is_critical_system_object": ad.get("is_critical_system_object"),
            }

        # AD OU info
        ad_ou_info = {}
        if ad:
            ad_ou_info = {
                "ou_root": ad.get("ou_root"),
                "ou_organization_type": ad.get("ou_organization_type"),
                "ou_organization": ad.get("ou_organization"),
                "ou_category": ad.get("ou_category"),
                "ou_division": ad.get("ou_division"),
                "ou_department": ad.get("ou_department"),
                "ou_subdepartment": ad.get("ou_subdepartment"),
                "ou_immediate_parent": ad.get("ou_immediate_parent"),
                "ou_full_path": ad.get("ou_full_path"),
                "distinguished_name": ad.get("distinguished_name"),
            }

        # Financial info (from TDX)
        financial_info = {}
        if tdx:
            financial_info = {
                "purchase_cost": float(tdx["purchase_cost"])
                if tdx.get("purchase_cost") and not pd.isna(tdx.get("purchase_cost"))
                else None,
                "acquisition_date": tdx["acquisition_date"].isoformat()
                if tdx.get("acquisition_date")
                and not pd.isna(tdx.get("acquisition_date"))
                else None,
                "expected_replacement_date": tdx[
                    "expected_replacement_date"
                ].isoformat()
                if tdx.get("expected_replacement_date")
                and not pd.isna(tdx.get("expected_replacement_date"))
                else None,
            }

        # Activity timestamps (all sources)
        activity_timestamps = {}
        if tdx:
            activity_timestamps["tdx_created"] = (
                tdx["created_date"].isoformat()
                if tdx.get("created_date") and not pd.isna(tdx.get("created_date"))
                else None
            )
            activity_timestamps["tdx_modified"] = (
                tdx["modified_date"].isoformat()
                if tdx.get("modified_date") and not pd.isna(tdx.get("modified_date"))
                else None
            )
        if kc:
            activity_timestamps["kc_last_audit"] = (
                kc["last_audit"].isoformat()
                if kc.get("last_audit") and not pd.isna(kc.get("last_audit"))
                else None
            )
            activity_timestamps["kc_last_session"] = (
                kc["last_session"].isoformat()
                if kc.get("last_session") and not pd.isna(kc.get("last_session"))
                else None
            )
            activity_timestamps["kc_last_startup"] = (
                kc["last_startup"].isoformat()
                if kc.get("last_startup") and not pd.isna(kc.get("last_startup"))
                else None
            )
            activity_timestamps["kc_base_audit"] = (
                kc["base_audit"].isoformat()
                if kc.get("base_audit") and not pd.isna(kc.get("base_audit"))
                else None
            )
        if ad:
            activity_timestamps["ad_last_logon"] = (
                ad["last_logon"].isoformat()
                if ad.get("last_logon") and not pd.isna(ad.get("last_logon"))
                else None
            )
            activity_timestamps["ad_last_logon_timestamp"] = (
                ad["last_logon_timestamp"].isoformat()
                if ad.get("last_logon_timestamp")
                and not pd.isna(ad.get("last_logon_timestamp"))
                else None
            )
            activity_timestamps["ad_pwd_last_set"] = (
                ad["pwd_last_set"].isoformat()
                if ad.get("pwd_last_set") and not pd.isna(ad.get("pwd_last_set"))
                else None
            )
            activity_timestamps["ad_when_created"] = (
                ad["when_created"].isoformat()
                if ad.get("when_created") and not pd.isna(ad.get("when_created"))
                else None
            )
            activity_timestamps["ad_when_changed"] = (
                ad["when_changed"].isoformat()
                if ad.get("when_changed") and not pd.isna(ad.get("when_changed"))
                else None
            )

        # TDX attributes
        tdx_attributes = {}
        if tdx:
            tdx_attributes = {
                "support_groups": {
                    "ids": tdx.get("attr_support_groups_ids"),
                    "text": tdx.get("attr_support_groups_text"),
                },
                "function": {
                    "id": tdx.get("attr_function_id"),
                    "name": tdx.get("attr_function_name"),
                },
                "purchase_shortcode": tdx.get("attr_purchase_shortcode"),
                "all_attributes": tdx.get("attributes"),
            }

        # TDX attachments
        tdx_attachments = tdx.get("attachments", []) if tdx else []

        # Source raw IDs (audit trail)
        source_raw_ids = {
            "tdx_raw_id": str(tdx["raw_id"]) if tdx and tdx.get("raw_id") else None,
            "ad_raw_id": str(ad["raw_id"]) if ad and ad.get("raw_id") else None,
            "keyconfigure_raw_id": str(kc["raw_id"])
            if kc and kc.get("raw_id")
            else None,
        }

        # ====================================================================
        # RETURN CONSOLIDATED RECORD
        # ====================================================================

        return {
            # Identifiers
            "computer_id": computer_id,
            "computer_name": computer_name,
            "computer_name_aliases": all_names,
            "serial_number": serial_number,
            "serial_numbers": all_serials,
            "mac_address": mac_address,
            "mac_addresses": all_macs,
            # Source system IDs
            "tdx_asset_id": tdx_asset_id,
            "tdx_tag": tdx_tag,
            "tdx_status_id": tdx_status_id,
            "tdx_status_name": tdx_status_name,
            "tdx_form_id": tdx_form_id,
            "tdx_form_name": tdx_form_name,
            "tdx_configuration_item_id": tdx_configuration_item_id,
            "tdx_external_id": tdx_external_id,
            "tdx_uri": tdx_uri,
            "ad_object_guid": ad_object_guid,
            "ad_object_sid": ad_object_sid,
            "ad_sam_account_name": ad_sam_account_name,
            "ad_dns_hostname": ad_dns_hostname,
            "ad_distinguished_name": ad_distinguished_name,
            "kc_computer_id": kc_computer_id,
            "kc_primary_mac": kc_primary_mac,
            "kc_nic_count": kc_nic_count,
            # Ownership (resolved FKs)
            "owner_uniqname": owner_uniqname,
            "financial_owner_uniqname": financial_owner_uniqname,
            "owner_department_id": owner_department_id,
            # Hardware/Software
            "manufacturer": manufacturer,
            "product_model": product_model,
            "os_family": os_family,
            "os_name": os_name,
            "os_version": os_version,
            "os_install_date": os_install_date,
            "cpu": cpu,
            "cpu_cores": cpu_cores,
            "cpu_sockets": cpu_sockets,
            "cpu_speed_mhz": cpu_speed_mhz,
            "ram_mb": ram_mb,
            "disk_gb": disk_gb,
            "disk_free_gb": disk_free_gb,
            # Activity/Status
            "is_active": is_active,
            "is_ad_enabled": is_ad_enabled,
            "has_recent_activity": has_recent_activity,
            "last_seen": last_seen,
            "last_user": last_user,
            # JSONB fields
            "location_info": location_info,
            "ownership_info": ownership_info,
            "hardware_specs": hardware_specs,
            "os_details": os_details,
            "network_info": network_info,
            "ad_security_info": ad_security_info,
            "ad_ou_info": ad_ou_info,
            "financial_info": financial_info,
            "activity_timestamps": activity_timestamps,
            "tdx_attributes": tdx_attributes,
            "tdx_attachments": tdx_attachments,
            "source_raw_ids": source_raw_ids,
            # Metadata
            "source_system": source_system,
            "quality_flags": quality_flags,
            # AD group memberships (for junction table)
            "_ad_member_of_groups": ad.get("member_of_groups") if ad else None,
        }

    # ========================================================================
    # DATA QUALITY SCORING
    # ========================================================================

    def _calculate_data_quality(self, record: Dict[str, Any]) -> Decimal:
        """
        Calculate data quality score (0.00-1.00) based on completeness.

        Scoring factors:
        - Has serial number: +0.20
        - Has MAC address: +0.10
        - Has computer name: +0.10 (required, always present)
        - Has TDX data: +0.15
        - Has AD data: +0.10
        - Has KC data: +0.10
        - Has valid owner: +0.10
        - Has valid department: +0.10
        - Has recent activity: +0.05
        """
        score = Decimal("0.10")  # Base for having computer_name (required)

        # Identity completeness
        if record.get("serial_number"):
            score += Decimal("0.20")
        if record.get("mac_address"):
            score += Decimal("0.10")

        # Source coverage
        sources = record.get("source_system", "").split(",")
        if "tdx" in sources:
            score += Decimal("0.15")
        if "ad" in sources:
            score += Decimal("0.10")
        if "keyconfigure" in sources:
            score += Decimal("0.10")

        # Ownership completeness
        if record.get("owner_uniqname") and "no_valid_owner" not in record.get(
            "quality_flags", []
        ):
            score += Decimal("0.10")
        if record.get(
            "owner_department_id"
        ) and "no_valid_department" not in record.get("quality_flags", []):
            score += Decimal("0.10")

        # Activity recency
        if record.get("has_recent_activity"):
            score += Decimal("0.05")

        return min(Decimal("1.00"), score)

    def _calculate_content_hash(self, record: Dict[str, Any]) -> str:
        """Calculate SHA-256 hash for change detection."""
        # Exclude metadata fields that change on every run
        exclude = {
            "data_quality_score",
            "quality_flags",
            "entity_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "silver_id",
            "_ad_member_of_groups",  # Handled separately in junction table
        }
        payload = {k: v for k, v in record.items() if k not in exclude}
        # Convert to JSON with sorted keys for consistent hashing
        json_str = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode("utf-8")).hexdigest()

    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================

    def _batch_upsert_computers(
        self, records: List[Dict[str, Any]], run_id: str, dry_run: bool
    ) -> Tuple[int, int, int]:
        """
        Batch upsert computer records to silver.computers.
        Returns: (processed, created, updated)
        """
        if not records:
            return 0, 0, 0

        if dry_run:
            logger.info(f"🔍 [DRY RUN] Would upsert {len(records)} computer records")
            return len(records), len(records), 0

        try:
            from sqlalchemy.dialects.postgresql import insert

            created = 0
            updated = 0

            # Process in batches
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                for record in batch:
                    # Prepare record for insertion
                    db_record = record.copy()

                    # Remove internal fields
                    db_record.pop("_ad_member_of_groups", None)

                    # Convert JSONB fields to JSON strings
                    jsonb_fields = [
                        "computer_name_aliases",
                        "serial_numbers",
                        "mac_addresses",
                        "location_info",
                        "ownership_info",
                        "hardware_specs",
                        "os_details",
                        "network_info",
                        "ad_security_info",
                        "ad_ou_info",
                        "financial_info",
                        "activity_timestamps",
                        "tdx_attributes",
                        "tdx_attachments",
                        "source_raw_ids",
                        "quality_flags",
                    ]
                    for field in jsonb_fields:
                        if field in db_record and db_record[field] is not None:
                            # Clean NaN values before JSON serialization
                            cleaned_data = clean_nan_for_json(db_record[field])
                            db_record[field] = json.dumps(cleaned_data, default=str)

                    # Handle pandas NaT/NaN values
                    for key, value in list(db_record.items()):
                        if pd.isna(value):
                            db_record[key] = None

                    # Add metadata
                    db_record["ingestion_run_id"] = run_id
                    db_record["updated_at"] = datetime.now(timezone.utc)

                    # Calculate quality score
                    db_record["data_quality_score"] = self._calculate_data_quality(
                        record
                    )

                    # Calculate hash
                    db_record["entity_hash"] = self._calculate_content_hash(record)

                    # Check if record exists
                    check_query = "SELECT entity_hash FROM silver.computers WHERE computer_id = :computer_id"
                    existing = self.db_adapter.query_to_dataframe(
                        check_query, {"computer_id": db_record["computer_id"]}
                    )

                    if existing.empty:
                        # Insert new record
                        db_record["created_at"] = datetime.now(timezone.utc)
                        created += 1
                    else:
                        # Check if hash changed
                        if existing.iloc[0]["entity_hash"] == db_record["entity_hash"]:
                            # No change, skip
                            continue
                        updated += 1

                    # Upsert using INSERT...ON CONFLICT
                    with self.db_adapter.engine.connect() as conn:
                        with conn.begin():
                            conn.execute(text("SET search_path TO silver, public"))

                            # Build upsert statement
                            columns = list(db_record.keys())
                            values_placeholders = ", ".join(
                                [f":{col}" for col in columns]
                            )
                            columns_str = ", ".join(columns)

                            # Update columns (all except computer_id)
                            update_set = ", ".join(
                                [
                                    f"{col} = EXCLUDED.{col}"
                                    for col in columns
                                    if col != "computer_id"
                                ]
                            )

                            upsert_sql = f"""
                            INSERT INTO computers ({columns_str})
                            VALUES ({values_placeholders})
                            ON CONFLICT (computer_id) DO UPDATE SET {update_set}
                            """

                            conn.execute(text(upsert_sql), db_record)

            logger.info(
                f"✅ Upserted {created} new, {updated} updated computer records"
            )
            return len(records), created, updated

        except Exception as e:
            logger.error(f"❌ Batch upsert failed: {e}")
            raise

    def _upsert_computer_groups(
        self, records: List[Dict[str, Any]], dry_run: bool
    ) -> int:
        """
        Upsert computer group memberships to silver.computer_groups junction table.
        Returns: count of memberships processed
        """
        if dry_run:
            total_memberships = sum(
                len(r.get("_ad_member_of_groups", []))
                if r.get("_ad_member_of_groups")
                else 0
                for r in records
            )
            logger.info(
                f"🔍 [DRY RUN] Would upsert {total_memberships} computer group memberships"
            )
            return total_memberships

        try:
            from sqlalchemy.dialects.postgresql import insert

            memberships = []

            for record in records:
                computer_id = record.get("computer_id")
                ad_groups = record.get("_ad_member_of_groups")

                if not computer_id or not ad_groups:
                    continue

                # ad_groups is JSONB array of DNs
                for group_dn in ad_groups:
                    # Extract CN from DN
                    match = re.match(r"CN=([^,]+)", group_dn)
                    group_cn = match.group(1) if match else None

                    # Try to resolve to silver.groups.group_id
                    group_id = None
                    if group_cn:
                        query = "SELECT group_id FROM silver.groups WHERE cn = :cn OR group_name = :cn"
                        try:
                            result = self.db_adapter.query_to_dataframe(
                                query, {"cn": group_cn}
                            )
                            if not result.empty:
                                group_id = result.iloc[0]["group_id"]
                        except Exception:
                            pass

                    memberships.append(
                        {
                            "computer_id": computer_id,
                            "group_id": group_id,
                            "group_dn": group_dn,
                            "group_cn": group_cn,
                            "source_system": "active_directory",
                            "created_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc),
                        }
                    )

            if not memberships:
                return 0

            # Batch upsert
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text("SET search_path TO silver, public"))

                    for membership in memberships:
                        upsert_sql = """
                        INSERT INTO computer_groups (computer_id, group_id, group_dn, group_cn, source_system, created_at, updated_at)
                        VALUES (:computer_id, :group_id, :group_dn, :group_cn, :source_system, :created_at, :updated_at)
                        ON CONFLICT (computer_id, group_dn) DO UPDATE SET
                            group_id = EXCLUDED.group_id,
                            updated_at = EXCLUDED.updated_at
                        """
                        conn.execute(text(upsert_sql), membership)

            logger.info(f"✅ Upserted {len(memberships)} computer group memberships")
            return len(memberships)

        except Exception as e:
            logger.error(f"❌ Computer groups upsert failed: {e}")
            raise

    # ========================================================================
    # RUN TRACKING
    # ========================================================================

    def create_transformation_run(self) -> str:
        """Create a new transformation run in meta.ingestion_runs."""
        run_id = str(uuid.uuid4())
        try:
            query = """
            INSERT INTO meta.ingestion_runs (
                run_id, source_system, entity_type, started_at, status, metadata
            ) VALUES (
                :run_id, 'silver_transformation', 'computers_consolidated',
                :started_at, 'running', :metadata
            )
            """
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(query),
                        {
                            "run_id": run_id,
                            "started_at": datetime.now(timezone.utc),
                            "metadata": json.dumps(
                                {"script": "013_transform_computers.py"}
                            ),
                        },
                    )
            logger.info(f"📝 Created transformation run: {run_id}")
            return run_id
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create transformation run: {e}")
            raise

    def complete_transformation_run(
        self,
        run_id: str,
        status: str,
        stats: Dict[str, int],
        error_message: Optional[str] = None,
    ):
        """Complete transformation run with statistics."""
        try:
            query = """
            UPDATE meta.ingestion_runs
            SET completed_at = :completed_at,
                status = :status,
                records_processed = :records_processed,
                records_created = :records_created,
                records_updated = :records_updated,
                error_message = :error_message,
                metadata = metadata || :stats
            WHERE run_id = :run_id
            """
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(query),
                        {
                            "run_id": run_id,
                            "completed_at": datetime.now(timezone.utc),
                            "status": status,
                            "records_processed": stats.get("processed", 0),
                            "records_created": stats.get("created", 0),
                            "records_updated": stats.get("updated", 0),
                            "error_message": error_message,
                            "stats": json.dumps(stats),
                        },
                    )
            logger.info(f"✅ Completed transformation run: {run_id} ({status})")
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    # ========================================================================
    # MAIN CONSOLIDATION LOGIC
    # ========================================================================

    def consolidate_computers(self, full_sync: bool = False, dry_run: bool = False):
        """
        Main consolidation logic.

        Args:
            full_sync: If True, reprocess all records. If False, only process changed records.
            dry_run: If True, don't write to database, just log what would happen.
        """
        run_id = self.create_transformation_run()
        stats = {"processed": 0, "created": 0, "updated": 0, "groups_processed": 0}

        try:
            # 1. Get last transformation timestamp
            since_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )

            # 2. Fetch source records
            logger.info("📥 Fetching source records...")
            source_data = self._fetch_source_records(since_timestamp, full_sync)

            # 3. Match records across sources
            logger.info("🧩 Matching records...")
            matched_groups = self._match_records(source_data)

            # 4. Merge into consolidated records
            logger.info("🔗 Merging matched groups...")
            consolidated_records = []
            for i, group in enumerate(matched_groups):
                if (i + 1) % 1000 == 0:
                    logger.info(
                        f"   ⏳ Processed {i + 1}/{len(matched_groups)} groups..."
                    )
                merged = self._merge_computer_group(group)
                consolidated_records.append(merged)

            logger.info(f"✅ Merged {len(consolidated_records)} computer records")

            # 5. Upsert to silver.computers
            logger.info("💾 Upserting to silver.computers...")
            processed, created, updated = self._batch_upsert_computers(
                consolidated_records, run_id, dry_run
            )
            stats["processed"] = processed
            stats["created"] = created
            stats["updated"] = updated

            # 6. Upsert to silver.computer_groups
            logger.info("💾 Upserting to silver.computer_groups...")
            groups_processed = self._upsert_computer_groups(
                consolidated_records, dry_run
            )
            stats["groups_processed"] = groups_processed

            # 7. Complete run
            if not dry_run:
                self.complete_transformation_run(run_id, "completed", stats)

            # 8. Summary
            logger.info("=" * 80)
            logger.info("🎉 CONSOLIDATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Computers processed: {stats['processed']}")
            logger.info(f"   ✨ Created: {stats['created']}")
            logger.info(f"   🔄 Updated: {stats['updated']}")
            logger.info(f"   👥 Group memberships: {stats['groups_processed']}")
            logger.info("=" * 80)

        except Exception as e:
            import traceback

            logger.error(f"❌ Consolidation failed: {e}")
            logger.error(traceback.format_exc())
            if not dry_run:
                self.complete_transformation_run(run_id, "failed", stats, str(e))
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔒 Database connection closed")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


def main():
    """Main entry point for computer consolidation."""
    parser = argparse.ArgumentParser(
        description="Consolidate computer records from TDX, KeyConfigure, and AD"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to database",
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Reprocess all records (ignore last transformation timestamp)",
    )
    args = parser.parse_args()

    # Load environment
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("❌ DATABASE_URL not set in environment")
        sys.exit(1)

    # Run consolidation
    service = None
    try:
        service = ComputerConsolidationService(db_url)
        service.consolidate_computers(full_sync=args.full_sync, dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)
    finally:
        if service:
            service.close()


if __name__ == "__main__":
    main()
