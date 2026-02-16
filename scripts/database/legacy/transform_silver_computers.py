#!/usr/bin/env python3
"""
Silver Layer Computer Transformation Service

Transforms raw computer/asset data from key_client, active_directory, and tdx
into standardized silver layer records with proper cross-source matching,
lab associations, and relationship tracking.

Key Features:
- Three-way matching across key_client, AD, and TDX
- Multi-method lab association with confidence scoring
- Computer group membership extraction
- TDX custom attribute preservation
- Hardware specification normalization
- Activity tracking and last-seen calculation

Usage:
    python transform_silver_computers.py [--full-sync] [--dry-run] [--batch-size 100]
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_batch, register_uuid

# Register UUID adapter
register_uuid()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ComputerSilverTransformationService:
    """
    Service for transforming bronze computer data into silver layer records.

    Handles:
    - Cross-source computer matching (key_client, AD, TDX)
    - Multi-method lab association (5 methods with confidence scores)
    - Group membership extraction
    - Hardware specification normalization
    - Activity tracking
    """

    # Regex patterns
    DN_PATTERN = re.compile(r"([A-Z]+)=([^,]+)", re.IGNORECASE)
    OU_PATTERN = re.compile(r"OU=([^,]+)", re.IGNORECASE)
    CN_PATTERN = re.compile(r"CN=([^,]+)", re.IGNORECASE)

    def __init__(self, db_config: Dict[str, str], dry_run: bool = False):
        """
        Initialize the transformation service.

        Args:
            db_config: Database connection configuration.
            dry_run: If True, don't commit changes to database.
        """
        self.db_config = db_config
        self.dry_run = dry_run
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
        logger.info(
            f"Connected to database for computer transformation (dry_run={dry_run})"
        )

    def _normalize_computer_name(self, name: Optional[str]) -> str:
        """
        Normalize computer name for matching.

        Args:
            name: Raw computer name.

        Returns:
            Normalized lowercase name.
        """
        if not name:
            return ""
        return name.strip().lower()

    def _normalize_mac_address(self, mac: Optional[str]) -> str:
        """
        Normalize MAC address to uppercase, no delimiters.

        Args:
            mac: Raw MAC address.

        Returns:
            Normalized MAC address (e.g., "F4390913EBAA").
        """
        if not mac:
            return ""
        # Remove all non-alphanumeric characters
        return re.sub(r"[^A-F0-9]", "", mac.upper())

    def _format_mac_address(self, mac: Optional[str]) -> Optional[str]:
        """
        Format MAC address with colons (standard format).

        Args:
            mac: Normalized MAC address.

        Returns:
            Formatted MAC address (e.g., "F4:39:09:13:EB:AA") or None.
        """
        if not mac or len(mac) != 12:
            return None
        return ":".join([mac[i : i + 2] for i in range(0, 12, 2)])

    def _normalize_serial_number(self, serial: Optional[str]) -> str:
        """
        Normalize serial number to uppercase, trimmed.

        Args:
            serial: Raw serial number.

        Returns:
            Normalized serial number.
        """
        if not serial:
            return ""
        return serial.strip().upper()

    def _get_tdx_attribute_value(
        self, tdx_raw_data: Dict[str, Any], attribute_name: str
    ) -> Optional[str]:
        """
        Extract attribute value from TDX Attributes list.

        Args:
            tdx_raw_data: TDX raw_data dictionary.
            attribute_name: Name of attribute to find (e.g., "MAC Address(es)").

        Returns:
            Attribute value or None if not found.
        """
        if not isinstance(tdx_raw_data, dict):
            return None

        attributes = tdx_raw_data.get("Attributes", [])
        if not isinstance(attributes, list):
            return None

        for attr in attributes:
            if isinstance(attr, dict) and attr.get("Name") == attribute_name:
                return attr.get("Value") or attr.get("ValueText")

        return None

    def _load_bronze_computers(
        self, source_system: str, since_timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Load bronze computer records from specific source.

        Args:
            source_system: 'key_client', 'active_directory', or 'tdx'.
            since_timestamp: Only load records ingested after this time.

        Returns:
            List of bronze record dictionaries.
        """
        entity_type = (
            "computer"
            if source_system in ["key_client", "active_directory"]
            else "asset"
        )

        query = """
            SELECT
                raw_id,
                entity_type,
                source_system,
                external_id,
                raw_data,
                ingested_at
            FROM bronze.raw_entities
            WHERE entity_type = %s
              AND source_system = %s
        """
        params = [entity_type, source_system]

        if since_timestamp:
            query += " AND ingested_at > %s"
            params.append(since_timestamp)

        query += " ORDER BY ingested_at DESC"

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            records = cur.fetchall()

        logger.info(f"📥 Loaded {len(records)} {source_system} records from bronze")
        # RealDictCursor returns RealDictRow objects, convert to dict
        return [dict(r) for r in records]

    def _match_computers_across_sources(
        self,
        kc_records: List[Dict],
        ad_records: List[Dict],
        tdx_records: List[Dict],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Three-way matching: name → MAC → serial.

        Args:
            kc_records: Key client records.
            ad_records: Active Directory records.
            tdx_records: TeamDynamix records.

        Returns:
            Dict mapping computer_id to matched records.
        """
        matches = {}

        # Build lookup indexes
        kc_by_name = {}
        kc_by_mac = {}
        kc_by_serial = {}

        ad_by_name = {}

        tdx_by_name = {}
        tdx_by_serial = {}

        # Index key_client
        for record in kc_records:
            raw_data = record.get("raw_data", {})
            if not raw_data:
                continue
            name = self._normalize_computer_name(raw_data.get("Name"))
            mac = self._normalize_mac_address(raw_data.get("MAC"))
            serial = self._normalize_serial_number(raw_data.get("OEM SN"))

            if name:
                kc_by_name[name] = record
            if mac:
                kc_by_mac[mac] = record
            if serial:
                kc_by_serial[serial] = record

        # Index AD
        for record in ad_records:
            raw_data = record.get("raw_data", {})
            if not raw_data:
                continue
            name = self._normalize_computer_name(raw_data.get("cn"))
            if name:
                ad_by_name[name] = record

        # Index TDX
        for record in tdx_records:
            raw_data = record.get("raw_data", {})
            if not raw_data:
                continue
            name = self._normalize_computer_name(raw_data.get("Name"))
            serial = self._normalize_serial_number(raw_data.get("SerialNumber"))

            if name:
                tdx_by_name[name] = record
            if serial:
                tdx_by_serial[serial] = record

        # Phase 1: Match by name (primary)
        all_names = (
            set(kc_by_name.keys()) | set(ad_by_name.keys()) | set(tdx_by_name.keys())
        )

        for name in all_names:
            if not name:
                continue

            computer_id = name
            matches[computer_id] = {
                "computer_id": computer_id,
                "kc_record": kc_by_name.get(name),
                "ad_record": ad_by_name.get(name),
                "tdx_record": tdx_by_name.get(name),
                "match_method": "name",
            }

        # Phase 2: Match remaining by MAC (kc ↔ tdx)
        matched_kc_ids = {
            m["kc_record"]["raw_id"] for m in matches.values() if m.get("kc_record")
        }
        matched_tdx_ids = {
            m["tdx_record"]["raw_id"] for m in matches.values() if m.get("tdx_record")
        }

        for mac, kc_record in kc_by_mac.items():
            if kc_record["raw_id"] in matched_kc_ids:
                continue

            # Try to find TDX record with same MAC
            tdx_mac_match = None
            for tdx_record in tdx_records:
                if tdx_record["raw_id"] in matched_tdx_ids:
                    continue

                tdx_raw_data = tdx_record.get("raw_data", {})
                if not isinstance(tdx_raw_data, dict):
                    continue

                # Extract MAC address from TDX attributes
                mac_attr = self._get_tdx_attribute_value(
                    tdx_raw_data, "MAC Address(es)"
                )
                tdx_mac = self._normalize_mac_address(mac_attr)
                if tdx_mac == mac:
                    tdx_mac_match = tdx_record
                    matched_tdx_ids.add(tdx_record["raw_id"])
                    break

            if tdx_mac_match:
                computer_id = f"mac_{mac[:8]}"
                matches[computer_id] = {
                    "computer_id": computer_id,
                    "kc_record": kc_record,
                    "ad_record": None,
                    "tdx_record": tdx_mac_match,
                    "match_method": "mac",
                }
                matched_kc_ids.add(kc_record["raw_id"])

        # Phase 3: Match remaining by serial (kc ↔ tdx)
        for serial, kc_record in kc_by_serial.items():
            if kc_record["raw_id"] in matched_kc_ids or not serial:
                continue

            if serial in tdx_by_serial:
                tdx_record = tdx_by_serial[serial]
                if tdx_record["raw_id"] not in matched_tdx_ids:
                    computer_id = f"serial_{serial[:8]}"
                    matches[computer_id] = {
                        "computer_id": computer_id,
                        "kc_record": kc_record,
                        "ad_record": None,
                        "tdx_record": tdx_record,
                        "match_method": "serial",
                    }
                    matched_kc_ids.add(kc_record["raw_id"])
                    matched_tdx_ids.add(tdx_record["raw_id"])

        logger.info(f"🔗 Matched {len(matches)} unique computers")
        logger.info(
            f"   - Name matches: {sum(1 for m in matches.values() if m['match_method'] == 'name')}"
        )
        logger.info(
            f"   - MAC matches: {sum(1 for m in matches.values() if m['match_method'] == 'mac')}"
        )
        logger.info(
            f"   - Serial matches: {sum(1 for m in matches.values() if m['match_method'] == 'serial')}"
        )

        return matches

    def _parse_ad_ou_hierarchy(self, dn: str) -> List[str]:
        """
        Extract OU hierarchy from AD DN.

        Args:
            dn: Distinguished name.

        Returns:
            List of OU components (ordered from specific to general).
        """
        if not dn:
            return []

        ous = self.OU_PATTERN.findall(dn)
        return ous

    def _extract_cn_from_dn(self, dn: str) -> str:
        """
        Extract CN from group DN.

        Args:
            dn: Group distinguished name.

        Returns:
            CN value or empty string.
        """
        if not dn:
            return ""

        match = self.CN_PATTERN.search(dn)
        return match.group(1) if match else ""

    def _merge_bronze_to_silver(
        self, computer_id: str, matched_records: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge up to 3 bronze records into silver schema.

        Args:
            computer_id: Computer ID for this record.
            matched_records: Dict with kc_record, ad_record, tdx_record.

        Returns:
            Silver computer record dictionary.
        """
        kc = (
            matched_records.get("kc_record", {}).get("raw_data", {})
            if matched_records.get("kc_record")
            else {}
        )
        ad = (
            matched_records.get("ad_record", {}).get("raw_data", {})
            if matched_records.get("ad_record")
            else {}
        )
        tdx = (
            matched_records.get("tdx_record", {}).get("raw_data", {})
            if matched_records.get("tdx_record")
            else {}
        )

        # Computer name priority: AD > TDX > KC
        computer_name = ad.get("cn") or tdx.get("Name") or kc.get("Name") or computer_id

        # Collect all name aliases from different sources
        name_aliases = []
        for name in [
            ad.get("cn"),
            tdx.get("Name"),
            kc.get("Name"),
            ad.get("dNSHostName"),
        ]:
            if name and name != computer_name:
                normalized = name.strip().lower()
                if (
                    normalized not in [computer_name.lower()]
                    and normalized not in name_aliases
                ):
                    name_aliases.append(normalized)

        # Hardware identifiers
        mac_kc = self._normalize_mac_address(kc.get("MAC"))
        mac_tdx = self._normalize_mac_address(
            self._get_tdx_attribute_value(tdx, "MAC Address(es)") if tdx else None
        )
        mac_primary = mac_kc or mac_tdx
        mac_formatted = self._format_mac_address(mac_primary)

        mac_list = list(filter(None, [mac_kc, mac_tdx]))
        mac_addresses = [self._format_mac_address(m) for m in mac_list if m]

        serial_tdx = self._normalize_serial_number(
            tdx.get("SerialNumber") if tdx else None
        )
        serial_kc = self._normalize_serial_number(kc.get("OEM SN") if kc else None)
        serial_primary = serial_tdx or serial_kc

        serial_list = list(filter(None, [serial_tdx, serial_kc]))

        # TDX identifiers
        tdx_asset_id = tdx.get("ID") if tdx else None
        tdx_asset_uid = tdx.get("UID") if tdx else None

        # AD identifiers
        ad_dn = ad.get("dn") if ad else None
        ad_ou_hierarchy = self._parse_ad_ou_hierarchy(ad_dn) if ad_dn else []

        # Hardware specs (priority: KC > TDX)
        cpu = (kc.get("CPU") if kc else None) or (
            self._get_tdx_attribute_value(tdx, "Processor(s)") if tdx else None
        )
        ram_mb = int(kc.get("RAM")) if kc.get("RAM") else None
        disk_gb = float(kc.get("Disk")) if kc.get("Disk") else None
        disk_free_gb = float(kc.get("Free")) if kc.get("Free") else None

        # OS info
        os_name = ad.get("operatingSystem") or kc.get("OS Family")
        os_version = kc.get("OS vers") or ad.get("operatingSystemVersion")

        # Activity tracking
        last_user = kc.get("Last User")
        last_audit = self._parse_datetime(kc.get("Last Audit"))
        last_session = self._parse_datetime(kc.get("Last Session"))
        last_logon = self._parse_datetime(ad.get("lastLogon"))
        last_logon_timestamp = self._parse_datetime(ad.get("lastLogonTimestamp"))

        # Calculate last_seen (max of all activity)
        activity_times = [
            t for t in [last_audit, last_session, last_logon, last_logon_timestamp] if t
        ]
        last_seen = max(activity_times) if activity_times else None

        # Recent activity check (90 days)
        has_recent_activity = False
        if last_seen:
            days_since = (datetime.now(timezone.utc) - last_seen).days
            has_recent_activity = days_since <= 90

        # Source flags
        has_key_client_data = bool(matched_records.get("kc_record"))
        has_ad_data = bool(matched_records.get("ad_record"))
        has_tdx_data = bool(matched_records.get("tdx_record"))

        sources = []
        if has_key_client_data:
            sources.append("key_client")
        if has_ad_data:
            sources.append("active_directory")
        if has_tdx_data:
            sources.append("tdx")
        data_source = "+".join(sources)

        silver_record = {
            "computer_id": computer_id,
            "computer_name": computer_name,
            "computer_name_aliases": json.dumps(
                name_aliases
            ),  # Convert to JSON string for JSONB
            "mac_address": mac_formatted,
            "mac_addresses": json.dumps(
                mac_addresses
            ),  # Convert to JSON string for JSONB
            "serial_number": serial_primary,
            "serial_numbers": json.dumps(
                serial_list
            ),  # Convert to JSON string for JSONB
            "tdx_asset_id": tdx_asset_id,
            "tdx_asset_uid": tdx_asset_uid,
            "tdx_tag": tdx.get("Tag"),
            "tdx_status_id": tdx.get("StatusID"),
            "tdx_form_id": tdx.get("FormID"),
            "ad_object_guid": ad.get("objectGUID"),
            "ad_object_sid": ad.get("objectSid"),
            "ad_sam_account_name": ad.get("sAMAccountName"),
            "ad_dns_hostname": ad.get("dNSHostName"),
            "kc_agid": kc.get("agid"),
            "kc_idnt": kc.get("idnt"),
            "owner_group": kc.get("Owner"),
            "tdx_owning_customer_uid": tdx.get("OwningCustomerID") if tdx else None,
            "tdx_owning_department_id": tdx.get("OwningDepartmentID") if tdx else None,
            "tdx_requesting_customer_uid": tdx.get("RequestingCustomerID")
            if tdx
            else None,
            "tdx_location_id": tdx.get("LocationID") if tdx else None,
            "tdx_location_room_id": tdx.get("LocationRoomID"),
            "ad_dn": ad_dn,
            "ad_ou_hierarchy": json.dumps(
                ad_ou_hierarchy
            ),  # Convert to JSON string for JSONB
            "ad_parent_ou": self._get_parent_ou(ad_dn) if ad_dn else None,
            "ad_ou_depth": len(ad_ou_hierarchy),
            "cpu": cpu,
            "cpu_speed_mhz": int(kc.get("Clock Speed (Mhz)"))
            if kc.get("Clock Speed (Mhz)")
            else None,
            "cpu_cores": int(kc.get("# of cores")) if kc.get("# of cores") else None,
            "cpu_sockets": int(kc.get("Sockets")) if kc.get("Sockets") else None,
            "ram_mb": ram_mb,
            "disk_gb": disk_gb,
            "disk_free_gb": disk_free_gb,
            "os_name": os_name,
            "os_version": os_version,
            "os_install_date": self._parse_datetime(kc.get("OS Install Date")),
            "os_serial_number": kc.get("OS SN"),
            "kc_client_version": kc.get("Client"),
            "last_user": last_user,
            "last_logon": last_logon,
            "last_logon_timestamp": last_logon_timestamp,
            "last_audit": last_audit,
            "last_session": last_session,
            "last_startup": self._parse_datetime(kc.get("Last Startup")),
            "base_audit": self._parse_datetime(kc.get("Base Audit")),
            "last_seen": last_seen,
            "ad_pwd_last_set": self._parse_datetime(ad.get("pwdLastSet")),
            "ad_when_created": self._parse_datetime(ad.get("whenCreated")),
            "ad_when_changed": self._parse_datetime(ad.get("whenChanged")),
            "is_active": True,  # Default to active
            "is_ad_enabled": not bool(int(ad.get("userAccountControl", 0)) & 0x0002)
            if ad and ad.get("userAccountControl")
            else None,
            "has_recent_activity": has_recent_activity,
            "has_key_client_data": has_key_client_data,
            "has_ad_data": has_ad_data,
            "has_tdx_data": has_tdx_data,
            "data_source": data_source,
            "source_system": data_source,
        }

        return silver_record

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse datetime from various formats, always returning UTC timezone-aware datetime.

        Args:
            value: Datetime string or object.

        Returns:
            Timezone-aware datetime object in UTC or None.
        """
        from datetime import timezone

        if not value:
            return None

        if isinstance(value, datetime):
            # If already timezone-aware, convert to UTC
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc)
            # If naive, assume UTC
            return value.replace(tzinfo=timezone.utc)

        if isinstance(value, str):
            # Try parsing common formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%f%z",  # With microseconds and timezone
                "%Y-%m-%dT%H:%M:%S%z",  # With timezone
                "%Y-%m-%dT%H:%M:%S.%fZ",  # With microseconds, Z suffix
                "%Y-%m-%dT%H:%M:%SZ",  # Z suffix (UTC)
                "%Y-%m-%d",  # Date only
                "%m/%d/%Y",  # US date format
            ]:
                try:
                    dt = datetime.strptime(value, fmt)
                    # Ensure timezone-aware
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except ValueError:
                    continue

        return None

    def _get_parent_ou(self, dn: str) -> Optional[str]:
        """
        Get parent OU from DN.

        Args:
            dn: Distinguished name.

        Returns:
            Parent OU DN or None.
        """
        if not dn:
            return None

        # Remove first component (CN=...,)
        parts = dn.split(",", 1)
        return parts[1] if len(parts) > 1 else None

    def _calculate_entity_hash(self, computer_record: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 hash for change detection.

        Args:
            computer_record: Silver computer record.

        Returns:
            SHA-256 hash string.
        """
        hashable = {
            "computer_name": computer_record.get("computer_name"),
            "mac_address": computer_record.get("mac_address"),
            "serial_number": computer_record.get("serial_number"),
            "tdx_asset_id": computer_record.get("tdx_asset_id"),
            "ad_object_guid": computer_record.get("ad_object_guid"),
            "last_seen": str(computer_record.get("last_seen")),
        }

        content = json.dumps(hashable, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()

    def _calculate_data_quality(
        self, computer_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score (0.00-1.00) and quality flags.

        Args:
            computer_record: Silver computer record.

        Returns:
            Tuple of (score, flags).
        """
        score = Decimal("1.00")
        flags = []

        # Critical identifiers
        if not computer_record.get("serial_number"):
            flags.append("no_serial")
            score -= Decimal("0.10")

        if not computer_record.get("mac_address"):
            flags.append("no_mac")
            score -= Decimal("0.10")

        # Ownership
        if not computer_record.get("owner_uniqname") and not computer_record.get(
            "tdx_owning_customer_uid"
        ):
            flags.append("no_owner")
            score -= Decimal("0.15")

        # Hardware specs
        if not computer_record.get("cpu") or not computer_record.get("ram_mb"):
            flags.append("no_hardware_specs")
            score -= Decimal("0.10")

        # Activity
        if computer_record.get("last_seen"):
            days_since_seen = (
                datetime.now(timezone.utc) - computer_record["last_seen"]
            ).days
            if days_since_seen > 180:
                flags.append("stale")
                score -= Decimal("0.10")

        # Source coverage
        source_count = sum(
            [
                computer_record.get("has_key_client_data", False),
                computer_record.get("has_ad_data", False),
                computer_record.get("has_tdx_data", False),
            ]
        )
        if source_count == 1:
            flags.append("single_source")
            score -= Decimal("0.15")

        # Department association
        if not computer_record.get("owner_department_id"):
            flags.append("no_department")
            score -= Decimal("0.10")

        # Lab association
        if not computer_record.get("primary_lab_id"):
            flags.append("no_lab")
            score -= Decimal("0.05")

        score = max(score, Decimal("0.00"))
        return score, flags

    def _resolve_owner_uniqname(
        self, tdx_uid: Optional[str], kc_last_user: Optional[str]
    ) -> Optional[str]:
        """
        Resolve owner by TDX UID lookup or use KC last_user.

        Args:
            tdx_uid: TDX OwningCustomerID.
            kc_last_user: Key client last user.

        Returns:
            Owner uniqname or None.
        """
        if not tdx_uid or tdx_uid == "00000000-0000-0000-0000-000000000000":
            return kc_last_user

        # Look up user by TDX UID
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT uniqname
                FROM silver.users
                WHERE tdx_user_uid = %s
            """,
                [tdx_uid],
            )

            result = cur.fetchone()
            return result["uniqname"] if result else kc_last_user

    def _resolve_owner_department(self, tdx_dept_id: Optional[int]) -> Optional[str]:
        """
        Resolve department ID from TDX department ID.

        Args:
            tdx_dept_id: TDX OwningDepartmentID.

        Returns:
            Department ID or None.
        """
        if not tdx_dept_id:
            return None

        # Look up department by TDX ID
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT dept_id
                FROM silver.departments
                WHERE tdx_id = %s
            """,
                [tdx_dept_id],
            )

            result = cur.fetchone()
            return result["dept_id"] if result else None

    def _associate_with_labs(
        self, computer_record: Dict[str, Any]
    ) -> List[Tuple[str, str, Decimal, Dict[str, Any]]]:
        """
        Associate computer with lab(s) using 5 methods.

        Returns:
            List of (lab_id, method, confidence, evidence) tuples.
        """
        associations = []

        # Method 1: AD OU Nested
        if computer_record.get("ad_dn"):
            dn = computer_record["ad_dn"]

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT lab_id, ad_ou_dn
                    FROM silver.labs
                    WHERE has_ad_ou = true
                      AND %s LIKE '%%' || ad_ou_dn
                """,
                    [dn],
                )

                for row in cur.fetchall():
                    associations.append(
                        (
                            row["lab_id"],
                            "ad_ou_nested",
                            Decimal("0.95"),
                            {"matched_ou": row["ad_ou_dn"]},
                        )
                    )

        # Method 2 & 5: Owner checks
        owner = computer_record.get("owner_uniqname")
        if owner:
            # Method 5: Owner is PI
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT lab_id
                    FROM silver.labs
                    WHERE pi_uniqname = %s
                """,
                    [owner],
                )

                result = cur.fetchone()
                if result:
                    associations.append(
                        (
                            result["lab_id"],
                            "owner_is_pi",
                            Decimal("0.90"),
                            {"matched_user": owner},
                        )
                    )

            # Method 4: Owner in lab members
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT lab_id
                    FROM silver.lab_members
                    WHERE member_uniqname = %s
                """,
                    [owner],
                )

                for row in cur.fetchall():
                    associations.append(
                        (
                            row["lab_id"],
                            "owner_member",
                            Decimal("0.60"),
                            {"matched_user": owner},
                        )
                    )

        # Method 3: Last user in lab members
        last_user = computer_record.get("last_user")
        if last_user:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT lab_id
                    FROM silver.lab_members
                    WHERE member_uniqname = %s
                """,
                    [last_user.lower()],
                )

                for row in cur.fetchall():
                    associations.append(
                        (
                            row["lab_id"],
                            "last_user_member",
                            Decimal("0.45"),
                            {"matched_user": last_user},
                        )
                    )

        # Deduplicate (same lab via multiple methods)
        unique_assoc = {}
        for lab_id, method, confidence, evidence in associations:
            if lab_id not in unique_assoc or confidence > unique_assoc[lab_id][1]:
                unique_assoc[lab_id] = (method, confidence, evidence)

        result = [
            (lab_id, method, conf, evidence)
            for lab_id, (method, conf, evidence) in unique_assoc.items()
        ]

        return result

    def _extract_computer_groups(
        self, computer_id: str, ad_record: Optional[Dict]
    ) -> List[Dict[str, Any]]:
        """
        Extract group memberships from AD memberOf.

        Args:
            computer_id: Computer ID.
            ad_record: AD bronze record.

        Returns:
            List of group membership dicts.
        """
        if not ad_record or not isinstance(ad_record, dict):
            return []

        raw_data = ad_record.get("raw_data", {})
        if not isinstance(raw_data, dict):
            return []

        member_of = raw_data.get("memberOf", [])
        if not isinstance(member_of, list):
            member_of = [member_of] if member_of else []

        groups = []
        for group_dn in member_of:
            if not group_dn:
                continue

            group_cn = self._extract_cn_from_dn(group_dn)

            # Try to find group in silver.groups
            group_id = None
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT group_id
                    FROM silver.groups
                    WHERE mcommunity_dn = %s
                       OR LOWER(group_name) = %s
                    LIMIT 1
                """,
                    [group_dn, group_cn.lower()],
                )

                result = cur.fetchone()
                if result:
                    group_id = result["group_id"]

            groups.append(
                {
                    "computer_id": computer_id,
                    "group_id": group_id,
                    "group_dn": group_dn,
                    "group_cn": group_cn,
                    "source_system": "active_directory",
                }
            )

        return groups

    def _extract_tdx_attributes(
        self, computer_id: str, tdx_record: Optional[Dict]
    ) -> List[Dict[str, Any]]:
        """
        Extract TDX custom attributes.

        Args:
            computer_id: Computer ID.
            tdx_record: TDX bronze record.

        Returns:
            List of attribute dicts.
        """
        if not tdx_record or not isinstance(tdx_record, dict):
            return []

        raw_data = tdx_record.get("raw_data", {})
        if not isinstance(raw_data, dict):
            return []

        attributes_list = raw_data.get("Attributes", [])
        if not isinstance(attributes_list, list):
            return []

        attributes = []
        form_id = raw_data.get("FormID")

        # Attributes is a list of dicts with Name, Value, ValueText fields
        for attr_data in attributes_list:
            if not isinstance(attr_data, dict):
                continue

            attr_name = attr_data.get("Name")
            attr_value = attr_data.get("Value") or attr_data.get("ValueText")
            attr_id = attr_data.get("ID")

            if attr_name:  # Only add if we have a name
                attributes.append(
                    {
                        "computer_id": computer_id,
                        "attribute_name": attr_name,
                        "attribute_value": str(attr_value) if attr_value else None,
                        "attribute_value_uid": attr_id,
                        "source_system": "tdx",
                        "tdx_form_id": form_id,
                    }
                )

        return attributes

    def _upsert_computer(self, computer_record: Dict[str, Any], run_id: UUID) -> str:
        """
        Insert or update computer record.

        Args:
            computer_record: Silver computer record.
            run_id: Ingestion run ID.

        Returns:
            'inserted', 'updated', or 'unchanged'.
        """
        # Calculate hash
        entity_hash = self._calculate_entity_hash(computer_record)

        # Check if exists
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT entity_hash
                FROM silver.computers
                WHERE computer_id = %s
            """,
                [computer_record["computer_id"]],
            )

            existing = cur.fetchone()

        if existing and existing["entity_hash"] == entity_hash:
            return "unchanged"

        # Resolve owner and department
        owner_uniqname = self._resolve_owner_uniqname(
            computer_record.get("tdx_owning_customer_uid"),
            computer_record.get("last_user"),
        )

        # Validate owner exists in silver.users (to satisfy foreign key constraint)
        if owner_uniqname:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT 1 FROM silver.users WHERE uniqname = %s LIMIT 1",
                    [owner_uniqname],
                )
                if cur.fetchone():
                    computer_record["owner_uniqname"] = owner_uniqname
                else:
                    computer_record["owner_uniqname"] = None  # User not in silver.users
        else:
            computer_record["owner_uniqname"] = None

        computer_record["owner_department_id"] = self._resolve_owner_department(
            computer_record.get("tdx_owning_department_id")
        )

        # Calculate quality
        quality_score, quality_flags = self._calculate_data_quality(computer_record)
        computer_record["data_quality_score"] = quality_score
        computer_record["quality_flags"] = json.dumps(
            quality_flags
        )  # Convert to JSON string for JSONB
        computer_record["entity_hash"] = entity_hash
        computer_record["ingestion_run_id"] = run_id

        # Upsert
        if self.dry_run:
            logger.debug(
                f"[DRY RUN] Would upsert computer: {computer_record['computer_id']}"
            )
            return "inserted" if not existing else "updated"

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver.computers (
                    computer_id, computer_name, computer_name_aliases,
                    mac_address, mac_addresses, serial_number, serial_numbers,
                    tdx_asset_id, tdx_asset_uid, tdx_tag, tdx_status_id, tdx_form_id,
                    ad_object_guid, ad_object_sid, ad_sam_account_name, ad_dns_hostname,
                    kc_agid, kc_idnt,
                    owner_uniqname, owner_department_id, owner_group,
                    tdx_owning_customer_uid, tdx_requesting_customer_uid,
                    tdx_location_id, tdx_location_room_id,
                    ad_dn, ad_ou_hierarchy, ad_parent_ou, ad_ou_depth,
                    cpu, cpu_speed_mhz, cpu_cores, cpu_sockets,
                    ram_mb, disk_gb, disk_free_gb,
                    os_name, os_version, os_install_date, os_serial_number,
                    kc_client_version,
                    last_user, last_logon, last_logon_timestamp,
                    last_audit, last_session, last_startup, base_audit, last_seen,
                    ad_pwd_last_set, ad_when_created, ad_when_changed,
                    is_active, is_ad_enabled, has_recent_activity,
                    has_key_client_data, has_ad_data, has_tdx_data, data_source,
                    data_quality_score, quality_flags,
                    source_system, entity_hash, ingestion_run_id
                ) VALUES (
                    %(computer_id)s, %(computer_name)s, %(computer_name_aliases)s,
                    %(mac_address)s, %(mac_addresses)s, %(serial_number)s, %(serial_numbers)s,
                    %(tdx_asset_id)s, %(tdx_asset_uid)s, %(tdx_tag)s, %(tdx_status_id)s, %(tdx_form_id)s,
                    %(ad_object_guid)s, %(ad_object_sid)s, %(ad_sam_account_name)s, %(ad_dns_hostname)s,
                    %(kc_agid)s, %(kc_idnt)s,
                    %(owner_uniqname)s, %(owner_department_id)s, %(owner_group)s,
                    %(tdx_owning_customer_uid)s, %(tdx_requesting_customer_uid)s,
                    %(tdx_location_id)s, %(tdx_location_room_id)s,
                    %(ad_dn)s, %(ad_ou_hierarchy)s, %(ad_parent_ou)s, %(ad_ou_depth)s,
                    %(cpu)s, %(cpu_speed_mhz)s, %(cpu_cores)s, %(cpu_sockets)s,
                    %(ram_mb)s, %(disk_gb)s, %(disk_free_gb)s,
                    %(os_name)s, %(os_version)s, %(os_install_date)s, %(os_serial_number)s,
                    %(kc_client_version)s,
                    %(last_user)s, %(last_logon)s, %(last_logon_timestamp)s,
                    %(last_audit)s, %(last_session)s, %(last_startup)s, %(base_audit)s, %(last_seen)s,
                    %(ad_pwd_last_set)s, %(ad_when_created)s, %(ad_when_changed)s,
                    %(is_active)s, %(is_ad_enabled)s, %(has_recent_activity)s,
                    %(has_key_client_data)s, %(has_ad_data)s, %(has_tdx_data)s, %(data_source)s,
                    %(data_quality_score)s, %(quality_flags)s,
                    %(source_system)s, %(entity_hash)s, %(ingestion_run_id)s
                )
                ON CONFLICT (computer_id) DO UPDATE SET
                    computer_name = EXCLUDED.computer_name,
                    computer_name_aliases = EXCLUDED.computer_name_aliases,
                    mac_address = EXCLUDED.mac_address,
                    mac_addresses = EXCLUDED.mac_addresses,
                    serial_number = EXCLUDED.serial_number,
                    serial_numbers = EXCLUDED.serial_numbers,
                    tdx_asset_id = EXCLUDED.tdx_asset_id,
                    tdx_asset_uid = EXCLUDED.tdx_asset_uid,
                    tdx_tag = EXCLUDED.tdx_tag,
                    tdx_status_id = EXCLUDED.tdx_status_id,
                    tdx_form_id = EXCLUDED.tdx_form_id,
                    ad_object_guid = EXCLUDED.ad_object_guid,
                    ad_object_sid = EXCLUDED.ad_object_sid,
                    ad_sam_account_name = EXCLUDED.ad_sam_account_name,
                    ad_dns_hostname = EXCLUDED.ad_dns_hostname,
                    kc_agid = EXCLUDED.kc_agid,
                    kc_idnt = EXCLUDED.kc_idnt,
                    owner_uniqname = EXCLUDED.owner_uniqname,
                    owner_department_id = EXCLUDED.owner_department_id,
                    owner_group = EXCLUDED.owner_group,
                    tdx_owning_customer_uid = EXCLUDED.tdx_owning_customer_uid,
                    tdx_requesting_customer_uid = EXCLUDED.tdx_requesting_customer_uid,
                    tdx_location_id = EXCLUDED.tdx_location_id,
                    tdx_location_room_id = EXCLUDED.tdx_location_room_id,
                    ad_dn = EXCLUDED.ad_dn,
                    ad_ou_hierarchy = EXCLUDED.ad_ou_hierarchy,
                    ad_parent_ou = EXCLUDED.ad_parent_ou,
                    ad_ou_depth = EXCLUDED.ad_ou_depth,
                    cpu = EXCLUDED.cpu,
                    cpu_speed_mhz = EXCLUDED.cpu_speed_mhz,
                    cpu_cores = EXCLUDED.cpu_cores,
                    cpu_sockets = EXCLUDED.cpu_sockets,
                    ram_mb = EXCLUDED.ram_mb,
                    disk_gb = EXCLUDED.disk_gb,
                    disk_free_gb = EXCLUDED.disk_free_gb,
                    os_name = EXCLUDED.os_name,
                    os_version = EXCLUDED.os_version,
                    os_install_date = EXCLUDED.os_install_date,
                    os_serial_number = EXCLUDED.os_serial_number,
                    kc_client_version = EXCLUDED.kc_client_version,
                    last_user = EXCLUDED.last_user,
                    last_logon = EXCLUDED.last_logon,
                    last_logon_timestamp = EXCLUDED.last_logon_timestamp,
                    last_audit = EXCLUDED.last_audit,
                    last_session = EXCLUDED.last_session,
                    last_startup = EXCLUDED.last_startup,
                    base_audit = EXCLUDED.base_audit,
                    last_seen = EXCLUDED.last_seen,
                    ad_pwd_last_set = EXCLUDED.ad_pwd_last_set,
                    ad_when_created = EXCLUDED.ad_when_created,
                    ad_when_changed = EXCLUDED.ad_when_changed,
                    is_active = EXCLUDED.is_active,
                    is_ad_enabled = EXCLUDED.is_ad_enabled,
                    has_recent_activity = EXCLUDED.has_recent_activity,
                    has_key_client_data = EXCLUDED.has_key_client_data,
                    has_ad_data = EXCLUDED.has_ad_data,
                    has_tdx_data = EXCLUDED.has_tdx_data,
                    data_source = EXCLUDED.data_source,
                    data_quality_score = EXCLUDED.data_quality_score,
                    quality_flags = EXCLUDED.quality_flags,
                    source_system = EXCLUDED.source_system,
                    entity_hash = EXCLUDED.entity_hash,
                    updated_at = CURRENT_TIMESTAMP,
                    ingestion_run_id = EXCLUDED.ingestion_run_id
            """,
                computer_record,
            )

        return "inserted" if not existing else "updated"

    def _upsert_computer_labs(
        self, computer_id: str, associations: List[Tuple[str, str, Decimal, Dict]]
    ) -> int:
        """
        Upsert computer-lab associations.

        Args:
            computer_id: Computer ID.
            associations: List of (lab_id, method, confidence, evidence) tuples.

        Returns:
            Number of associations upserted.
        """
        if not associations:
            return 0

        if self.dry_run:
            logger.debug(f"[DRY RUN] Would upsert {len(associations)} lab associations")
            return len(associations)

        # Delete existing
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM silver.computer_labs
                WHERE computer_id = %s
            """,
                [computer_id],
            )

        # Determine primary (highest confidence)
        primary_lab = max(associations, key=lambda x: x[2])[0] if associations else None

        # Insert new
        records = []
        for lab_id, method, confidence, evidence in associations:
            records.append(
                {
                    "computer_id": computer_id,
                    "lab_id": lab_id,
                    "association_method": method,
                    "confidence_score": confidence,
                    "matched_ou": evidence.get("matched_ou"),
                    "matched_group_id": evidence.get("matched_group_id"),
                    "matched_user": evidence.get("matched_user"),
                    "is_primary": lab_id == primary_lab,
                }
            )

        if records:
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO silver.computer_labs (
                        computer_id, lab_id, association_method,
                        confidence_score, matched_ou, matched_group_id, matched_user,
                        is_primary
                    ) VALUES (
                        %(computer_id)s, %(lab_id)s, %(association_method)s,
                        %(confidence_score)s, %(matched_ou)s, %(matched_group_id)s, %(matched_user)s,
                        %(is_primary)s
                    )
                """,
                    records,
                )

            # Update computer primary_lab_id
            if primary_lab:
                primary_method = [m for l, m, c, e in associations if l == primary_lab][
                    0
                ]

                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE silver.computers
                        SET primary_lab_id = %s,
                            primary_lab_method = %s,
                            lab_association_count = %s
                        WHERE computer_id = %s
                    """,
                        [primary_lab, primary_method, len(associations), computer_id],
                    )

        return len(records)

    def _upsert_computer_groups(self, groups: List[Dict[str, Any]]) -> int:
        """
        Batch upsert computer groups.

        Args:
            groups: List of group membership dicts.

        Returns:
            Number of groups upserted.
        """
        if not groups:
            return 0

        if self.dry_run:
            logger.debug(f"[DRY RUN] Would upsert {len(groups)} group memberships")
            return len(groups)

        # Delete existing for these computers
        computer_ids = list(set(g["computer_id"] for g in groups))
        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                """
                DELETE FROM silver.computer_groups
                WHERE computer_id = %s
            """,
                [(cid,) for cid in computer_ids],
            )

        # Insert new
        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO silver.computer_groups (
                    computer_id, group_id, group_dn, group_cn, source_system
                ) VALUES (
                    %(computer_id)s, %(group_id)s, %(group_dn)s, %(group_cn)s, %(source_system)s
                )
                ON CONFLICT (computer_id, group_dn) DO NOTHING
            """,
                groups,
            )

        return len(groups)

    def _upsert_computer_attributes(self, attributes: List[Dict[str, Any]]) -> int:
        """
        Batch upsert computer attributes.

        Args:
            attributes: List of attribute dicts.

        Returns:
            Number of attributes upserted.
        """
        if not attributes:
            return 0

        if self.dry_run:
            logger.debug(f"[DRY RUN] Would upsert {len(attributes)} attributes")
            return len(attributes)

        # Delete existing for these computers
        computer_ids = list(set(a["computer_id"] for a in attributes))
        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                """
                DELETE FROM silver.computer_attributes
                WHERE computer_id = %s
            """,
                [(cid,) for cid in computer_ids],
            )

        # Insert new
        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO silver.computer_attributes (
                    computer_id, attribute_name, attribute_value,
                    attribute_value_uid, source_system, tdx_form_id
                ) VALUES (
                    %(computer_id)s, %(attribute_name)s, %(attribute_value)s,
                    %(attribute_value_uid)s, %(source_system)s, %(tdx_form_id)s
                )
                ON CONFLICT (computer_id, attribute_name) DO UPDATE SET
                    attribute_value = EXCLUDED.attribute_value,
                    attribute_value_uid = EXCLUDED.attribute_value_uid,
                    tdx_form_id = EXCLUDED.tdx_form_id,
                    updated_at = CURRENT_TIMESTAMP
            """,
                attributes,
            )

        return len(attributes)

    def transform_computers(
        self, full_sync: bool = False, batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Main transformation entry point.

        Args:
            full_sync: If True, process all computers. If False, incremental.
            batch_size: Number of computers to process per batch.

        Returns:
            Statistics dictionary.
        """
        stats = {
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "lab_associations": 0,
            "group_memberships": 0,
            "attributes": 0,
        }

        # Create ingestion run
        run_id = uuid4()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    run_id, source_system, entity_type, status, started_at
                ) VALUES (
                    %s, 'silver_transformation', 'computer', 'running', CURRENT_TIMESTAMP
                )
            """,
                [run_id],
            )

        try:
            # Load bronze data
            since_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )

            kc_records = self._load_bronze_computers("key_client", since_timestamp)
            ad_records = self._load_bronze_computers(
                "active_directory", since_timestamp
            )
            tdx_records = self._load_bronze_computers("tdx", since_timestamp)

            # Match across sources
            matches = self._match_computers_across_sources(
                kc_records, ad_records, tdx_records
            )

            # Process in batches
            computer_ids = list(matches.keys())
            for i in range(0, len(computer_ids), batch_size):
                batch_ids = computer_ids[i : i + batch_size]
                logger.info(
                    f"📦 Processing batch {i // batch_size + 1}/{(len(computer_ids) + batch_size - 1) // batch_size}"
                )

                for computer_id in batch_ids:
                    try:
                        match = matches[computer_id]

                        # Merge to silver
                        silver_record = self._merge_bronze_to_silver(computer_id, match)

                        # Upsert computer
                        result = self._upsert_computer(silver_record, run_id)
                        stats[result] += 1
                        stats["processed"] += 1

                        # Lab associations
                        associations = self._associate_with_labs(silver_record)
                        lab_count = self._upsert_computer_labs(
                            computer_id, associations
                        )
                        stats["lab_associations"] += lab_count

                        # Group memberships
                        groups = self._extract_computer_groups(
                            computer_id, match.get("ad_record")
                        )
                        if groups:
                            group_count = self._upsert_computer_groups(groups)
                            stats["group_memberships"] += group_count

                        # TDX attributes
                        attributes = self._extract_tdx_attributes(
                            computer_id, match.get("tdx_record")
                        )
                        if attributes:
                            attr_count = self._upsert_computer_attributes(attributes)
                            stats["attributes"] += attr_count

                    except Exception as e:
                        logger.error(f"❌ Error processing computer {computer_id}: {e}")
                        continue

                # Commit batch
                if not self.dry_run:
                    self.conn.commit()
                    logger.info(f"✅ Committed batch {i // batch_size + 1}")

            # Complete ingestion run
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meta.ingestion_runs
                    SET status = 'completed',
                        completed_at = CURRENT_TIMESTAMP,
                        records_processed = %s,
                        records_created = %s,
                        records_updated = %s
                    WHERE run_id = %s
                """,
                    [stats["processed"], stats["inserted"], stats["updated"], run_id],
                )

            if not self.dry_run:
                self.conn.commit()

            logger.info(f"""
✨ Transformation complete!
   - Processed: {stats["processed"]}
   - Inserted: {stats["inserted"]}
   - Updated: {stats["updated"]}
   - Unchanged: {stats["unchanged"]}
   - Lab associations: {stats["lab_associations"]}
   - Group memberships: {stats["group_memberships"]}
   - TDX attributes: {stats["attributes"]}
            """)

        except Exception as e:
            import traceback

            logger.error(f"❌ Transformation failed: {e}")
            logger.error(traceback.format_exc())

            # Mark run as failed
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meta.ingestion_runs
                    SET status = 'failed',
                        completed_at = CURRENT_TIMESTAMP,
                        error_message = %s
                    WHERE run_id = %s
                """,
                    [str(e), run_id],
                )

            if not self.dry_run:
                self.conn.commit()

            raise

        return stats

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """Get timestamp of last successful transformation."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT MAX(completed_at) as last_completed
                FROM meta.ingestion_runs
                WHERE source_system = 'silver_transformation'
                  AND entity_type = 'computer'
                  AND status = 'completed'
            """)

            result = cur.fetchone()
            return result["last_completed"] if result else None

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Transform bronze computers to silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all computers (default: incremental)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't commit changes")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size (default: 100)"
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()

    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "lsats_db"),
        "user": os.getenv("DB_USER", "lsats_user"),
        "password": os.getenv("DB_PASSWORD"),
    }

    # Run transformation
    service = ComputerSilverTransformationService(db_config, dry_run=args.dry_run)

    try:
        stats = service.transform_computers(
            full_sync=args.full_sync, batch_size=args.batch_size
        )

        logger.info(f"✅ Transformation completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        sys.exit(1)

    finally:
        service.close()


if __name__ == "__main__":
    main()
