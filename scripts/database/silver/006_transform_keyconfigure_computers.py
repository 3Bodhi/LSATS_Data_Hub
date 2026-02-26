#!/usr/bin/env python3
"""
KeyConfigure Computers Source-Specific Silver Layer Transformation Service

This service transforms bronze KeyConfigure computer records into the source-specific
silver.keyconfigure_computers table. This is TIER 1 of the two-tier silver architecture.

MULTI-NIC CONSOLIDATION:
KeyConfigure stores one record per network interface card (NIC). This script consolidates
multiple NIC records into single computer records by grouping on (computer_name, serial_number).

Key features:
- Multi-NIC consolidation: N NIC records → 1 computer record
- Collects all MAC addresses and IP addresses into JSONB arrays
- Selects most recently active NIC for primary values
- Content hash-based change detection
- Incremental processing (only transform computers with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The consolidated records enable accurate matching with TDX assets and AD computers
during Tier 2 consolidation (silver.computers).
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import dateutil.parser
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/silver"
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


class KeyConfigureComputerTransformationService:
    """
    Service for transforming bronze KeyConfigure computer records into source-specific silver layer.

    This service creates silver.keyconfigure_computers records from bronze.raw_entities where:
    - entity_type = 'computer'
    - source_system = 'key_client'

    Transformation Logic:
    - Consolidate multiple NIC records (same name+serial) into single computer record
    - Extract all KeyConfigure computer fields from JSONB to typed columns
    - Collect all MAC addresses and IPs into JSONB arrays
    - Parse timestamp strings to TIMESTAMP WITH TIME ZONE
    - Normalize numeric fields (RAM MB, Disk GB, CPU cores, etc.)
    - Calculate entity_hash for change detection
    - Track all raw_ids for traceability back to bronze

    This is TIER 1 (source-specific). Tier 2 consolidation (013_transform_computers.py)
    will merge keyconfigure_computers + tdx_assets + ad_computers into silver.computers.
    """

    def __init__(self, database_url: str):
        """
        Initialize the transformation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info(
            "🔌 KeyConfigure computers silver transformation service initialized"
        )

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful KeyConfigure computers transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'keyconfigure_computer'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all computers"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_computers_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[Tuple[str, str]]:
        """
        Find KeyConfigure computers (name+serial combinations) that have new/updated bronze records.

        NEW: Returns (name, serial) tuples instead of just MAC addresses to handle multi-NIC.

        Args:
            since_timestamp: Only include computers with bronze records after this time
            full_sync: If True, return ALL KeyConfigure computers regardless of timestamp

        Returns:
            Set of (computer_name, serial_number) tuples that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                raw_data->>'Name' as computer_name,
                raw_data->>'OEM SN' as serial_number
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
              AND source_system = 'key_client'
              {time_filter}
              AND raw_data->>'Name' IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)

            # Create tuples of (name, serial) - use empty string if serial is None
            computers = set()
            for _, row in result_df.iterrows():
                name = row["computer_name"]
                serial = row["serial_number"] if row["serial_number"] else ""
                computers.add((name, serial))

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(computers)} KeyConfigure computers needing transformation ({sync_mode} mode)"
            )
            return computers

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get computers needing transformation: {e}")
            raise

    def _fetch_all_bronze_records_for_computer(
        self, computer_name: str, serial_number: str
    ) -> List[Tuple[Dict, str]]:
        """
        Fetch ALL bronze records for a computer (all NICs).

        Args:
            computer_name: The computer name
            serial_number: The OEM serial number (may be empty string)

        Returns:
            List of (raw_data dict, raw_id UUID) tuples for all NICs
        """
        try:
            # Build query based on whether serial exists
            if serial_number:
                query = """
                SELECT raw_data, raw_id
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                  AND source_system = 'key_client'
                  AND raw_data->>'Name' = :computer_name
                  AND raw_data->>'OEM SN' = :serial_number
                ORDER BY ingested_at DESC
                """
                params = {
                    "computer_name": computer_name,
                    "serial_number": serial_number,
                }
            else:
                query = """
                SELECT raw_data, raw_id
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                  AND source_system = 'key_client'
                  AND raw_data->>'Name' = :computer_name
                  AND (raw_data->>'OEM SN' IS NULL OR raw_data->>'OEM SN' = '')
                ORDER BY ingested_at DESC
                """
                params = {"computer_name": computer_name}

            result_df = self.db_adapter.query_to_dataframe(query, params)

            if result_df.empty:
                return []

            records = []
            for _, row in result_df.iterrows():
                records.append((row["raw_data"], row["raw_id"]))

            logger.debug(f"📦 Found {len(records)} NIC records for {computer_name}")
            return records

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze records for {computer_name}: {e}")
            raise

    def _normalize_mac(self, mac: Any) -> Optional[str]:
        """
        Normalize MAC address (remove all separators, uppercase).

        Args:
            mac: MAC address in any format

        Returns:
            Normalized MAC (12 hex chars) or None
        """
        if not mac or pd.isna(mac):
            return None
        m = str(mac).strip().upper()
        # Remove ALL possible separators
        m = m.replace(":", "").replace("-", "").replace(".", "").replace(" ", "")
        # Valid MAC is 12 hex characters
        if len(m) == 12 and all(c in "0123456789ABCDEF" for c in m):
            return m
        logger.debug(f"Invalid MAC format: {mac}")
        return None

    def _generate_computer_id(self, computer_name: str, serial_number: str) -> str:
        """
        Generate stable computer_id from name + serial.

        Args:
            computer_name: The computer name
            serial_number: The OEM serial number (may be empty)

        Returns:
            Stable identifier: "COMPUTERNAME-SERIAL" or "COMPUTERNAME" if no serial
        """
        name = str(computer_name).strip().upper()
        serial = str(serial_number).strip().upper() if serial_number else ""

        if not name:
            raise ValueError("Computer name is required")

        # Filter out junk serials
        if serial and serial not in ("", "N/A", "NONE", "UNKNOWN"):
            return f"{name}-{serial}"
        else:
            return name

    def _calculate_content_hash(self, consolidated_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        For consolidated records, hash includes all MACs, IPs, and significant fields.

        Args:
            consolidated_data: Consolidated computer data

        Returns:
            SHA-256 hash string
        """
        significant_fields = {
            "computer_id": consolidated_data.get("computer_id"),
            "computer_name": consolidated_data.get("computer_name"),
            "oem_serial_number": consolidated_data.get("oem_serial_number"),
            "mac_addresses": consolidated_data.get("mac_addresses"),
            "ip_addresses": consolidated_data.get("ip_addresses"),
            "cpu": consolidated_data.get("cpu"),
            "cpu_cores": consolidated_data.get("cpu_cores"),
            "ram_mb": consolidated_data.get("ram_mb"),
            "disk_gb": consolidated_data.get("disk_gb"),
            "os": consolidated_data.get("os"),
            "os_version": consolidated_data.get("os_version"),
            "owner": consolidated_data.get("owner"),
            "last_session": str(consolidated_data.get("last_session")),
            "last_audit": str(consolidated_data.get("last_audit")),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse KeyConfigure timestamp strings into Python datetime objects.

        Handles multiple formats:
        - ISO 8601: "2025-04-23T00:01:21"
        - Short date: "5/21/2025"
        - Long date: "2024-07-18T14:14:17"

        Args:
            timestamp_str: Timestamp string from KeyConfigure

        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            parsed_dt = dateutil.parser.parse(timestamp_str)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return parsed_dt
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _consolidate_multi_nic_records(
        self,
        bronze_records: List[Tuple[Dict, str]],
        computer_name: str,
        serial_number: str,
    ) -> Dict[str, Any]:
        """
        Consolidate multiple NIC records into single computer record.

        Strategy:
        - MAC addresses: Collect all into array, use most recent as primary
        - IP addresses: Collect all into array
        - Activity timestamps: Use max (most recent activity)
        - Hardware specs: Use from most recently active NIC
        - Audit trail: Keep all raw_ids

        Args:
            bronze_records: List of (raw_data, raw_id) for all NICs
            computer_name: Computer name for logging
            serial_number: Serial number for logging

        Returns:
            Consolidated silver record
        """
        if not bronze_records:
            raise ValueError(f"No bronze records for {computer_name}")

        # Sort by most recent activity (last_session timestamp)
        def get_last_session(rec):
            raw_data, _ = rec
            ts = self._parse_timestamp(raw_data.get("Last Session"))
            return ts if ts else datetime.min.replace(tzinfo=timezone.utc)

        sorted_records = sorted(bronze_records, key=get_last_session, reverse=True)

        # Use most recent record as base
        primary_raw_data, primary_raw_id = sorted_records[0]

        # Helper functions for type conversion
        def to_int(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        def to_decimal(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def normalize_str(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            return str(val).strip()

        # Collect all MACs and IPs
        all_macs = []
        all_ips = []
        all_raw_ids = []

        for raw_data, raw_id in sorted_records:
            mac = self._normalize_mac(raw_data.get("MAC"))
            if mac and mac not in all_macs:
                all_macs.append(mac)

            ip = normalize_str(raw_data.get("Last Addr"))
            if ip and ip not in all_ips:
                all_ips.append(ip)

            all_raw_ids.append(str(raw_id))

        # Generate computer_id
        computer_id = self._generate_computer_id(computer_name, serial_number)

        # Build consolidated record (use primary/most recent values)
        consolidated_record = {
            # Primary key
            "computer_id": computer_id,
            # Core identity
            "computer_name": normalize_str(primary_raw_data.get("Name")),
            "oem_serial_number": normalize_str(primary_raw_data.get("OEM SN")),
            # Multi-NIC consolidation fields
            "primary_mac_address": all_macs[0] if all_macs else None,
            "mac_addresses": all_macs,
            "ip_addresses": all_ips,
            "nic_count": len(bronze_records),
            # Hardware specifications (from most recent NIC)
            "cpu": normalize_str(primary_raw_data.get("CPU")),
            "cpu_cores": to_int(primary_raw_data.get("# of cores")),
            "cpu_sockets": to_int(primary_raw_data.get("Sockets")),
            "clock_speed_mhz": to_int(primary_raw_data.get("Clock Speed (Mhz)")),
            "ram_mb": to_int(primary_raw_data.get("RAM")),
            "disk_gb": to_decimal(primary_raw_data.get("Disk")),
            "disk_free_gb": to_decimal(primary_raw_data.get("Free")),
            # Operating system (from most recent NIC)
            "os": normalize_str(primary_raw_data.get("OS")),
            "os_family": normalize_str(primary_raw_data.get("OS Family")),
            "os_version": normalize_str(primary_raw_data.get("OS vers")),
            "os_serial_number": normalize_str(primary_raw_data.get("OS SN")),
            "os_install_date": self._parse_timestamp(
                primary_raw_data.get("OS Install Date")
            ),
            # User and owner (from most recent NIC)
            "last_user": normalize_str(primary_raw_data.get("Last User")),
            "owner": normalize_str(primary_raw_data.get("Owner")),
            "login_type": normalize_str(primary_raw_data.get("Login")),
            # Timestamps: max across all NICs
            "last_session": max(
                (
                    self._parse_timestamp(r[0].get("Last Session"))
                    for r in bronze_records
                ),
                default=None,
                key=lambda x: x if x else datetime.min.replace(tzinfo=timezone.utc),
            ),
            "last_startup": max(
                (
                    self._parse_timestamp(r[0].get("Last Startup"))
                    for r in bronze_records
                ),
                default=None,
                key=lambda x: x if x else datetime.min.replace(tzinfo=timezone.utc),
            ),
            "last_audit": max(
                (self._parse_timestamp(r[0].get("Last Audit")) for r in bronze_records),
                default=None,
                key=lambda x: x if x else datetime.min.replace(tzinfo=timezone.utc),
            ),
            "base_audit": min(
                (
                    self._parse_timestamp(r[0].get("Base Audit"))
                    for r in bronze_records
                    if r[0].get("Base Audit")
                ),
                default=None,
                key=lambda x: x if x else datetime.max.replace(tzinfo=timezone.utc),
            ),
            # Client version
            "keyconfigure_client_version": normalize_str(
                primary_raw_data.get("Client")
            ),
            # Audit trail: all raw_ids
            "consolidated_raw_ids": all_raw_ids,
            "raw_id": primary_raw_id,  # Most recent
            # Standard metadata
            "source_system": "key_client",
        }

        # Calculate hash last (after all fields are set)
        consolidated_record["entity_hash"] = self._calculate_content_hash(
            consolidated_record
        )

        if len(bronze_records) > 1:
            logger.debug(
                f"🔗 Consolidated {len(bronze_records)} NICs for {computer_id}: "
                f"MACs={len(all_macs)}, IPs={len(all_ips)}"
            )

        return consolidated_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.keyconfigure_computers record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new computers and updates to existing ones.

        Args:
            silver_record: The consolidated silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        computer_id = silver_record["computer_id"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert computer: {computer_id}, "
                f"NICs={silver_record.get('nic_count')}, "
                f"MACs={len(silver_record.get('mac_addresses', []))}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.keyconfigure_computers
            WHERE computer_id = :computer_id
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"computer_id": computer_id}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(f"⏭️  Computer unchanged, skipping: {computer_id}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.keyconfigure_computers (
                        computer_id, computer_name, oem_serial_number,
                        primary_mac_address, mac_addresses, ip_addresses, nic_count,
                        cpu, cpu_cores, cpu_sockets, clock_speed_mhz, ram_mb, disk_gb, disk_free_gb,
                        os, os_family, os_version, os_serial_number, os_install_date,
                        last_user, owner, login_type,
                        last_session, last_startup, last_audit, base_audit,
                        keyconfigure_client_version,
                        consolidated_raw_ids, raw_id, source_system, entity_hash,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :computer_id, :computer_name, :oem_serial_number,
                        :primary_mac_address, CAST(:mac_addresses AS jsonb), CAST(:ip_addresses AS jsonb), :nic_count,
                        :cpu, :cpu_cores, :cpu_sockets, :clock_speed_mhz, :ram_mb, :disk_gb, :disk_free_gb,
                        :os, :os_family, :os_version, :os_serial_number, :os_install_date,
                        :last_user, :owner, :login_type,
                        :last_session, :last_startup, :last_audit, :base_audit,
                        :keyconfigure_client_version,
                        CAST(:consolidated_raw_ids AS jsonb), :raw_id, :source_system, :entity_hash,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (computer_id) DO UPDATE SET
                        computer_name = EXCLUDED.computer_name,
                        oem_serial_number = EXCLUDED.oem_serial_number,
                        primary_mac_address = EXCLUDED.primary_mac_address,
                        mac_addresses = EXCLUDED.mac_addresses,
                        ip_addresses = EXCLUDED.ip_addresses,
                        nic_count = EXCLUDED.nic_count,
                        cpu = EXCLUDED.cpu,
                        cpu_cores = EXCLUDED.cpu_cores,
                        cpu_sockets = EXCLUDED.cpu_sockets,
                        clock_speed_mhz = EXCLUDED.clock_speed_mhz,
                        ram_mb = EXCLUDED.ram_mb,
                        disk_gb = EXCLUDED.disk_gb,
                        disk_free_gb = EXCLUDED.disk_free_gb,
                        os = EXCLUDED.os,
                        os_family = EXCLUDED.os_family,
                        os_version = EXCLUDED.os_version,
                        os_serial_number = EXCLUDED.os_serial_number,
                        os_install_date = EXCLUDED.os_install_date,
                        last_user = EXCLUDED.last_user,
                        owner = EXCLUDED.owner,
                        login_type = EXCLUDED.login_type,
                        last_session = EXCLUDED.last_session,
                        last_startup = EXCLUDED.last_startup,
                        last_audit = EXCLUDED.last_audit,
                        base_audit = EXCLUDED.base_audit,
                        keyconfigure_client_version = EXCLUDED.keyconfigure_client_version,
                        consolidated_raw_ids = EXCLUDED.consolidated_raw_ids,
                        raw_id = EXCLUDED.raw_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.keyconfigure_computers.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        "mac_addresses": json.dumps(
                            silver_record.get("mac_addresses", [])
                        ),
                        "ip_addresses": json.dumps(
                            silver_record.get("ip_addresses", [])
                        ),
                        "consolidated_raw_ids": json.dumps(
                            silver_record.get("consolidated_raw_ids", [])
                        ),
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} computer: {computer_id} "
                f"(NICs: {silver_record.get('nic_count')}, "
                f"MACs: {len(silver_record.get('mac_addresses', []))})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert computer {computer_id}: {e}")
            raise

    def create_transformation_run(
        self, incremental_since: Optional[datetime] = None, full_sync: bool = False
    ) -> str:
        """
        Create a transformation run record for tracking.

        Args:
            incremental_since: Timestamp for incremental processing
            full_sync: Whether this is a full sync

        Returns:
            Run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "transformation_type": "bronze_to_silver_keyconfigure_computers_consolidated",
                "entity_type": "keyconfigure_computer",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.keyconfigure_computers",
                "tier": "source_specific",
                "multi_nic_consolidation": True,
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'keyconfigure_computer', :started_at, 'running', :metadata
                    )
                """)

                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            logger.info(f"📝 Created transformation run {run_id}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create transformation run: {e}")
            raise

    def complete_transformation_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        records_skipped: int,
        nics_consolidated: int = 0,
        error_message: Optional[str] = None,
    ):
        """Mark a transformation run as completed with comprehensive statistics."""
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_updated,
                        error_message = :error_message,
                        metadata = metadata || jsonb_build_object('nics_consolidated', :nics_consolidated)
                    WHERE run_id = :run_id
                """)

                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_processed": records_processed,
                        "records_created": records_created,
                        "records_updated": records_updated,
                        "nics_consolidated": nics_consolidated,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_keyconfigure_computers(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Transform KeyConfigure computers from bronze to silver layer with multi-NIC consolidation.

        This method:
        1. Determines which computers need transformation (incremental or full)
        2. Fetches ALL bronze records for those computers (all NICs)
        3. Consolidates multi-NIC records into single computer records
        4. Upserts into silver.keyconfigure_computers
        5. Provides detailed statistics

        Args:
            full_sync: If True, process all computers regardless of timestamps
            dry_run: If True, preview without committing

        Returns:
            Dictionary with comprehensive transformation statistics
        """
        stats = {
            "run_id": None,
            "records_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "nics_consolidated": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "🚀 Starting KeyConfigure computers silver transformation with multi-NIC consolidation"
                + (" (FULL SYNC)" if full_sync else " (incremental)")
                + (" [DRY RUN]" if dry_run else "")
            )

            # Step 1: Determine incremental timestamp
            incremental_since = None
            if not full_sync:
                incremental_since = self._get_last_transformation_timestamp()

            # Create transformation run
            run_id = self.create_transformation_run(incremental_since, full_sync)
            stats["run_id"] = run_id

            # Step 2: Get computers needing transformation
            computers_to_process = self._get_computers_needing_transformation(
                incremental_since, full_sync
            )

            if not computers_to_process:
                logger.info("✨ No computers need transformation - all up to date!")
                self.complete_transformation_run(run_id, 0, 0, 0, 0, 0, None)
                return stats

            # Step 3: Process each computer (with multi-NIC consolidation)
            for computer_name, serial_number in computers_to_process:
                try:
                    # Fetch ALL bronze records for this computer (all NICs)
                    bronze_records = self._fetch_all_bronze_records_for_computer(
                        computer_name, serial_number
                    )

                    if not bronze_records:
                        logger.warning(
                            f"⚠️  No bronze records found for {computer_name}"
                        )
                        continue

                    # Track total NICs processed
                    stats["nics_consolidated"] += len(bronze_records)

                    # Consolidate multi-NIC records into single computer record
                    consolidated_record = self._consolidate_multi_nic_records(
                        bronze_records, computer_name, serial_number
                    )

                    # Upsert to silver
                    action = self._upsert_silver_record(
                        consolidated_record, run_id, dry_run
                    )

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["records_processed"] += 1

                    # Progress logging
                    if stats["records_processed"] % 100 == 0:
                        logger.info(
                            f"📊 Progress: {stats['records_processed']}/{len(computers_to_process)} computers processed "
                            f"({stats['nics_consolidated']} NICs consolidated, "
                            f"{stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Failed to process computer {computer_name}: {record_error}"
                    )
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            # Complete the run
            error_summary = None
            if stats["errors"]:
                error_summary = (
                    f"{len(stats['errors'])} individual record errors occurred"
                )

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    stats["nics_consolidated"],
                    error_summary,
                )

            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

            # Log comprehensive results
            logger.info(
                f"🎉 KeyConfigure computers transformation completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Computers: {stats['records_processed']}")
            logger.info(f"   Total NICs Consolidated: {stats['nics_consolidated']}")
            logger.info(f"   Created: {stats['records_created']}")
            logger.info(f"   Updated: {stats['records_updated']}")
            logger.info(f"   Skipped (unchanged): {stats['records_skipped']}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            if stats["records_processed"] > 0:
                avg_nics = stats["nics_consolidated"] / stats["records_processed"]
                logger.info(f"   Average NICs per computer: {avg_nics:.2f}")

            return stats

        except Exception as e:
            error_msg = f"KeyConfigure computers transformation failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if stats["run_id"]:
                self.complete_transformation_run(
                    stats["run_id"],
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    stats["nics_consolidated"],
                    error_msg,
                )

            raise


def main():
    """Main entry point for KeyConfigure computers silver transformation."""
    parser = argparse.ArgumentParser(
        description="Transform KeyConfigure computers from bronze to silver layer (with multi-NIC consolidation)"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all computers (ignore incremental timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview transformation without committing to database",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Run transformation
    try:
        service = KeyConfigureComputerTransformationService(database_url)
        stats = service.transform_keyconfigure_computers(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        if stats["errors"]:
            logger.warning(
                f"⚠️  Transformation completed with {len(stats['errors'])} errors"
            )
            sys.exit(1)
        else:
            logger.info("✅ Transformation completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
