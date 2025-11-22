#!/usr/bin/env python3
"""
KeyConfigure Computers Source-Specific Silver Layer Transformation Service

This service transforms bronze KeyConfigure computer records into the source-specific
silver.keyconfigure_computers table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts all KeyConfigure computer fields from JSONB to typed columns
- Content hash-based change detection
- Incremental processing (only transform computers with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The extracted typed columns enable efficient joins with TDX assets, AD computers, and other
computer sources during consolidated silver.computers transformation.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

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
log_dir = "logs/silver"
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
    - Extract all KeyConfigure computer fields from JSONB to typed columns
    - Parse timestamp strings to TIMESTAMP WITH TIME ZONE
    - Normalize numeric fields (RAM MB, Disk GB, CPU cores, etc.)
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze

    This is TIER 1 (source-specific). Future consolidated tier 2 will merge
    keyconfigure_computers + tdx_assets + ad_computers into silver.computers.
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
        logger.info("🔌 KeyConfigure computers silver transformation service initialized")

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
    ) -> Set[str]:
        """
        Find KeyConfigure computer MAC addresses that have new/updated bronze records.

        Args:
            since_timestamp: Only include computers with bronze records after this time
            full_sync: If True, return ALL KeyConfigure computers regardless of timestamp

        Returns:
            Set of MAC addresses (strings) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id as mac_address
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
              AND source_system = 'key_client'
              {time_filter}
              AND external_id IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            mac_addresses = set(result_df["mac_address"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(mac_addresses)} KeyConfigure computers needing transformation ({sync_mode} mode)"
            )
            return mac_addresses

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get computers needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(
        self, mac_address: str
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for a KeyConfigure computer.

        Args:
            mac_address: The MAC address (external_id)

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
              AND source_system = 'key_client'
              AND external_id = :mac_address
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"mac_address": mac_address}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for MAC {mac_address}: {e}"
            )
            raise

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Only includes significant fields (not metadata like _content_hash, _source_file).

        Args:
            raw_data: Raw computer data from bronze layer

        Returns:
            SHA-256 hash string
        """
        # Include only significant fields for change detection
        # Exclude metadata fields starting with '_'
        significant_fields = {
            "Name": raw_data.get("Name"),
            "MAC": raw_data.get("MAC"),
            "CPU": raw_data.get("CPU"),
            "# of cores": raw_data.get("# of cores"),
            "Clock Speed (Mhz)": raw_data.get("Clock Speed (Mhz)"),
            "Sockets": raw_data.get("Sockets"),
            "RAM": raw_data.get("RAM"),
            "Disk": raw_data.get("Disk"),
            "Free": raw_data.get("Free"),
            "OEM SN": raw_data.get("OEM SN"),
            "OS": raw_data.get("OS"),
            "OS Family": raw_data.get("OS Family"),
            "OS vers": raw_data.get("OS vers"),
            "OS SN": raw_data.get("OS SN"),
            "OS Install Date": raw_data.get("OS Install Date"),
            "Last Addr": raw_data.get("Last Addr"),
            "Last User": raw_data.get("Last User"),
            "Login": raw_data.get("Login"),
            "Last Session": raw_data.get("Last Session"),
            "Last Startup": raw_data.get("Last Startup"),
            "Last Audit": raw_data.get("Last Audit"),
            "Base Audit": raw_data.get("Base Audit"),
            "Owner": raw_data.get("Owner"),
            "Client": raw_data.get("Client"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
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

    def _extract_keyconfigure_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast KeyConfigure computer fields from bronze JSONB to silver columns.

        This includes:
        1. All standard KeyConfigure computer fields
        2. Timestamp parsing for dates
        3. Numeric field extraction
        4. String normalization

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.keyconfigure_computers columns
        """

        # Helper to safely convert to int
        def to_int(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # Helper to safely convert to decimal
        def to_decimal(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Helper to normalize strings
        def normalize_str(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            return str(val).strip()

        silver_record = {
            # Primary identifier
            "mac_address": normalize_str(raw_data.get("MAC")),
            # Core identity fields
            "computer_name": normalize_str(raw_data.get("Name")),
            "oem_serial_number": normalize_str(raw_data.get("OEM SN")),
            "owner": normalize_str(raw_data.get("Owner")),
            # Hardware specifications
            "cpu": normalize_str(raw_data.get("CPU")),
            "cpu_cores": to_int(raw_data.get("# of cores")),
            "cpu_sockets": to_int(raw_data.get("Sockets")),
            "clock_speed_mhz": to_int(raw_data.get("Clock Speed (Mhz)")),
            "ram_mb": to_int(raw_data.get("RAM")),
            "disk_gb": to_decimal(raw_data.get("Disk")),
            "disk_free_gb": to_decimal(raw_data.get("Free")),
            # Operating system information
            "os": normalize_str(raw_data.get("OS")),
            "os_family": normalize_str(raw_data.get("OS Family")),
            "os_version": normalize_str(raw_data.get("OS vers")),
            "os_serial_number": normalize_str(raw_data.get("OS SN")),
            "os_install_date": self._parse_timestamp(raw_data.get("OS Install Date")),
            # Network information
            "last_ip_address": normalize_str(raw_data.get("Last Addr")),
            "last_user": normalize_str(raw_data.get("Last User")),
            # Session and audit information
            "login_type": normalize_str(raw_data.get("Login")),
            "last_session": self._parse_timestamp(raw_data.get("Last Session")),
            "last_startup": self._parse_timestamp(raw_data.get("Last Startup")),
            "last_audit": self._parse_timestamp(raw_data.get("Last Audit")),
            "base_audit": self._parse_timestamp(raw_data.get("Base Audit")),
            # Client information
            "keyconfigure_client_version": normalize_str(raw_data.get("Client")),
            # Traceability
            "raw_id": raw_id,
            "raw_data_snapshot": None,  # Optional: set to raw_data for full audit
            # Standard metadata
            "source_system": "key_client",
            "entity_hash": self._calculate_content_hash(raw_data),
        }

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.keyconfigure_computers record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new computers and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        mac_address = silver_record["mac_address"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert computer: MAC={mac_address}, "
                f"name={silver_record.get('computer_name')}, owner={silver_record.get('owner')}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.keyconfigure_computers
            WHERE mac_address = :mac_address
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"mac_address": mac_address}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(f"⏭️  Computer unchanged, skipping: {mac_address}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.keyconfigure_computers (
                        mac_address, computer_name, oem_serial_number, owner,
                        cpu, cpu_cores, cpu_sockets, clock_speed_mhz, ram_mb, disk_gb, disk_free_gb,
                        os, os_family, os_version, os_serial_number, os_install_date,
                        last_ip_address, last_user,
                        login_type, last_session, last_startup, last_audit, base_audit,
                        keyconfigure_client_version,
                        raw_id, raw_data_snapshot, source_system, entity_hash,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :mac_address, :computer_name, :oem_serial_number, :owner,
                        :cpu, :cpu_cores, :cpu_sockets, :clock_speed_mhz, :ram_mb, :disk_gb, :disk_free_gb,
                        :os, :os_family, :os_version, :os_serial_number, :os_install_date,
                        :last_ip_address, :last_user,
                        :login_type, :last_session, :last_startup, :last_audit, :base_audit,
                        :keyconfigure_client_version,
                        :raw_id, CAST(:raw_data_snapshot AS jsonb), :source_system, :entity_hash,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (mac_address) DO UPDATE SET
                        computer_name = EXCLUDED.computer_name,
                        oem_serial_number = EXCLUDED.oem_serial_number,
                        owner = EXCLUDED.owner,
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
                        last_ip_address = EXCLUDED.last_ip_address,
                        last_user = EXCLUDED.last_user,
                        login_type = EXCLUDED.login_type,
                        last_session = EXCLUDED.last_session,
                        last_startup = EXCLUDED.last_startup,
                        last_audit = EXCLUDED.last_audit,
                        base_audit = EXCLUDED.base_audit,
                        keyconfigure_client_version = EXCLUDED.keyconfigure_client_version,
                        raw_id = EXCLUDED.raw_id,
                        raw_data_snapshot = EXCLUDED.raw_data_snapshot,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.keyconfigure_computers.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        "raw_data_snapshot": json.dumps(
                            silver_record.get("raw_data_snapshot")
                        )
                        if silver_record.get("raw_data_snapshot")
                        else None,
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} computer: {mac_address} "
                f"(name: {silver_record.get('computer_name')}, owner: {silver_record.get('owner')})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert computer {mac_address}: {e}")
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
                "transformation_type": "bronze_to_silver_keyconfigure_computers",
                "entity_type": "keyconfigure_computer",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.keyconfigure_computers",
                "tier": "source_specific",
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
                        error_message = :error_message
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
        Transform KeyConfigure computers from bronze to silver layer.

        This method:
        1. Determines which computers need transformation (incremental or full)
        2. Fetches latest bronze records for those computers
        3. Extracts fields and calculates content hashes
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
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "🚀 Starting KeyConfigure computers silver transformation"
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
                self.complete_transformation_run(
                    run_id, 0, 0, 0, 0, None
                )
                return stats

            # Step 3: Process each computer
            for mac_address in computers_to_process:
                try:
                    # Fetch latest bronze record
                    bronze_data = self._fetch_latest_bronze_record(mac_address)
                    if not bronze_data:
                        logger.warning(
                            f"⚠️  No bronze record found for MAC {mac_address}"
                        )
                        continue

                    raw_data, raw_id = bronze_data

                    # Extract fields to silver schema
                    silver_record = self._extract_keyconfigure_fields(raw_data, raw_id)

                    # Upsert to silver
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

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
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = f"Failed to process computer {mac_address}: {record_error}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            # Complete the run
            error_summary = None
            if stats["errors"]:
                error_summary = f"{len(stats['errors'])} individual record errors occurred"

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    error_summary,
                )

            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

            # Log comprehensive results
            logger.info(
                f"🎉 KeyConfigure computers transformation completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {stats['records_processed']}")
            logger.info(f"   Created: {stats['records_created']}")
            logger.info(f"   Updated: {stats['records_updated']}")
            logger.info(f"   Skipped (unchanged): {stats['records_skipped']}")
            logger.info(f"   Errors: {len(stats['errors'])}")

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
                    error_msg,
                )

            raise


def main():
    """Main entry point for KeyConfigure computers silver transformation."""
    parser = argparse.ArgumentParser(
        description="Transform KeyConfigure computers from bronze to silver layer"
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
