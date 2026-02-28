#!/usr/bin/env python3
"""
KeyConfigure Computer Ingestion Service

This service ingests computer data from KeyConfigure Excel exports into the bronze layer
for cross-referencing and analysis.

KeyConfigure provides computer inventory information including:
- Computer identifiers (Name, MAC address, OEM Serial Number)
- Hardware specifications (CPU, RAM, Disk)
- Operating system information (OS Family, OS, OS version)
- Network information (Last Address, MAC)
- User and session data (Last User, Login, Last Session)
- Audit timestamps (Base Audit, Last Audit, Last Startup)

The script automatically finds the newest keyconfigure_computers*.xlsx file in the
configured data folder. The MAC address is used as the external_id since computer
names are not guaranteed to be unique.
"""

import argparse
import glob
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add your LSATS project to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/bronze"
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Configuration for data folder
# Default follows Linux FHS: /var is for variable data that changes during normal operation.
# Override with --data-folder CLI argument or set LSATS_DATA_FOLDER env var.
DATA_FOLDER = os.environ.get("LSATS_DATA_FOLDER", "/var/lsats/data")
FILE_PATTERN = "keyconfigure_computers*.xlsx"


class KeyConfigureComputerIngestionService:
    """
    Computer ingestion service for KeyConfigure Excel exports.

    Uses content hashing for change detection. This approach:

    1. Finds the newest keyconfigure_computers*.xlsx file in the data folder
    2. Reads computer data from the Excel file
    3. Calculates content hashes for each computer
    4. Compares against stored hashes from previous ingestions
    5. Only creates new bronze records when computer content has actually changed
    6. Preserves complete change history for computer analysis

    Key Features:
    - Automatic discovery of latest Excel export file
    - Efficient change detection without requiring timestamps
    - Hardware and OS specification tracking
    - User session tracking
    - Comprehensive audit trail for computer changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(
        self,
        database_url: str,
        data_folder: str = DATA_FOLDER,
        force_full_sync: bool = False,
        dry_run: bool = False,
    ):
        """
        Initialize the KeyConfigure computer ingestion service.

        Args:
            database_url: PostgreSQL connection string
            data_folder: Path to folder containing Excel files (default: 'data')
            force_full_sync: If True, bypass timestamp filtering (not used for content hash but kept for standard)
            dry_run: If True, preview changes without committing to database
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        self.data_folder = data_folder
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run

        logger.info(
            f"KeyConfigure computer ingestion service initialized with data folder: {data_folder} "
            f"(dry_run={'enabled' if dry_run else 'disabled'})"
        )

    def _find_latest_keyconfigure_file(self) -> Optional[str]:
        """
        Find the newest keyconfigure_computers*.xlsx file in the data folder.

        Returns:
            Path to the newest file, or None if no files found
        """
        search_pattern = os.path.join(self.data_folder, FILE_PATTERN)
        files = glob.glob(search_pattern)

        # Filter out temporary Excel files (starting with ~$)
        files = [f for f in files if not os.path.basename(f).startswith("~$")]

        if not files:
            logger.warning(
                f"No files matching pattern '{FILE_PATTERN}' found in {self.data_folder}"
            )
            return None

        # Get the newest file by modification time
        newest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest KeyConfigure file: {newest_file}")

        return newest_file

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize values for consistent hashing and JSON serialization.

        Handles NaN, NaT, timestamps, and other pandas data types.

        Args:
            value: Raw value from Excel/pandas

        Returns:
            Normalized value suitable for JSON serialization
        """
        if pd.isna(value):
            return None
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, bool):
            return value
        else:
            return str(value).strip()

    def _normalize_computer_data(self, computer_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize all values in computer data dictionary.

        Also renames keys for better readability:
        - '#' -> '# of cores'
        - 'Mhz' -> 'Clock Speed (Mhz)'

        Args:
            computer_data: Raw computer data from Excel

        Returns:
            Dictionary with all values normalized and keys renamed
        """
        normalized = {}
        for key, value in computer_data.items():
            # Rename keys for clarity
            if key == "#":
                new_key = "# of cores"
            elif key == "Mhz":
                new_key = "Clock Speed (Mhz)"
            else:
                new_key = key

            normalized[new_key] = self._normalize_value(value)
        return normalized

    def _calculate_computer_content_hash(self, computer_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for KeyConfigure computer data to detect meaningful changes.

        This hash represents the "content fingerprint" of the computer record.
        We include all fields that would represent meaningful computer changes.

        IMPORTANT: Metadata fields (starting with '_') are explicitly EXCLUDED from
        the hash calculation to ensure that changes in source file names or ingestion
        timestamps don't affect change detection.

        Note: This function works with both original Excel column names and renamed
        database column names to ensure consistent hashing.

        Args:
            computer_data: Raw computer data from Excel or database

        Returns:
            SHA-256 hash of the normalized computer content
        """

        # Handle both original Excel column names and renamed database column names
        # Try original name first, then renamed version
        # Exclude any metadata fields starting with '_'
        def get_value(original_key: str, renamed_key: str = None) -> Any:
            """Get value trying both original and renamed keys, excluding metadata."""
            if original_key in computer_data:
                return self._normalize_value(computer_data.get(original_key))
            elif renamed_key and renamed_key in computer_data:
                return self._normalize_value(computer_data.get(renamed_key))
            else:
                return self._normalize_value(None)

        # Extract significant fields for change detection
        # Use consistent naming for hash calculation
        significant_fields = {
            "idnt": get_value("idnt"),
            "agid": get_value("agid"),
            "Name": get_value("Name"),
            "MAC": get_value("MAC"),
            "CPU": get_value("CPU"),
            "Mhz": get_value("Mhz", "Clock Speed (Mhz)"),
            "#": get_value("#", "# of cores"),
            "Sockets": get_value("Sockets"),
            "RAM": get_value("RAM"),
            "Disk": get_value("Disk"),
            "Free": get_value("Free"),
            "% Used": get_value("% Used"),
            "% Free": get_value("% Free"),
            "OEM SN": get_value("OEM SN"),
            "OS Family": get_value("OS Family"),
            "OS": get_value("OS"),
            "OS vers": get_value("OS vers"),
            "OS SN": get_value("OS SN"),
            "OS Install Date": get_value("OS Install Date"),
            "Last Addr": get_value("Last Addr"),
            "Last User": get_value("Last User"),
            "Login": get_value("Login"),
            "Last Session": get_value("Last Session"),
            "Last Startup": get_value("Last Startup"),
            "Base Audit": get_value("Base Audit"),
            "Last Audit": get_value("Last Audit"),
            "Client": get_value("Client"),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        name = computer_data.get("Name") or computer_data.get("Name", "unknown")
        logger.debug(f"Content hash for computer {name}: {content_hash}")
        logger.debug(f"  Hash input (first 200 chars): {normalized_json[:200]}")

        return content_hash

    def _get_existing_computer_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each KeyConfigure computer from the bronze layer.

        This uses a window function to get only the most recent record for each
        computer, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping MAC address (external_id) -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each computer
            query = """
            WITH latest_computers AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                AND source_system = 'key_client'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_computers
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                mac_address = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_computer_content_hash(raw_data)
                existing_hashes[mac_address] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing KeyConfigure computers"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing computer hashes: {e}")
            raise

    def create_ingestion_run(
        self, source_system: str, entity_type: str, source_file: str
    ) -> str:
        """
        Create a new ingestion run record for tracking purposes.

        Args:
            source_system: Source system identifier
            entity_type: Entity type being ingested
            source_file: Path to the source Excel file
        """
        if self.dry_run:
            run_id = f"dry-run-{uuid.uuid4()}"
            logger.info(f"[DRY RUN] Would create ingestion run {run_id}")
            return run_id

        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to KeyConfigure Excel ingestion
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "keyconfigure_excel",
                "source_file": source_file,
                "file_pattern": FILE_PATTERN,
                "data_folder": self.data_folder,
                "change_detection_method": "sha256_content_hash",
                "includes_hardware_specs": True,
                "includes_os_information": True,
                "includes_user_session_data": True,
            }

            with self.db_adapter.engine.connect() as conn:
                # Mark any stale 'running' runs as failed before starting a new one.
                # Stale runs occur when a process is OOM-killed or force-stopped before
                # it can update its own status.
                conn.execute(text("""
                    UPDATE meta.ingestion_runs
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = 'stale - process terminated before completing (OOM kill or force stop)'
                    WHERE source_system = :source_system
                      AND entity_type = :entity_type
                      AND status = 'running'
                """), {"source_system": source_system, "entity_type": entity_type})

                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, :source_system, :entity_type, :started_at, 'running', :metadata
                    )
                """)

                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "source_system": source_system,
                        "entity_type": entity_type,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"Created KeyConfigure ingestion run {run_id} for {source_system}/{entity_type}"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_skipped: int = 0,
        error_message: Optional[str] = None,
    ):
        """Mark an ingestion run as completed with comprehensive statistics."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would complete ingestion run {run_id}")
            return

        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_skipped,
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
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"Completed KeyConfigure ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_keyconfigure_computers_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest KeyConfigure computers using intelligent content hashing.

        This method:
        1. Finds the latest keyconfigure_computers*.xlsx file
        2. Reads computer data from the Excel file
        3. Calculates content hashes for each computer
        4. Compares against existing bronze records
        5. Only creates new records when content has actually changed
        6. Provides detailed statistics about computer changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        ingestion_stats = {
            "run_id": None,
            "records_read_from_file": 0,
            "duplicate_mac_records_removed": 0,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_computers": 0,
            "changed_computers": 0,
            "os_types": {},
            "total_ram_gb": 0,
            "total_disk_gb": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
            "source_file": None,
        }

        try:
            logger.info(
                "🚀 Starting KeyConfigure computer ingestion with content hash change detection..."
            )

            # Step 1: Find the latest Excel file
            source_file = self._find_latest_keyconfigure_file()
            if not source_file:
                error_msg = f"No KeyConfigure files found matching pattern '{FILE_PATTERN}' in {self.data_folder}"
                logger.error(error_msg)
                ingestion_stats["errors"].append(error_msg)
                return ingestion_stats

            ingestion_stats["source_file"] = source_file

            # Create ingestion run for tracking
            run_id = self.create_ingestion_run("key_client", "computer", source_file)
            ingestion_stats["run_id"] = run_id

            # Step 2: Get existing computer content hashes from bronze layer
            existing_hashes = self._get_existing_computer_hashes()

            # Step 3: Read data from Excel file
            logger.info(f"Reading computer data from {source_file}...")
            # Use calamine engine for robust reading of potentially malformed Excel files
            # This handles files that openpyxl fails on (e.g. type mismatches, invalid XML)
            df = pd.read_excel(source_file, engine="calamine")

            if df.empty:
                logger.warning("No computers found in Excel file")
                return ingestion_stats

            logger.info(f"Retrieved {len(df)} computers from Excel file")
            ingestion_stats["records_read_from_file"] = len(df)

            # Step 3.5: Deduplicate by MAC address, keeping most recent Last Session
            original_count = len(df)

            # Check for duplicates
            if df["MAC"].duplicated().any():
                duplicate_count = df["MAC"].duplicated(keep=False).sum()
                logger.info(
                    f"Found {duplicate_count} records with duplicate MAC addresses"
                )

                # Convert Last Session to datetime for sorting (handle NaT/None values)
                df["_temp_last_session"] = pd.to_datetime(
                    df["Last Session"], errors="coerce"
                )

                # Sort by Last Session descending (most recent first), with NaT values last
                df = df.sort_values(
                    "_temp_last_session", ascending=False, na_position="last"
                )

                # Keep only the first occurrence of each MAC (most recent Last Session)
                df = df.drop_duplicates(subset="MAC", keep="first")

                # Drop the temporary column
                df = df.drop(columns=["_temp_last_session"])

                removed_count = original_count - len(df)
                ingestion_stats["duplicate_mac_records_removed"] = removed_count
                logger.info(
                    f"Removed {removed_count} duplicate MAC address records, keeping most recent Last Session"
                )
                logger.info(f"Proceeding with {len(df)} unique computers")
            else:
                logger.info(
                    f"No duplicate MAC addresses found, proceeding with all {len(df)} computers"
                )

            # Step 4: Process each computer with content hash change detection
            for idx, row in df.iterrows():
                try:
                    computer_data = row.to_dict()

                    # Extract computer identifiers
                    name = self._normalize_value(computer_data.get("Name"))
                    mac = self._normalize_value(computer_data.get("MAC"))
                    oem_sn = self._normalize_value(computer_data.get("OEM SN"))

                    # Skip if no MAC address (required as external_id)
                    if not mac:
                        logger.warning(
                            f"Skipping row {idx} - missing MAC address for computer: {name}"
                        )
                        continue

                    # Track analytics for reporting
                    os_type = self._normalize_value(computer_data.get("OS"))
                    if os_type:
                        ingestion_stats["os_types"][os_type] = (
                            ingestion_stats["os_types"].get(os_type, 0) + 1
                        )

                    # Track hardware stats
                    ram = computer_data.get("RAM")
                    if pd.notna(ram):
                        ingestion_stats["total_ram_gb"] += (
                            ram / 1024
                        )  # Convert MB to GB

                    disk = computer_data.get("Disk")
                    if pd.notna(disk):
                        ingestion_stats["total_disk_gb"] += disk

                    # Calculate content hash for this computer
                    current_hash = self._calculate_computer_content_hash(computer_data)

                    # Check if this computer is new or has changed (using MAC as unique identifier)
                    existing_hash = existing_hashes.get(mac)

                    logger.debug(f"Checking computer: {name} (MAC: {mac})")
                    logger.debug(f"  Current hash:  {current_hash}")
                    logger.debug(f"  Existing hash: {existing_hash}")

                    should_insert = False

                    if existing_hash is None:
                        # This is a completely new computer
                        logger.info(f"🆕 New computer detected: {name} (MAC: {mac})")
                        should_insert = True
                        ingestion_stats["new_computers"] += 1

                    elif existing_hash != current_hash:
                        # This computer exists but has changed
                        logger.info(f"📝 Computer changed: {name} (MAC: {mac})")
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_computers"] += 1

                    else:
                        # This computer exists and hasn't changed - skip it
                        logger.debug(
                            f"⏭️  Computer unchanged, skipping: {name} (MAC: {mac})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the computer is new or changed
                    if should_insert:
                        if self.dry_run:
                            logger.info(f"[DRY RUN] Would insert computer {mac}")
                        else:
                            # Normalize all raw data for JSON serialization
                            normalized_data = self._normalize_computer_data(
                                computer_data
                            )

                            # Enhance with metadata for future reference
                            normalized_data["_content_hash"] = current_hash
                            normalized_data["_change_detection"] = "content_hash_based"
                            normalized_data["_source_file"] = source_file
                            normalized_data["_ingestion_timestamp"] = datetime.now(
                                timezone.utc
                            ).isoformat()

                            # Insert into bronze layer using MAC address as external_id
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="computer",
                                source_system="key_client",
                                external_id=mac,
                                raw_data=normalized_data,
                                ingestion_run_id=run_id,
                            )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} computers processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    name_safe = (
                        computer_data.get("Name", "unknown")
                        if "Name" in computer_data
                        else "unknown"
                    )
                    error_msg = (
                        f"Failed to process computer {name_safe}: {record_error}"
                    )
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats["errors"]:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_summary,
            )

            ingestion_stats["completed_at"] = datetime.now(timezone.utc)
            duration = (
                ingestion_stats["completed_at"] - ingestion_stats["started_at"]
            ).total_seconds()

            # Log comprehensive results
            logger.info(
                f"🎉 KeyConfigure computer ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Source File: {source_file}")
            logger.info(
                f"   Records Read from File: {ingestion_stats['records_read_from_file']}"
            )
            if ingestion_stats["duplicate_mac_records_removed"] > 0:
                logger.info(
                    f"   Duplicate MAC Records Removed: {ingestion_stats['duplicate_mac_records_removed']}"
                )
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Computers: {ingestion_stats['new_computers']}")
            logger.info(
                f"   └─ Changed Computers: {ingestion_stats['changed_computers']}"
            )
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   Computer Analytics:")
            logger.info(f"   ├─ Total RAM: {ingestion_stats['total_ram_gb']:.2f} GB")
            logger.info(f"   ├─ Total Disk: {ingestion_stats['total_disk_gb']:.2f} GB")
            logger.info(f"   └─ OS Types: {len(ingestion_stats['os_types'])}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"KeyConfigure computer ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if ingestion_stats["run_id"]:
                self.complete_ingestion_run(
                    run_id=ingestion_stats["run_id"],
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_msg,
                )

            raise

    def get_computer_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze KeyConfigure computer data from bronze layer.

        This provides insights into the computer fleet and can help
        identify patterns or anomalies in the computer data.

        Returns:
            Dictionary containing DataFrames for different computer analyses
        """
        try:
            # Query for computer analytics
            analytics_query = """
            WITH latest_computers AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                AND source_system = 'key_client'
            )
            SELECT
                raw_data->>'Name' as name,
                raw_data->>'MAC' as mac_address,
                raw_data->>'OEM SN' as oem_serial,
                raw_data->>'OS' as os,
                raw_data->>'OS vers' as os_version,
                raw_data->>'CPU' as cpu,
                CAST(raw_data->>'RAM' AS FLOAT) as ram_mb,
                CAST(raw_data->>'Disk' AS FLOAT) as disk_gb,
                CAST(raw_data->>'Free' AS FLOAT) as free_gb,
                raw_data->>'Last User' as last_user,
                raw_data->>'Last Addr' as last_address,
                raw_data->>'Last Audit' as last_audit
            FROM latest_computers
            WHERE row_num = 1
            ORDER BY name
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Operating system distribution
            if not analytics_df.empty and "os" in analytics_df.columns:
                os_summary = (
                    analytics_df.groupby("os").size().reset_index(name="computer_count")
                )
                analyses["os_summary"] = os_summary.sort_values(
                    "computer_count", ascending=False
                )

            # CPU distribution
            if not analytics_df.empty and "cpu" in analytics_df.columns:
                cpu_summary = (
                    analytics_df.groupby("cpu")
                    .size()
                    .reset_index(name="computer_count")
                )
                analyses["cpu_summary"] = cpu_summary.sort_values(
                    "computer_count", ascending=False
                )

            # Computer features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_computers": len(analytics_df),
                    "total_ram_gb": (analytics_df["ram_mb"].sum() / 1024)
                    if "ram_mb" in analytics_df.columns
                    else 0,
                    "total_disk_gb": analytics_df["disk_gb"].sum()
                    if "disk_gb" in analytics_df.columns
                    else 0,
                    "avg_ram_gb": (analytics_df["ram_mb"].mean() / 1024)
                    if "ram_mb" in analytics_df.columns
                    else 0,
                    "avg_disk_gb": analytics_df["disk_gb"].mean()
                    if "disk_gb" in analytics_df.columns
                    else 0,
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full computer list
            analyses["full_computer_list"] = analytics_df

            logger.info(
                f"Generated computer analytics with {len(analytics_df)} computers from KeyConfigure"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate computer analytics: {e}")
            raise

    def get_computer_change_history(self, mac_address: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific KeyConfigure computer.

        Args:
            mac_address: The computer MAC address (external_id)

        Returns:
            DataFrame with all historical versions of the computer
        """
        try:
            query = """
            SELECT
                raw_id,
                ingested_at,
                raw_data->>'Name' as name,
                raw_data->>'OS' as os,
                raw_data->>'Last User' as last_user,
                raw_data->>'_content_hash' as content_hash
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
            AND source_system = 'key_client'
            AND external_id = :mac_address
            ORDER BY ingested_at DESC
            """

            return self.db_adapter.query_to_dataframe(
                query, {"mac_address": mac_address}
            )

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve computer history: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("KeyConfigure computer ingestion service closed")


def main():
    """
    Main function to run KeyConfigure computer ingestion from command line.
    """
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Ingest KeyConfigure computers into bronze layer"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Force full sync (bypass timestamp filtering - not applicable for content hash but kept for consistency)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--data-folder",
            type=str,
            default=DATA_FOLDER,
            help=f"Path to folder containing KeyConfigure Excel files (default: {DATA_FOLDER})",
        )
        args = parser.parse_args()

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        # Create and run ingestion service
        ingestion_service = KeyConfigureComputerIngestionService(
            database_url=database_url,
            data_folder=args.data_folder,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
        )

        # Run the content hash-based ingestion process
        print("�️  Starting KeyConfigure computer ingestion with content hashing...")
        results = (
            ingestion_service.ingest_keyconfigure_computers_with_change_detection()
        )

        # Display comprehensive summary
        print(f"\n📊 KeyConfigure Computer Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Source File: {results['source_file']}")
        print(f"   Total Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ New Computers: {results['new_computers']}")
        print(f"     └─ Changed Computers: {results['changed_computers']}")
        print(f"   Skipped (Unchanged): {results['records_skipped_unchanged']}")
        print(f"   Computer Analytics:")
        print(f"     ├─ Total RAM: {results['total_ram_gb']:.2f} GB")
        print(f"     ├─ Total Disk: {results['total_disk_gb']:.2f} GB")
        print(f"     └─ OS Types: {len(results['os_types'])}")
        print(f"   Errors: {len(results['errors'])}")

        if (
            results["records_skipped_unchanged"] > 0
            and results["records_processed"] > 0
        ):
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of computers were unchanged and skipped"
            )

        # Clean up
        ingestion_service.close()

        print("✅ KeyConfigure computer ingestion completed successfully!")

    except Exception as e:
        logger.error(f"KeyConfigure computer ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
