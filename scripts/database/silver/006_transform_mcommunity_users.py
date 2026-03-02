#!/usr/bin/env python3
"""
MCommunity Users Source-Specific Silver Layer Transformation Service

This service transforms bronze MCommunity LDAP user records into the source-specific
silver.mcommunity_users table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts MCommunity LDAP user fields from JSONB to typed columns
- Stores multi-value fields (ou, cn, sn, objectClass) as JSONB arrays
- Content hash-based change detection
- Incremental processing (only transform users with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
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
from typing import Any, Dict, List, Optional, Set, Tuple

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


class MCommunityUserTransformationService:
    """
    Service for transforming bronze MCommunity user records into source-specific silver layer.

    This service creates silver.mcommunity_users records from bronze.raw_entities where:
    - entity_type = 'user'
    - source_system = 'mcommunity_ldap'

    Transformation Logic:
    - Extract MCommunity LDAP fields from JSONB to typed columns
    - Normalize lists (ou, cn, sn, objectClass) to JSONB arrays
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze
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
        logger.info("🔌 MCommunity users silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful MCommunity users transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'mcommunity_user'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all users"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_users_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find MCommunity user UIDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include users with bronze records after this time
            full_sync: If True, return ALL MCommunity users regardless of timestamp

        Returns:
            Set of UIDs (uniqnames) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id as uid
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'mcommunity_ldap'
              {time_filter}
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            uids = set(result_df["uid"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(uids)} MCommunity users needing transformation ({sync_mode} mode)"
            )
            return uids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get users needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(self, uid: str) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for a MCommunity user.

        Args:
            uid: The MCommunity uniqname (uid)

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'mcommunity_ldap'
              AND external_id = :uid
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(query, {"uid": uid})

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze record for UID {uid}: {e}")
            raise

    def _calculate_content_hash(self, silver_record: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Args:
            silver_record: The prepared silver record

        Returns:
            SHA-256 hash string
        """
        # Exclude metadata fields from hash calculation
        exclude_fields = {
            "raw_id",
            "entity_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "source_system",
        }

        content_to_hash = {
            k: v for k, v in silver_record.items() if k not in exclude_fields
        }

        normalized_json = json.dumps(
            content_to_hash, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _normalize_list_field(self, value: Any) -> List[str]:
        """
        Normalize a field that might be a string or a list of strings into a list of strings.

        MCommunity LDAP can return fields as either strings or arrays depending on the number
        of values. This normalizes to always return an array.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    def _extract_mcommunity_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast MCommunity LDAP fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.mcommunity_users columns
        """

        # Helper for bigint conversion (uidNumber, gidNumber)
        def to_bigint(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        sn_values = self._normalize_list_field(raw_data.get("sn"))

        silver_record = {
            # Primary identifier (business key)
            "uniqname": raw_data.get("uid"),
            # Core identity fields
            "full_name": raw_data.get("displayName"),
            "first_name": raw_data.get("givenName"),
            "last_name": sn_values[0] if sn_values else None,
            "cn": self._normalize_list_field(raw_data.get("cn")),
            # Contact information
            "primary_email": raw_data.get("mail"),
            "work_phone": raw_data.get("telephoneNumber"),
            # Organizational affiliations
            "ou": self._normalize_list_field(raw_data.get("ou")),
            # Work/Position information
            "umich_title": raw_data.get("umichTitle"),
            # Postal address (structured and raw)
            "umich_postal_address": raw_data.get("umichPostalAddress"),
            "umich_postal_address_data": raw_data.get("umichPostalAddressData"),
            # POSIX/System fields
            "uid_number": to_bigint(raw_data.get("uidNumber")),
            "gid_number": to_bigint(raw_data.get("gidNumber")),
            "home_directory": raw_data.get("homeDirectory"),
            "login_shell": raw_data.get("loginShell"),
            # LDAP metadata
            "object_class": self._normalize_list_field(raw_data.get("objectClass")),
            # LDAP server metadata (from bronze ingestion)
            "ldap_server": raw_data.get("_ldap_server"),
            "search_base": raw_data.get("_search_base"),
            # Traceability
            "raw_id": raw_id,
        }

        # Calculate hash after populating fields
        silver_record["entity_hash"] = self._calculate_content_hash(silver_record)

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.mcommunity_users record.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        uid = silver_record["uniqname"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert user: uniqname={uid}, name={silver_record.get('full_name')}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.mcommunity_users
            WHERE uniqname = :uniqname
            """
            existing_df = self.db_adapter.query_to_dataframe(check_query, {"uniqname": uid})

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.mcommunity_users (
                        uniqname, full_name, first_name, last_name, cn,
                        primary_email, work_phone,
                        ou,
                        umich_title,
                        umich_postal_address, umich_postal_address_data,
                        uid_number, gid_number, home_directory, login_shell,
                        object_class,
                        ldap_server, search_base,
                        raw_id, entity_hash, ingestion_run_id,
                        created_at, updated_at
                    ) VALUES (
                        :uniqname, :full_name, :first_name, :last_name, CAST(:cn AS jsonb),
                        :primary_email, :work_phone,
                        CAST(:ou AS jsonb),
                        :umich_title,
                        :umich_postal_address, :umich_postal_address_data,
                        :uid_number, :gid_number, :home_directory, :login_shell,
                        CAST(:object_class AS jsonb),
                        :ldap_server, :search_base,
                        :raw_id, :entity_hash, :ingestion_run_id,
                        :created_at, :updated_at
                    )
                    ON CONFLICT (uniqname) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        cn = EXCLUDED.cn,
                        primary_email = EXCLUDED.primary_email,
                        work_phone = EXCLUDED.work_phone,
                        ou = EXCLUDED.ou,
                        umich_title = EXCLUDED.umich_title,
                        umich_postal_address = EXCLUDED.umich_postal_address,
                        umich_postal_address_data = EXCLUDED.umich_postal_address_data,
                        uid_number = EXCLUDED.uid_number,
                        gid_number = EXCLUDED.gid_number,
                        home_directory = EXCLUDED.home_directory,
                        login_shell = EXCLUDED.login_shell,
                        object_class = EXCLUDED.object_class,
                        ldap_server = EXCLUDED.ldap_server,
                        search_base = EXCLUDED.search_base,
                        raw_id = EXCLUDED.raw_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.mcommunity_users.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        # JSON serialization for JSONB fields
                        "cn": json.dumps(silver_record.get("cn", [])),
                        "ou": json.dumps(silver_record.get("ou", [])),
                        "object_class": json.dumps(
                            silver_record.get("object_class", [])
                        ),
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert user {uid}: {e}")
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
                "transformation_type": "bronze_to_silver_mcommunity_users",
                "entity_type": "mcommunity_user",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.mcommunity_users",
                "tier": "source_specific",
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
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
                    WHERE source_system = 'silver_transformation'
                      AND entity_type = 'mcommunity_user'
                      AND status = 'running'
                """))

                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'mcommunity_user', :started_at, 'running', :metadata
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

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(f"📝 Created transformation run {run_id} ({mode})")
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
        """
        Mark a transformation run as completed.

        Args:
            run_id: The run ID to complete
            records_processed: Total users processed
            records_created: New silver records created
            records_updated: Existing silver records updated
            records_skipped: Records skipped (unchanged)
            error_message: Error message if run failed
        """
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
                        metadata = jsonb_set(
                            metadata,
                            '{records_skipped}',
                            to_jsonb(:records_skipped)
                        )
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
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_incremental(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Main entry point: Transform bronze MCommunity users to silver.mcommunity_users incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find MCommunity users with bronze records newer than that timestamp
        3. For each user:
           a. Fetch latest bronze record
           b. Extract fields to silver columns
           c. Calculate entity hash
           d. Upsert to silver.mcommunity_users
        4. Track statistics and return results

        Args:
            full_sync: If True, process all users regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation
        last_transformation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run (skip for dry runs to avoid stale 'running' records)
        run_id = self.create_transformation_run(last_transformation, full_sync) if not dry_run else "dry-run"

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "users_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Processing ALL MCommunity users")
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing users since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL MCommunity users")

            logger.info("🚀 Starting MCommunity users silver transformation...")

            # Find users needing transformation
            uids = self._get_users_needing_transformation(
                last_transformation, full_sync
            )

            if not uids:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(uids)} MCommunity users")

            # Process each user
            for idx, uid in enumerate(uids, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(uid)

                    if not bronze_result:
                        logger.warning(f"⚠️  No bronze data found for UID {uid}")
                        stats["errors"].append(f"No bronze data for {uid}")
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract MCommunity fields to silver columns
                    silver_record = self._extract_mcommunity_fields(raw_data, raw_id)

                    # Upsert to silver layer
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["users_processed"] += 1

                    # Log progress periodically
                    if idx % 1000 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(uids)} users processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = f"Error processing user {uid}: {str(record_error)}"
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other users

            # Calculate duration
            duration = (
                datetime.now(timezone.utc) - stats["started_at"]
            ).total_seconds()

            logger.info("=" * 60)
            logger.info(f"✅ Transformation completed in {duration:.2f} seconds")
            logger.info(f"   Total Processed: {stats['users_processed']:,}")
            logger.info(f"   Created: {stats['records_created']:,}")
            logger.info(f"   Updated: {stats['records_updated']:,}")
            logger.info(f"   Skipped: {stats['records_skipped']:,}")
            logger.info(f"   Errors: {len(stats['errors'])}")
            logger.info("=" * 60)

            self.complete_transformation_run(
                run_id,
                stats["users_processed"],
                stats["records_created"],
                stats["records_updated"],
                stats["records_skipped"],
                f"{len(stats['errors'])} errors" if stats["errors"] else None,
            )

            return stats

        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}", exc_info=True)
            self.complete_transformation_run(
                run_id,
                stats["users_processed"],
                stats["records_created"],
                stats["records_updated"],
                stats["records_skipped"],
                str(e),
            )
            raise

    def close(self):
        """Close database connections."""
        if self.db_adapter:
            self.db_adapter.close()
            logger.info("🔌 Database connections closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform Bronze MCommunity Users to Silver Layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all records regardless of last run time",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    service = None
    try:
        service = MCommunityUserTransformationService(database_url)
        service.transform_incremental(full_sync=args.full_sync, dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
    finally:
        if service:
            service.close()
