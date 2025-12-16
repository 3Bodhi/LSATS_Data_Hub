#!/usr/bin/env python3
"""
MCommunity Groups Source-Specific Silver Layer Transformation Service

This service transforms bronze MCommunity group records into the source-specific
silver.mcommunity_groups table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts MCommunity group fields from JSONB to typed columns
- Stores membership (members, owners, etc.) as JSONB arrays
- Content hash-based change detection
- Incremental processing (only transform groups with new bronze data)
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
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path
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


class MCommunityGroupTransformationService:
    """
    Service for transforming bronze MCommunity group records into source-specific silver layer.

    This service creates silver.mcommunity_groups records from bronze.raw_entities where:
    - entity_type = 'group'
    - source_system = 'mcommunity_ldap'

    Transformation Logic:
    - Extract MCommunity fields from JSONB to typed columns
    - Normalize members and owners to JSONB arrays
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
        logger.info("🔌 MCommunity groups silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful MCommunity groups transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'mcommunity_group'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("🆕 No previous transformation found - processing all groups")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_groups_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find MCommunity group IDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include groups with bronze records after this time
            full_sync: If True, return ALL MCommunity groups regardless of timestamp

        Returns:
            Set of external_ids (group names/emails) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id
            FROM bronze.raw_entities
            WHERE entity_type = 'group'
              AND source_system = 'mcommunity_ldap'
              {time_filter}
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            group_ids = set(result_df["external_id"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(group_ids)} MCommunity groups needing transformation ({sync_mode} mode)"
            )
            return group_ids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get groups needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(self, external_id: str) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for an MCommunity group.

        Args:
            external_id: The group identifier (usually group email or name)

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'group'
              AND source_system = 'mcommunity_ldap'
              AND external_id = :external_id
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"external_id": external_id}
            )

            if result_df.empty:
                return None

            # Handle stringified JSON if necessary
            raw_data = result_df.iloc[0]["raw_data"]
            if isinstance(raw_data, str):
                raw_data = json.loads(raw_data)
                
            return raw_data, result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze record for ID {external_id}: {e}")
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
            "raw_id", "entity_hash", "ingestion_run_id", 
            "created_at", "updated_at", "mcommunity_group_uid"
        }
        
        content_to_hash = {
            k: v for k, v in silver_record.items() 
            if k not in exclude_fields
        }

        normalized_json = json.dumps(
            content_to_hash, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_mcommunity_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse MCommunity timestamp strings (YYYYMMDDHHMMSSZ) into Python datetime objects.
        """
        if not timestamp_str:
            return None

        try:
            # Format: 20260212230252Z
            dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%SZ")
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _normalize_list_field(self, value: Any) -> List[str]:
        """
        Normalize a field that might be a string or a list of strings into a list of strings.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    def _get_first_value(self, value: Any) -> Optional[str]:
        """
        Get the first value from a potential list or string.
        """
        if value is None:
            return None
        if isinstance(value, list):
            return str(value[0]) if value else None
        return str(value)

    def _extract_mcommunity_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast MCommunity fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.mcommunity_groups columns
        """
        # Helper for boolean conversion
        def to_bool(val):
            if val is None:
                return None
            if isinstance(val, bool):
                return val
            return str(val).lower() in ('true', 't', 'yes', 'y', '1')

        # Helper for integer conversion
        def to_int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # Build contact info JSONB
        contact_info = {}
        for field in ['telephoneNumber', 'facsimileTelephoneNumber', 'umichPostalAddress', 'labeledUri']:
            val = self._get_first_value(raw_data.get(field))
            if val:
                contact_info[field] = val

        silver_record = {
            # Business Key
            "group_email": self._get_first_value(raw_data.get("umichGroupEmail")),
            
            # Core Identity
            "group_name": self._get_first_value(raw_data.get("cn")),
            "distinguished_name": self._get_first_value(raw_data.get("dn")),
            "description": self._get_first_value(raw_data.get("umichDescription")),
            "gid_number": to_int(self._get_first_value(raw_data.get("gidNumber"))),
            
            # Flags & Status
            "is_private": to_bool(self._get_first_value(raw_data.get("umichPrivate"))),
            "is_members_only": to_bool(self._get_first_value(raw_data.get("Membersonly"))),
            "is_joinable": to_bool(self._get_first_value(raw_data.get("joinable"))),
            "expiry_timestamp": self._parse_mcommunity_timestamp(self._get_first_value(raw_data.get("umichExpiryTimestamp"))),
            
            # Membership & Relationships
            "owners": self._normalize_list_field(raw_data.get("owner")),
            "members": self._normalize_list_field(raw_data.get("member")),
            "direct_members": self._normalize_list_field(raw_data.get("umichDirectMember")),
            "nested_members": self._normalize_list_field(raw_data.get("umichDirectGroupMember")),
            "requests_to": self._normalize_list_field(raw_data.get("requestsTo")),
            "aliases": self._normalize_list_field(raw_data.get("cn")),
            
            # Contact Info
            "contact_info": contact_info,
            
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
        Insert or update a silver.mcommunity_groups record.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        group_email = silver_record["group_email"]

        if dry_run:
            logger.info(f"[DRY RUN] Would upsert group: email={group_email}, name={silver_record.get('group_name')}")
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash 
            FROM silver.mcommunity_groups 
            WHERE group_email = :group_email
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"group_email": group_email}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                # logger.debug(f"⏭️  Group unchanged, skipping: {group_email}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.mcommunity_groups (
                        group_email, group_name, distinguished_name, description, gid_number,
                        is_private, is_members_only, is_joinable, expiry_timestamp,
                        owners, members, direct_members, nested_members, requests_to, aliases,
                        contact_info,
                        raw_id, entity_hash, ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :group_email, :group_name, :distinguished_name, :description, :gid_number,
                        :is_private, :is_members_only, :is_joinable, :expiry_timestamp,
                        CAST(:owners AS jsonb), CAST(:members AS jsonb), CAST(:direct_members AS jsonb),
                        CAST(:nested_members AS jsonb), CAST(:requests_to AS jsonb), CAST(:aliases AS jsonb),
                        CAST(:contact_info AS jsonb),
                        :raw_id, :entity_hash, :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (mcommunity_group_uid) DO UPDATE SET
                        group_email = EXCLUDED.group_email,
                        group_name = EXCLUDED.group_name,
                        distinguished_name = EXCLUDED.distinguished_name,
                        description = EXCLUDED.description,
                        gid_number = EXCLUDED.gid_number,
                        is_private = EXCLUDED.is_private,
                        is_members_only = EXCLUDED.is_members_only,
                        is_joinable = EXCLUDED.is_joinable,
                        expiry_timestamp = EXCLUDED.expiry_timestamp,
                        owners = EXCLUDED.owners,
                        members = EXCLUDED.members,
                        direct_members = EXCLUDED.direct_members,
                        nested_members = EXCLUDED.nested_members,
                        requests_to = EXCLUDED.requests_to,
                        aliases = EXCLUDED.aliases,
                        contact_info = EXCLUDED.contact_info,
                        raw_id = EXCLUDED.raw_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.mcommunity_groups.entity_hash != EXCLUDED.entity_hash
                """)
                
                # Note: The ON CONFLICT clause above targets mcommunity_group_uid (PK), 
                # but we don't have that in source data. We need to handle conflict on a unique key.
                # Since we don't have a unique constraint on group_email in the DDL (only an index),
                # we should probably add a unique constraint or index to support upsert, 
                # OR check existence first (which we did above).
                # 
                # Wait, the DDL has:
                # mcommunity_group_uid UUID PRIMARY KEY DEFAULT uuid_generate_v4()
                # group_email VARCHAR(255) NOT NULL
                # 
                # And indexes:
                # CREATE INDEX idx_silver_mcommunity_groups_email ON silver.mcommunity_groups (group_email);
                # 
                # It does NOT have a UNIQUE constraint on group_email. 
                # However, for an upsert to work via ON CONFLICT, we need a unique constraint/index.
                # I should probably modify the query to use UPDATE if exists, INSERT if not, based on the check we already did.
                # OR better, rely on the check we did:
                
                if is_new:
                    insert_query = text("""
                        INSERT INTO silver.mcommunity_groups (
                            group_email, group_name, distinguished_name, description, gid_number,
                            is_private, is_members_only, is_joinable, expiry_timestamp,
                            owners, members, direct_members, nested_members, requests_to, aliases,
                            contact_info,
                            raw_id, entity_hash, ingestion_run_id, created_at, updated_at
                        ) VALUES (
                            :group_email, :group_name, :distinguished_name, :description, :gid_number,
                            :is_private, :is_members_only, :is_joinable, :expiry_timestamp,
                            CAST(:owners AS jsonb), CAST(:members AS jsonb), CAST(:direct_members AS jsonb),
                            CAST(:nested_members AS jsonb), CAST(:requests_to AS jsonb), CAST(:aliases AS jsonb),
                            CAST(:contact_info AS jsonb),
                            :raw_id, :entity_hash, :ingestion_run_id, :created_at, :updated_at
                        )
                    """)
                    conn.execute(insert_query, {
                        **silver_record,
                        "owners": json.dumps(silver_record.get("owners", [])),
                        "members": json.dumps(silver_record.get("members", [])),
                        "direct_members": json.dumps(silver_record.get("direct_members", [])),
                        "nested_members": json.dumps(silver_record.get("nested_members", [])),
                        "requests_to": json.dumps(silver_record.get("requests_to", [])),
                        "aliases": json.dumps(silver_record.get("aliases", [])),
                        "contact_info": json.dumps(silver_record.get("contact_info", {})),
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    })
                else:
                    update_query = text("""
                        UPDATE silver.mcommunity_groups SET
                            group_name = :group_name,
                            distinguished_name = :distinguished_name,
                            description = :description,
                            gid_number = :gid_number,
                            is_private = :is_private,
                            is_members_only = :is_members_only,
                            is_joinable = :is_joinable,
                            expiry_timestamp = :expiry_timestamp,
                            owners = CAST(:owners AS jsonb),
                            members = CAST(:members AS jsonb),
                            direct_members = CAST(:direct_members AS jsonb),
                            nested_members = CAST(:nested_members AS jsonb),
                            requests_to = CAST(:requests_to AS jsonb),
                            aliases = CAST(:aliases AS jsonb),
                            contact_info = CAST(:contact_info AS jsonb),
                            raw_id = :raw_id,
                            entity_hash = :entity_hash,
                            ingestion_run_id = :ingestion_run_id,
                            updated_at = :updated_at
                        WHERE group_email = :group_email
                    """)
                    conn.execute(update_query, {
                        **silver_record,
                        "owners": json.dumps(silver_record.get("owners", [])),
                        "members": json.dumps(silver_record.get("members", [])),
                        "direct_members": json.dumps(silver_record.get("direct_members", [])),
                        "nested_members": json.dumps(silver_record.get("nested_members", [])),
                        "requests_to": json.dumps(silver_record.get("requests_to", [])),
                        "aliases": json.dumps(silver_record.get("aliases", [])),
                        "contact_info": json.dumps(silver_record.get("contact_info", {})),
                        "ingestion_run_id": run_id,
                        "updated_at": datetime.now(timezone.utc),
                    })

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} group: {group_email} (name: {silver_record.get('group_name')})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert group {group_email}: {e}")
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
                "transformation_type": "bronze_to_silver_mcommunity_groups",
                "entity_type": "mcommunity_group",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.mcommunity_groups",
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
                        :run_id, 'silver_transformation', 'mcommunity_group', :started_at, 'running', :metadata
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
            records_processed: Total groups processed
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
        Main entry point: Transform bronze MCommunity groups to silver.mcommunity_groups incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find MCommunity groups with bronze records newer than that timestamp
        3. For each group:
           a. Fetch latest bronze record
           b. Extract fields to silver columns
           c. Calculate entity hash
           d. Upsert to silver.mcommunity_groups
        4. Track statistics and return results

        Args:
            full_sync: If True, process all groups regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation
        last_transformation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_transformation, full_sync)

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "groups_processed": 0,
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
                logger.info("🔄 Full sync mode: Processing ALL MCommunity groups")
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing groups since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL MCommunity groups")

            logger.info("🚀 Starting MCommunity groups silver transformation...")

            # Find groups needing transformation
            group_ids = self._get_groups_needing_transformation(
                last_transformation, full_sync
            )

            if not group_ids:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(group_ids)} MCommunity groups")

            # Process each group
            for idx, external_id in enumerate(group_ids, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(external_id)

                    if not bronze_result:
                        logger.warning(f"⚠️  No bronze data found for ID {external_id}")
                        stats["errors"].append(f"No bronze data for {external_id}")
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

                    stats["groups_processed"] += 1

                    # Log progress periodically
                    if idx % 100 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(group_ids)} groups processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = f"Error processing group {external_id}: {str(record_error)}"
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other groups

            # Calculate duration
            duration = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds()
            
            logger.info(f"✅ Transformation finished in {duration:.2f} seconds")
            logger.info(f"   Total Processed: {stats['groups_processed']}")
            logger.info(f"   Created: {stats['records_created']}")
            logger.info(f"   Updated: {stats['records_updated']}")
            logger.info(f"   Skipped: {stats['records_skipped']}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            self.complete_transformation_run(
                run_id,
                stats["groups_processed"],
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
                stats["groups_processed"],
                stats["records_created"],
                stats["records_updated"],
                stats["records_skipped"],
                str(e),
            )
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform Bronze MCommunity Groups to Silver Layer"
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

    try:
        service = MCommunityGroupTransformationService(database_url)
        service.transform_incremental(full_sync=args.full_sync, dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
