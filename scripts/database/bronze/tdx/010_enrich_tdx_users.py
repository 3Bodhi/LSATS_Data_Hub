#!/usr/bin/env python3
"""
TDX User Enrichment Service

Enriches existing TDX user records in bronze layer with detailed data from get_user_by_uid endpoint.

The search_user endpoint (used in 002_ingest_tdx_users.py) returns basic user data.
The get_user_by_uid endpoint returns comprehensive details including:
- OrgApplications: Complete list of portal access and security roles
- Attributes: Custom fields (Institutional Role, Affiliations, etc.)
- Permissions: Detailed permission arrays

This enrichment is essential for:
- Understanding multi-unit user access patterns
- Tracking institutional roles (student/staff/faculty)
- Mapping users to all their organizational affiliations
- Permission and access auditing
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Add LSATS project to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Configure logging
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/bronze"
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


class TDXUserEnrichmentService:
    """
    Service for enriching TDX user records with detailed API data.

    Queries bronze layer for users with incomplete data, fetches detailed
    information via get_user_by_uid, and creates new enriched bronze records.
    """

    def __init__(
        self,
        database_url: str,
        tdx_base_url: str,
        tdx_api_token: str = None,
        tdx_username: str = None,
        tdx_password: str = None,
        tdx_beid: str = None,
        tdx_web_services_key: str = None,
        tdx_app_id: str = None,
        max_concurrent_enrichments: int = 3,
        api_rate_limit_delay: float = 3.5,
    ):
        """
        Initialize the enrichment service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token (optional if using other auth)
            tdx_username: TDX username for JWT auth (optional)
            tdx_password: TDX password for JWT auth (optional)
            tdx_beid: TDX BEID for admin auth (optional)
            tdx_web_services_key: TDX web services key for admin auth (optional)
            tdx_app_id: TeamDynamix application ID
            max_concurrent_enrichments: Maximum concurrent API calls
            api_rate_limit_delay: Delay between API calls (seconds)
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url,
            pool_size=10,
            max_overflow=20,
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url,
            app_id=tdx_app_id,
            api_token=tdx_api_token,
            username=tdx_username,
            password=tdx_password,
            beid=tdx_beid,
            web_services_key=tdx_web_services_key,
        )

        # Async processing configuration
        self.max_concurrent_enrichments = max_concurrent_enrichments
        self.api_rate_limit_delay = api_rate_limit_delay

        # Semaphore for controlling concurrency
        self.enrichment_semaphore = asyncio.Semaphore(max_concurrent_enrichments)

        # Thread pool for synchronous operations
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_enrichments)

        logger.info(f"🔌 TDX user enrichment service initialized:")
        logger.info(f"   Max concurrent enrichments: {max_concurrent_enrichments}")
        logger.info(f"   API rate limit delay: {api_rate_limit_delay}s")

    def _get_last_enrichment_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful enrichment run.

        Returns:
            Timestamp of last completed enrichment, or None if first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_run
            FROM meta.ingestion_runs
            WHERE entity_type = 'user'
              AND source_system = 'tdx'
              AND status = 'completed'
              AND metadata->>'enrichment_type' = 'detailed_user_data'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if result_df.empty or pd.isna(result_df.iloc[0]["last_run"]):
                return None

            return result_df.iloc[0]["last_run"]

        except SQLAlchemyError as e:
            logger.error(f"Failed to get last enrichment timestamp: {e}")
            return None

    def _get_users_needing_enrichment(
        self, full_sync: bool = False, since_timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Query bronze layer for users that need enrichment.

        A user needs enrichment if:
        - OrgApplications is NULL or missing
        - Attributes is empty array or missing
        - Never been enriched (_enriched_at missing)
        - In incremental mode: only users modified since last enrichment

        Args:
            full_sync: If True, get all users. If False, only recently updated
            since_timestamp: Only get users modified after this timestamp

        Returns:
            DataFrame with columns: uid, full_name, external_id, raw_data
        """
        try:
            # Base query to find users needing enrichment
            query = """
            WITH latest_users AS (
                SELECT DISTINCT ON (raw_data->>'UID')
                    raw_data->>'UID' as uid,
                    raw_data->>'FullName' as full_name,
                    raw_data->>'ExternalID' as external_id,
                    external_id as bronze_external_id,
                    raw_data,
                    ingested_at,
                    raw_data->'_enriched_at' as enriched_at
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                  AND source_system = 'tdx'
                  AND raw_data->>'UID' IS NOT NULL
                ORDER BY raw_data->>'UID', ingested_at DESC
            )
            SELECT
                uid,
                full_name,
                external_id,
                bronze_external_id,
                ingested_at,
                enriched_at
            FROM latest_users
            WHERE (
                -- Missing or incomplete enrichment data
                raw_data->'OrgApplications' IS NULL OR
                raw_data->'Attributes' = '[]'::jsonb OR
                raw_data->'_enriched_at' IS NULL
            )
            """

            # Add incremental filter if not full sync
            if not full_sync and since_timestamp:
                query += """
                AND ingested_at > :since_timestamp
                """
                params = {"since_timestamp": since_timestamp}
            else:
                params = {}

            query += """
            ORDER BY ingested_at DESC
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)

            if result_df.empty:
                logger.info("✨ All users have complete enrichment data")
                return result_df

            logger.info(f"🔍 Found {len(result_df)} users needing enrichment")

            # Log sample users
            sample_users = result_df.head(5)
            logger.info("   Sample users:")
            for _, user in sample_users.iterrows():
                logger.info(f"     - {user['full_name']} (UID: {user['uid']})")

            return result_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to query users needing enrichment: {e}")
            raise

    def _calculate_basic_content_hash(self, user_data: Dict[str, Any]) -> str:
        """
        Calculate basic content hash (matching ingest script).

        Only includes fields from search_user API to match 002_ingest_tdx_users.py hash.

        Args:
            user_data: User data (from any source)

        Returns:
            SHA-256 hash of basic fields only
        """
        # Match the hash calculation in 002_ingest_tdx_users.py
        significant_fields = {
            "UID": user_data.get("UID", ""),
            "UserName": user_data.get("UserName", ""),
            "FirstName": user_data.get("FirstName", ""),
            "LastName": user_data.get("LastName", ""),
            "FullName": user_data.get("FullName", ""),
            "PrimaryEmail": user_data.get("PrimaryEmail", ""),
            "AlternateEmail": user_data.get("AlternateEmail", ""),
            "ExternalID": user_data.get("ExternalID", ""),
            "AlternateID": user_data.get("AlternateID", ""),
            "IsActive": user_data.get("IsActive", False),
            "SecurityRoleName": user_data.get("SecurityRoleName", ""),
            "AuthenticationUserName": user_data.get("AuthenticationUserName", ""),
            "TypeID": user_data.get("TypeID", ""),
            "Attributes": user_data.get("Attributes", []),
            "Accounts": user_data.get("Accounts", []),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _calculate_enriched_content_hash(self, user_data: Dict[str, Any]) -> str:
        """
        Calculate content hash for enriched user data.

        Includes all significant fields including OrgApplications and Attributes.

        Args:
            user_data: Complete user data from get_user_by_uid

        Returns:
            SHA-256 hash of significant fields
        """
        # Include all significant fields
        significant_fields = {
            "UID": user_data.get("UID", ""),
            "UserName": user_data.get("UserName", ""),
            "FirstName": user_data.get("FirstName", ""),
            "LastName": user_data.get("LastName", ""),
            "FullName": user_data.get("FullName", ""),
            "PrimaryEmail": user_data.get("PrimaryEmail", ""),
            "AlternateEmail": user_data.get("AlternateEmail", ""),
            "ExternalID": user_data.get("ExternalID", ""),
            "AlternateID": user_data.get("AlternateID", ""),
            "IsActive": user_data.get("IsActive", False),
            "SecurityRoleName": user_data.get("SecurityRoleName", ""),
            "AuthenticationUserName": user_data.get("AuthenticationUserName", ""),
            "TypeID": user_data.get("TypeID", ""),
            "OrgApplications": user_data.get("OrgApplications", []),
            "Attributes": user_data.get("Attributes", []),
            "Permissions": user_data.get("Permissions", []),
            "GroupIDs": user_data.get("GroupIDs", []),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    async def enrich_user_record(
        self,
        uid: str,
        full_name: str,
        external_id: str,
        ingestion_run_id: str,
        loop: asyncio.AbstractEventLoop,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Enrich a single user record by fetching detailed data from TDX API.

        Args:
            uid: User's TDX UID
            full_name: User's full name (for logging)
            external_id: User's external ID (for bronze record)
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, log what would be done but don't commit

        Returns:
            Dictionary with enrichment result
        """
        async with self.enrichment_semaphore:  # Limit concurrent API calls
            enrichment_result = {
                "uid": uid,
                "full_name": full_name,
                "external_id": external_id,
                "success": False,
                "action": None,  # 'enriched', 'skipped', 'error'
                "error_message": None,
                "started_at": datetime.now(timezone.utc),
                "org_applications_count": 0,
                "attributes_count": 0,
                "permissions_count": 0,
            }

            try:
                logger.debug(f"🔍 Fetching detailed data for: {full_name} ({uid})")

                # Execute the synchronous API call in a thread pool
                def make_api_call():
                    time.sleep(self.api_rate_limit_delay)  # Rate limiting
                    return self.tdx_facade.users.get_user_by_uid(uid)

                user_data = await loop.run_in_executor(self.executor, make_api_call)

                if not user_data:
                    enrichment_result["error_message"] = "API returned no data"
                    enrichment_result["action"] = "error"
                    logger.warning(f"⚠️  No data returned for user: {full_name} ({uid})")
                    return enrichment_result

                # Extract enrichment metadata
                org_apps = user_data.get("OrgApplications") or []
                attributes = user_data.get("Attributes") or []
                permissions = user_data.get("Permissions") or []

                enrichment_result["org_applications_count"] = (
                    len(org_apps) if isinstance(org_apps, list) else 0
                )
                enrichment_result["attributes_count"] = (
                    len(attributes) if isinstance(attributes, list) else 0
                )
                enrichment_result["permissions_count"] = (
                    len(permissions) if isinstance(permissions, list) else 0
                )

                # Add enrichment metadata
                enhanced_user_data = user_data.copy()
                enhanced_user_data["_enrichment_method"] = "get_user_by_uid"
                enhanced_user_data["_ingestion_source"] = "get_user_by_uid"
                enhanced_user_data["_enriched_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                enhanced_user_data["_content_hash_enriched"] = (
                    self._calculate_enriched_content_hash(user_data)
                )

                # Preserve basic hash for ingest compatibility
                enhanced_user_data["_content_hash_basic"] = (
                    self._calculate_basic_content_hash(user_data)
                )

                if dry_run:
                    enrichment_result["success"] = True
                    enrichment_result["action"] = "enriched"
                    enrichment_result["raw_id"] = str(uuid.uuid4())  # Mock ID
                    logger.info(
                        f"[DRY RUN] Would enrich user: {full_name} - "
                        f"Apps: {enrichment_result['org_applications_count']}, "
                        f"Attrs: {enrichment_result['attributes_count']}, "
                        f"Perms: {enrichment_result['permissions_count']}"
                    )
                    return enrichment_result

                # Insert enriched record into bronze layer
                def perform_ingestion():
                    return self.db_adapter.insert_raw_entity(
                        entity_type="user",
                        source_system="tdx",
                        external_id=external_id,
                        raw_data=enhanced_user_data,
                        ingestion_run_id=ingestion_run_id,
                    )

                raw_id = await loop.run_in_executor(self.executor, perform_ingestion)

                enrichment_result["raw_id"] = raw_id
                enrichment_result["success"] = True
                enrichment_result["action"] = "enriched"

                logger.debug(
                    f"✅ Enriched user: {full_name} - "
                    f"Apps: {enrichment_result['org_applications_count']}, "
                    f"Attrs: {enrichment_result['attributes_count']}"
                )

                return enrichment_result

            except Exception as e:
                error_msg = f"Failed to enrich user {uid}: {str(e)}"
                logger.error(f"❌ {error_msg}")

                enrichment_result["error_message"] = error_msg
                enrichment_result["action"] = "error"
                return enrichment_result

    async def process_users_concurrently(
        self,
        users_df: pd.DataFrame,
        ingestion_run_id: str,
        loop: asyncio.AbstractEventLoop,
        dry_run: bool = False,
        progress_interval: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Process multiple users concurrently with enrichment.

        Args:
            users_df: DataFrame of users needing enrichment
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, preview changes without committing
            progress_interval: Log progress every N users (default: 100)

        Returns:
            List of enrichment results for all users
        """
        if users_df.empty:
            logger.warning("⚠️  No users to process")
            return []

        total_users = len(users_df)
        logger.info(f"🔄 Starting concurrent enrichment of {total_users:,} users...")

        # Track progress
        start_time = datetime.now(timezone.utc)
        processed_results = []

        # Process users in batches for better progress tracking
        batch_size = progress_interval
        num_batches = (total_users + batch_size - 1) // batch_size

        for batch_idx in range(num_batches):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, total_users)
            batch_df = users_df.iloc[batch_start:batch_end]

            # Create enrichment tasks for this batch
            enrichment_tasks = [
                self.enrich_user_record(
                    uid=row["uid"],
                    full_name=row["full_name"],
                    external_id=row["external_id"],
                    ingestion_run_id=ingestion_run_id,
                    loop=loop,
                    dry_run=dry_run,
                )
                for _, row in batch_df.iterrows()
            ]

            # Execute batch enrichment tasks concurrently
            batch_results = await asyncio.gather(
                *enrichment_tasks, return_exceptions=True
            )

            # Handle any exceptions that occurred during enrichment
            for result in batch_results:
                if isinstance(result, Exception):
                    error_result = {
                        "uid": "unknown",
                        "full_name": "Unknown User",
                        "success": False,
                        "action": "error",
                        "error_message": f"Async enrichment exception: {str(result)}",
                    }
                    processed_results.append(error_result)
                    logger.error(f"❌ Async enrichment exception: {result}")
                else:
                    processed_results.append(result)

            # Calculate progress statistics
            elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            users_processed = len(processed_results)
            enriched = sum(
                1 for r in processed_results if r.get("action") == "enriched"
            )
            errors = sum(1 for r in processed_results if r.get("action") == "error")

            # Calculate rate and ETA
            users_per_second = users_processed / elapsed_time if elapsed_time > 0 else 0
            users_remaining = total_users - users_processed
            eta_seconds = (
                users_remaining / users_per_second if users_per_second > 0 else 0
            )

            # Format ETA
            if eta_seconds < 60:
                eta_str = f"{eta_seconds:.0f}s"
            elif eta_seconds < 3600:
                eta_str = f"{eta_seconds / 60:.1f}m"
            else:
                eta_str = f"{eta_seconds / 3600:.1f}h"

            # Log progress update
            logger.info(
                f"📊 Progress: {users_processed:>6,}/{total_users:,} users "
                f"({users_processed * 100 / total_users:>5.1f}%) | "
                f"✅ {enriched:>5,} enriched | "
                f"❌ {errors:>3,} errors | "
                f"⏱️  {users_per_second:.1f}/s | "
                f"ETA: {eta_str}"
            )

        # Calculate final enrichment statistics
        enriched = sum(1 for r in processed_results if r.get("action") == "enriched")
        errors = sum(1 for r in processed_results if r.get("action") == "error")
        total_apps = sum(r.get("org_applications_count", 0) for r in processed_results)
        total_attrs = sum(r.get("attributes_count", 0) for r in processed_results)
        total_perms = sum(r.get("permissions_count", 0) for r in processed_results)

        logger.info(
            f"✅ Enrichment complete - {enriched:,} enriched, {errors:,} errors"
        )
        logger.info(f"   Total OrgApplications: {total_apps:,}")
        logger.info(f"   Total Attributes: {total_attrs:,}")
        logger.info(f"   Total Permissions: {total_perms:,}")

        return processed_results

    def create_enrichment_run(
        self,
        total_users: int,
        full_sync: bool = False,
        incremental_since: Optional[datetime] = None,
    ) -> str:
        """
        Create an enrichment run record.

        Args:
            total_users: Total number of users to enrich
            full_sync: Whether this is a full sync or incremental
            incremental_since: Timestamp of last successful run (if incremental)

        Returns:
            UUID string of the created run
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "enrichment_type": "detailed_user_data",
                "total_users": total_users,
                "max_concurrent_enrichments": self.max_concurrent_enrichments,
                "api_rate_limit_delay": self.api_rate_limit_delay,
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
                "enrichment_fields": ["OrgApplications", "Attributes", "Permissions"],
            }

            with self.db_adapter.engine.connect() as conn:
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
                        "source_system": "tdx",
                        "entity_type": "user",
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(
                f"📝 Created enrichment run {run_id} ({mode}) - {total_users} users"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create enrichment run: {e}")
            raise

    def complete_enrichment_run(
        self,
        run_id: str,
        total_users_processed: int,
        total_users_enriched: int,
        total_errors: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark the enrichment run as completed with statistics.

        Args:
            run_id: UUID of the enrichment run
            total_users_processed: Total users processed
            total_users_enriched: Users actually enriched
            total_errors: Number of errors
            error_message: Error summary if failures occurred
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
                        records_updated = 0,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_processed": total_users_processed,
                        "records_created": total_users_enriched,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed enrichment run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete enrichment run: {e}")

    async def run_async_user_enrichment(
        self,
        full_sync: bool = False,
        dry_run: bool = False,
        progress_interval: int = 100,
    ) -> Dict[str, Any]:
        """
        Run the complete async user enrichment process.

        Args:
            full_sync: If True, enrich all users. If False, use incremental mode
            dry_run: If True, preview changes without committing to database
            progress_interval: Log progress every N users (default: 100)

        Returns:
            Dictionary with comprehensive enrichment statistics
        """
        enrichment_stats = {
            "started_at": datetime.now(timezone.utc),
            "total_users_needing_enrichment": 0,
            "total_users_enriched": 0,
            "total_users_failed": 0,
            "total_org_applications": 0,
            "total_attributes": 0,
            "total_permissions": 0,
            "errors": [],
            "full_sync": full_sync,
            "dry_run": dry_run,
        }

        try:
            logger.info("🚀 Starting async user enrichment process...")

            # Determine processing mode
            last_timestamp = (
                None if full_sync else self._get_last_enrichment_timestamp()
            )

            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Enriching ALL users")
            elif last_timestamp:
                logger.info(
                    f"⚡ Incremental mode: Enriching users modified since {last_timestamp}"
                )
            else:
                logger.info("🆕 First run: Enriching ALL users needing data")

            # Step 1: Get users needing enrichment
            users_df = self._get_users_needing_enrichment(
                full_sync=full_sync, since_timestamp=last_timestamp
            )

            if users_df.empty:
                logger.info("✨ All users have complete enrichment data")
                return enrichment_stats

            enrichment_stats["total_users_needing_enrichment"] = len(users_df)

            # Step 2: Create enrichment run for tracking
            run_id = self.create_enrichment_run(
                len(users_df), full_sync=full_sync, incremental_since=last_timestamp
            )
            enrichment_stats["run_id"] = run_id

            # Step 3: Process all users concurrently
            loop = asyncio.get_event_loop()

            enrichment_results = await self.process_users_concurrently(
                users_df=users_df,
                ingestion_run_id=run_id,
                loop=loop,
                dry_run=dry_run,
                progress_interval=progress_interval,
            )

            # Step 4: Calculate statistics
            for result in enrichment_results:
                if result.get("action") == "enriched":
                    enrichment_stats["total_users_enriched"] += 1
                    enrichment_stats["total_org_applications"] += result.get(
                        "org_applications_count", 0
                    )
                    enrichment_stats["total_attributes"] += result.get(
                        "attributes_count", 0
                    )
                    enrichment_stats["total_permissions"] += result.get(
                        "permissions_count", 0
                    )
                elif result.get("action") == "error":
                    enrichment_stats["total_users_failed"] += 1
                    if result.get("error_message"):
                        enrichment_stats["errors"].append(result["error_message"])

            # Step 5: Complete the enrichment run
            error_summary = None
            if enrichment_stats["errors"]:
                error_summary = f"{len(enrichment_stats['errors'])} errors occurred during enrichment"

            self.complete_enrichment_run(
                run_id=run_id,
                total_users_processed=len(users_df),
                total_users_enriched=enrichment_stats["total_users_enriched"],
                total_errors=enrichment_stats["total_users_failed"],
                error_message=error_summary,
            )

            enrichment_stats["completed_at"] = datetime.now(timezone.utc)
            total_duration = (
                enrichment_stats["completed_at"] - enrichment_stats["started_at"]
            ).total_seconds()

            # Calculate averages
            avg_apps_per_user = (
                enrichment_stats["total_org_applications"]
                / enrichment_stats["total_users_enriched"]
                if enrichment_stats["total_users_enriched"] > 0
                else 0
            )

            avg_attrs_per_user = (
                enrichment_stats["total_attributes"]
                / enrichment_stats["total_users_enriched"]
                if enrichment_stats["total_users_enriched"] > 0
                else 0
            )

            # Log comprehensive results
            logger.info("=" * 80)
            logger.info("🎉 USER ENRICHMENT COMPLETED")
            logger.info("=" * 80)
            logger.info(f"📊 Results Summary:")
            logger.info(
                f"   Mode:                   {'FULL SYNC' if full_sync else 'INCREMENTAL'}"
            )
            logger.info(f"   Dry Run:                {dry_run}")
            logger.info(f"")
            logger.info(
                f"   Users Needing Enrich:   {enrichment_stats['total_users_needing_enrichment']:>6,}"
            )
            logger.info(
                f"   ├─ Enriched:            {enrichment_stats['total_users_enriched']:>6,}"
            )
            logger.info(
                f"   └─ Failed:              {enrichment_stats['total_users_failed']:>6,}"
            )
            logger.info(f"")
            logger.info(f"   Enrichment Data:")
            logger.info(
                f"   ├─ OrgApplications:     {enrichment_stats['total_org_applications']:>6,} (avg: {avg_apps_per_user:.1f}/user)"
            )
            logger.info(
                f"   ├─ Attributes:          {enrichment_stats['total_attributes']:>6,} (avg: {avg_attrs_per_user:.1f}/user)"
            )
            logger.info(
                f"   └─ Permissions:         {enrichment_stats['total_permissions']:>6,}"
            )
            logger.info(f"")
            logger.info(f"   Duration:               {total_duration:.2f}s")
            logger.info(
                f"   Errors:                 {len(enrichment_stats['errors']):>6,}"
            )
            logger.info("=" * 80)

            return enrichment_stats

        except Exception as e:
            error_msg = f"Async user enrichment failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            if "run_id" in enrichment_stats:
                self.complete_enrichment_run(
                    run_id=enrichment_stats["run_id"],
                    total_users_processed=enrichment_stats[
                        "total_users_needing_enrichment"
                    ],
                    total_users_enriched=enrichment_stats["total_users_enriched"],
                    total_errors=enrichment_stats["total_users_failed"],
                    error_message=error_msg,
                )

            enrichment_stats["errors"].append(error_msg)
            raise

    def close(self):
        """Clean up database connections and thread pool."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.executor:
            self.executor.shutdown(wait=True)
        logger.info("🔌 User enrichment service closed")


async def main():
    """
    Main async function to run user enrichment from command line.
    """
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description="Enrich TDX user records with detailed API data (OrgApplications, Attributes, Permissions)"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Enrich all users (ignore last enrichment timestamp)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--max-concurrent",
            type=int,
            default=3,
            help="Maximum concurrent API calls (default: 3, safe for 60/min rate limit)",
        )
        parser.add_argument(
            "--api-delay",
            type=float,
            default=3.5,
            help="API rate limit delay in seconds (default: 3.5, safe for 60/min rate limit)",
        )
        parser.add_argument(
            "--progress-interval",
            type=int,
            default=100,
            help="Log progress every N users (default: 100)",
        )

        args = parser.parse_args()

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")
        tdx_base_url = os.getenv("TDX_BASE_URL")
        tdx_api_token = os.getenv("TDX_API_TOKEN")
        tdx_username = os.getenv("TDX_USERNAME")
        tdx_password = os.getenv("TDX_PASSWORD")
        tdx_beid = os.getenv("TDX_BEID")
        tdx_web_services_key = os.getenv("TDX_WEB_SERVICES_KEY")
        tdx_app_id = os.getenv("TDX_APP_ID")

        # Validate configuration
        required_vars = {
            "DATABASE_URL": database_url,
            "TDX_BASE_URL": tdx_base_url,
            "TDX_APP_ID": tdx_app_id,
        }

        missing_vars = [name for name, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        has_credentials = (
            (tdx_beid and tdx_web_services_key)
            or (tdx_username and tdx_password)
            or tdx_api_token
        )
        if not has_credentials:
            raise ValueError("Missing TDX credentials: provide BEID+WebServicesKey, Username+Password, or API_TOKEN")

        # Create and run enrichment service
        enrichment_service = TDXUserEnrichmentService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_username=tdx_username,
            tdx_password=tdx_password,
            tdx_beid=tdx_beid,
            tdx_web_services_key=tdx_web_services_key,
            tdx_app_id=tdx_app_id,
            max_concurrent_enrichments=args.max_concurrent,
            api_rate_limit_delay=args.api_delay,
        )

        # Run the async enrichment process
        print("=" * 80)
        print("🚀 STARTING TDX USER ENRICHMENT")
        print("=" * 80)
        print(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        print(f"Dry Run:             {args.dry_run}")
        print(f"Max Concurrent:      {args.max_concurrent} API calls")
        print(f"API Delay:           {args.api_delay}s")
        print(f"Progress Interval:   {args.progress_interval} users")
        print("=" * 80)
        print()

        results = await enrichment_service.run_async_user_enrichment(
            full_sync=args.full_sync,
            dry_run=args.dry_run,
            progress_interval=args.progress_interval,
        )

        # Display comprehensive summary
        total_duration = (
            results["completed_at"] - results["started_at"]
        ).total_seconds()

        print()
        print("=" * 80)
        print("📊 ENRICHMENT SUMMARY")
        print("=" * 80)
        print(f"Run ID:              {results.get('run_id', 'N/A')}")
        print(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        print(f"Dry Run:             {args.dry_run}")
        print(f"Duration:            {total_duration:.2f} seconds")
        print(f"")
        print(f"Users Needing Data:  {results['total_users_needing_enrichment']:>6,}")
        print(f"├─ Enriched:         {results['total_users_enriched']:>6,}")
        print(f"└─ Failed:           {results['total_users_failed']:>6,}")
        print(f"")
        print(f"Data Collected:")
        print(f"├─ OrgApplications:  {results['total_org_applications']:>6,}")
        print(f"├─ Attributes:       {results['total_attributes']:>6,}")
        print(f"└─ Permissions:      {results['total_permissions']:>6,}")
        print(f"")
        print(f"Errors:              {len(results['errors']):>6,}")
        print("=" * 80)

        # Show sample errors if any occurred
        if results["errors"]:
            print()
            print("⚠️  Sample Errors (first 3):")
            for error in results["errors"][:3]:
                print(f"   - {error}")
            print()

        # Clean up
        enrichment_service.close()

        if args.dry_run:
            print()
            print("⚠️  DRY RUN COMPLETED - No changes committed to database")
        else:
            print()
            print("✅ Enrichment completed successfully!")

    except Exception as e:
        logger.error(f"❌ User enrichment failed: {e}", exc_info=True)
        print(f"\n❌ Enrichment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
