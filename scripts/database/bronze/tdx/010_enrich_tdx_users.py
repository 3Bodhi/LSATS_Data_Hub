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
        max_enrichment_age_days: int = 30,
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
            max_enrichment_age_days: Force re-enrichment even on basic hash match if the
                existing enriched row is older than this many days. Catches changes to
                enriched-only fields (OrgApplications, Attributes, Permissions) that are
                invisible to the basic hash. Default: 30 days.
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
        self.max_enrichment_age_days = max_enrichment_age_days

        # Semaphore for controlling concurrency
        self.enrichment_semaphore = asyncio.Semaphore(max_concurrent_enrichments)

        # Thread pool for synchronous operations
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_enrichments)

        logger.info(f"🔌 TDX user enrichment service initialized:")
        logger.info(f"   Max concurrent enrichments: {max_concurrent_enrichments}")
        logger.info(f"   API rate limit delay: {api_rate_limit_delay}s")
        logger.info(f"   Max enrichment age: {max_enrichment_age_days} days")

    def _get_last_enrichment_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of the most recent enrichment run attempt for informational logging.

        Returns the completed_at of the last successful run, or the started_at of
        the last failed run if no successful run exists. Used for display only —
        this value is no longer used as a filter in the candidate query.

        Returns:
            Timestamp of the most recent run attempt, or None if no runs exist
        """
        try:
            query = """
            SELECT COALESCE(
                MAX(completed_at) FILTER (WHERE status = 'completed'),
                MAX(started_at)   FILTER (WHERE status = 'failed')
            ) AS last_run
            FROM meta.ingestion_runs
            WHERE entity_type = 'user'
              AND source_system = 'tdx'
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
        self, full_sync: bool = False
    ) -> pd.DataFrame:
        """
        Query bronze layer for users whose latest bronze record has not been enriched.

        Uses DISTINCT ON to get the most recent row per UID. A user is a candidate
        if their latest row lacks _enriched_at — meaning 002 has ingested a newer
        basic row since the last enrichment run.

        Hash comparison (done in Python via _get_existing_basic_hashes) then
        determines whether an API call is actually needed. Timestamp-based filtering
        is intentionally absent: hash comparison is the sole correctness mechanism,
        preventing silent data drift from partial failures.

        Args:
            full_sync: If True, all candidates are enriched regardless of hash state
                       (hash comparison bypassed downstream). Used after algorithm
                       changes to regenerate stored hashes.

        Returns:
            DataFrame with columns: uid, full_name, external_id, bronze_external_id,
            ingested_at, current_basic_hash
        """
        try:
            query = """
            WITH latest_rows AS (
                SELECT DISTINCT ON (raw_data->>'UID')
                    raw_data->>'UID'                    AS uid,
                    raw_data->>'FullName'               AS full_name,
                    raw_data->>'ExternalID'             AS external_id,
                    external_id                         AS bronze_external_id,
                    ingested_at,
                    raw_data->>'_content_hash_basic'    AS current_basic_hash,
                    raw_data->>'_enriched_at'           AS enriched_at
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
                current_basic_hash
            FROM latest_rows
            WHERE enriched_at IS NULL             -- latest row not yet enriched
              AND current_basic_hash IS NOT NULL  -- properly ingested by 002
            ORDER BY ingested_at DESC
            """

            result_df = self.db_adapter.query_to_dataframe(query)

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
        Calculate basic content hash using only identity fields common to both
        people/search and people/{uid} endpoints.

        TDX's people/search endpoint explicitly excludes these collection fields:
        Attributes, OrgApplications, GroupIDs, Permissions, Applications.
        These are excluded here so hashes computed by 002_ingest_tdx_users.py
        (from search results) and by this script (from full detail) are directly
        comparable — enabling hash-based skip logic in enrich_user_record().

        Args:
            user_data: User data from any TDX endpoint

        Returns:
            SHA-256 hash of stable identity fields only
        """
        significant_fields = {
            "UID":                    user_data.get("UID", ""),
            "UserName":               user_data.get("UserName", ""),
            "FirstName":              user_data.get("FirstName", ""),
            "LastName":               user_data.get("LastName", ""),
            "FullName":               user_data.get("FullName", ""),
            "PrimaryEmail":           user_data.get("PrimaryEmail", ""),
            "AlternateEmail":         user_data.get("AlternateEmail", ""),
            "ExternalID":             user_data.get("ExternalID", ""),
            "AlternateID":            user_data.get("AlternateID", ""),
            "IsActive":               user_data.get("IsActive", False),
            "SecurityRoleName":       user_data.get("SecurityRoleName", ""),
            "AuthenticationUserName": user_data.get("AuthenticationUserName", ""),
            "TypeID":                 user_data.get("TypeID", ""),
            # Hardcoded [] — people/search never returns Attributes, so 002 always
            # stores [] here. Hardcoding ensures this hash matches 002's stored hash.
            "Attributes":             [],
            # Accounts is returned by people/search and should be consistent between
            # endpoints, so we keep it to detect account-level changes.
            "Accounts":               user_data.get("Accounts", []),
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

    def _get_existing_enrichment_state(
        self, uids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch-fetch the enrichment state from the most recent enriched row per UID.

        Returns both the stored basic hash (for identity-change detection) and the
        enriched_at timestamp (for staleness detection). Called once before the async
        loop to build the skip map for enrich_user_record().

        Args:
            uids: List of TDX UIDs to look up (candidates from _get_users_needing_enrichment)

        Returns:
            Dict mapping uid -> {"hash": str, "enriched_at": datetime}.
            UIDs with no enriched row are absent (first-time enrichment proceeds normally).
        """
        if not uids:
            return {}
        try:
            query = """
            SELECT DISTINCT ON (raw_data->>'UID')
                raw_data->>'UID'                                AS uid,
                raw_data->>'_content_hash_basic'               AS existing_basic_hash,
                (raw_data->>'_enriched_at')::timestamptz       AS enriched_at
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'tdx'
              AND raw_data->>'_enriched_at' IS NOT NULL
              AND raw_data->>'UID' = ANY(:uids)
            ORDER BY raw_data->>'UID', ingested_at DESC
            """
            result_df = self.db_adapter.query_to_dataframe(query, {"uids": uids})
            if result_df.empty:
                return {}
            return {
                row["uid"]: {
                    "hash": row["existing_basic_hash"],
                    "enriched_at": row["enriched_at"],
                }
                for _, row in result_df.iterrows()
            }
        except SQLAlchemyError as e:
            logger.warning(
                f"⚠️  Could not fetch existing enrichment state ({e}). "
                f"Proceeding without skip check — all candidates will be enriched."
            )
            return {}

    async def enrich_user_record(
        self,
        uid: str,
        full_name: str,
        external_id: str,
        ingestion_run_id: str,
        loop: asyncio.AbstractEventLoop,
        dry_run: bool = False,
        current_basic_hash: Optional[str] = None,
        existing_basic_hash: Optional[str] = None,
        existing_enriched_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Enrich a single user record by fetching detailed data from TDX API.

        Skip logic (both conditions must be true):
          1. Basic hash match: current_basic_hash == existing_basic_hash
             → identity fields (name, email, etc.) are unchanged
          2. Freshness: existing_enriched_at is within max_enrichment_age_days
             → catches changes to enriched-only fields (OrgApplications, Attributes,
                Permissions) that are invisible to the basic hash

        The skip check runs before acquiring the semaphore so skipped users do not
        consume a concurrency slot.

        Args:
            uid: User's TDX UID
            full_name: User's full name (for logging)
            external_id: User's external ID (for bronze record)
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, log what would be done but don't commit
            current_basic_hash: Hash of the current basic row (stored by 002)
            existing_basic_hash: Hash stored in the most recent enriched row (stored by 010)
            existing_enriched_at: Timestamp of the most recent enriched row

        Returns:
            Dictionary with enrichment result (action: 'enriched', 'skipped', or 'error')
        """
        # Skip before acquiring semaphore — unchanged users don't need a concurrency slot.
        # Require both hash match AND freshness so that enriched-only field changes
        # (OrgApplications, Attributes, Permissions) are caught within the age window.
        now = datetime.now(timezone.utc)
        enrichment_is_fresh = (
            existing_enriched_at is not None
            and (now - existing_enriched_at).days < self.max_enrichment_age_days
        )
        if (
            current_basic_hash
            and existing_basic_hash
            and current_basic_hash == existing_basic_hash
            and enrichment_is_fresh
        ):
            logger.debug(f"⏭️  Skipping unchanged user: {full_name} ({uid})")
            return {
                "uid": uid,
                "full_name": full_name,
                "external_id": external_id,
                "success": True,
                "action": "skipped",
                "error_message": None,
                "started_at": now,
                "org_applications_count": 0,
                "attributes_count": 0,
                "permissions_count": 0,
            }

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
        full_sync: bool = False,
        progress_interval: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Process multiple users concurrently with enrichment.

        Before dispatching async tasks, performs a single batch DB query to fetch
        existing basic hashes for all candidate UIDs. Users whose basic hash matches
        their last enriched hash are skipped without making an API call.

        Args:
            users_df: DataFrame of users needing enrichment
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, preview changes without committing
            full_sync: If True, bypass hash comparison and enrich all candidates
            progress_interval: Log progress every N users (default: 100)

        Returns:
            List of enrichment results for all users
        """
        if users_df.empty:
            logger.warning("⚠️  No users to process")
            return []

        total_users = len(users_df)
        logger.info(f"🔄 Starting concurrent enrichment of {total_users:,} users...")

        # Pre-check: batch-fetch existing enrichment state to identify skippable users
        uids = users_df["uid"].tolist()
        existing_state: Dict[str, Dict[str, Any]] = {}
        if not full_sync:
            logger.info(f"🔍 Pre-checking existing enrichment state for {total_users:,} users...")
            existing_state = self._get_existing_enrichment_state(uids)
            now = datetime.now(timezone.utc)
            expected_skips = sum(
                1 for _, row in users_df.iterrows()
                if (
                    row.get("current_basic_hash")
                    and existing_state.get(row["uid"], {}).get("hash") == row.get("current_basic_hash")
                    and existing_state.get(row["uid"], {}).get("enriched_at") is not None
                    and (now - existing_state[row["uid"]]["enriched_at"]).days < self.max_enrichment_age_days
                )
            )
            logger.info(
                f"   ⏭️  {expected_skips:,} users unchanged and fresh (no API call needed)"
            )
            logger.info(
                f"   🔄 {total_users - expected_skips:,} users to enrich"
            )
        else:
            logger.info("   Full sync mode: bypassing hash comparison, enriching all candidates")

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
                    current_basic_hash=row.get("current_basic_hash"),
                    existing_basic_hash=existing_state.get(row["uid"], {}).get("hash"),
                    existing_enriched_at=existing_state.get(row["uid"], {}).get("enriched_at"),
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
            skipped = sum(1 for r in processed_results if r.get("action") == "skipped")
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
                f"⏭️  {skipped:>5,} skipped | "
                f"❌ {errors:>3,} errors | "
                f"⏱️  {users_per_second:.1f}/s | "
                f"ETA: {eta_str}"
            )

        # Calculate final enrichment statistics
        enriched = sum(1 for r in processed_results if r.get("action") == "enriched")
        skipped = sum(1 for r in processed_results if r.get("action") == "skipped")
        errors = sum(1 for r in processed_results if r.get("action") == "error")
        total_apps = sum(r.get("org_applications_count", 0) for r in processed_results)
        total_attrs = sum(r.get("attributes_count", 0) for r in processed_results)
        total_perms = sum(r.get("permissions_count", 0) for r in processed_results)

        logger.info(
            f"✅ Enrichment complete - {enriched:,} enriched, {skipped:,} skipped, {errors:,} errors"
        )
        logger.info(f"   Total OrgApplications: {total_apps:,}")
        logger.info(f"   Total Attributes: {total_attrs:,}")
        logger.info(f"   Total Permissions: {total_perms:,}")

        return processed_results

    def create_enrichment_run(
        self,
        total_users: int,
        full_sync: bool = False,
    ) -> str:
        """
        Create an enrichment run record.

        Args:
            total_users: Total number of candidate users
            full_sync: Whether hash comparison is bypassed (re-enriches all candidates)

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

            mode = "FULL SYNC" if full_sync else "HASH-DRIVEN"
            logger.info(
                f"📝 Created enrichment run {run_id} ({mode}) - {total_users} candidates"
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

        Candidates are users whose most recent bronze row lacks _enriched_at
        (i.e. 002 has ingested a newer basic row since last enrichment).
        Hash comparison eliminates API calls for users whose basic profile
        has not changed. No timestamp-based filtering is used.

        full_sync bypasses hash comparison and re-enriches all candidates —
        use this after algorithm changes to regenerate stored hashes.

        Args:
            full_sync: If True, bypass hash comparison and enrich all candidates
            dry_run: If True, preview changes without committing to database
            progress_interval: Log progress every N users (default: 100)

        Returns:
            Dictionary with comprehensive enrichment statistics
        """
        enrichment_stats = {
            "started_at": datetime.now(timezone.utc),
            "total_users_needing_enrichment": 0,
            "total_users_enriched": 0,
            "total_users_skipped": 0,
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

            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info(
                    "🔄 Full sync mode: bypassing hash comparison, enriching ALL candidates"
                )
            else:
                # Informational only — no longer used as a filter
                last_attempt = self._get_last_enrichment_timestamp()
                if last_attempt:
                    logger.info(f"ℹ️  Last enrichment attempt: {last_attempt}")
                else:
                    logger.info("ℹ️  No previous enrichment runs found")
                logger.info("🔍 Hash-driven mode: skipping users with unchanged basic profile")

            # Step 1: Get candidates (latest bronze row per UID lacks _enriched_at)
            users_df = self._get_users_needing_enrichment(full_sync=full_sync)

            if users_df.empty:
                logger.info("✨ All users have complete enrichment data")
                return enrichment_stats

            enrichment_stats["total_users_needing_enrichment"] = len(users_df)

            # Step 2: Create enrichment run for tracking
            run_id = self.create_enrichment_run(len(users_df), full_sync=full_sync)
            enrichment_stats["run_id"] = run_id

            # Step 3: Process all users concurrently (hash pre-check inside)
            loop = asyncio.get_event_loop()

            enrichment_results = await self.process_users_concurrently(
                users_df=users_df,
                ingestion_run_id=run_id,
                loop=loop,
                dry_run=dry_run,
                full_sync=full_sync,
                progress_interval=progress_interval,
            )

            # Step 4: Calculate statistics
            for result in enrichment_results:
                action = result.get("action")
                if action == "enriched":
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
                elif action == "skipped":
                    enrichment_stats["total_users_skipped"] += 1
                elif action == "error":
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
                f"   Mode:                   {'FULL SYNC' if full_sync else 'HASH-DRIVEN'}"
            )
            logger.info(f"   Dry Run:                {dry_run}")
            logger.info(f"")
            logger.info(
                f"   Candidates:             {enrichment_stats['total_users_needing_enrichment']:>6,}"
            )
            logger.info(
                f"   ├─ Enriched:            {enrichment_stats['total_users_enriched']:>6,}"
            )
            logger.info(
                f"   ├─ Skipped (unchanged): {enrichment_stats['total_users_skipped']:>6,}"
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
            help="Bypass hash comparison and re-enrich all candidates. Use after algorithm changes to regenerate stored hashes.",
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
        parser.add_argument(
            "--max-enrichment-age",
            type=int,
            default=30,
            help=(
                "Days before a hash-matching enriched row is considered stale and "
                "re-enriched anyway (default: 30). Catches changes to enriched-only "
                "fields (OrgApplications, Attributes, Permissions) that are invisible "
                "to the basic hash."
            ),
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
            max_enrichment_age_days=args.max_enrichment_age,
        )

        # Run the async enrichment process
        print("=" * 80)
        print("🚀 STARTING TDX USER ENRICHMENT")
        print("=" * 80)
        print(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'HASH-DRIVEN'}"
        )
        print(f"Dry Run:             {args.dry_run}")
        print(f"Max Concurrent:      {args.max_concurrent} API calls")
        print(f"API Delay:           {args.api_delay}s")
        print(f"Progress Interval:   {args.progress_interval} users")
        print(f"Max Enrichment Age:  {args.max_enrichment_age} days")
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
            f"Mode:                {'FULL SYNC' if args.full_sync else 'HASH-DRIVEN'}"
        )
        print(f"Dry Run:             {args.dry_run}")
        print(f"Duration:            {total_duration:.2f} seconds")
        print(f"")
        print(f"Candidates:          {results['total_users_needing_enrichment']:>6,}")
        print(f"├─ Enriched:         {results['total_users_enriched']:>6,}")
        print(f"├─ Skipped:          {results['total_users_skipped']:>6,}")
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
