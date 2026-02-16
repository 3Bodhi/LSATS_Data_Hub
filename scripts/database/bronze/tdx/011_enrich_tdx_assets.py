#!/usr/bin/env python3
"""
TDX Asset Enrichment Service

Enriches existing TDX asset records in bronze layer with detailed data from get_asset endpoint.

The search_asset endpoint (used in 003_ingest_tdx_assets.py) returns basic asset data.
The get_asset endpoint returns comprehensive details including:
- Attributes: Custom fields specific to the asset
- Attachments: Files attached to the asset record
- Complete CI associations and location details

This enrichment is essential for:
- Understanding custom asset attributes
- Tracking asset documentation (attachments)
- Full asset configuration and history
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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Determine log directory based on script location
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs/bronze"
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class TDXAssetEnrichmentService:
    """
    Service for enriching TDX asset records with detailed API data.

    Queries bronze layer for assets with incomplete data, fetches detailed
    information via get_asset, and creates new enriched bronze records.
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
        max_concurrent_enrichments: int = 10,
        api_rate_limit_delay: float = 0.1,
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

        logger.info(f"🔌 TDX asset enrichment service initialized:")
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
            WHERE entity_type = 'asset'
              AND source_system = 'tdx'
              AND status = 'completed'
              AND metadata->>'enrichment_type' = 'detailed_asset_data'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if result_df.empty or pd.isna(result_df.iloc[0]["last_run"]):
                return None

            return result_df.iloc[0]["last_run"]

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get last enrichment timestamp: {e}")
            return None

    def _get_assets_needing_enrichment(
        self, full_sync: bool = False, since_timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Query bronze layer for assets that need enrichment.

        An asset needs enrichment if:
        - Attributes is NULL or missing
        - Attachments is NULL or missing
        - Never been enriched (_enriched_at missing)
        - In incremental mode: only assets modified since last enrichment

        Args:
            full_sync: If True, get all assets. If False, only recently updated
            since_timestamp: Only get assets modified after this timestamp

        Returns:
            DataFrame with columns: asset_id, asset_name, asset_tag, external_id, raw_data
        """
        try:
            # Base query to find assets needing enrichment
            query = """
            WITH latest_assets AS (
                SELECT DISTINCT ON (raw_data->>'ID')
                    raw_data->>'ID' as asset_id,
                    raw_data->>'Name' as asset_name,
                    raw_data->>'Tag' as asset_tag,
                    external_id,
                    raw_data,
                    ingested_at,
                    raw_data->'_enriched_at' as enriched_at
                FROM bronze.raw_entities
                WHERE entity_type = 'asset'
                  AND source_system = 'tdx'
                  AND raw_data->>'ID' IS NOT NULL
                ORDER BY raw_data->>'ID', ingested_at DESC
            )
            SELECT
                asset_id,
                asset_name,
                asset_tag,
                external_id,
                ingested_at,
                enriched_at
            FROM latest_assets
            WHERE (
                -- Missing or incomplete enrichment data
                raw_data->'Attributes' IS NULL OR
                raw_data->'Attachments' IS NULL OR
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
                logger.info("✨ All assets have complete enrichment data")
                return result_df

            logger.info(f"🔍 Found {len(result_df)} assets needing enrichment")

            # Log sample assets
            sample_assets = result_df.head(5)
            logger.info("   Sample assets:")
            for _, asset in sample_assets.iterrows():
                logger.info(
                    f"     - {asset['asset_name']} (ID: {asset['asset_id']}, Tag: {asset['asset_tag']})"
                )

            return result_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to query assets needing enrichment: {e}")
            raise

    def _calculate_basic_content_hash(self, asset_data: Dict[str, Any]) -> str:
        """
        Calculate basic content hash (matching ingest script).

        Only includes fields from search_asset API to match 003_ingest_tdx_assets.py hash.

        Args:
            asset_data: Asset data (from any source)

        Returns:
            SHA-256 hash of basic fields only
        """
        # Match the hash calculation in 003_ingest_tdx_assets.py
        significant_fields = {
            "ID": asset_data.get("ID", ""),
            "Tag": asset_data.get("Tag", ""),
            "Name": asset_data.get("Name", ""),
            "SerialNumber": asset_data.get("SerialNumber", ""),
            "StatusID": asset_data.get("StatusID", ""),
            "StatusName": asset_data.get("StatusName", ""),
            "ProductModelID": asset_data.get("ProductModelID", ""),
            "ProductModelName": asset_data.get("ProductModelName", ""),
            "ManufacturerID": asset_data.get("ManufacturerID", ""),
            "ManufacturerName": asset_data.get("ManufacturerName", ""),
            "SupplierID": asset_data.get("SupplierID", ""),
            "SupplierName": asset_data.get("SupplierName", ""),
            "OwningDepartmentID": asset_data.get("OwningDepartmentID", ""),
            "OwningDepartmentName": asset_data.get("OwningDepartmentName", ""),
            "LocationID": asset_data.get("LocationID", ""),
            "LocationName": asset_data.get("LocationName", ""),
            "LocationRoomID": asset_data.get("LocationRoomID", ""),
            "LocationRoomName": asset_data.get("LocationRoomName", ""),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _calculate_enriched_content_hash(self, asset_data: Dict[str, Any]) -> str:
        """
        Calculate content hash for enriched asset data.

        Includes all significant fields including Attributes and Attachments.

        Args:
            asset_data: Complete asset data from get_asset

        Returns:
            SHA-256 hash of significant fields
        """
        # Include all significant fields from enriched data
        significant_fields = {
            "ID": asset_data.get("ID", ""),
            "Tag": asset_data.get("Tag", ""),
            "Name": asset_data.get("Name", ""),
            "SerialNumber": asset_data.get("SerialNumber", ""),
            "StatusID": asset_data.get("StatusID", ""),
            "StatusName": asset_data.get("StatusName", ""),
            "ProductModelID": asset_data.get("ProductModelID", ""),
            "ProductModelName": asset_data.get("ProductModelName", ""),
            "ManufacturerID": asset_data.get("ManufacturerID", ""),
            "ManufacturerName": asset_data.get("ManufacturerName", ""),
            "SupplierID": asset_data.get("SupplierID", ""),
            "SupplierName": asset_data.get("SupplierName", ""),
            "OwningDepartmentID": asset_data.get("OwningDepartmentID", ""),
            "OwningDepartmentName": asset_data.get("OwningDepartmentName", ""),
            "LocationID": asset_data.get("LocationID", ""),
            "LocationName": asset_data.get("LocationName", ""),
            "LocationRoomID": asset_data.get("LocationRoomID", ""),
            "LocationRoomName": asset_data.get("LocationRoomName", ""),
            # Enriched fields
            "Attributes": asset_data.get("Attributes", []),
            "Attachments": asset_data.get("Attachments", []),
            "ConfigurationItems": asset_data.get("ConfigurationItems", []),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    async def enrich_asset_record(
        self,
        asset_id: str,
        asset_name: str,
        asset_tag: str,
        external_id: str,
        ingestion_run_id: str,
        loop: asyncio.AbstractEventLoop,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Enrich a single asset record by fetching detailed data from TDX API.

        Args:
            asset_id: Asset's TDX ID
            asset_name: Asset's name (for logging)
            asset_tag: Asset's tag (for logging)
            external_id: Asset's external ID (for bronze record)
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, log what would be done but don't commit

        Returns:
            Dictionary with enrichment result
        """
        async with self.enrichment_semaphore:  # Limit concurrent API calls
            enrichment_result = {
                "asset_id": asset_id,
                "asset_name": asset_name,
                "asset_tag": asset_tag,
                "external_id": external_id,
                "success": False,
                "action": None,  # 'enriched', 'skipped', 'error'
                "error_message": None,
                "started_at": datetime.now(timezone.utc),
                "attributes_count": 0,
                "attachments_count": 0,
                "cis_count": 0,
            }

            try:
                logger.debug(
                    f"🔍 Fetching detailed data for: {asset_name} (ID: {asset_id})"
                )

                # Execute the synchronous API call in a thread pool
                def make_api_call():
                    time.sleep(self.api_rate_limit_delay)  # Rate limiting
                    return self.tdx_facade.assets.get_asset(int(asset_id))

                asset_data = await loop.run_in_executor(self.executor, make_api_call)

                if not asset_data:
                    enrichment_result["error_message"] = "API returned no data"
                    enrichment_result["action"] = "error"
                    logger.warning(
                        f"⚠️  No data returned for asset: {asset_name} (ID: {asset_id})"
                    )
                    return enrichment_result

                # Extract enrichment metadata
                attributes = asset_data.get("Attributes") or []
                attachments = asset_data.get("Attachments") or []
                cis = asset_data.get("ConfigurationItems") or []

                enrichment_result["attributes_count"] = (
                    len(attributes) if isinstance(attributes, list) else 0
                )
                enrichment_result["attachments_count"] = (
                    len(attachments) if isinstance(attachments, list) else 0
                )
                enrichment_result["cis_count"] = (
                    len(cis) if isinstance(cis, list) else 0
                )

                # Add enrichment metadata
                enhanced_asset_data = asset_data.copy()
                enhanced_asset_data["_enrichment_method"] = "get_asset"
                enhanced_asset_data["_ingestion_source"] = "get_asset"
                enhanced_asset_data["_enriched_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                enhanced_asset_data["_content_hash_enriched"] = (
                    self._calculate_enriched_content_hash(asset_data)
                )

                # Preserve basic hash for ingest compatibility
                enhanced_asset_data["_content_hash_basic"] = (
                    self._calculate_basic_content_hash(asset_data)
                )

                if dry_run:
                    enrichment_result["success"] = True
                    enrichment_result["action"] = "enriched"
                    enrichment_result["raw_id"] = str(uuid.uuid4())  # Mock ID
                    logger.info(
                        f"[DRY RUN] Would enrich asset: {asset_name} - "
                        f"Attrs: {enrichment_result['attributes_count']}, "
                        f"Attachments: {enrichment_result['attachments_count']}, "
                        f"CIs: {enrichment_result['cis_count']}"
                    )
                    return enrichment_result

                # Insert enriched record into bronze layer
                def perform_ingestion():
                    return self.db_adapter.insert_raw_entity(
                        entity_type="asset",
                        source_system="tdx",
                        external_id=external_id,
                        raw_data=enhanced_asset_data,
                        ingestion_run_id=ingestion_run_id,
                    )

                raw_id = await loop.run_in_executor(self.executor, perform_ingestion)

                enrichment_result["raw_id"] = raw_id
                enrichment_result["success"] = True
                enrichment_result["action"] = "enriched"

                logger.debug(
                    f"✅ Enriched asset: {asset_name} - "
                    f"Attrs: {enrichment_result['attributes_count']}, "
                    f"Attachments: {enrichment_result['attachments_count']}"
                )

                return enrichment_result

            except Exception as e:
                error_msg = f"Failed to enrich asset {asset_id}: {str(e)}"
                logger.error(f"❌ {error_msg}")

                enrichment_result["error_message"] = error_msg
                enrichment_result["action"] = "error"
                return enrichment_result

    async def process_assets_concurrently(
        self,
        assets_df: pd.DataFrame,
        ingestion_run_id: str,
        loop: asyncio.AbstractEventLoop,
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Process multiple assets concurrently with enrichment.

        Args:
            assets_df: DataFrame of assets needing enrichment
            ingestion_run_id: UUID of the current enrichment run
            loop: Event loop for async execution
            dry_run: If True, preview changes without committing

        Returns:
            List of enrichment results for all assets
        """
        if assets_df.empty:
            logger.warning("⚠️  No assets to process")
            return []

        total_assets = len(assets_df)
        logger.info(f"🔄 Starting concurrent enrichment of {total_assets} assets...")

        # Create enrichment tasks for all assets
        enrichment_tasks = [
            self.enrich_asset_record(
                asset_id=row["asset_id"],
                asset_name=row["asset_name"],
                asset_tag=row["asset_tag"],
                external_id=row["external_id"],
                ingestion_run_id=ingestion_run_id,
                loop=loop,
                dry_run=dry_run,
            )
            for _, row in assets_df.iterrows()
        ]

        # Execute all enrichment tasks concurrently with progress tracking
        enrichment_results = []
        completed = 0

        for future in asyncio.as_completed(enrichment_tasks):
            result = await future
            enrichment_results.append(result)
            completed += 1

            # Log progress every 50 assets
            if completed % 50 == 0:
                enriched = sum(
                    1 for r in enrichment_results if r.get("action") == "enriched"
                )
                errors = sum(
                    1 for r in enrichment_results if r.get("action") == "error"
                )
                logger.info(
                    f"📈 Progress: {completed}/{total_assets} assets processed "
                    f"({enriched} enriched, {errors} errors)"
                )

        # Handle any exceptions that occurred during enrichment
        processed_results = []
        for result in enrichment_results:
            if isinstance(result, Exception):
                error_result = {
                    "asset_id": "unknown",
                    "asset_name": "Unknown Asset",
                    "success": False,
                    "action": "error",
                    "error_message": f"Async enrichment exception: {str(result)}",
                }
                processed_results.append(error_result)
                logger.error(f"❌ Async enrichment exception: {result}")
            else:
                processed_results.append(result)

        # Count action types
        enriched = sum(1 for r in processed_results if r.get("action") == "enriched")
        errors = sum(1 for r in processed_results if r.get("action") == "error")

        # Calculate enrichment statistics
        total_attrs = sum(r.get("attributes_count", 0) for r in processed_results)
        total_attachments = sum(
            r.get("attachments_count", 0) for r in processed_results
        )
        total_cis = sum(r.get("cis_count", 0) for r in processed_results)

        logger.info(f"✅ Enrichment complete - {enriched} enriched, {errors} errors")
        logger.info(f"   Total Attributes: {total_attrs}")
        logger.info(f"   Total Attachments: {total_attachments}")
        logger.info(f"   Total CIs: {total_cis}")

        return processed_results

    def create_enrichment_run(
        self,
        total_assets: int,
        full_sync: bool = False,
        incremental_since: Optional[datetime] = None,
    ) -> str:
        """
        Create an enrichment run record.

        Args:
            total_assets: Total number of assets to enrich
            full_sync: Whether this is a full sync or incremental
            incremental_since: Timestamp of last successful run (if incremental)

        Returns:
            UUID string of the created run
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "enrichment_type": "detailed_asset_data",
                "total_assets": total_assets,
                "max_concurrent_enrichments": self.max_concurrent_enrichments,
                "api_rate_limit_delay": self.api_rate_limit_delay,
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
                "enrichment_fields": [
                    "Attributes",
                    "Attachments",
                    "ConfigurationItems",
                ],
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
                        "entity_type": "asset",
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(
                f"📝 Created enrichment run {run_id} ({mode}) - {total_assets} assets"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create enrichment run: {e}")
            raise

    def complete_enrichment_run(
        self,
        run_id: str,
        total_assets_processed: int,
        total_assets_enriched: int,
        total_errors: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark the enrichment run as completed with statistics.

        Args:
            run_id: UUID of the enrichment run
            total_assets_processed: Total assets processed
            total_assets_enriched: Assets actually enriched
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
                        "records_processed": total_assets_processed,
                        "records_created": total_assets_enriched,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed enrichment run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete enrichment run: {e}")

    async def run_async_asset_enrichment(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Run the complete async asset enrichment process.

        Args:
            full_sync: If True, enrich all assets. If False, use incremental mode
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with comprehensive enrichment statistics
        """
        enrichment_stats = {
            "started_at": datetime.now(timezone.utc),
            "total_assets_needing_enrichment": 0,
            "total_assets_enriched": 0,
            "total_assets_failed": 0,
            "total_attributes": 0,
            "total_attachments": 0,
            "total_cis": 0,
            "errors": [],
            "full_sync": full_sync,
            "dry_run": dry_run,
        }

        try:
            logger.info("🚀 Starting async asset enrichment process...")

            # Determine processing mode
            last_timestamp = (
                None if full_sync else self._get_last_enrichment_timestamp()
            )

            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Enriching ALL assets")
            elif last_timestamp:
                logger.info(
                    f"⚡ Incremental mode: Enriching assets modified since {last_timestamp}"
                )
            else:
                logger.info("🆕 First run: Enriching ALL assets needing data")

            # Step 1: Get assets needing enrichment
            assets_df = self._get_assets_needing_enrichment(
                full_sync=full_sync, since_timestamp=last_timestamp
            )

            if assets_df.empty:
                logger.info("✨ All assets have complete enrichment data")
                return enrichment_stats

            enrichment_stats["total_assets_needing_enrichment"] = len(assets_df)

            # Step 2: Create enrichment run for tracking
            run_id = self.create_enrichment_run(
                len(assets_df), full_sync=full_sync, incremental_since=last_timestamp
            )
            enrichment_stats["run_id"] = run_id

            # Step 3: Process all assets concurrently
            loop = asyncio.get_event_loop()

            enrichment_results = await self.process_assets_concurrently(
                assets_df=assets_df, ingestion_run_id=run_id, loop=loop, dry_run=dry_run
            )

            # Step 4: Calculate statistics
            for result in enrichment_results:
                if result.get("action") == "enriched":
                    enrichment_stats["total_assets_enriched"] += 1
                    enrichment_stats["total_attributes"] += result.get(
                        "attributes_count", 0
                    )
                    enrichment_stats["total_attachments"] += result.get(
                        "attachments_count", 0
                    )
                    enrichment_stats["total_cis"] += result.get("cis_count", 0)
                elif result.get("action") == "error":
                    enrichment_stats["total_assets_failed"] += 1
                    if result.get("error_message"):
                        enrichment_stats["errors"].append(result["error_message"])

            # Step 5: Complete the enrichment run
            error_summary = None
            if enrichment_stats["errors"]:
                error_summary = f"{len(enrichment_stats['errors'])} errors occurred during enrichment"

            self.complete_enrichment_run(
                run_id=run_id,
                total_assets_processed=len(assets_df),
                total_assets_enriched=enrichment_stats["total_assets_enriched"],
                total_errors=enrichment_stats["total_assets_failed"],
                error_message=error_summary,
            )

            enrichment_stats["completed_at"] = datetime.now(timezone.utc)
            total_duration = (
                enrichment_stats["completed_at"] - enrichment_stats["started_at"]
            ).total_seconds()

            # Calculate averages
            avg_attrs_per_asset = (
                enrichment_stats["total_attributes"]
                / enrichment_stats["total_assets_enriched"]
                if enrichment_stats["total_assets_enriched"] > 0
                else 0
            )

            avg_attachments_per_asset = (
                enrichment_stats["total_attachments"]
                / enrichment_stats["total_assets_enriched"]
                if enrichment_stats["total_assets_enriched"] > 0
                else 0
            )

            # Log comprehensive results
            logger.info("=" * 80)
            logger.info("🎉 ASSET ENRICHMENT COMPLETED")
            logger.info("=" * 80)
            logger.info(f"📊 Results Summary:")
            logger.info(
                f"   Mode:                   {'FULL SYNC' if full_sync else 'INCREMENTAL'}"
            )
            logger.info(f"   Dry Run:                {dry_run}")
            logger.info(f"")
            logger.info(
                f"   Assets Needing Enrich:  {enrichment_stats['total_assets_needing_enrichment']:>6,}"
            )
            logger.info(
                f"   ├─ Enriched:            {enrichment_stats['total_assets_enriched']:>6,}"
            )
            logger.info(
                f"   └─ Failed:              {enrichment_stats['total_assets_failed']:>6,}"
            )
            logger.info(f"")
            logger.info(f"   Enrichment Data:")
            logger.info(
                f"   ├─ Attributes:          {enrichment_stats['total_attributes']:>6,} (avg: {avg_attrs_per_asset:.1f}/asset)"
            )
            logger.info(
                f"   ├─ Attachments:         {enrichment_stats['total_attachments']:>6,} (avg: {avg_attachments_per_asset:.1f}/asset)"
            )
            logger.info(
                f"   └─ CIs:                 {enrichment_stats['total_cis']:>6,}"
            )
            logger.info(f"")
            logger.info(f"   Duration:               {total_duration:.2f}s")
            logger.info(
                f"   Errors:                 {len(enrichment_stats['errors']):>6,}"
            )
            logger.info("=" * 80)

            return enrichment_stats

        except Exception as e:
            error_msg = f"Async asset enrichment failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            if "run_id" in enrichment_stats:
                self.complete_enrichment_run(
                    run_id=enrichment_stats["run_id"],
                    total_assets_processed=enrichment_stats[
                        "total_assets_needing_enrichment"
                    ],
                    total_assets_enriched=enrichment_stats["total_assets_enriched"],
                    total_errors=enrichment_stats["total_assets_failed"],
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
        logger.info("🔌 Asset enrichment service closed")


async def main():
    """
    Main async function to run asset enrichment from command line.
    """
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description="Enrich TDX asset records with detailed API data (Attributes, Attachments, CIs)"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Enrich all assets (ignore last enrichment timestamp)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--max-concurrent",
            type=int,
            default=10,
            help="Maximum concurrent API calls (default: 10)",
        )
        parser.add_argument(
            "--api-delay",
            type=float,
            default=0.1,
            help="API rate limit delay in seconds (default: 0.1)",
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
        enrichment_service = TDXAssetEnrichmentService(
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
        print("🚀 STARTING TDX ASSET ENRICHMENT")
        print("=" * 80)
        print(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        print(f"Dry Run:             {args.dry_run}")
        print(f"Max Concurrent:      {args.max_concurrent} API calls")
        print(f"API Delay:           {args.api_delay}s")
        print("=" * 80)
        print()

        results = await enrichment_service.run_async_asset_enrichment(
            full_sync=args.full_sync, dry_run=args.dry_run
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
        print(f"Assets Needing Data: {results['total_assets_needing_enrichment']:>6,}")
        print(f"├─ Enriched:         {results['total_assets_enriched']:>6,}")
        print(f"└─ Failed:           {results['total_assets_failed']:>6,}")
        print(f"")
        print(f"Data Collected:")
        print(f"├─ Attributes:       {results['total_attributes']:>6,}")
        print(f"├─ Attachments:      {results['total_attachments']:>6,}")
        print(f"└─ CIs:              {results['total_cis']:>6,}")
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
        logger.error(f"❌ Asset enrichment failed: {e}", exc_info=True)
        print(f"\n❌ Enrichment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
