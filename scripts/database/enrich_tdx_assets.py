#!/usr/bin/env python3
"""
Progressive Bronze Layer Enrichment Service for TDX Assets

This service implements a two-stage bronze ingestion pattern:
1. Rapid ingestion using search_asset() for change detection
2. Detailed enrichment using get_asset(ID) for complete data including Attributes and Attachments

This approach optimizes API usage while ensuring comprehensive data capture.
Since get_asset() doesn't appear to be rate limited, we use concurrent processing with batching.
"""

import asyncio
import concurrent.futures
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# Core Python imports for PostgreSQL operations
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Add your LSATS project to Python path (adjust path as needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Set up logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/progressive_bronze_enrichment_assets.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ProgressiveBronzeEnrichmentService:
    """
    Service that implements progressive bronze layer enrichment for TeamDynamix assets.

    This service handles the detailed enrichment using individual get_asset(ID) calls
    to complete the data with Attributes and Attachments that are not included in search_asset().

    Uses concurrent processing with batching for high-performance enrichment.
    """

    def __init__(
        self,
        database_url: str,
        tdx_base_url: str,
        tdx_api_token: str,
        tdx_app_id: str,
        max_workers: int = 10,
        batch_size: int = 100,
    ):
        """
        Initialize the progressive enrichment service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
            max_workers: Number of concurrent workers for API calls
            batch_size: Number of records to process in each batch
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url, app_id=tdx_app_id, api_token=tdx_api_token
        )

        # Concurrency settings - can be tuned if rate limiting is discovered
        self.max_workers = max_workers
        self.batch_size = batch_size

        logger.info(
            f"Progressive bronze enrichment service initialized "
            f"(max_workers: {max_workers}, batch_size: {batch_size})"
        )

    def get_assets_needing_enrichment(
        self, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Find bronze layer records that need detailed enrichment.

        This queries for records where is_enriched is False and returns
        the information needed to enrich them.

        Args:
            limit: Maximum number of records to return for processing

        Returns:
            DataFrame with asset records needing enrichment
        """
        try:
            limit_clause = f"LIMIT {limit}" if limit else ""

            query = text(f"""
                SELECT
                    raw_id,
                    external_id,
                    raw_data,
                    ingestion_metadata,
                    ingested_at
                FROM bronze.raw_entities
                WHERE entity_type = 'asset'
                  AND source_system = 'tdx'
                  AND (ingestion_metadata->>'is_enriched')::boolean = false
                ORDER BY ingested_at DESC
                {limit_clause}
            """)

            df = self.db_adapter.query_to_dataframe(query.text)

            if df.empty:
                logger.info("No assets needing enrichment found")
            else:
                logger.info(f"Found {len(df)} assets needing enrichment")

            return df

        except SQLAlchemyError as e:
            logger.error(f"Failed to query assets needing enrichment: {e}")
            return pd.DataFrame()

    def enrich_asset_record(
        self, raw_id: str, external_id: str
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Enrich a single asset record by fetching complete data from TeamDynamix.

        This calls get_asset(ID) to get the full record including Attributes and Attachments.

        Args:
            raw_id: The bronze layer raw_id (UUID)
            external_id: The TeamDynamix asset ID

        Returns:
            Tuple of (success: bool, enriched_data: Optional[Dict])
        """
        try:
            # Fetch complete asset data from TeamDynamix
            asset_id = int(external_id)
            enriched_data = self.tdx_facade.assets.get_asset(asset_id)

            if not enriched_data:
                logger.warning(f"No data returned for asset ID {external_id}")
                return (False, None)

            return (True, enriched_data)

        except ValueError as e:
            logger.error(f"Invalid asset ID format '{external_id}': {e}")
            return (False, None)
        except Exception as e:
            logger.error(f"Failed to enrich asset {external_id}: {e}")
            return (False, None)

    def update_original_records_metadata(
        self, raw_id: str, enriched_data: Dict[str, Any], enrichment_run_id: str
    ) -> bool:
        """
        Update the original bronze record with enriched data and mark as enriched.

        Args:
            raw_id: The bronze layer raw_id (UUID)
            enriched_data: The complete enriched data from get_asset()
            enrichment_run_id: UUID of the enrichment run

        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            # Prepare the enriched data with metadata embedded
            # Similar to the accounts enrichment pattern
            enriched_raw_data = enriched_data.copy()
            enriched_raw_data["_enrichment_timestamp"] = datetime.now(
                timezone.utc
            ).isoformat()
            enriched_raw_data["_enrichment_run_id"] = enrichment_run_id

            # Update the raw_data and mark as enriched
            # Using the same pattern as enrich_tdx_accounts.py
            update_query = text("""
                UPDATE bronze.raw_entities
                SET raw_data = :enriched_raw_data,
                    ingestion_metadata = jsonb_set(
                        COALESCE(ingestion_metadata, '{}'::jsonb),
                        '{is_enriched}',
                        'true'::jsonb
                    )
                WHERE raw_id = :raw_id
            """)

            with self.db_adapter.engine.connect() as conn:
                conn.execute(
                    update_query,
                    {
                        "raw_id": raw_id,
                        "enriched_raw_data": json.dumps(enriched_raw_data),
                    },
                )
                conn.commit()

            return True

        except SQLAlchemyError as e:
            logger.error(f"Failed to update bronze record {raw_id}: {e}")
            return False

    def create_enrichment_run(self) -> str:
        """
        Create a new enrichment run record in the meta schema.

        Returns:
            UUID of the created enrichment run
        """
        try:
            run_id = str(uuid.uuid4())

            insert_query = text("""
                INSERT INTO meta.ingestion_runs
                (run_id, source_system, entity_type, started_at, status, metadata)
                VALUES (:run_id, :source_system, :entity_type, :started_at, :status, :metadata)
            """)

            with self.db_adapter.engine.connect() as conn:
                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "source_system": "tdx",
                        "entity_type": "asset",
                        "started_at": datetime.now(timezone.utc),
                        "status": "running",
                        "metadata": json.dumps({"run_type": "enrichment"}),
                    },
                )
                conn.commit()

            logger.info(f"Created enrichment run: {run_id}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create enrichment run: {e}")
            raise

    def complete_enrichment_run(
        self,
        run_id: str,
        records_processed: int,
        records_enriched: int,
        records_failed: int,
    ) -> None:
        """
        Mark an enrichment run as completed and record statistics.

        Args:
            run_id: UUID of the enrichment run
            records_processed: Total number of records processed
            records_enriched: Number of records successfully enriched
            records_failed: Number of records that failed enrichment
        """
        try:
            update_query = text("""
                UPDATE meta.ingestion_runs
                SET status = 'completed',
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_enriched,
                    records_updated = :records_failed
                WHERE run_id = :run_id
            """)

            with self.db_adapter.engine.connect() as conn:
                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "records_processed": records_processed,
                        "records_enriched": records_enriched,
                        "records_failed": records_failed,
                    },
                )
                conn.commit()

            logger.info(
                f"Completed enrichment run {run_id}: "
                f"{records_processed} processed, {records_enriched} enriched, {records_failed} failed"
            )

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete enrichment run: {e}")
            raise

    def run_progressive_enrichment(
        self, batch_limit: Optional[int] = None, max_total_records: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Run the progressive enrichment process using concurrent workers.

        This method:
        1. Identifies assets needing enrichment
        2. Processes them in batches using concurrent workers
        3. Updates original records with enriched data
        4. Tracks progress in meta schema

        Args:
            batch_limit: Number of records to process in each batch (uses self.batch_size if None)
            max_total_records: Maximum total records to process (None for unlimited)

        Returns:
            Dictionary with enrichment statistics
        """
        stats = {"processed": 0, "enriched": 0, "failed": 0, "skipped": 0}

        batch_limit = batch_limit or self.batch_size

        logger.info("Starting progressive asset enrichment")
        enrichment_run_id = self.create_enrichment_run()

        try:
            # Get assets needing enrichment
            assets_df = self.get_assets_needing_enrichment(limit=max_total_records)

            if assets_df.empty:
                logger.info("No assets need enrichment")
                self.complete_enrichment_run(enrichment_run_id, 0, 0, 0)
                return stats

            total_assets = len(assets_df)
            logger.info(f"Found {total_assets} assets to enrich")

            # Process in batches
            for batch_start in range(0, total_assets, batch_limit):
                batch_end = min(batch_start + batch_limit, total_assets)
                batch_df = assets_df.iloc[batch_start:batch_end]

                logger.info(
                    f"Processing batch {batch_start // batch_limit + 1}: "
                    f"records {batch_start + 1}-{batch_end} of {total_assets}"
                )

                # Process batch concurrently
                batch_stats = self._process_batch_concurrent(
                    batch_df=batch_df, enrichment_run_id=enrichment_run_id
                )

                # Update overall stats
                stats["processed"] += batch_stats["processed"]
                stats["enriched"] += batch_stats["enriched"]
                stats["failed"] += batch_stats["failed"]
                stats["skipped"] += batch_stats["skipped"]

                # Log batch progress
                logger.info(
                    f"Batch completed: "
                    f"{batch_stats['enriched']} enriched, "
                    f"{batch_stats['failed']} failed"
                )

            # Complete the enrichment run
            self.complete_enrichment_run(
                run_id=enrichment_run_id,
                records_processed=stats["processed"],
                records_enriched=stats["enriched"],
                records_failed=stats["failed"],
            )

            logger.info(f"Asset enrichment completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Asset enrichment failed: {e}")
            # Mark run as failed
            try:
                with self.db_adapter.engine.connect() as conn:
                    conn.execute(
                        text("""
                        UPDATE meta.ingestion_runs
                        SET status = 'failed', completed_at = :completed_at
                        WHERE run_id = :run_id
                    """),
                        {
                            "run_id": enrichment_run_id,
                            "completed_at": datetime.now(timezone.utc),
                        },
                    )
                    conn.commit()
            except:
                pass
            raise

    def _process_batch_concurrent(
        self, batch_df: pd.DataFrame, enrichment_run_id: str
    ) -> Dict[str, int]:
        """
        Process a batch of assets concurrently using ThreadPoolExecutor.

        Args:
            batch_df: DataFrame containing the batch of assets to process
            enrichment_run_id: UUID of the enrichment run

        Returns:
            Dictionary with batch statistics
        """
        batch_stats = {"processed": 0, "enriched": 0, "failed": 0, "skipped": 0}

        # Use ThreadPoolExecutor for concurrent API calls
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Submit all enrichment tasks
            future_to_asset = {}
            for _, row in batch_df.iterrows():
                future = executor.submit(
                    self.enrich_asset_record, row["raw_id"], row["external_id"]
                )
                future_to_asset[future] = (row["raw_id"], row["external_id"])

            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_asset):
                raw_id, external_id = future_to_asset[future]
                batch_stats["processed"] += 1

                try:
                    success, enriched_data = future.result()

                    if success and enriched_data:
                        # Update the bronze record with enriched data
                        update_success = self.update_original_records_metadata(
                            raw_id=raw_id,
                            enriched_data=enriched_data,
                            enrichment_run_id=enrichment_run_id,
                        )

                        if update_success:
                            batch_stats["enriched"] += 1
                        else:
                            batch_stats["failed"] += 1
                            logger.warning(
                                f"Failed to update bronze record for asset {external_id}"
                            )
                    else:
                        batch_stats["failed"] += 1

                except Exception as e:
                    logger.error(f"Exception processing asset {external_id}: {e}")
                    batch_stats["failed"] += 1

        return batch_stats

    def get_enrichment_status_summary(self) -> pd.DataFrame:
        """
        Get a summary of enrichment status across all assets.

        Returns:
            DataFrame with enrichment statistics
        """
        try:
            query = text("""
                SELECT
                    COUNT(*) as total_assets,
                    SUM(CASE WHEN (ingestion_metadata->>'is_enriched')::boolean = true
                        THEN 1 ELSE 0 END) as enriched_count,
                    SUM(CASE WHEN (ingestion_metadata->>'is_enriched')::boolean = false
                        THEN 1 ELSE 0 END) as pending_enrichment,
                    MIN(ingested_at) as oldest_record,
                    MAX(ingested_at) as newest_record
                FROM bronze.raw_entities
                WHERE entity_type = 'asset'
                  AND source_system = 'tdx'
            """)

            return self.db_adapter.query_to_dataframe(query.text)

        except SQLAlchemyError as e:
            logger.error(f"Failed to get enrichment status: {e}")
            return pd.DataFrame()

    def close(self) -> None:
        """
        Close database connections and cleanup resources.
        """
        self.db_adapter.close()
        logger.info("Progressive bronze enrichment service closed")


def main():
    """
    Main entry point for the asset enrichment script.
    """
    # Load environment variables
    load_dotenv()

    # Get configuration from environment
    database_url = os.getenv("DATABASE_URL")
    tdx_base_url = os.getenv("TDX_BASE_URL")
    tdx_api_token = os.getenv("TDX_API_TOKEN")
    tdx_app_id = os.getenv("TDX_APP_ID", "48")  # Default to 48 for LSA-TS Assets/CIs

    # Validate required environment variables
    if not all([database_url, tdx_base_url, tdx_api_token]):
        logger.error(
            "Missing required environment variables: DATABASE_URL, TDX_BASE_URL, TDX_API_TOKEN"
        )
        sys.exit(1)

    # Concurrency settings - can be tuned based on performance and rate limiting
    max_workers = int(os.getenv("ENRICHMENT_MAX_WORKERS", "10"))
    batch_size = int(os.getenv("ENRICHMENT_BATCH_SIZE", "100"))
    max_total_records = os.getenv("ENRICHMENT_MAX_RECORDS")
    max_total_records = int(max_total_records) if max_total_records else None

    try:
        # Initialize the enrichment service
        service = ProgressiveBronzeEnrichmentService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
            max_workers=max_workers,
            batch_size=batch_size,
        )

        logger.info("=" * 80)
        logger.info("Starting TDX Asset Enrichment")
        logger.info("=" * 80)

        # Show current enrichment status
        status_df = service.get_enrichment_status_summary()
        if not status_df.empty:
            logger.info("Current Enrichment Status:")
            logger.info(f"  Total Assets: {status_df.iloc[0]['total_assets']}")
            logger.info(f"  Enriched: {status_df.iloc[0]['enriched_count']}")
            logger.info(
                f"  Pending Enrichment: {status_df.iloc[0]['pending_enrichment']}"
            )
            logger.info("-" * 80)

        # Run the enrichment
        stats = service.run_progressive_enrichment(max_total_records=max_total_records)

        logger.info("=" * 80)
        logger.info("Enrichment Summary:")
        logger.info(f"  Total Processed: {stats['processed']}")
        logger.info(f"  Successfully Enriched: {stats['enriched']}")
        logger.info(f"  Failed: {stats['failed']}")
        logger.info(f"  Skipped: {stats['skipped']}")
        logger.info("=" * 80)

        # Show updated enrichment status
        status_df = service.get_enrichment_status_summary()
        if not status_df.empty:
            logger.info("Updated Enrichment Status:")
            logger.info(f"  Total Assets: {status_df.iloc[0]['total_assets']}")
            logger.info(f"  Enriched: {status_df.iloc[0]['enriched_count']}")
            logger.info(
                f"  Pending Enrichment: {status_df.iloc[0]['pending_enrichment']}"
            )

        # Close the service
        service.close()

        sys.exit(0)

    except Exception as e:
        logger.error(f"Asset enrichment failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
