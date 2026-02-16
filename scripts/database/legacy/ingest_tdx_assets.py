#!/usr/bin/env python3
"""
Timestamp-Based TDX Asset Ingestion with Content Verification

This script ingests TeamDynamix assets using the search_asset() API with status filters.
It uses ModifiedDate for efficient change detection while optionally validating with
content hashing for data quality assurance.
"""

import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import dateutil.parser

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
        logging.FileHandler("logs/asset_ingestion.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class TimestampBasedAssetIngestionService:
    """
    Asset ingestion service that uses TeamDynamix ModifiedDate for efficient change detection.

    This approach leverages TDX's built-in change tracking to minimize processing overhead
    while optionally providing content verification for data quality assurance.

    Key Benefits:
    - Much faster processing by filtering on timestamps
    - Relies on TeamDynamix's reliable change tracking
    - Optional content verification for quality assurance
    - Supports incremental ingestion for large datasets
    """

    def __init__(
        self,
        database_url: str,
        tdx_base_url: str,
        tdx_api_token: str,
        tdx_app_id: str,
        enable_content_verification: bool = False,
    ):
        """
        Initialize the timestamp-based ingestion service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
            enable_content_verification: Whether to calculate content hashes for verification
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url, app_id=tdx_app_id, api_token=tdx_api_token
        )

        # Feature flag for optional content verification
        self.enable_content_verification = enable_content_verification

        logger.info(
            f"Timestamp-based asset ingestion service initialized "
            f"(content verification: {'enabled' if enable_content_verification else 'disabled'})"
        )

    def _parse_tdx_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        TDX uses ISO format with Z suffix, like: "2024-07-23T00:09:00Z"

        Args:
            timestamp_str: Timestamp string from TeamDynamix

        Returns:
            datetime object with timezone info
        """
        try:
            # Use dateutil parser to handle various ISO formats
            dt = dateutil.parser.isoparse(timestamp_str)

            # Ensure we have timezone info (assume UTC if not present)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            # Return a very old date as fallback to ensure record gets processed
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 hash of the asset's content for change detection.

        This is optional and used for content verification when timestamp-based
        detection needs additional validation.

        Args:
            raw_data: The raw asset data from TeamDynamix

        Returns:
            SHA-256 hash string
        """
        # Convert to canonical JSON string (sorted keys for consistency)
        canonical_json = json.dumps(raw_data, sort_keys=True)
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    def _get_last_ingestion_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful asset ingestion.

        This is used as the starting point for incremental ingestion,
        fetching only assets modified since the last run.

        Returns:
            datetime of last successful ingestion, or None for first run
        """
        try:
            query = text("""
                SELECT MAX(completed_at) as last_completed
                FROM meta.ingestion_runs
                WHERE source_system = 'tdx'
                  AND entity_type = 'asset'
                  AND status = 'completed'
                  AND completed_at IS NOT NULL
            """)

            with self.db_adapter.engine.connect() as conn:
                result = conn.execute(query)
                row = result.fetchone()

                if row and row[0]:
                    logger.info(f"Last asset ingestion completed at: {row[0]}")
                    return row[0]
                else:
                    logger.info(
                        "No previous asset ingestion found - performing full ingestion"
                    )
                    return None

        except SQLAlchemyError as e:
            logger.error(f"Failed to get last ingestion timestamp: {e}")
            return None

    def _get_assets_modified_since(
        self, since_timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all assets from TeamDynamix with specified status IDs that were modified since the given timestamp.

        Uses the search_asset() API with status filters as specified:
        statusIDs: [38, 39, 40, 41, 46, 59, 141, 201, 202]

        Args:
            since_timestamp: Only return assets modified after this time (None for all assets)

        Returns:
            List of asset records from TeamDynamix
        """
        try:
            logger.info(f"Fetching assets from TeamDynamix...")

            # Call search_asset with the specified status IDs
            search_data = {"statusIDs": [38, 39, 40, 41, 46, 59, 141, 201, 202]}

            assets = self.tdx_facade.assets.search_asset(data=search_data)

            logger.info(f"Retrieved {len(assets)} total assets from TeamDynamix")

            # Filter by ModifiedDate if timestamp provided
            if since_timestamp:
                filtered_assets = []
                for asset in assets:
                    modified_date_str = asset.get("ModifiedDate")
                    if modified_date_str:
                        modified_date = self._parse_tdx_timestamp(modified_date_str)
                        if modified_date > since_timestamp:
                            filtered_assets.append(asset)

                logger.info(
                    f"Filtered to {len(filtered_assets)} assets modified since {since_timestamp}"
                )
                return filtered_assets

            return assets

        except Exception as e:
            logger.error(f"Failed to fetch assets from TeamDynamix: {e}")
            raise

    def create_ingestion_run(self, run_type: str = "incremental") -> str:
        """
        Create a new ingestion run record in the meta schema.

        This tracks the ingestion process and provides audit trail.

        Args:
            run_type: Type of ingestion run ('full' or 'incremental')

        Returns:
            UUID of the created ingestion run
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
                        "metadata": json.dumps({"run_type": run_type}),
                    },
                )
                conn.commit()

            logger.info(f"Created ingestion run: {run_id} ({run_type})")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        records_processed: int,
        records_inserted: int,
        records_updated: int,
    ) -> None:
        """
        Mark an ingestion run as completed and record statistics.

        Args:
            run_id: UUID of the ingestion run
            records_processed: Total number of records processed
            records_inserted: Number of new records inserted
            records_updated: Number of existing records updated
        """
        try:
            update_query = text("""
                UPDATE meta.ingestion_runs
                SET status = 'completed',
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_inserted,
                    records_updated = :records_updated
                WHERE run_id = :run_id
            """)

            with self.db_adapter.engine.connect() as conn:
                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "records_processed": records_processed,
                        "records_inserted": records_inserted,
                        "records_updated": records_updated,
                    },
                )
                conn.commit()

            logger.info(
                f"Completed ingestion run {run_id}: "
                f"{records_processed} processed, {records_inserted} inserted, {records_updated} updated"
            )

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")
            raise

    def ingest_assets_timestamp_based(
        self, force_full_ingestion: bool = False
    ) -> Dict[str, int]:
        """
        Ingest assets using timestamp-based change detection.

        This method:
        1. Determines the last successful ingestion timestamp
        2. Fetches only assets modified since that time from TeamDynamix
        3. Inserts new records or updates existing ones based on ModifiedDate
        4. Tracks the ingestion run in meta schema

        Args:
            force_full_ingestion: If True, ignore timestamp and process all assets

        Returns:
            Dictionary with ingestion statistics
        """
        stats = {"processed": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Determine if this is a full or incremental run
        if force_full_ingestion:
            since_timestamp = None
            run_type = "full"
            logger.info("Starting FULL asset ingestion (forced)")
        else:
            since_timestamp = self._get_last_ingestion_timestamp()
            run_type = "incremental" if since_timestamp else "full"
            logger.info(f"Starting {run_type.upper()} asset ingestion")

        # Create ingestion run record
        run_id = self.create_ingestion_run(run_type=run_type)

        try:
            # Fetch assets from TeamDynamix
            assets = self._get_assets_modified_since(since_timestamp)

            if not assets:
                logger.info("No assets to process")
                self.complete_ingestion_run(run_id, 0, 0, 0)
                return stats

            # Process each asset
            for asset in assets:
                try:
                    stats["processed"] += 1

                    # Use ID field as external_id
                    external_id = str(asset.get("ID", ""))
                    if not external_id:
                        logger.warning(f"Asset missing ID field, skipping: {asset}")
                        stats["skipped"] += 1
                        continue

                    modified_date = self._parse_tdx_timestamp(
                        asset.get("ModifiedDate", "")
                    )

                    # Check if this asset already exists in bronze layer
                    check_query = text("""
                        SELECT raw_id, ingestion_metadata->>'modified_date' as stored_modified_date
                        FROM bronze.raw_entities
                        WHERE entity_type = 'asset'
                          AND source_system = 'tdx'
                          AND external_id = :external_id
                        ORDER BY ingested_at DESC
                        LIMIT 1
                    """)

                    with self.db_adapter.engine.connect() as conn:
                        result = conn.execute(check_query, {"external_id": external_id})
                        existing = result.fetchone()

                        if existing:
                            # Compare ModifiedDate to determine if update is needed
                            stored_modified_str = existing[1]
                            if stored_modified_str:
                                stored_modified = self._parse_tdx_timestamp(
                                    stored_modified_str
                                )

                                if modified_date <= stored_modified:
                                    # No changes, skip this asset
                                    stats["skipped"] += 1
                                    continue

                        # Calculate content hash if verification enabled
                        content_hash = None
                        if self.enable_content_verification:
                            content_hash = self._calculate_content_hash(asset)

                        # Prepare ingestion metadata
                        ingestion_metadata = {
                            "modified_date": asset.get("ModifiedDate"),
                            "ingestion_method": "timestamp_based",
                            "is_enriched": False,  # Will be enriched later
                            "content_hash": content_hash,
                        }

                        # Insert the raw asset data
                        insert_query = text("""
                            INSERT INTO bronze.raw_entities
                            (entity_type, source_system, external_id, raw_data,
                             ingestion_run_id, ingestion_metadata)
                            VALUES (:entity_type, :source_system, :external_id, :raw_data,
                                    :ingestion_run_id, :ingestion_metadata)
                        """)

                        conn.execute(
                            insert_query,
                            {
                                "entity_type": "asset",
                                "source_system": "tdx",
                                "external_id": external_id,
                                "raw_data": json.dumps(asset),
                                "ingestion_run_id": run_id,
                                "ingestion_metadata": json.dumps(ingestion_metadata),
                            },
                        )
                        conn.commit()

                        if existing:
                            stats["updated"] += 1
                        else:
                            stats["inserted"] += 1

                    # Log progress periodically
                    if stats["processed"] % 100 == 0:
                        logger.info(f"Processed {stats['processed']} assets...")

                except Exception as e:
                    logger.error(f"Failed to process asset {external_id}: {e}")
                    stats["errors"] += 1
                    continue

            # Complete the ingestion run
            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=stats["processed"],
                records_inserted=stats["inserted"],
                records_updated=stats["updated"],
            )

            logger.info(f"Asset ingestion completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Asset ingestion failed: {e}")
            # Mark run as failed
            try:
                with self.db_adapter.engine.connect() as conn:
                    conn.execute(
                        text("""
                        UPDATE meta.ingestion_runs
                        SET status = 'failed', completed_at = :completed_at
                        WHERE run_id = :run_id
                    """),
                        {"run_id": run_id, "completed_at": datetime.now(timezone.utc)},
                    )
                    conn.commit()
            except:
                pass
            raise

    def get_recent_asset_changes(self, days: int = 7) -> pd.DataFrame:
        """
        Get assets that have changed in the last N days.

        Useful for monitoring and verification.

        Args:
            days: Number of days to look back

        Returns:
            DataFrame with recent asset changes
        """
        try:
            query = text("""
                SELECT
                    external_id,
                    raw_data->>'Name' as asset_name,
                    raw_data->>'Tag' as asset_tag,
                    raw_data->>'StatusName' as status,
                    ingestion_metadata->>'modified_date' as modified_date,
                    ingested_at
                FROM bronze.raw_entities
                WHERE entity_type = 'asset'
                  AND source_system = 'tdx'
                  AND ingested_at >= NOW() - INTERVAL ':days days'
                ORDER BY ingested_at DESC
            """)

            return self.db_adapter.query_to_dataframe(
                query.text.replace(":days", str(days))
            )

        except SQLAlchemyError as e:
            logger.error(f"Failed to get recent asset changes: {e}")
            return pd.DataFrame()

    def close(self) -> None:
        """
        Close database connections and cleanup resources.
        """
        self.db_adapter.close()
        logger.info("Asset ingestion service closed")


def main():
    """
    Main entry point for the asset ingestion script.
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

    # Optional: Enable content verification (slower but more thorough)
    enable_verification = (
        os.getenv("ENABLE_CONTENT_VERIFICATION", "false").lower() == "true"
    )

    try:
        # Initialize the ingestion service
        service = TimestampBasedAssetIngestionService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
            enable_content_verification=enable_verification,
        )

        # Run the ingestion
        # Set force_full_ingestion=True for the first run or to rebuild everything
        force_full = os.getenv("FORCE_FULL_INGESTION", "false").lower() == "true"

        logger.info("=" * 80)
        logger.info("Starting TDX Asset Ingestion")
        logger.info("=" * 80)

        stats = service.ingest_assets_timestamp_based(force_full_ingestion=force_full)

        logger.info("=" * 80)
        logger.info("Ingestion Summary:")
        logger.info(f"  Total Processed: {stats['processed']}")
        logger.info(f"  Inserted: {stats['inserted']}")
        logger.info(f"  Updated: {stats['updated']}")
        logger.info(f"  Skipped (no changes): {stats['skipped']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info("=" * 80)

        # Close the service
        service.close()

        sys.exit(0)

    except Exception as e:
        logger.error(f"Asset ingestion failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
