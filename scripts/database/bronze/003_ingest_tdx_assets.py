#!/usr/bin/env python3
"""
TDX Asset Ingestion Service

Ingests TeamDynamix assets using the search_asset() API with status filters.
Uses ModifiedDate for efficient timestamp-based change detection.

Status IDs used: [38, 39, 40, 41, 46, 59, 141, 201, 202]

This is the initial ingestion script that fetches basic asset data.
For detailed enrichment (Attributes, Attachments), see 030_enrich_tdx_assets.py.
"""

import argparse
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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Determine log directory based on script location
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs/bronze"
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


class TDXAssetIngestionService:
    """
    Asset ingestion service that uses TeamDynamix ModifiedDate for efficient change detection.

    This approach leverages TDX's built-in change tracking to minimize processing overhead.
    Stores basic asset data with _content_hash_basic for compatibility with enrichment script.

    Key Benefits:
    - Fast processing by filtering on timestamps
    - Relies on TeamDynamix's reliable change tracking
    - Supports incremental ingestion for large datasets
    - Compatible with enrichment workflow
    """

    def __init__(
        self,
        database_url: str,
        tdx_base_url: str,
        tdx_api_token: str,
        tdx_app_id: str,
    ):
        """
        Initialize the timestamp-based ingestion service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url, app_id=tdx_app_id, api_token=tdx_api_token
        )

        logger.info(f"🔌 TDX asset ingestion service initialized")

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
            logger.warning(f"⚠️  Failed to parse timestamp '{timestamp_str}': {e}")
            # Return a very old date as fallback to ensure record gets processed
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _calculate_basic_content_hash(self, asset_data: Dict[str, Any]) -> str:
        """
        Calculate basic content hash (matching enrich script).

        Only includes fields from search_asset API to match 030_enrich_tdx_assets.py hash.
        This allows the enrichment script to detect changes in basic data.

        Args:
            asset_data: Asset data from search_asset API

        Returns:
            SHA-256 hash of basic fields only
        """
        # Match the hash calculation in 030_enrich_tdx_assets.py
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
                    logger.info(f"📅 Last asset ingestion completed at: {row[0]}")
                    return row[0]
                else:
                    logger.info(
                        "🆕 No previous asset ingestion found - performing full ingestion"
                    )
                    return None

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get last ingestion timestamp: {e}")
            return None

    def _get_existing_asset_hashes(self) -> Dict[str, str]:
        """
        Load basic content hashes for all existing assets in bronze layer.

        Only checks _content_hash_basic to avoid collision with enrichment hashes.

        Returns:
            Dictionary mapping external_id to _content_hash_basic
        """
        try:
            query = """
            SELECT DISTINCT ON (external_id)
                external_id,
                raw_data->>'_content_hash_basic' as content_hash_basic
            FROM bronze.raw_entities
            WHERE entity_type = 'asset'
              AND source_system = 'tdx'
              AND raw_data->>'_content_hash_basic' IS NOT NULL
            ORDER BY external_id, ingested_at DESC
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if result_df.empty:
                return {}

            logger.info(f"📚 Loaded {len(result_df)} existing asset hashes")
            return dict(zip(result_df["external_id"], result_df["content_hash_basic"]))

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to load existing asset hashes: {e}")
            return {}

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
            logger.info(f"🔍 Fetching assets from TeamDynamix...")

            # Call search_asset with the specified status IDs
            search_data = {"statusIDs": [38, 39, 40, 41, 46, 59, 141, 201, 202]}

            assets = self.tdx_facade.assets.search_asset(data=search_data)

            logger.info(f"📦 Retrieved {len(assets)} total assets from TeamDynamix")

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
                    f"⚡ Filtered to {len(filtered_assets)} assets modified since {since_timestamp}"
                )
                return filtered_assets

            return assets

        except Exception as e:
            logger.error(f"❌ Failed to fetch assets from TeamDynamix: {e}")
            raise

    def create_ingestion_run(
        self, run_type: str = "incremental", total_assets: int = 0
    ) -> str:
        """
        Create a new ingestion run record in the meta schema.

        This tracks the ingestion process and provides audit trail.

        Args:
            run_type: Type of ingestion run ('full' or 'incremental')
            total_assets: Total number of assets to process

        Returns:
            UUID of the created ingestion run
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "ingestion_type": "timestamp_based_search",
                "run_type": run_type,
                "total_assets": total_assets,
                "status_ids_filter": [38, 39, 40, 41, 46, 59, 141, 201, 202],
                "ingestion_source": "search_asset",
            }

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
                        "metadata": json.dumps(metadata),
                    },
                )
                conn.commit()

            logger.info(f"📝 Created ingestion run {run_id} ({run_type})")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        records_processed: int,
        records_inserted: int,
        records_updated: int,
        records_skipped: int,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark an ingestion run as completed and record statistics.

        Args:
            run_id: UUID of the ingestion run
            records_processed: Total number of records processed
            records_inserted: Number of new records inserted
            records_updated: Number of existing records updated
            records_skipped: Number of unchanged records skipped
            error_message: Error summary if failures occurred
        """
        try:
            status = "failed" if error_message else "completed"

            update_query = text("""
                UPDATE meta.ingestion_runs
                SET status = :status,
                    completed_at = :completed_at,
                    records_processed = :records_processed,
                    records_created = :records_inserted,
                    records_updated = :records_updated,
                    error_message = :error_message
                WHERE run_id = :run_id
            """)

            with self.db_adapter.engine.connect() as conn:
                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "status": status,
                        "completed_at": datetime.now(timezone.utc),
                        "records_processed": records_processed,
                        "records_inserted": records_inserted,
                        "records_updated": records_updated,
                        "error_message": error_message,
                    },
                )
                conn.commit()

            logger.info(f"✅ Completed ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete ingestion run: {e}")
            raise

    def ingest_assets_timestamp_based(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Ingest assets using timestamp-based change detection.

        This method:
        1. Determines the last successful ingestion timestamp
        2. Fetches only assets modified since that time from TeamDynamix
        3. Inserts new records or updates existing ones based on ModifiedDate
        4. Tracks the ingestion run in meta schema

        Args:
            full_sync: If True, ignore timestamp and process all assets
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with ingestion statistics
        """
        stats = {
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
        }

        # Determine if this is a full or incremental run
        if full_sync:
            since_timestamp = None
            run_type = "full"
            logger.info("🔄 Full sync mode: Processing ALL assets")
        else:
            since_timestamp = self._get_last_ingestion_timestamp()
            run_type = "incremental" if since_timestamp else "full"
            if since_timestamp:
                logger.info(
                    f"⚡ Incremental mode: Processing assets since {since_timestamp}"
                )
            else:
                logger.info("🆕 First run: Processing ALL assets")

        if dry_run:
            logger.info("⚠️  DRY RUN MODE - No changes will be committed")

        # Fetch assets from TeamDynamix
        assets = self._get_assets_modified_since(since_timestamp)

        if not assets:
            logger.info("✨ No assets to process")
            return stats

        # Create ingestion run record
        run_id = self.create_ingestion_run(run_type=run_type, total_assets=len(assets))

        # Load existing asset hashes for change detection
        logger.info("📚 Loading existing asset hashes for change detection...")
        existing_hashes = self._get_existing_asset_hashes()

        try:
            # Process each asset
            for asset in assets:
                try:
                    stats["processed"] += 1

                    # Use ID field as external_id
                    external_id = str(asset.get("ID", ""))
                    if not external_id:
                        logger.warning(f"⚠️  Asset missing ID field, skipping: {asset}")
                        stats["skipped"] += 1
                        continue

                    # Calculate basic content hash for change detection
                    current_hash = self._calculate_basic_content_hash(asset)

                    # Check if this asset exists and is unchanged
                    if external_id in existing_hashes:
                        if existing_hashes[external_id] == current_hash:
                            # No changes, skip this asset
                            logger.debug(
                                f"⏭️  Asset unchanged, skipping: {asset.get('Name')} (ID: {external_id})"
                            )
                            stats["skipped"] += 1
                            continue
                        else:
                            action = "updated"
                    else:
                        action = "inserted"

                    # Prepare enhanced asset data with metadata
                    enhanced_asset_data = asset.copy()
                    enhanced_asset_data["_ingestion_method"] = "timestamp_based_search"
                    enhanced_asset_data["_ingestion_source"] = "search_asset"
                    enhanced_asset_data["_ingestion_timestamp"] = datetime.now(
                        timezone.utc
                    ).isoformat()
                    enhanced_asset_data["_content_hash_basic"] = current_hash

                    if dry_run:
                        stats[action] += 1
                        logger.info(
                            f"[DRY RUN] Would {action} asset: {asset.get('Name')} "
                            f"(ID: {external_id}, Tag: {asset.get('Tag')})"
                        )
                        continue

                    # Insert the raw asset data
                    insert_query = text("""
                        INSERT INTO bronze.raw_entities
                        (entity_type, source_system, external_id, raw_data,
                         ingestion_run_id)
                        VALUES (:entity_type, :source_system, :external_id, :raw_data,
                                :ingestion_run_id)
                    """)

                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(
                            insert_query,
                            {
                                "entity_type": "asset",
                                "source_system": "tdx",
                                "external_id": external_id,
                                "raw_data": json.dumps(enhanced_asset_data),
                                "ingestion_run_id": run_id,
                            },
                        )
                        conn.commit()

                    stats[action] += 1

                    # Log based on action
                    if action == "inserted":
                        logger.debug(
                            f"🆕 New asset: {asset.get('Name')} (ID: {external_id})"
                        )
                    else:
                        logger.debug(
                            f"📝 Updated asset: {asset.get('Name')} (ID: {external_id})"
                        )

                    # Log progress periodically
                    if stats["processed"] % 50 == 0:
                        logger.info(
                            f"📈 Progress: {stats['processed']}/{len(assets)} assets processed "
                            f"({stats['inserted']} new, {stats['updated']} updated, {stats['skipped']} skipped)"
                        )

                except Exception as e:
                    logger.error(f"❌ Failed to process asset {external_id}: {e}")
                    stats["errors"] += 1
                    continue

            # Complete the ingestion run
            if not dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    records_processed=stats["processed"],
                    records_inserted=stats["inserted"],
                    records_updated=stats["updated"],
                    records_skipped=stats["skipped"],
                )

            return stats

        except Exception as e:
            logger.error(f"❌ Asset ingestion failed: {e}")
            # Mark run as failed
            if not dry_run:
                try:
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(
                            text("""
                            UPDATE meta.ingestion_runs
                            SET status = 'failed',
                                completed_at = :completed_at,
                                error_message = :error_message
                            WHERE run_id = :run_id
                        """),
                            {
                                "run_id": run_id,
                                "completed_at": datetime.now(timezone.utc),
                                "error_message": str(e),
                            },
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
            query = f"""
            SELECT
                external_id,
                raw_data->>'Name' as asset_name,
                raw_data->>'Tag' as asset_tag,
                raw_data->>'StatusName' as status,
                raw_data->>'ModifiedDate' as modified_date,
                ingested_at
            FROM bronze.raw_entities
            WHERE entity_type = 'asset'
              AND source_system = 'tdx'
              AND ingested_at >= NOW() - INTERVAL '{days} days'
            ORDER BY ingested_at DESC
            """

            return self.db_adapter.query_to_dataframe(query)

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get recent asset changes: {e}")
            return pd.DataFrame()

    def close(self) -> None:
        """
        Close database connections and cleanup resources.
        """
        self.db_adapter.close()
        logger.info("🔌 Asset ingestion service closed")


def main():
    """
    Main entry point for the asset ingestion script.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Ingest TeamDynamix assets to bronze layer with timestamp-based change detection"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all assets (ignore last ingestion timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of assets to process per batch (default: 100)",
    )

    args = parser.parse_args()

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
            "❌ Missing required environment variables: DATABASE_URL, TDX_BASE_URL, TDX_API_TOKEN"
        )
        sys.exit(1)

    try:
        # Initialize the ingestion service
        service = TDXAssetIngestionService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
        )

        # Run the ingestion
        logger.info("=" * 80)
        logger.info("🚀 STARTING TDX ASSET INGESTION")
        logger.info("=" * 80)
        logger.info(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        logger.info(f"Dry Run:             {args.dry_run}")
        logger.info("=" * 80)

        stats = service.ingest_assets_timestamp_based(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        # Display summary
        logger.info("=" * 80)
        logger.info("🎉 ASSET INGESTION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"📊 Results Summary:")
        logger.info(f"   Total Processed:      {stats['processed']:>6,}")
        logger.info(f"   ├─ New Inserted:      {stats['inserted']:>6,}")
        logger.info(f"   ├─ Updated:           {stats['updated']:>6,}")
        logger.info(f"   └─ Skipped:           {stats['skipped']:>6,}")
        logger.info(f"")
        logger.info(f"   Errors:               {stats['errors']:>6,}")
        logger.info("=" * 80)

        if args.dry_run:
            logger.info("")
            logger.info("⚠️  DRY RUN COMPLETED - No changes committed to database")
        else:
            logger.info("")
            logger.info("✅ Ingestion completed successfully!")

        # Close the service
        service.close()

        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Asset ingestion failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
