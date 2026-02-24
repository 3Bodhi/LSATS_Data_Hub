#!/usr/bin/env python3
"""
Script to sync lab computers to TeamDynamix Lab Configuration Items.

This script:
1. Reads from silver.v_labs_monitored to get labs with tdx_ci_id
2. Reads from silver.v_lab_computers_tdx_reference to get computers for each lab
3. Uses bulk API to add computer relationships to lab CIs
4. Tracks sync status in meta.ingestion_runs

Relationship structure:
- ParentID: Lab CI ID (from silver.v_labs_monitored.tdx_ci_id)
- ChildID: Computer CI ID (from silver.v_lab_computers_tdx_reference.tdx_configuration_item_id)
- RelationshipTypeID: 10016 ("Place" / "Located in")
"""

import argparse
import logging
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Load environment variables
load_dotenv()

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
RELATIONSHIP_TYPE_ID_LOCATED_IN = 10016  # "Place" / "Located in" relationship
BATCH_SIZE = 100  # Process computers in batches


class LabComputerSyncService:
    """Service for syncing lab computers to TeamDynamix Lab CIs."""

    def __init__(self, db: PostgresAdapter, tdx: TeamDynamixFacade):
        """
        Initialize the sync service.

        Args:
            db: PostgreSQL database adapter
            tdx: TeamDynamix facade
        """
        self.db = db
        self.tdx = tdx
        self.run_id = None

    def fetch_labs_with_ci_ids(
        self, lab_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch labs that have TDX CI IDs assigned.

        Args:
            lab_id: Optional lab_id to filter to a specific lab

        Returns:
            List of lab records with tdx_ci_id
        """
        query = """
            SELECT lab_id, tdx_ci_id
            FROM silver.v_labs_monitored
            WHERE tdx_ci_id IS NOT NULL
        """
        params = {}

        if lab_id:
            query += " AND lab_id = :lab_id"
            params = {"lab_id": lab_id}

        logger.info(
            f"Fetching labs with CI IDs (filter: {lab_id if lab_id else 'all labs'})..."
        )
        df = self.db.query_to_dataframe(query, params)
        labs = df.to_dict("records")
        logger.info(f"Found {len(labs)} labs with TDX CI IDs")
        return labs

    def fetch_lab_computers(
        self, lab_id: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch computers for labs from the reference view.

        Args:
            lab_id: Optional lab_id to filter to a specific lab

        Returns:
            Dictionary mapping lab_id to list of computer records
        """
        query = """
            SELECT
                lab_id,
                computer_id,
                tdx_configuration_item_id
            FROM silver.v_lab_computers_tdx_reference
            WHERE tdx_configuration_item_id IS NOT NULL
        """
        params = {}

        if lab_id:
            query += " AND lab_id = :lab_id"
            params = {"lab_id": lab_id}

        logger.info(
            f"Fetching lab computers (filter: {lab_id if lab_id else 'all labs'})..."
        )
        df = self.db.query_to_dataframe(query, params)

        # Group by lab_id
        computers_by_lab = defaultdict(list)
        for record in df.to_dict("records"):
            computers_by_lab[record["lab_id"]].append(record)

        logger.info(f"Found computers for {len(computers_by_lab)} labs")
        return dict(computers_by_lab)

    def get_existing_relationships(self, lab_ci_id: int) -> List[Dict[str, Any]]:
        """
        Get existing relationships for a lab CI.

        Args:
            lab_ci_id: The lab's TDX CI ID

        Returns:
            List of existing relationship records
        """
        try:
            relationships = self.tdx.configuration_items.get_relationships(lab_ci_id)
            return relationships or []
        except Exception as e:
            logger.error(f"Error fetching relationships for CI {lab_ci_id}: {e}")
            return []

    def build_relationship_mappings(
        self,
        lab_ci_id: int,
        computers: List[Dict[str, Any]],
        existing_relationships: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Build relationship mappings for bulk add, excluding existing relationships.

        Args:
            lab_ci_id: The lab's TDX CI ID
            computers: List of computer records
            existing_relationships: Existing relationships for this lab

        Returns:
            List of relationship mapping dictionaries
        """
        # Build set of existing child IDs for this relationship type
        existing_child_ids = set()
        for rel in existing_relationships:
            if (
                rel.get("ParentID") == lab_ci_id
                and rel.get("RelationshipTypeID") == RELATIONSHIP_TYPE_ID_LOCATED_IN
            ):
                existing_child_ids.add(rel.get("ChildID"))

        # Build mappings for computers not already related
        mappings = []
        for computer in computers:
            computer_id = computer["tdx_configuration_item_id"]

            # Skip if this relationship already exists
            if computer_id in existing_child_ids:
                logger.debug(
                    f"Skipping computer {computer['computer_id']} - relationship already exists"
                )
                continue

            mappings.append(
                {
                    "ParentItemID": int(lab_ci_id),
                    "ChildItemID": int(computer_id),
                    "RelationshipTypeID": RELATIONSHIP_TYPE_ID_LOCATED_IN,
                }
            )

        return mappings

    def sync_lab_computers(
        self,
        lab_id: str,
        lab_ci_id: int,
        computers: List[Dict[str, Any]],
        dry_run: bool = True,
    ) -> Dict[str, int]:
        """
        Sync computers for a single lab.

        Args:
            lab_id: The lab identifier
            lab_ci_id: The lab's TDX CI ID
            computers: List of computer records for this lab
            dry_run: If True, log actions without making API calls

        Returns:
            Dictionary with counts: {added: int, skipped: int, errors: int}
        """
        logger.info(
            f"Processing lab {lab_id} (CI ID: {lab_ci_id}) with {len(computers)} computers"
        )

        # Get existing relationships
        existing_relationships = self.get_existing_relationships(lab_ci_id)
        logger.info(
            f"Lab {lab_id} has {len(existing_relationships)} existing relationships"
        )

        # Build relationship mappings
        mappings = self.build_relationship_mappings(
            lab_ci_id, computers, existing_relationships
        )

        if not mappings:
            logger.info(f"No new relationships to add for lab {lab_id}")
            return {"added": 0, "skipped": len(computers), "errors": 0}

        logger.info(f"Lab {lab_id}: {len(mappings)} new relationships to add")

        if dry_run:
            logger.info(
                f"[DRY RUN] Would add {len(mappings)} relationships for lab {lab_id}"
            )
            logger.debug(f"Sample mapping: {mappings[0] if mappings else 'none'}")
            return {"added": 0, "skipped": len(computers) - len(mappings), "errors": 0}

        # Process in batches
        total_added = 0
        total_errors = 0

        for i in range(0, len(mappings), BATCH_SIZE):
            batch = mappings[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(mappings) + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} relationships)"
            )

            try:
                result = self.tdx.configuration_items.bulk_add_relationships(batch)

                # Handle None response (API returns empty body on success)
                if result is None:
                    # Empty response typically means success - verify by checking relationships after
                    logger.info(
                        f"Batch {batch_num} complete: API returned empty response (likely success)"
                    )
                    # Assume all were added since we pre-filtered existing relationships
                    total_added += len(batch)
                else:
                    # Parse response with counts
                    added = result.get("AddedCount", 0)
                    not_added = result.get("NotAddedCount", 0)
                    errors = result.get("ErrorMessages", [])

                    total_added += added

                    logger.info(
                        f"Batch {batch_num} complete: {added} added, {not_added} not added"
                    )

                    if errors:
                        total_errors += len(errors)
                        for error in errors[:5]:  # Log first 5 errors
                            logger.error(f"API Error: {error}")
                        if len(errors) > 5:
                            logger.error(f"... and {len(errors) - 5} more errors")

            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                total_errors += len(batch)

        logger.info(
            f"Lab {lab_id} complete: {total_added} added, {total_errors} errors"
        )

        return {
            "added": total_added,
            "skipped": len(computers) - len(mappings),
            "errors": total_errors,
        }

    def create_ingestion_run(self) -> uuid.UUID:
        """Create a new ingestion run record."""
        import json

        from sqlalchemy import text

        self.run_id = uuid.uuid4()

        insert_query = """
            INSERT INTO meta.ingestion_runs
                (run_id, source_system, entity_type, started_at, status, metadata)
            VALUES
                (:run_id, :source_system, :entity_type, :started_at, :status, :metadata)
        """

        # Use the engine directly for execute operations
        with self.db.engine.connect() as conn:
            conn.execute(
                text(insert_query),
                {
                    "run_id": str(self.run_id),
                    "source_system": "tdx",
                    "entity_type": "lab_computer_relationships",
                    "started_at": datetime.now(timezone.utc),
                    "status": "running",
                    "metadata": json.dumps({}),
                },
            )
            conn.commit()

        logger.info(f"Created ingestion run: {self.run_id}")
        return self.run_id

    def complete_ingestion_run(
        self,
        total_labs: int,
        total_relationships_added: int,
        total_relationships_skipped: int,
        total_errors: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark the ingestion run as complete.

        Args:
            total_labs: Total number of labs processed
            total_relationships_added: Total relationships added across all labs
            total_relationships_skipped: Total relationships skipped (already existed)
            total_errors: Total errors encountered
            error_message: Optional error message if run failed
        """
        import json

        from sqlalchemy import text

        status = "failed" if error_message else "completed"

        update_query = """
            UPDATE meta.ingestion_runs
            SET completed_at = :completed_at,
                status = :status,
                records_processed = :records_processed,
                records_created = :records_created,
                records_updated = :records_updated,
                error_message = :error_message,
                metadata = :metadata
            WHERE run_id = :run_id
        """

        # Use the engine directly for execute operations
        with self.db.engine.connect() as conn:
            conn.execute(
                text(update_query),
                {
                    "run_id": str(self.run_id),
                    "completed_at": datetime.now(timezone.utc),
                    "status": status,
                    "records_processed": total_labs,
                    "records_created": total_relationships_added,
                    "records_updated": 0,  # We don't update relationships, only add
                    "error_message": error_message,
                    "metadata": json.dumps(
                        {
                            "total_relationships_skipped": total_relationships_skipped,
                            "total_errors": total_errors,
                        }
                    ),
                },
            )
            conn.commit()

        logger.info(f"Ingestion run {self.run_id} marked as {status}")

    def sync(
        self, lab_id: Optional[str] = None, dry_run: bool = True
    ) -> Dict[str, int]:
        """
        Main sync process.

        Args:
            lab_id: Optional lab_id to sync a specific lab
            dry_run: If True, log actions without making API calls

        Returns:
            Summary statistics
        """
        logger.info("=" * 80)
        logger.info("Lab Computer Sync to TeamDynamix")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        logger.info(f"Filter: {lab_id if lab_id else 'All labs'}")
        logger.info("=" * 80)

        # Create ingestion run
        if not dry_run:
            self.create_ingestion_run()

        try:
            # Fetch data
            labs = self.fetch_labs_with_ci_ids(lab_id)
            computers_by_lab = self.fetch_lab_computers(lab_id)

            # Validate
            if not labs:
                logger.warning(
                    "No labs found with TDX CI IDs. Run 002_sync_tdx_lab_ci_ids.py first."
                )
                return {
                    "total_labs": 0,
                    "total_added": 0,
                    "total_skipped": 0,
                    "total_errors": 0,
                }

            # Process each lab
            total_added = 0
            total_skipped = 0
            total_errors = 0
            labs_processed = 0

            for lab in labs:
                lab_id_val = lab["lab_id"]
                lab_ci_id_val = lab["tdx_ci_id"]

                computers = computers_by_lab.get(lab_id_val, [])

                if not computers:
                    logger.info(f"Lab {lab_id_val} has no computers to sync")
                    continue

                result = self.sync_lab_computers(
                    lab_id_val, lab_ci_id_val, computers, dry_run
                )

                total_added += result["added"]
                total_skipped += result["skipped"]
                total_errors += result["errors"]
                labs_processed += 1

            # Summary
            logger.info("=" * 80)
            logger.info("Sync Complete")
            logger.info(f"Labs processed: {labs_processed}")
            logger.info(f"Relationships added: {total_added}")
            logger.info(f"Relationships skipped (already exist): {total_skipped}")
            logger.info(f"Errors: {total_errors}")
            logger.info("=" * 80)

            # Complete ingestion run
            if not dry_run:
                self.complete_ingestion_run(
                    total_labs=labs_processed,
                    total_relationships_added=total_added,
                    total_relationships_skipped=total_skipped,
                    total_errors=total_errors,
                    error_message=None
                    if total_errors == 0
                    else f"{total_errors} errors encountered",
                )

            return {
                "total_labs": labs_processed,
                "total_added": total_added,
                "total_skipped": total_skipped,
                "total_errors": total_errors,
            }

        except Exception as e:
            logger.error(f"Fatal error during sync: {e}", exc_info=True)

            if not dry_run and self.run_id:
                self.complete_ingestion_run(
                    total_labs=0,
                    total_relationships_added=0,
                    total_relationships_skipped=0,
                    total_errors=1,
                    error_message=str(e),
                )

            raise

    def close(self):
        """Close database connection."""
        self.db.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync lab computers to TeamDynamix Lab Configuration Items",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for all labs
  python scripts/tdx/003_sync_lab_computers.py --dry-run

  # Sync specific lab (dry run)
  python scripts/tdx/003_sync_lab_computers.py --lab-id aabol --dry-run

  # Live sync for all labs
  python scripts/tdx/003_sync_lab_computers.py --no-dry-run

  # Live sync for specific lab
  python scripts/tdx/003_sync_lab_computers.py --lab-id aabol --no-dry-run
        """,
    )

    parser.add_argument("--lab-id", help="Specific lab ID to process (e.g., 'aabol')")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry run mode (default: True)",
    )

    parser.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="Execute actual API calls",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of relationships to process per batch (default: 100)",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Configure logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Update batch size if specified
    global BATCH_SIZE
    BATCH_SIZE = args.batch_size

    # Get database URL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL environment variable must be set")
        sys.exit(1)

    # Get TDX credentials
    tdx_base_url = os.getenv("TDX_BASE_URL")
    tdx_token = os.getenv("TDX_API_TOKEN")
    tdx_username = os.getenv("TDX_USERNAME")
    tdx_password = os.getenv("TDX_PASSWORD")
    tdx_beid = os.getenv("TDX_BEID")
    tdx_web_services_key = os.getenv("TDX_WEB_SERVICES_KEY")
    tdx_app_id = 48  # Asset/CI App ID

    has_credentials = (
        (tdx_beid and tdx_web_services_key)
        or (tdx_username and tdx_password)
        or tdx_token
    )
    if not tdx_base_url or not has_credentials:
        logger.error("TDX_BASE_URL and valid credentials (BEID+WebServicesKey, Username+Password, or API_TOKEN) must be set")
        sys.exit(1)

    # Initialize adapters
    db = PostgresAdapter(db_url)
    tdx = TeamDynamixFacade(
        tdx_base_url,
        tdx_app_id,
        api_token=tdx_token,
        username=tdx_username,
        password=tdx_password,
        beid=tdx_beid,
        web_services_key=tdx_web_services_key,
    )

    # Create service and run sync
    service = LabComputerSyncService(db, tdx)

    try:
        service.sync(lab_id=args.lab_id, dry_run=args.dry_run)
    finally:
        service.close()


if __name__ == "__main__":
    main()
