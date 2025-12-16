#!/usr/bin/env python3
"""
Script to sync lab-related tickets to Lab CIs in TeamDynamix.

Searches for tickets where:
1. RequestorUID matches any lab member (from v_lab_members_all_tdx_reference)
2. ConfigurationItemID matches any lab computer (from v_lab_computers_tdx_reference)

Then associates those tickets with the Lab CI.
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from sqlalchemy import text

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


class LabTicketSyncService:
    """Service to sync lab-related tickets to Lab CIs."""

    def __init__(
        self,
        db_adapter: PostgresAdapter,
        tdx_facade: TeamDynamixFacade,
        dry_run: bool = True,
        delay: float = 0.0,
    ):
        """
        Initialize the lab ticket sync service.

        Args:
            db_adapter: PostgreSQL database adapter
            tdx_facade: TeamDynamix API facade
            dry_run: If True, show what would be done without making changes
            delay: Delay in seconds between ticket additions (default: 0.0)
        """
        self.db = db_adapter
        self.tdx = tdx_facade
        self.dry_run = dry_run
        self.delay = delay
        self.run_id = uuid.uuid4()
        self.current_lab_id = None
        self.stats = {
            "labs_processed": 0,
            "tickets_found": 0,
            "tickets_added": 0,
            "tickets_already_linked": 0,
            "errors": [],
        }

    def fetch_labs_with_ci_ids(
        self, lab_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get labs that have TDX CI IDs assigned.

        Args:
            lab_id: Optional specific lab ID to process
            limit: Optional limit on number of labs to process

        Returns:
            List of dictionaries with lab_id and tdx_ci_id
        """
        query = """
            SELECT lab_id, tdx_ci_id
            FROM silver.labs
            WHERE tdx_ci_id IS NOT NULL
        """
        params = {}

        if lab_id:
            query += " AND lab_id = :lab_id"
            params["lab_id"] = lab_id

        query += " ORDER BY lab_id"

        if limit:
            query += " LIMIT :limit"
            params["limit"] = limit

        df = self.db.query_to_dataframe(query, params)
        return df.to_dict("records")

    def fetch_lab_member_uids(self, lab_id: str) -> List[str]:
        """
        Get all TDX user UIDs for lab members.

        Args:
            lab_id: The lab ID to fetch members for

        Returns:
            List of TDX user UIDs (as strings)
        """
        query = """
            SELECT DISTINCT tdx_user_uid::text as tdx_user_uid
            FROM silver.v_lab_members_all_tdx_reference
            WHERE lab_id = :lab_id
              AND tdx_user_uid IS NOT NULL
        """
        df = self.db.query_to_dataframe(query, {"lab_id": lab_id})
        return df["tdx_user_uid"].tolist()

    def fetch_lab_computer_ci_ids(self, lab_id: str) -> List[int]:
        """
        Get all TDX Configuration Item IDs for lab computers.

        Args:
            lab_id: The lab ID to fetch computers for

        Returns:
            List of TDX CI IDs (as integers)
        """
        query = """
            SELECT DISTINCT tdx_configuration_item_id
            FROM silver.v_lab_computers_tdx_reference
            WHERE lab_id = :lab_id
              AND tdx_configuration_item_id IS NOT NULL
        """
        df = self.db.query_to_dataframe(query, {"lab_id": lab_id})
        return df["tdx_configuration_item_id"].astype(int).tolist()

    def search_lab_related_tickets(
        self, member_uids: List[str], computer_ci_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Search TDX for tickets related to this lab.

        Args:
            member_uids: List of TDX user UIDs for lab members
            computer_ci_ids: List of TDX CI IDs for lab computers

        Returns:
            List of ticket dictionaries from TDX
        """
        if not member_uids and not computer_ci_ids:
            logger.debug("  No member UIDs or computer CI IDs to search")
            return []

        search_params = {}
        if member_uids:
            search_params["RequestorUids"] = member_uids
        if computer_ci_ids:
            search_params["ConfigurationItemIDs"] = computer_ci_ids

        logger.debug(
            f"  Searching with {len(member_uids)} member UIDs and {len(computer_ci_ids)} computer CI IDs"
        )

        try:
            tickets = self.tdx.tickets.search_tickets(search_params)
            return tickets if tickets else []
        except Exception as e:
            logger.error(f"  Failed to search tickets: {e}")
            return []

    def get_existing_ci_tickets(self, ci_id: int) -> List[int]:
        """
        Get tickets already associated with the Lab CI.

        Args:
            ci_id: The Lab CI ID

        Returns:
            List of ticket IDs already on the CI
        """
        try:
            tickets = self.tdx.configuration_items.get_ci_tickets(ci_id)
            if tickets:
                return [t["ID"] for t in tickets]
            return []
        except Exception as e:
            logger.error(f"  Failed to get existing CI tickets: {e}")
            return []

    def add_tickets_to_ci(self, ci_id: int, ticket_ids: List[int]) -> int:
        """
        Add tickets to the Lab CI.

        Args:
            ci_id: The Lab CI ID
            ticket_ids: List of ticket IDs to add

        Returns:
            Number of tickets successfully added
        """
        added_count = 0

        for i, ticket_id in enumerate(ticket_ids):
            try:
                # Rate limiting is handled automatically by teamdynamix_api.py
                # Additional configurable delay can be set to prevent hitting rate limits
                self.tdx.configuration_items.add_ticket_to_ci(ci_id, ticket_id)
                added_count += 1
                self.stats["tickets_added"] += 1

                # Progress logging every 100 tickets
                if (i + 1) % 100 == 0:
                    logger.info(
                        f"    Progress: {i + 1}/{len(ticket_ids)} tickets added"
                    )

                # Add delay between requests if configured
                if (
                    self.delay > 0 and i < len(ticket_ids) - 1
                ):  # Don't delay after last ticket
                    time.sleep(self.delay)

            except Exception as e:
                logger.error(f"    Failed to add ticket {ticket_id}: {e}")
                self.stats["errors"].append(
                    {
                        "lab_id": self.current_lab_id,
                        "ci_id": ci_id,
                        "ticket_id": ticket_id,
                        "error": str(e),
                    }
                )

        return added_count

    def create_ingestion_run(self):
        """Create ingestion run record in meta.ingestion_runs."""
        try:
            with self.db.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                            INSERT INTO meta.ingestion_runs
                            (run_id, source_system, entity_type, started_at, status, metadata)
                            VALUES (:run_id, :source_system, :entity_type, :started_at, 'running', CAST(:metadata_json AS jsonb))
                        """),
                        {
                            "run_id": str(self.run_id),
                            "source_system": "tdx",
                            "entity_type": "lab_ticket_associations",
                            "started_at": datetime.now(timezone.utc),
                            "metadata_json": json.dumps(
                                {
                                    "script": "004_sync_lab_tickets.py",
                                    "dry_run": self.dry_run,
                                }
                            ),
                        },
                    )
            logger.info(f"Created ingestion run: {self.run_id}")
        except Exception as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self, status: str = "completed", error: Optional[Exception] = None
    ):
        """
        Mark ingestion run as completed with statistics.

        Handles errors gracefully including KeyboardInterrupt.

        Args:
            status: Override status ('completed', 'interrupted', 'failed')
            error: Exception object if fatal error occurred
        """
        try:
            # Determine final status
            if error:
                final_status = "failed"
                error_msg = str(error)
            elif status == "interrupted":
                final_status = "failed"
                error_msg = "Script interrupted by user"
            elif self.stats["errors"]:
                final_status = "completed_with_errors"
                error_msg = f"{len(self.stats['errors'])} errors"
            else:
                final_status = "completed"
                error_msg = None

            with self.db.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                            UPDATE meta.ingestion_runs SET
                                completed_at = :completed_at,
                                status = :status,
                                records_processed = :processed,
                                records_created = :created,
                                error_message = :error_msg,
                                metadata = CAST(:metadata_json AS jsonb)
                            WHERE run_id = :run_id
                        """),
                        {
                            "run_id": str(self.run_id),
                            "completed_at": datetime.now(timezone.utc),
                            "status": final_status,
                            "processed": self.stats["labs_processed"],
                            "created": self.stats["tickets_added"],
                            "error_msg": error_msg,
                            "metadata_json": json.dumps(self.stats, default=str),
                        },
                    )
            logger.info(f"Completed ingestion run with status: {final_status}")
        except Exception as e:
            logger.error(f"Failed to update ingestion run: {e}")

    def log_summary(self):
        """Log final summary statistics."""
        logger.info("=" * 80)
        logger.info("Lab Ticket Sync Summary")
        logger.info("=" * 80)
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"Labs processed: {self.stats['labs_processed']}")
        logger.info(f"Tickets found: {self.stats['tickets_found']}")
        logger.info(f"Tickets already linked: {self.stats['tickets_already_linked']}")
        logger.info(f"Tickets added: {self.stats['tickets_added']}")
        logger.info(f"Errors: {len(self.stats['errors'])}")

        if self.stats["errors"]:
            logger.error("\nErrors encountered:")
            for err in self.stats["errors"][:10]:  # Show first 10
                logger.error(
                    f"  Lab {err.get('lab_id')}, Ticket {err.get('ticket_id')}: {err.get('error')}"
                )
            if len(self.stats["errors"]) > 10:
                logger.error(f"  ... and {len(self.stats['errors']) - 10} more")

        logger.info("=" * 80)

    def sync(self, lab_id: Optional[str] = None, limit: Optional[int] = None):
        """
        Main orchestration method with error handling.

        Args:
            lab_id: Optional specific lab to process
            limit: Optional limit on number of labs to process
        """
        try:
            self.create_ingestion_run()

            # Fetch labs
            logger.info("Fetching labs with CI IDs...")
            labs = self.fetch_labs_with_ci_ids(lab_id, limit)
            logger.info(f"Found {len(labs)} labs to process")

            if not labs:
                logger.warning("No labs found with TDX CI IDs")
                self.complete_ingestion_run()
                return

            # Process each lab
            for i, lab in enumerate(labs):
                lab_id = lab["lab_id"]
                ci_id = lab["tdx_ci_id"]
                self.current_lab_id = lab_id

                logger.info(
                    f"\n[{i + 1}/{len(labs)}] Processing lab: {lab_id} (CI ID: {ci_id})"
                )

                # Fetch member UIDs
                logger.info("  Fetching lab member UIDs...")
                member_uids = self.fetch_lab_member_uids(lab_id)
                logger.info(f"  Found {len(member_uids)} member UIDs")

                # Fetch computer CI IDs
                logger.info("  Fetching lab computer CI IDs...")
                computer_ci_ids = self.fetch_lab_computer_ci_ids(lab_id)
                logger.info(f"  Found {len(computer_ci_ids)} computer CI IDs")

                if not member_uids and not computer_ci_ids:
                    logger.warning(
                        f"  No member UIDs or computer CI IDs for lab {lab_id}, skipping"
                    )
                    continue

                # Search for related tickets
                logger.info("  Searching for related tickets...")
                tickets = self.search_lab_related_tickets(member_uids, computer_ci_ids)
                logger.info(f"  Found {len(tickets)} related tickets")
                self.stats["tickets_found"] += len(tickets)

                if not tickets:
                    logger.info(f"  No tickets found for lab {lab_id}")
                    self.stats["labs_processed"] += 1
                    continue

                # Get existing ticket associations
                logger.info("  Checking existing ticket associations...")
                existing_ticket_ids = self.get_existing_ci_tickets(ci_id)
                logger.info(f"  Found {len(existing_ticket_ids)} existing associations")

                # Calculate new tickets to add
                all_ticket_ids = [t["ID"] for t in tickets]
                new_ticket_ids = [
                    tid for tid in all_ticket_ids if tid not in existing_ticket_ids
                ]
                already_linked = len(all_ticket_ids) - len(new_ticket_ids)
                self.stats["tickets_already_linked"] += already_linked

                logger.info(f"  {len(new_ticket_ids)} new tickets to add")

                if new_ticket_ids:
                    if self.dry_run:
                        logger.info(
                            f"  [DRY RUN] Would add {len(new_ticket_ids)} tickets to CI {ci_id}"
                        )
                        self.stats["tickets_added"] += len(new_ticket_ids)
                    else:
                        logger.info(
                            f"  Adding {len(new_ticket_ids)} tickets to CI {ci_id}..."
                        )
                        added = self.add_tickets_to_ci(ci_id, new_ticket_ids)
                        logger.info(f"  Successfully added {added} tickets")

                self.stats["labs_processed"] += 1

            self.complete_ingestion_run()

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Script interrupted by user (Ctrl+C)")
            self.complete_ingestion_run(status="interrupted")
            raise
        except Exception as e:
            logger.error(f"❌ Fatal error during sync: {e}")
            self.complete_ingestion_run(error=e)
            raise
        finally:
            self.log_summary()
            self.close()

    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()
            logger.debug("Database connection closed")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync lab-related tickets to Lab CIs in TeamDynamix"
    )
    parser.add_argument("--lab-id", help="Specific Lab ID to process (e.g., 'aabol')")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry run mode - show what would be done without making changes (default: True)",
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="Execute actual API calls to add tickets to CIs",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging (DEBUG level)"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of labs to process (for testing)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between ticket additions to prevent rate limiting (default: 0.0)",
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Setup logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Get environment variables
    db_url = os.getenv("DATABASE_URL")
    tdx_base_url = os.getenv("TDX_BASE_URL")
    tdx_token = os.getenv("TDX_API_TOKEN")

    if not db_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    if not tdx_base_url or not tdx_token:
        logger.error("TDX_BASE_URL and TDX_API_TOKEN environment variables must be set")
        sys.exit(1)

    # Initialize connections
    logger.info("Initializing database and TDX connections...")
    db = PostgresAdapter(db_url)
    tdx = TeamDynamixFacade(
        tdx_base_url,
        48,  # Asset/CI App ID
        tdx_token,
    )

    # Create service
    service = LabTicketSyncService(db, tdx, dry_run=args.dry_run, delay=args.delay)

    # Log delay setting if configured
    if args.delay > 0:
        logger.info(f"Using {args.delay}s delay between ticket additions")

    try:
        # Run sync (handles its own errors and ingestion run tracking)
        service.sync(lab_id=args.lab_id, limit=args.limit)
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
