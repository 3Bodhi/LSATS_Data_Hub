"""
TeamDynamix Ticket Queue Daemon

Monitors a TeamDynamix report and performs idempotent actions on tickets.
Supports both single-run and continuous daemon modes with configurable actions.
"""

import argparse
import functools
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter
from scripts.queue.actions import (
    AddAssetAction,
    BaseAction,
    CommentAction,
    SummaryCommentAction,
)
from scripts.queue.state.state_tracker import StateTracker
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

logger = logging.getLogger(__name__)


def handle_keyboard_interrupt(exit_message="Daemon interrupted by user"):
    """Decorator to handle KeyboardInterrupt and exit gracefully."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt:
                logger.info(f"\n{exit_message}")
                sys.exit(0)

        return wrapper

    return decorator


class TicketQueueDaemon:
    """
    Daemon service for processing TeamDynamix ticket queues.

    Fetches tickets from a TDX report and executes configured actions
    on each ticket with full idempotency tracking.
    """

    def __init__(
        self,
        facade: TeamDynamixFacade,
        state_tracker: StateTracker,
        report_id: int,
        actions: List[BaseAction],
        dry_run: bool = False,
    ):
        """
        Initialize ticket queue daemon.

        Args:
            facade: TeamDynamixFacade instance for API operations
            state_tracker: StateTracker for action idempotency
            report_id: TeamDynamix report ID to monitor
            actions: List of BaseAction instances to execute on tickets
            dry_run: If True, simulate without making changes
        """
        self.facade = facade
        self.state_tracker = state_tracker
        self.report_id = report_id
        self.actions = actions
        self.dry_run = dry_run

        self.stats = {
            "runs": 0,
            "tickets_processed": 0,
            "actions_executed": 0,
            "actions_succeeded": 0,
            "actions_failed": 0,
            "actions_skipped": 0,
        }

    def fetch_report_tickets(self) -> List[Dict[str, Any]]:
        """
        Fetch tickets from the configured TDX report.

        Returns:
            List of ticket data rows from the report
        """
        try:
            logger.info(f"Fetching report {self.report_id}...")

            # Fetch report with data
            report = self.facade.reports.get_report(id=self.report_id, withData=True)

            if not report:
                logger.error(f"Failed to fetch report {self.report_id}")
                return []

            report_name = report.get("Name", "Unknown")
            tickets = report.get("DataRows", [])

            logger.info(
                f"Fetched report '{report_name}' (ID: {self.report_id}): "
                f"{len(tickets)} tickets found"
            )

            return tickets

        except Exception as e:
            logger.exception(f"Error fetching report {self.report_id}")
            return []

    def process_ticket(self, ticket_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single ticket by executing all configured actions.

        Args:
            ticket_data: Ticket data row from report

        Returns:
            Processing results dictionary
        """
        ticket_id = ticket_data.get("TicketID")
        if not ticket_id:
            logger.warning("Ticket data missing TicketID field, skipping")
            return {"success": False, "message": "Missing TicketID", "actions": []}

        ticket_title = ticket_data.get("Title", "Unknown")

        logger.info(f"\nProcessing ticket {ticket_id}: '{ticket_title}'")

        # Initialize action context for this ticket
        action_context = {
            "summaries": [],
            "ticket_id": ticket_id,
            "ticket_data": ticket_data,
        }

        results = {
            "ticket_id": ticket_id,
            "ticket_title": ticket_title,
            "success": True,
            "actions": [],
        }

        # Execute each action on the ticket
        for action in self.actions:
            try:
                action_result = action.execute(
                    ticket_id=ticket_id,
                    facade=self.facade,
                    state_tracker=self.state_tracker,
                    dry_run=self.dry_run,
                    action_context=action_context,
                )

                results["actions"].append(action_result)

                # Update stats
                if action_result.get("executed", False):
                    self.stats["actions_executed"] += 1

                    if action_result.get("success", False):
                        self.stats["actions_succeeded"] += 1
                        logger.info(
                            f"  ✓ {action.get_action_type()}: "
                            f"{action_result.get('message', 'Success')}"
                        )
                    else:
                        self.stats["actions_failed"] += 1
                        logger.error(
                            f"  ✗ {action.get_action_type()}: "
                            f"{action_result.get('message', 'Failed')}"
                        )
                        results["success"] = False
                else:
                    self.stats["actions_skipped"] += 1
                    logger.debug(
                        f"  - {action.get_action_type()}: "
                        f"{action_result.get('message', 'Skipped')}"
                    )

            except Exception as e:
                logger.exception(
                    f"Unexpected error executing {action} on ticket {ticket_id}"
                )
                results["success"] = False
                results["actions"].append(
                    {
                        "action_id": action.get_action_id(),
                        "success": False,
                        "message": f"Unexpected error: {str(e)}",
                    }
                )
                self.stats["actions_failed"] += 1

        return results

    def run_once(self) -> Dict[str, Any]:
        """
        Run the daemon once (process all tickets in report).

        Returns:
            Run statistics dictionary
        """
        self.stats["runs"] += 1
        run_start = datetime.now()

        logger.info(f"\n{'=' * 70}")
        logger.info(f"Starting daemon run #{self.stats['runs']}")
        logger.info(f"Report ID: {self.report_id}")
        logger.info(f"Actions configured: {len(self.actions)}")
        logger.info(f"Dry run: {self.dry_run}")
        logger.info(f"{'=' * 70}\n")

        # Fetch tickets from report
        tickets = self.fetch_report_tickets()

        if not tickets:
            logger.warning("No tickets found in report or error fetching report")
            return {"tickets_found": 0, "tickets_processed": 0, "duration_seconds": 0}

        # Process each ticket
        processed_count = 0
        for i, ticket_data in enumerate(tickets, 1):
            logger.info(f"\n[{i}/{len(tickets)}] ", extra={"no_newline": True})

            try:
                result = self.process_ticket(ticket_data)
                if result.get("ticket_id"):
                    processed_count += 1
                    self.stats["tickets_processed"] += 1

            except Exception as e:
                logger.exception(
                    f"Error processing ticket {ticket_data.get('TicketID', 'unknown')}"
                )

        # Calculate run statistics
        run_duration = (datetime.now() - run_start).total_seconds()

        run_stats = {
            "tickets_found": len(tickets),
            "tickets_processed": processed_count,
            "actions_executed": self.stats["actions_executed"],
            "actions_succeeded": self.stats["actions_succeeded"],
            "actions_failed": self.stats["actions_failed"],
            "actions_skipped": self.stats["actions_skipped"],
            "duration_seconds": run_duration,
        }

        # Log summary
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Run #{self.stats['runs']} completed")
        logger.info(f"Tickets: {processed_count}/{len(tickets)} processed")
        logger.info(
            f"Actions: {run_stats['actions_executed']} executed "
            f"({run_stats['actions_succeeded']} succeeded, "
            f"{run_stats['actions_failed']} failed, "
            f"{run_stats['actions_skipped']} skipped)"
        )
        logger.info(f"Duration: {run_duration:.2f} seconds")
        logger.info(f"{'=' * 70}\n")

        return run_stats

    def run_continuous(self, interval_seconds: int = 300):
        """
        Run the daemon continuously with periodic polling.

        Args:
            interval_seconds: Seconds to wait between runs (default: 300 = 5 minutes)
        """
        logger.info(f"Starting continuous daemon mode (interval: {interval_seconds}s)")
        logger.info("Press Ctrl+C to stop")

        while True:
            try:
                self.run_once()

                logger.info(f"Waiting {interval_seconds} seconds until next run...")
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                logger.info("\nDaemon stopped by user")
                break

            except Exception as e:
                logger.exception("Unexpected error in daemon loop")
                logger.info(f"Waiting {interval_seconds} seconds before retry...")
                time.sleep(interval_seconds)

    def get_stats(self) -> Dict[str, Any]:
        """Get daemon statistics."""
        return self.stats.copy()


@handle_keyboard_interrupt("Script interrupted by user")
def main():
    """Main entry point for ticket queue daemon."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="TeamDynamix Ticket Queue Daemon - Process tickets from a TDX report with idempotent actions"
    )
    parser.add_argument(
        "--report-id",
        type=int,
        help="TeamDynamix report ID to monitor (default: from DAEMON_REPORT_ID env var)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously as daemon (default: run once)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Polling interval in seconds for daemon mode (default: 300 = 5 minutes)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Simulate actions without making changes"
    )
    parser.add_argument(
        "--log",
        nargs="?",
        const="ticket_queue_daemon.log",
        help="Enable logging to file (default: ticket_queue_daemon.log)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Set up logging
    log_level = getattr(logging, args.log_level)

    if args.log:
        log_path = args.log
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
        )
        logger.info(f"Logging to file: {log_path}")
    else:
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )

    # Display mode indicators
    if args.dry_run:
        logger.info("*** DRY RUN MODE - No changes will be made ***")

    if args.daemon:
        logger.info(f"*** DAEMON MODE - Polling every {args.interval} seconds ***")

    # Load environment variables
    logger.info("Loading environment variables...")
    load_dotenv()

    # Get configuration
    report_id = args.report_id or int(os.getenv("DAEMON_REPORT_ID", "0"))
    if not report_id:
        logger.error(
            "Report ID not specified. Use --report-id or set DAEMON_REPORT_ID env var"
        )
        sys.exit(1)

    TDX_BASE_URL = os.getenv("TDX_BASE_URL")
    TDX_APP_ID = os.getenv("TDX_APP_ID")
    TDX_API_TOKEN = os.getenv("TDX_API_TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")

    if not all([TDX_BASE_URL, TDX_APP_ID, TDX_API_TOKEN, DATABASE_URL]):
        logger.error("Missing required environment variables")
        logger.error("Required: TDX_BASE_URL, TDX_APP_ID, TDX_API_TOKEN, DATABASE_URL")
        sys.exit(1)

    # Detect environment
    is_sandbox = "SB" in TDX_BASE_URL or "sandbox" in TDX_BASE_URL.lower()
    environment = "SANDBOX" if is_sandbox else "PRODUCTION"
    logger.info(f"Environment: {environment}")
    logger.info(f"TDX Base URL: {TDX_BASE_URL}")
    logger.info(f"Report ID: {report_id}")

    # Initialize facade and adapters
    logger.info("Initializing TeamDynamix facade...")
    facade = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, TDX_API_TOKEN)

    logger.info("Initializing database connection...")
    db_adapter = PostgresAdapter(DATABASE_URL)

    logger.info("Initializing state tracker...")
    state_tracker = StateTracker(db_adapter)

    # Configure actions
    logger.info("Configuring actions...")

    # Multi-action workflow: Add assets + post cumulative summary
    actions = [
        # Phase 1: Automatically add computer assets to tickets
        AddAssetAction(
            add_summary_comment=True,  # Add summary for cumulative comment
            max_assets_to_add=10,  # Safety limit
            skip_if_requestor_asset_exists=True,  # Skip requestor fallback if assets exist
            active_status_only=True,  # Only active assets
            computer_form_id=2448,  # Computer Form
            database_url=DATABASE_URL,  # Use bronze layer queries
            version="v2",
        ),
        # Phase 2: Post cumulative summary of all actions
        SummaryCommentAction(
            comment_prefix="🤖 Automated Actions Summary",
            is_private=True,  # Private comment
            skip_if_empty=True,  # Only post if actions executed
            separator="\n",
            version="v2",
        ),
    ]

    logger.info(f"Configured {len(actions)} action(s):")
    for action in actions:
        logger.info(f"  - {action}")

    # Initialize daemon
    daemon = TicketQueueDaemon(
        facade=facade,
        state_tracker=state_tracker,
        report_id=report_id,
        actions=actions,
        dry_run=args.dry_run,
    )

    # Run daemon
    try:
        if args.daemon:
            daemon.run_continuous(interval_seconds=args.interval)
        else:
            daemon.run_once()

    finally:
        # Cleanup
        logger.info("Closing database connection...")
        db_adapter.close()
        logger.info("Daemon shutdown complete")


if __name__ == "__main__":
    main()
