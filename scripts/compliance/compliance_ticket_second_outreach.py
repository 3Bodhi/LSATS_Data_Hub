from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import argparse
import logging
import sys
import functools

def handle_keyboard_interrupt(exit_message="Script interrupted by user"):
    """Decorator to handle KeyboardInterrupt and exit gracefully."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt:
                logging.info(f"\n{exit_message}")
                sys.exit(0)
        return wrapper
    return decorator

@handle_keyboard_interrupt("Script interrupted by user")
def main():
    """Main function for compliance ticket second outreach script."""
    try:
        # Add command line argument parsing
        parser = argparse.ArgumentParser(description='Update Non-Responsive Compliance Tickets -- Second Outreach.')
        parser.add_argument('--dry-run', action='store_true', help='Run without making any changes to tickets')
        parser.add_argument('--log', nargs='?', const='compliance_update1.log',
                            help='Enable logging to a file. Optionally specify a file path (defaults to compliance_update.log in current directory)')
        args = parser.parse_args()

        # Set up logging
        if args.log:
            log_path = args.log
            log_dir = os.path.dirname(log_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Configure logging to file
            logging.basicConfig(
                encoding='utf-8',
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_path),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            logging.info(f"Logging to file: {log_path}")
        else:
            # Configure logging to console only
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[logging.StreamHandler(sys.stdout)]
            )

        # If in dry run mode, display a notification
        if args.dry_run:
            logging.info("*** DRY RUN MODE ENABLED - No changes will be made to tickets ***")

        # Load environment variables
        load_dotenv()
        TDX_BASE_URL = os.getenv('TDX_BASE_URL')
        TDX_APP_ID = os.getenv('TDX_APP_ID')
        API_TOKEN = os.getenv('TDX_API_TOKEN')

        # Validate environment variables
        if not all([TDX_BASE_URL, TDX_APP_ID, API_TOKEN]):
            logging.error("Missing required environment variables: TDX_BASE_URL, TDX_APP_ID, or TDX_API_TOKEN")
            sys.exit(2)

        # Define ticket status IDs
        AWAITING_INPUT_STATUS_ID = 620
        UNIFIED_LIST_MANAGEMENT_GROUP_ID = 1678
        TDX_REPORT_ID = 31623  # ID of the report that contains the tickets

        # Initialize TeamDynamix service
        logging.info("Initializing TeamDynamix service...")
        try:
            tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
        except Exception as e:
            logging.error(f"Failed to initialize TeamDynamix service: {str(e)}")
            sys.exit(1)

        # Wrapper for tdx_service.tickets.update_ticket that respects dry run mode
        def safe_update_ticket(id, comments, private, commrecord, rich=True, status=0, cascade=False, notify='null'):
            if args.dry_run:
                logging.info(f"  [DRY RUN] Would update ticket {id}")
                logging.info(f"  [DRY RUN] Comments: {comments[:50]}..." if len(comments) > 50 else f"  [DRY RUN] Comments: {comments}")
                logging.info(f"  [DRY RUN] Private: {private}, Communication Record: {commrecord}, Rich HTML: {rich}")
                logging.info(f"  [DRY RUN] Status: {status}, Cascade: {cascade}, Notify: {notify}")
            else:
                return tdx_service.tickets.update_ticket(
                    id=id,
                    comments=comments,
                    private=private,
                    commrecord=commrecord,
                    status=status,
                    cascade=cascade,
                    notify=notify,
                    rich=rich
                )

        # Get tickets directly from TeamDynamix report
        logging.info(f"Fetching tickets from TeamDynamix report {TDX_REPORT_ID}...")
        try:
            report_data = tdx_service.reports.get_report(TDX_REPORT_ID, withData=True)
            if not report_data or 'DataRows' not in report_data:
                logging.error(f"Failed to retrieve data from report {TDX_REPORT_ID}")
                sys.exit(1)

            tickets = report_data['DataRows']
            logging.info(f"Retrieved {len(tickets)} tickets from report")

        except Exception as e:
            logging.error(f"Error retrieving report: {str(e)}")
            sys.exit(1)

        # Counter for tracking processed tickets
        tickets_processed = 0
        tickets_updated = 0

        # Process each ticket from the report
        for ticket_data in tickets:
            ticket_id = ticket_data.get('TicketID')
            customer_name = ticket_data.get('CustomerName')
            status_name = ticket_data.get('StatusName')

            if not ticket_id:
                logging.warning(f"Missing ticket ID in report data entry")
                continue

            logging.info(f"Processing ticket {ticket_id} for {customer_name}")
            tickets_processed += 1

            try:
                # Get full ticket details from TeamDynamix
                ticket = tdx_service.tickets.get_ticket(ticket_id)

                if not ticket:
                    logging.warning(f"Could not retrieve ticket {ticket_id}")
                    continue

                # Check if this ticket meets our criteria for sending a second outreach
                ticket_status_id = ticket.get('StatusID')
                responsible_group_id = ticket.get('ResponsibleGroupID')
                requestor_email = ticket.get('RequestorEmail')

                if ticket_status_id == AWAITING_INPUT_STATUS_ID and responsible_group_id == UNIFIED_LIST_MANAGEMENT_GROUP_ID:
                    logging.info(f"Ticket {ticket_id} is Awaiting Input and owned by Unified List Management")

                    # Update ticket in TeamDynamix
                    # Repost the description and notify the user
                    comment = ticket.get('Description', '')
                    safe_update_ticket(
                        id=ticket_id,
                        comments=comment,
                        private=False,
                        commrecord=True,
                        notify=[requestor_email],
                        rich=True
                    )

                    logging.info(f"Sent second outreach for ticket {ticket_id} to {requestor_email}")
                    tickets_updated += 1
                else:
                    group_name = ticket.get('ResponsibleGroupName', 'Unknown')
                    logging.info(f"Ticket {ticket_id} has status {status_name} (ID: {ticket_status_id}) and responsible group {group_name} (ID: {responsible_group_id}), no action needed")

            except Exception as e:
                logging.error(f"Error processing ticket {ticket_id}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())

        logging.info("Processing complete!")
        logging.info(f"Processed {tickets_processed} tickets, sent {tickets_updated} second outreach notifications")
        logging.info("*** NOTE: This was a dry run, no changes were made ***" if args.dry_run else "All changes have been applied.")

        # Exit successfully
        sys.exit(0)

    except KeyboardInterrupt:
        logging.info("\nScript interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.info("\nScript interrupted by user.")
        sys.exit(130)
