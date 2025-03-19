from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
import re
import argparse
import logging
import sys
from datetime import timedelta
from urllib.parse import urljoin
from pathlib import Path

# Add command line argument parsing
parser = argparse.ArgumentParser(description='Update Non-Responsive Compliance Tickets -- Second Outreach.')
parser.add_argument('--dry-run', action='store_true', help='Run without making any changes to sheets or tickets')
parser.add_argument('--log', nargs='?', const='compliance_update.log',
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
    logging.info("*** DRY RUN MODE ENABLED - No changes will be made to sheets or tickets ***")

# Load environment variables
load_dotenv()
TDX_BASE_URL = os.getenv('TDX_BASE_URL')
path_replacements = {
    '/SBTDWebApi/api': '/SBTDNext/',
    '/TDWebApi/api': '/TDNext/'
}
TDX_TICKET_DOMAIN = TDX_BASE_URL
for old_path, new_path in path_replacements.items():
    TDX_TICKET_DOMAIN = TDX_TICKET_DOMAIN.replace(old_path, new_path)
TDX_APP_ID = os.getenv('TDX_APP_ID')
API_TOKEN = os.getenv('TDX_API_TOKEN')
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME')

# Define ticket status IDs and their meanings
RESOLVED_STATUS_ID = 118
CANCELLED_STATUS_ID = 120

# Initialize services
logging.info("Initializing services...")
tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
sheet_adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
sheet = Sheet(sheet_adapter, SPREADSHEET_ID, SHEET_NAME, header_row=1)

# Current date for comparison
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%m/%d/%Y")
current_month = current_date.strftime("%B")

# Function to extract ticket number from hyperlink
def extract_ticket_number(hyperlink_cell):
    if not hyperlink_cell or not isinstance(hyperlink_cell, str):
        return None

    # Match either just the ID number or the full hyperlink format
    if hyperlink_cell.isdigit():
        return hyperlink_cell

    match = re.search(r'TicketID=(\d+)|", (\d+)\)', hyperlink_cell)
    if match:
        # Return the first non-None group
        return next((group for group in match.groups() if group is not None), None)
    return None

# Function to convert zero-based column index to spreadsheet column letter (A, B, C, ..., Z, AA, AB, etc.)
def index_to_column_letter(index):
    """
    Convert a zero-based column index to an Excel-style column letter.
    Examples:
    - 0 -> 'A'
    - 25 -> 'Z'
    - 26 -> 'AA'
    - 701 -> 'ZZ'
    - 702 -> 'AAA'

    This works for columns beyond 'ZZ', supporting the full Excel column naming pattern:
    A-Z, AA-AZ, BA-BZ, ..., ZA-ZZ, AAA-AAZ, etc.
    """
    result = ""
    while index >= 0:
        remainder = index % 26
        result = chr(65 + remainder) + result
        index = index // 26 - 1
    return result

# Wrapper for sheet.write_data that respects dry run mode
def safe_write_data(range_name, values, value_InputOption="RAW"):
    if args.dry_run:
        logging.info(f"  [DRY RUN] Would write to sheet range {range_name}: {values}")
    else:
        return sheet.write_data(range_name, values, value_InputOption)

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

# Load sheet data
logging.info("Loading sheet data...")
the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[the_list['Delete'] == 'FALSE'] # Don't send email if slated to be deleted anyway'

column_indices = {col: idx for idx, col in enumerate(sheet.data[1])}

# Find rows with "First Outreach" status and no response
logging.info("Finding tickets with 'First Outreach' status and no response...")
first_outreach_rows = the_list[the_list['Status'] == 'First outreach']
first_outreach_rows = first_outreach_rows[first_outreach_rows['Response'] == ""]

# Process each ticket
logging.info(f"Found {len(first_outreach_rows)} tickets to process")
for idx, row in first_outreach_rows.iterrows():
    # Convert from 0-based pandas index to 1-based sheet row (accounting for header)
    sheet_row = idx + 2  # +1 for 0-indexing to 1-indexing, +1 for header row

    # Extract ticket number and owner email
    ticket_number = extract_ticket_number(row.get('Ticket'))
    print(row.get('Ticket'))
    owner_email = row.get('Owner Email')
    owner = row.get('Owner')

    if not ticket_number:
        logging.warning(f"Row {sheet_row}: Could not extract ticket number from TDX# {row.get('Ticket') if row.get('Ticket') else 'Unknown'} Computer {row.get('Computer Name')}")
        continue

    logging.info(f"Processing ticket {ticket_number} for {owner} (Row {sheet_row}, email: {owner_email})")

    try:
        # Get ticket details from TeamDynamix
        ticket = tdx_service.tickets.get_ticket(ticket_number)

        if not ticket:
            logging.warning(f"Row {sheet_row}: Could not retrieve ticket {ticket_number}")
            continue

        # Check ticket status ID and handle accordingly
        ticket_status_id = ticket.get('StatusID')

        # Handle Resolved tickets (StatusID 118)
        if ticket_status_id == RESOLVED_STATUS_ID:
            logging.info(f"Row {sheet_row}: Ticket {ticket_number} is Resolved (StatusID {ticket_status_id})")

            # Get ticket feed to check if user responded
            ticket_feed = tdx_service.tickets.get_ticket_feed(ticket_number)
            ticket_requestor = ticket.get('RequestorName')
            days_since_response = tdx_service.days_since_requestor_response(ticket_number, ticket_requestor)

            response_col = index_to_column_letter(column_indices['Response'])
            resolution_col = index_to_column_letter(column_indices['Resolution'])

            if days_since_response != float('inf'):
                # User responded at some point
                logging.info(f"Row {sheet_row}: User responded before resolution")
                safe_write_data(f"{response_col}{sheet_row}", [["Responded after 1st email"]])
                safe_write_data(f"{resolution_col}{sheet_row}", [["Fixed w/response"]])
                logging.info(f"Row {sheet_row}: {'Would update' if args.dry_run else 'Updated'} Response to 'Responded after 1st email' and Resolution to 'Fixed w/response'")
            else:
                # User never responded
                logging.info(f"Row {sheet_row}: User never responded before resolution")
                safe_write_data(f"{response_col}{sheet_row}", [["Fixed without response"]])
                safe_write_data(f"{resolution_col}{sheet_row}", [["Fixed w/out response"]])
                logging.info(f"Row {sheet_row}: {'Would update' if args.dry_run else 'Updated'} Response to 'Fixed without response' and Resolution to 'Fixed w/out response'")

            # Skip to the next ticket
            continue

        # Handle Cancelled tickets (StatusID 120)
        elif ticket_status_id == CANCELLED_STATUS_ID:
            logging.info(f"Row {sheet_row}: Ticket {ticket_number} is Cancelled (StatusID {ticket_status_id})")

            response_col = index_to_column_letter(column_indices['Response'])
            resolution_col = index_to_column_letter(column_indices['Resolution'])
            notes_col = index_to_column_letter(column_indices['Notes'])

            # Update columns
            safe_write_data(f"{response_col}{sheet_row}", [["OTHER"]])
            safe_write_data(f"{resolution_col}{sheet_row}", [["OTHER"]])

            # Get current notes value and append to it
            current_notes = row.get('Notes', '')
            updated_notes = f"{current_notes} Ticket was canceled." if current_notes else "Ticket was canceled."
            safe_write_data(f"{notes_col}{sheet_row}", [[updated_notes]])

            logging.info(f"Row {sheet_row}: {'Would update' if args.dry_run else 'Updated'} Response to 'OTHER', Resolution to 'OTHER', and added note about cancellation")

            # Skip to the next ticket
            continue

        # Get days since last requestor activity using the facade method
        ticket_requestor = ticket.get('RequestorName')
        days_since_response = tdx_service.days_since_requestor_response(ticket_number, ticket_requestor)

        # Get feed details for debugging
        ticket_feed = tdx_service.tickets.get_ticket_feed(ticket_number)
        logging.info(f"Ticket has {len(ticket_feed) if ticket_feed else 0} feed entries")

        # Output key information for debugging
        logging.info(f"Requestor: {ticket_requestor}")
        logging.info(f"Days since last requestor response: {days_since_response}")

        # Check if we've had a response (infinity means no response ever)
        if days_since_response == float('inf'):
            logging.info("Requestor has never responded to the ticket")

        # Determine actions based on response time
        needs_update = days_since_response == float('inf') or days_since_response > 7
        user_responded = days_since_response < 8 and days_since_response != float('inf') # should be +1 from days_since respone or no action on x day

        # Update sheet and ticket as needed
        if needs_update:
            logging.info(f"Row {sheet_row}: No response in over a week or never responded, sending second outreach")

            # For testing purposes, override email
            notification_email = owner_email  # Use this for production
            #notification_email = "myodhes@umich.edu"  # Use this for testing

            # Update ticket in TeamDynamix
            # Repost the description and notify the user
            comment = ticket.get('Description', '')
            safe_update_ticket(
                id=ticket_number,
                comments=comment,
                private=False,
                commrecord=True,
                notify=[notification_email],
                rich=True
            )

            # Update the sheet - set Status to "Second outreach"
            status_col = index_to_column_letter(column_indices['Status'])
            safe_write_data(f"{status_col}{sheet_row}", [["Second outreach"]])
            logging.info(f"Row {sheet_row}: {'Would update' if args.dry_run else 'Updated'} to 'Second outreach'")

        elif user_responded:
            logging.info(f"Row {sheet_row}: User responded {days_since_response} days ago")

            # Update Response column to "Responded after 1st email"
            response_col = index_to_column_letter(column_indices['Response'])
            safe_write_data(f"{response_col}{sheet_row}", [["Responded after 1st email"]])
            logging.info(f"Row {sheet_row}: {'Would update' if args.dry_run else 'Updated'} Response to 'Responded after 1st email'")

    except Exception as e:
        logging.error(f"Error processing row {sheet_row}, ticket {ticket_number}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

logging.info("Processing complete!")
logging.info("*** NOTE: This was a dry run, no changes were made ***" if args.dry_run else "All changes have been applied.")
