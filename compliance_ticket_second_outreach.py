from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
import re
from datetime import timedelta
from urllib.parse import urljoin

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

# Initialize services
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

# Load sheet data
print("Loading sheet data...")
the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[the_list['Delete'] == 'FALSE'] # Don't send email if slated to be deleted anyway'

column_indices = {col: idx for idx, col in enumerate(sheet.data[1])}

# Find rows with "First Outreach" status and no response
print("Finding tickets with 'First Outreach' status and no response...")
print()
first_outreach_rows = the_list[the_list['Status'] == 'First outreach']
first_outreach_rows = first_outreach_rows[first_outreach_rows['Response'] == ""]

# Process each ticket
print(f"Found {len(first_outreach_rows)} tickets to process")
for idx, row in first_outreach_rows.iterrows():
    # Convert from 0-based pandas index to 1-based sheet row (accounting for header)
    sheet_row = idx + 2  # +1 for 0-indexing to 1-indexing, +1 for header row

    # Extract ticket number and owner email
    ticket_number = extract_ticket_number(row.get('Ticket'))
    owner_email = row.get('Owner Email')
    owner = row.get('Owner')

    if not ticket_number:
        print(f"Row {sheet_row}: Could not extract ticket number from TDX# {row.get('Ticket') if row.get('Ticket') else "Unknown"} Computer {row.get('Computer Name')}")
        continue

    print(f"Processing ticket {ticket_number} for {owner} (Row {sheet_row}, email: {owner_email})")

    try:
        # Get ticket details from TeamDynamix
        ticket = tdx_service.tickets.get_ticket(ticket_number)

        if not ticket:
            print(f"Row {sheet_row}: Could not retrieve ticket {ticket_number}")
            continue

        # Get days since last requestor activity using the new facade method
        ticket_requestor = ticket.get('RequestorName')
        days_since_response = tdx_service.days_since_requestor_response(ticket_number, ticket_requestor)

        # Get feed details for debugging
        ticket_feed = tdx_service.tickets.get_ticket_feed(ticket_number)
        print(f"Ticket has {len(ticket_feed) if ticket_feed else 0} feed entries")

        # Output key information for debugging
        print(f"Requestor: {ticket_requestor}")
        print(f"Days since last requestor response: {days_since_response}")

        # Check if we've had a response (infinity means no response ever)
        if days_since_response == float('inf'):
            print("Requestor has never responded to the ticket")

        # Determine actions based on response time
        needs_update = days_since_response == float('inf') or days_since_response > 7
        user_responded = days_since_response < 7 and days_since_response != float('inf')

        # Update sheet and ticket as needed
        if needs_update:
            print(f"Row {sheet_row}: No response in over a week or never responded, sending second outreach")

            # For testing purposes, override email
            # notification_email = "myodhes@umich.edu"  # Use this for testing
            notification_email = owner_email  # Use this for production
            print(f"notifying {notification_email}")
            # Update ticket in TeamDynamix
            # Repost the description and notify the user
            comment = ticket.get('Description', '')
            tdx_service.tickets.update_ticket(
                id=ticket_number,
                comments=comment,
                private=False,
                commrecord=True,
                #notify=[notification_email],
                rich=True
            )

            # Update the sheet - set Status to "Second outreach"
            status_col = chr(65 + column_indices['Status'])  # Convert to column letter
            sheet.write_data(f"{status_col}{sheet_row}", [["Second outreach"]])
            print(f"Row {sheet_row}: Updated to 'Second outreach'")

        elif user_responded:
            print(f"Row {sheet_row}: User responded {days_since_response} days ago")

            # Update Response column to "Responded after 1st email"
            response_col = chr(65 + column_indices['Response'])  # Convert to column letter
            sheet.write_data(f"{response_col}{sheet_row}", [["Responded after 1st email"]])
            print(f"Row {sheet_row}: Updated Response to 'Responded after 1st email'")

    except Exception as e:
        print(f"Error processing row {sheet_row}, ticket {ticket_number}: {str(e)}")
        import traceback
        traceback.print_exc()

print("Processing complete!")
