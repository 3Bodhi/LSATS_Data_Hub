from teamdynamix import TeamDynamixFacade
from google_drive import GoogleSheetsAdapter, Sheet
from dotenv import load_dotenv
import os
import argparse
import logging
import sys
import re
import functools

# Define ticket status IDs and constants at module level
AWAITING_INPUT_STATUS_ID = 620
UNIFIED_LIST_MANAGEMENT_GROUP_ID = 1678
TDX_REPORT_ID = 31623  # ID of the report that contains the tickets, "Unified List Mgmt - Awaiting Input"
UNIT_ASSIGNMENTS_SPREADSHEET_ID = '1Lb11KyJjsG_peafphDrQQIYlFbqbYP7UiS6ZwHevris' # LSA Finance BA BO Unit Assignments
UNIT_ASSIGNMENTS_SHEET_NAME = 'Unit Assignments'


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

# Create department to CA email lookup table
def create_department_ca_lookup(sheet):
    """
    Create a lookup table mapping department names to CA emails.
    """
    logging.info("Creating department to CA email lookup table...")
    dept_ca_map = {}

    try:
        # Get column indices for "Departments" and "CA Email"
        column_names = sheet.get_column_names()

        if "Department" not in column_names or "CA Email" not in column_names:
            logging.error(f"Required columns not found in sheet. Available columns: {column_names}")
            return {}

        dept_index = column_names.index("Department")
        ca_email_index = column_names.index("CA Email")

        # Process each row in the sheet
        for row in sheet.data[1:]:  # Skip header row
            if len(row) > max(dept_index, ca_email_index):
                dept_name = row[dept_index].strip() if row[dept_index] else ""
                ca_email = row[ca_email_index].strip() if ca_email_index < len(row) and row[ca_email_index] else ""

                if dept_name and ca_email:
                    dept_ca_map[dept_name] = ca_email

        logging.info(f"Created lookup table with {len(dept_ca_map)} department-CA mappings")
        return dept_ca_map

    except Exception as e:
        logging.error(f"Error creating department-CA lookup table: {str(e)}")
        return {}

def create_department_ca_lastname_lookup(sheet):
    """
    Create a lookup table mapping department names to CA last names.
    Extracts last names from the "**Chief Administrator / Department Manager" column.
    """
    logging.info("Creating department to CA last name lookup table...")
    dept_ca_lastname_map = {}

    try:
        # Get column indices for "Department" and "**Chief Administrator / Department Manager"
        column_names = sheet.get_column_names()

        dept_col_name = "Department"
        ca_manager_col_name = "Chief Administrator / Department Manager"

        if dept_col_name not in column_names or ca_manager_col_name not in column_names:
            logging.error(f"Required columns not found in sheet. Available columns: {column_names}")
            logging.error(f"Looking for: '{dept_col_name}' and '{ca_manager_col_name}'")
            return {}

        dept_index = column_names.index(dept_col_name)
        ca_manager_index = column_names.index(ca_manager_col_name)

        # Process each row in the sheet
        for row in sheet.data[1:]:  # Skip header row
            if len(row) > max(dept_index, ca_manager_index):
                dept_name = row[dept_index].strip() if row[dept_index] else ""
                ca_manager_full = row[ca_manager_index].strip() if ca_manager_index < len(row) and row[ca_manager_index] else ""

                # Extract last name (everything before the first comma)
                ca_lastname = ca_manager_full.split(',')[0].strip() if ca_manager_full else ""

                if dept_name and ca_lastname:
                    dept_ca_lastname_map[dept_name] = ca_lastname

        logging.info(f"Created CA last name lookup table with {len(dept_ca_lastname_map)} department-CA lastname mappings")
        return dept_ca_lastname_map

    except Exception as e:
        logging.error(f"Error creating department-CA lastname lookup table: {str(e)}")
        return {}

# Clean department name for lookup
def clean_department_name(department_name):
    """
    Clean department name by removing numbers and trailing spaces.
    """
    if not department_name:
        return ""

    # Remove numbers and trim spaces
    cleaned_name = re.sub(r'\d+', '', department_name).strip()
    return cleaned_name

def add_finanical_owners(ticket_id, tdx_service, safe_add_ticket_contact):
    assets = tdx_service.tickets.get_ticket_assets(ticket_id)
    fo_email = []
    if assets:
        for asset in assets:
            computer = tdx_service.assets.get_asset(asset["BackingItemID"]) # the asset id, ID is CI ID
            o_uid = computer["OwningCustomerID"]
            o_name = computer["OwningCustomerName"]
            fo_data = next((entry for entry in computer["Attributes"] if entry.get("ID") == 10896), None) # Financial owner attribute
            if fo_data:
                fo_uid = fo_data.get("Value")
                fo_name = fo_data.get("ValueText")
            else:
                logging.warning(f"No Financial Owner Data Found for asset {computer['Name']} ")
            if o_uid != fo_uid:
                safe_add_ticket_contact(ticket_id,fo_uid)
                logging.info(f"Added {asset['Name']}'s Financial owner, {fo_name}, to ticket {ticket_id}.")
                user = tdx_service.users.get_user_by_uid(fo_uid)
                email = user["PrimaryEmail"]
                fo_email.append(email)
            else:
                logging.info(f"{o_name} is {fo_name}. No Contact Added. Owner and Financial Owner are Identical.")
        return fo_email
    else:
        logging.warning(f"No assets found for ticket # {ticket_id}")

def add_chief_administrators(ticket_id, tdx_service, dept_ca_map, dept_ln_map, safe_add_ticket_contact):
    assets = tdx_service.tickets.get_ticket_assets(ticket_id)
    ca_emails = []
    if assets: # Get each assets Dept, CA email
        for asset in assets:
            logging.info(f"Finding CA for asset {asset['Name']}")
            computer = tdx_service.assets.get_asset(asset["BackingItemID"]) # BackingItemID is asset id, ID is CI ID
            dept_name = computer["OwningDepartmentName"]
            cleaned_dept_name = clean_department_name(dept_name)
            ca_email = dept_ca_map.get(cleaned_dept_name)

            if ca_email: # Get CA's UID
                logging.info(f"Found CA {ca_email} for department '{cleaned_dept_name}'")

                # Extract uniqname from CA email
                ca_uniqname = ca_email.split('@')[0] if '@' in ca_email else ca_email

                # Get CA's UID
                ca_uid = tdx_service.users.get_user_attribute(ca_uniqname, 'UID')

                if ca_uid: # Add CA as contact to the ticket
                    safe_add_ticket_contact(ticket_id, ca_uid)
                    logging.info(f"Added CA {ca_email} (UID: {ca_uid}) as contact to ticket {ticket_id}")
                    # Add CA to notification list
                    ca_emails.append(ca_email) # add CA to contact
                else: # Search for CA by Last Name
                    last_name = dept_ln_map.get(cleaned_dept_name)
                    logging.info(f"Could not find UID for CA {ca_email} in ca_email lookup. Searching by last name {last_name}...")
                    ln_results = tdx_service.users.search_user({"SearchText":last_name})
                    logging.debug(f"Last Name Search Results: \n {ln_results}")
                    ca_results = []
                    for item in ln_results: # Find CAs in Search
                        if "Chief Administrator" in item["Title"]:
                            ca_results.append(item)
                    logging.debug(ca_results)
                    if ca_results: # Try to Add Correct CA
                        if len(ca_results) > 1: # Find Correct CA by matching Departments
                            logging.warning(f"multiple Chief Adminstrators found. Try to match on Department. ")
                            logging.debug(ca_results)
                            for item in ca_results: # Check if department matches
                                if dept_name == item["DefaultAccountName"]: # add CA
                                    ca_uid = item["UID"]
                                    ca_email = item["UserName"]
                                    logging.info(f"Found UID for {ca_email} with deparment {dept_name} ")
                            if not ca_uid: # Warn that no CA was found
                                logging.warning("Chief Admin Could not be determined.")
                                logging.debug(f"Administrators found in search:\n\n{ca_results}")
                                break
                        else: # add CA
                            ca_uid = ca_results[0]["UID"]
                            ca_email = ca_results[0]["UserName"]
                            dept_ca_map[cleaned_dept_name] = ca_email
                            logging.info(f"Found UID for {ca_email}")

                        # Add CA as contact to the ticket
                        safe_add_ticket_contact(ticket_id, ca_uid)
                        logging.info(f"Added CA {ca_email} (UID: {ca_uid}) as contact to ticket {ticket_id}")

                        # Add CA to notification list
                        ca_email = ca_email if "@" in ca_email else ca_email + "@umich.edu"
                        ca_emails.append(ca_email)# search
            else:
                logging.info(f"No ca_email found for {asset['Name']}")
        return [email if "@" in email else email + "@umich.edu" for email in ca_emails]
    else:
        logging.warning(f"No assets found for ticket # {ticket_id}")

# Wrapper for tdx_service.tickets.update_ticket that respects dry run mode
def safe_update_ticket(tdx_service, args, id, comments, private, commrecord, rich=True, status=0, cascade=False, notify=None):
    if notify is None:
        notify = ['null']

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

# Wrapper for tdx_service.tickets.add_ticket_contact that respects dry run mode
def safe_add_ticket_contact(tdx_service, args, ticket_id, contact_uid):
    if args.dry_run:
        logging.info(f"  [DRY RUN] Would add contact {contact_uid} to ticket {ticket_id}")
    else:
        return tdx_service.tickets.add_ticket_contact(ticket_id, contact_uid)

@handle_keyboard_interrupt("Script interrupted by user")
def main():
    """Main function for compliance third outreach script."""
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description='Update Non-Responsive Compliance Tickets -- Third Outreach with CA notification.')
    parser.add_argument('--dry-run', action='store_true', help='Run without making any changes to tickets')
    parser.add_argument('--log', nargs='?', const='compliance_update_third_outreach.log',
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
        logging.info("*** DRY RUN MODE ENABLED - No changes will be made to tickets ***")

    # Load environment variables
    load_dotenv()
    TDX_BASE_URL = os.getenv('TDX_BASE_URL')
    TDX_APP_ID = os.getenv('TDX_APP_ID')
    API_TOKEN = os.getenv('TDX_API_TOKEN')
    CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')

    # Initialize TeamDynamix service
    logging.info("Initializing TeamDynamix service...")
    try:
        tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
    except Exception as e:
        logging.error(f"Failed to initialize TeamDynamix service: {str(e)}")
        return 1

    # Initialize Google Sheets adapter
    logging.info("Initializing Google Sheets adapter...")
    try:
        sheets_adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
        sheet = Sheet(sheets_adapter, UNIT_ASSIGNMENTS_SPREADSHEET_ID, UNIT_ASSIGNMENTS_SHEET_NAME, header_row=5)
        logging.info(f"Successfully connected to Google Sheet '{UNIT_ASSIGNMENTS_SHEET_NAME}'")
    except Exception as e:
        logging.error(f"Failed to initialize Google Sheets adapter: {str(e)}")
        return 1

    # Build the department to CA email lookup table
    dept_ca_map = create_department_ca_lookup(sheet)
    dept_ln_map = create_department_ca_lastname_lookup(sheet)

    # Create wrapper functions with bound arguments
    def safe_add_ticket_contact_bound(ticket_id, contact_uid):
        return safe_add_ticket_contact(tdx_service, args, ticket_id, contact_uid)

    def safe_update_ticket_bound(id, comments, private, commrecord, rich=True, status=0, cascade=False, notify=None):
        return safe_update_ticket(tdx_service, args, id, comments, private, commrecord, rich, status, cascade, notify)

    # Get tickets directly from TeamDynamix report
    logging.info(f"Fetching tickets from TeamDynamix report {TDX_REPORT_ID}...")
    try:
        report_data = tdx_service.reports.get_report(TDX_REPORT_ID, withData=True)
        if not report_data or 'DataRows' not in report_data:
            logging.error(f"Failed to retrieve data from report {TDX_REPORT_ID}")
            return 1

        tickets = report_data['DataRows']
        logging.info(f"Retrieved {len(tickets)} tickets from report")

    except Exception as e:
        logging.error(f"Error retrieving report: {str(e)}")
        return 1

    # Counter for tracking processed tickets
    tickets_processed = 0
    tickets_updated = 0
    cas_added = 0

    # Process each ticket from the report
    for ticket_data in tickets: # add CA and notify
        ticket_id = ticket_data.get('TicketID')
        customer_name = ticket_data.get('CustomerName')
        status_name = ticket_data.get('StatusName')

        if not ticket_id: # warn but move on
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

            # Check if this ticket meets our criteria for sending a third outreach
            ticket_status_id = ticket.get('StatusID')
            responsible_group_id = ticket.get('ResponsibleGroupID')
            requestor_email = ticket.get('RequestorEmail')
            account_name = ticket.get('AccountName')

            # This if check is technically not needed, for Unified List Mgmt - Awaiting Input, but allows the updating of reports w/out awaiting input filtering.
            if ticket_status_id == AWAITING_INPUT_STATUS_ID and responsible_group_id == UNIFIED_LIST_MANAGEMENT_GROUP_ID: # Add CAs and notify
                logging.info(f"Ticket {ticket_id} is Awaiting Input and owned by Unified List Management")

                ca_added = False
                # Find the Financial Owners for the assets on this ticket and add them
                chief_ads = add_chief_administrators(ticket_id, tdx_service, dept_ca_map, dept_ln_map, safe_add_ticket_contact_bound) #TODO: Separate CA lookup from contact add
                fin_owners = add_finanical_owners(ticket_id, tdx_service, safe_add_ticket_contact)
                chief_ads = chief_ads + fin_owners
                # Prepare notification list
                notify_list = [requestor_email]
                if chief_ads and chief_ads != notify_list: # Avoid adding CA to notify twice if owner is CA
                    chief_ads = set(chief_ads) # only add unique fin owners.
                    notify_list.extend(chief_ads)
                    ca_added = True
                    cas_added += len((chief_ads))
                else:
                    logging.info(f"Chief Administrator {notify_list} is the owner of the asset(s). ")

                # Update ticket in TeamDynamix
                original_description = ticket.get('Description', '')

                # Prepend the CA notification message if a CA was added
                if ca_added:
                    description = f"CA added for awareness:\n\n{original_description}"
                else:
                    description = original_description

                # Update ticket and notify all recipients
                safe_update_ticket_bound(
                    id=ticket_id,
                    comments=description,
                    private=False,
                    commrecord=True,
                    notify=notify_list,
                    rich=True
                )

                logging.info(f"Sent third outreach for ticket {ticket_id} to {', '.join(notify_list)}")
                tickets_updated += 1
            else:
                group_name = ticket.get('ResponsibleGroupName', 'Unknown')
                logging.info(f"Ticket {ticket_id} has status {status_name} (ID: {ticket_status_id}) and responsible group {group_name} (ID: {responsible_group_id}), no action needed")

        except Exception as e:
            logging.error(f"Error processing ticket {ticket_id}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())

    logging.info("Processing complete!")
    logging.info(f"Processed {tickets_processed} tickets, sent {tickets_updated} third outreach notifications, added {cas_added} CAs to tickets")
    logging.info("*** NOTE: This was a dry run, no changes were made ***" if args.dry_run else "All changes have been applied.")

    return 0

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\nScript interrupted by user.")
        sys.exit(130)
