from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
from urllib.parse import urljoin
import argparse
import logging
import sys

# Add command line argument parsing
parser = argparse.ArgumentParser(description='Automate creation of compliance tickets for non-compliant computers.')
parser.add_argument('--dry-run', action='store_true', help='Run without making any changes to tickets or spreadsheets')
parser.add_argument('--log', nargs='?', const='compliance_automator.log',
                    help='Enable logging to a file. Optionally specify a file path (defaults to compliance_automator.log in current directory)')
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
    logging.info("*** DRY RUN MODE ENABLED - No changes will be made to tickets or spreadsheets ***")

# Date setup
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%m/%d/%Y")
current_month = current_date.strftime("%B")
logging.info(f"Starting compliance ticket automation for {current_month} on {formatted_date}")

# Load environment variables
logging.info("Loading environment variables...")
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

logging.info(f"TeamDynamix Base URL: {TDX_BASE_URL}")
logging.info(f"TeamDynamix App ID: {TDX_APP_ID}")
logging.info(f"Google Spreadsheet ID: {SPREADSHEET_ID}")
logging.info(f"Google Sheet Name: {SHEET_NAME}")

# Debug logging for environment variables
logging.debug(f"Environment variables loaded - TDX_BASE_URL: {TDX_BASE_URL}, TDX_APP_ID: {TDX_APP_ID}, SPREADSHEET_ID: {SPREADSHEET_ID}")

# Detect environment (sandbox vs production)
is_sandbox = 'SB' in TDX_BASE_URL or 'sandbox' in TDX_BASE_URL.lower()
environment = "SANDBOX" if is_sandbox else "PRODUCTION"
logging.info(f"Detected environment: {environment}")

# Determine notification settings
def determine_notification_setting():
    """Determine whether to notify requestors based on environment and run mode."""
    if args.dry_run:
        logging.info("Notifications DISABLED: Dry run mode active")
        return False

    if is_sandbox:
        logging.info("Notifications DISABLED: Sandbox environment detected")
        return False

    # Production environment - ask user for confirmation
    logging.warning("=" * 60)
    logging.warning("PRODUCTION ENVIRONMENT DETECTED")
    logging.warning("=" * 60)
    logging.warning("This script will create tickets and NOTIFY REQUESTORS via email!")
    logging.warning("Users will receive compliance notifications about their computers.")
    logging.warning("")

    while True:
        try:
            response = input("Do you want to proceed and send notifications to users? (Y/N): ").strip().upper()
            if response == 'Y':
                logging.info("User confirmed: Notifications ENABLED")
                return True
            elif response == 'N':
                logging.info("User declined: Aborting script execution")
                logging.info("Script terminated by user choice")
                sys.exit(0)
            else:
                print("Please enter Y (yes) or N (no)")
        except KeyboardInterrupt:
            logging.info("\nScript interrupted by user")
            sys.exit(0)

notify_requestors = determine_notification_setting()
logging.info(f"Final notification setting: {'ENABLED' if notify_requestors else 'DISABLED'}")

# Initialize services
logging.info("Initializing TeamDynamix service... (establishing API connection)")
try:
    tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
    logging.info("Successfully connected to TeamDynamix")
    logging.debug(f"TeamDynamix service initialized with app_id: {TDX_APP_ID}")
except Exception as e:
    logging.error(f"Failed to initialize TeamDynamix service connection: {str(e)}")
    sys.exit(1)

logging.info("Initializing Google Sheets service... (authenticating and connecting)")
try:
    sheet_adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
    sheet = Sheet(sheet_adapter, SPREADSHEET_ID, SHEET_NAME, header_row=1)
    logging.info(f"Successfully connected to Google Sheet '{SHEET_NAME}'")
    logging.debug(f"Google Sheets service connected to sheet: {SHEET_NAME}")
except Exception as e:
    logging.error(f"Failed to connect to Google Sheets API: {str(e)}")
    sys.exit(1)

# Safe wrapper functions for dry run mode
def safe_create_ticket(ticket_data, notify_requestor=False, notify_responsible=False, allow_requestor_creation=False):
    """Create a ticket respecting dry run mode."""
    if args.dry_run:
        logging.info("  [DRY RUN] Would create ticket with the following data:")
        logging.info(f"  [DRY RUN] Title: {ticket_data.get('Title', 'N/A')}")
        logging.info(f"  [DRY RUN] Requestor UID: {ticket_data.get('RequestorUid', 'N/A')}")
        logging.info(f"  [DRY RUN] Account ID: {ticket_data.get('AccountID', 'N/A')}")
        logging.info(f"  [DRY RUN] Service ID: {ticket_data.get('ServiceID', 'N/A')}")
        logging.info(f"  [DRY RUN] Notify Requestor: {notify_requestor}")
        logging.info(f"  [DRY RUN] Notify Responsible: {notify_responsible}")
        # Return a mock ticket object for dry run
        return {
            'ID': 'DRY_RUN_TICKET_ID',
            'Title': ticket_data.get('Title', 'Dry Run Ticket')
        }
    else:
        logging.info(f"  Creating ticket with notifications: Requestor={notify_requestor}, Responsible={notify_responsible}")
        return tdx_service.tickets.create_ticket(
            ticket_data=ticket_data,
            notify_requestor=notify_requestor,
            notify_responsible=notify_responsible,
            allow_requestor_creation=allow_requestor_creation
        )

def safe_add_asset_to_ticket(asset_id, ticket_id):
    """Add an asset to a ticket respecting dry run mode."""
    if args.dry_run:
        logging.info(f"  [DRY RUN] Would add asset {asset_id} to ticket {ticket_id}")
        return True
    else:
        return tdx_service.assets.add_asset(asset_id, ticket_id)

def safe_write_sheet_data(range_name, values, value_input_option="USER_ENTERED"):
    """Write data to Google Sheet respecting dry run mode."""
    if args.dry_run:
        logging.info(f"  [DRY RUN] Would write to sheet range {range_name}: {values}")
        return True
    else:
        return sheet.write_data(range_name=range_name, values=values, value_InputOption=value_input_option)

def safe_search_asset(search_criteria):
    """Search for assets (read-only operation, safe in dry run)."""
    return tdx_service.assets.search_asset(search_criteria)

# TICKET METADATA Fields
# These variables represent the column header names in your google sheet
Region = "Support"  # support region data, shortened version (eg. MLB, East Hall, Randall)
Dept = "Owning Dept"  # Full department name and code
Owner = "Owner"  # Full name of owner
Owner_Email = "Owner Email"  # owner's umich email. unqinames may work too.
Dept_list = "TDX!H2:H"  # list of all departments used by the computers. currently raw tdx data
Computer_Name = "Hostname"
Serial_Number = "Serial"
Ticket = "Ticket"

# Issue/Fix variables generate the table of computers that need attended for the user.
Issue = "Fix"
Fix_is_Sheet = True
Fix = "FIX"
Fix_Header = 1  # Note, while cells are 1 indexed, header references are 0-indexed

logging.info("Processing spreadsheet data... (loading and filtering records)")
the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:]  # drop repeated column in dataset
logging.info(f"Loaded {len(the_list)} total records from spreadsheet")

# Filter out records that already have tickets
initial_count = len(the_list)
the_list = the_list[(the_list[Ticket] == "")]  # ignore where ticket already exists
filtered_count = len(the_list)
logging.info(f"Filtered to {filtered_count} records without existing tickets ({initial_count - filtered_count} records already have tickets)")

if Fix_is_Sheet:  # build lookup from fix spreadsheet if fix and issue are not main sheet columns
    logging.info(f"Loading fix information from '{Fix}' sheet... (mapping issues to solutions)")
    try:
        fixes = Sheet(sheet_adapter, SPREADSHEET_ID, Fix, header_row=Fix_Header)
        fixes = fixes.get_columns_as_dict("Issue", "Fix")  # Changes these if column headers change.
        the_list[Fix] = the_list[Issue].map(fixes)
        logging.info(f"Successfully mapped {len(fixes)} issues to their fixes")
    except Exception as e:
        logging.error(f"Failed to load fix information from Google Sheets FIX tab: {str(e)}")
        sys.exit(1)

ticket_column_letter = sheet.get_column_letter(Ticket)  # used for writing ticket data to Google Sheet
logging.info(f"Ticket column identified as: {ticket_column_letter}")

# Get department data from TeamDynamix
logging.info("Fetching department information from TeamDynamix... (this may take a moment)")
try:
    dept_data = tdx_service.accounts.get_accounts()  # get ALL department objects in TDX
    logging.debug(f"TDX Department Data:\n {dept_data}")
    departments = {item['Name']: item['ID'] for item in dept_data}  # dictionary mapping dept name to dept's TDX ID
    logging.info(f"Retrieved {len(departments)} departments from TeamDynamix")
except Exception as e:
    logging.error(f"Failed to retrieve department data from TeamDynamix API: {str(e)}")
    sys.exit(1)

# All departments listed in TDX Database Sheet Owning Acct/Dept row.
logging.info(f"Fetching regional department list from subsheet {Dept_list.split('!')[0]} from column {Dept_list.split('!')[1][0]}")
logging.debug(f"Range used: {Dept_list}")
try:
    regional_departments = sheet_adapter.fetch_data(SPREADSHEET_ID, range_name=Dept_list)  # all tdx departments that appear in list.
    regional_departments = [dept for dept_row in regional_departments for dept in dept_row]  # convert to 1D list
    regional_departments = list(set(dept for dept in regional_departments if dept != 'None'))  # unique set of all departments
    regional_departments = [departments.get(item, item) for item in regional_departments]  # list of departmental tdx codes
    logging.info(f"Found {len(regional_departments)} unique regional departments")
    logging.debug(f"Regional departments:\n {regional_departments}")
except Exception as e:
    logging.error(f"Failed to fetch regional department list from Google Sheets: {str(e)}")
    sys.exit(1)

# Region responsible group IDs
# Modify region in ticket:data to var region to assign tickets to their responsibility group
region_respGUIDs = {
    'BSB': 370,
    'CHEM': 368,
    'MLB': 366,
    'Randall': 365,
    'East Hall': 367,
    'LSA': 364,
    'Infrastructure': 371
}
logging.debug(f"Responsible region group mappings: {region_respGUIDs}")

# Build ticket metadata required to create TDX Ticket.
logging.info("Building ticket metadata... (analyzing user requirements)")
ticket_metadata = the_list[[Region, Dept, Owner, Owner_Email]].drop_duplicates(subset=Owner_Email, keep='first')
logging.info(f"Created ticket metadata for {len(ticket_metadata)} unique users")

# Convert Dept & Region to respective TDX IDs
logging.info("Converting ticket metadata names to TDX friendly format... (mapping IDs)")
ticket_metadata[Dept] = ticket_metadata[Dept].map(departments)
ticket_metadata[Region] = ticket_metadata[Region].map(region_respGUIDs)
ticket_metadata['Uniqnames'] = ticket_metadata[Owner_Email].apply(lambda x: x.split('@')[0])

# Get user data from TeamDynamix
logging.info("Finding users from each region in TeamDynamix... (this may take a moment)")
try:
    user_data = tdx_service.users.search_user({'AccountIDs': regional_departments})  # returns ALL users from departments
    requestor_uids = {item['AuthenticationUserName']: item['UID'] for item in user_data}
    first_names = {item['AuthenticationUserName']: item['FirstName'] for item in user_data}
    logging.info(f"Retrieved data for {len(user_data)} users from TeamDynamix")
    logging.debug(f"User data sample (first 3): {user_data[:3] if user_data else 'None'}")
    logging.debug(f"Requestor UIDs mapping created: {len(requestor_uids)} entries")
except Exception as e:
    logging.error(f"Failed to retrieve user data from TeamDynamix for regional departments: {str(e)}")
    sys.exit(1)

# Map user information
ticket_metadata['RequestorUIDs'] = ticket_metadata['Uniqnames'].map(requestor_uids)
ticket_metadata['FirstName'] = ticket_metadata['Uniqnames'].map(first_names)

# Handle missing user data
na_count_before = ticket_metadata['RequestorUIDs'].isna().sum()
logging.info(f"Found {na_count_before} users without UID mappings, attempting individual lookups...")

# Look up missing users individually (including inactive users)
ticket_metadata['FirstName'] = ticket_metadata.apply(
    lambda x: tdx_service.users.get_user_attribute(uniqname=x['Uniqnames'], attribute='FirstName', isActive=None) \
    if pd.isna(x['FirstName']) else x['FirstName'],
    axis=1
)
ticket_metadata['RequestorUIDs'] = ticket_metadata.apply(
    lambda x: tdx_service.users.get_user_attribute(uniqname=x['Uniqnames'], attribute='UID', isActive=None) \
    if pd.isna(x['RequestorUIDs']) else x['RequestorUIDs'],
    axis=1
)

na_count_after = ticket_metadata['RequestorUIDs'].isna().sum()
logging.info(f"After individual lookups: {na_count_after} users still without UID mappings")

if na_count_after > 0:
    missing_users = ticket_metadata[ticket_metadata['RequestorUIDs'].isna()]['Uniqnames'].tolist()
    logging.warning(f"Could not find UIDs for users: {missing_users}")

# Add debug logging for ticket metadata
logging.debug(f"Ticket metadata sample:\n{ticket_metadata.head(3).to_string()}")

# Remove records with missing required data
initial_metadata_count = len(ticket_metadata)
ticket_metadata = ticket_metadata.dropna(subset=[Owner, Owner_Email, Dept, Region])  # remove NA Values before loop
final_metadata_count = len(ticket_metadata)
logging.info(f"Removed {initial_metadata_count - final_metadata_count} records with missing required data")
logging.info(f"Processing tickets for {final_metadata_count} users")

# Counters for tracking
tickets_created = 0
assets_added = 0
errors_encountered = 0

# Log the operational mode
logging.info("=" * 60)
logging.info("TICKET PROCESSING MODE SUMMARY")
logging.info("=" * 60)
logging.info(f"Environment: {environment}")
logging.info(f"Dry Run: {'YES' if args.dry_run else 'NO'}")
logging.info(f"Notify Requestors: {'YES' if notify_requestors else 'NO'}")
if notify_requestors:
    logging.warning("⚠️  USERS WILL RECEIVE EMAIL NOTIFICATIONS ⚠️")
else:
    logging.info("No email notifications will be sent to users")
logging.info("=" * 60)

# Process each user and create tickets
logging.info("Starting ticket creation process... (this will take several minutes)")
user_index = 0
for index, row in ticket_metadata.iterrows():
    user_email = row[Owner_Email]
    user_name = row[Owner]
    first_name = row['FirstName']
    user_index += 1
    logging.info(f"Processing user {user_index} of {final_metadata_count}: {user_name} ({user_email})")

    # Add periodic progress updates
    if (index + 1) % 10 == 0:
        logging.info(f"Progress update: {user_index + 1} of {final_metadata_count} users processed ({tickets_created} tickets created)")

    try:
        # Generate ticket description and metadata
        title = f"{current_month} Computer Compliance report for {row[Owner]}"
        logging.info(f"  Creating ticket: '{title}'")

        comment = f"""
        Hello {first_name},
        <br>
        <br>
        We understand that keeping technology up to date can sometimes be tedious work. To make this process a bit easier on you, LSA Technology Services Desktop Support team will be reaching out monthly with a list of your computers that need attention.<br><br>
        Below we've listed computer names, their issues and directions on how to fix them. Once you have applied the fix, it would be greatly appreciated if you could reply to this email letting us know. This allows us to verify everything is working as it should.
        If you have questions or need assistance with these issues you can also simply reply to this email.
        We appreciate your help keeping our computing environment secure!
        """

        # Build table of computers for this user
        table = the_list[the_list[Owner_Email] == user_email].copy()
        computer_count = len(table)
        logging.info(f"  Found {computer_count} computers needing attention for this user")

        table = table.rename(columns={Computer_Name: "Name", Issue: "Issue(s)", Fix: "Fix(es)"})
        table_columns = ['Name', 'OS', 'Serial', 'Issue(s)', 'Fix(es)']
        table['Issue(s)'] = table['Issue(s)'].str.replace('\n', '<br>', regex=True)
        table['Fix(es)'] = table['Fix(es)'].str.replace('\n', '<br>', regex=True)
        table = table.to_html(index=False, columns=table_columns, render_links=True, escape=False)
        walk_in = "Need help finding your Local IT team? <a href=https://lsa.umich.edu/technology-services/help-support/walk-in-support.html>Click here</a> to find a walk-in location near you."
        comment = comment + table + '<br>' + walk_in + '<br><br>' 'Thank you,' + '<br>' + 'LSA Technology Services Desktop Support team'

        # Prepare ticket data
        region = int(row[Region])
        dept = int(row[Dept])
        requestor = str(row['RequestorUIDs'])

        ticket_data = {
            "TypeID": 652,  # Desktop and Mobile Computing
            "TypeCategoryID": 6,  # Desktop and Mobile Computing
            "FormID": 107,
            "Title": title,
            "Description": comment,
            "isRichHtml": True,
            "AccountID": dept,  # Dept
            "SourceID": 8,
            "StatusID": 620,  # Awaiting Input
            "RequestorUid": requestor,
            "ResponsibleGroupID": 1678,  # LSA-TS-UnifiedListManagement || Change to region to automatically assign this to regions.
            "ServiceID": 2325,  # LSA-TS-Desktop-and-MobileDeviceSupport
            "ServiceOfferingID": 281,  # LSA-TS-Desktop-OperatingSystemManagement
            "ServiceCategoryID": 307  # LSA-TS-Desktop-and-MobileComputing
        }

        logging.info(f"  Ticket will be assigned to department ID {dept} and region {region}")

        # Find all computers that need fixing owned by the user
        computers_to_fix, row_index = sheet.search_columns(user_email, columns=[Owner_Email])

        if computers_to_fix:
            logging.info(f"  Found {len(computers_to_fix)} computers owned by this user")

            ticket_cells = {}
            for i, computer in enumerate(computers_to_fix):
                row_dict = {
                    'user': computer[Owner],
                    'computer': computer[Computer_Name],
                    'ticket': computer[Ticket],
                    'sn': computer[Serial_Number]
                }
                ticket_cells[ticket_column_letter + row_index[i]] = row_dict

            # Check which computers don't already have tickets
            no_ticket = {cell: entry for cell, entry in ticket_cells.items() if not entry['ticket']}

            if no_ticket:
                logging.info(f"  {len(no_ticket)} computers need tickets created")

                # Create the ticket
                ticket = safe_create_ticket(
                    ticket_data=ticket_data,
                    notify_requestor=notify_requestors,
                    notify_responsible=False,
                    allow_requestor_creation=False
                )

                ticket_number = ticket['ID']
                tickets_created += 1

                # Generate ticket URL
                url = f"Apps/46/Tickets/TicketDet?TicketID={ticket['ID']}"
                full_url = urljoin(TDX_TICKET_DOMAIN, url)
                cell_value = f'=HYPERLINK(\"{full_url}\", {ticket_number})'

                logging.info(f"  Created ticket #{ticket_number}")
                if notify_requestors:
                    logging.info(f"  📧 Email notification sent to {user_email}")
                else:
                    logging.info(f"  📧 No email notification sent (notifications disabled)")
                logging.info(f"  Ticket URL: {full_url}")

                # Process each computer for this ticket
                for cell, entry in no_ticket.items():
                    computer_name = entry['computer']
                    serial_number = entry['sn']

                    logging.info(f"    Processing computer: {computer_name} (S/N: {serial_number})")

                    # Search for the asset in TeamDynamix
                    assets = None
                    if serial_number:
                        logging.info(f"      Searching for asset by serial number: {serial_number} (exact match required)")
                        assets = safe_search_asset({"SerialLike": serial_number})
                    else:
                        logging.info(f"      No serial number, searching by computer name: {computer_name} (fuzzy match)")
                        assets = safe_search_asset({"SearchText": computer_name})
                        logging.debug(f"      Asset search results: {assets}")

                    if assets:
                        # Find exact match by computer name
                        asset_id = None
                        for asset in assets:
                            if asset["Name"].strip().lower() == computer_name.strip().lower():
                                asset_id = asset['ID']
                                logging.info(f"      Found matching asset with ID: {asset_id}")
                                break

                        if asset_id:
                            # Add asset to ticket
                            safe_add_asset_to_ticket(asset_id, ticket_number)
                            assets_added += 1
                            logging.info(f"      Added asset {asset_id} to ticket #{ticket_number}")
                        else:
                            logging.warning(f"      Exact asset match not found in TeamDynamix for S/N: {serial_number}, name: {computer_name}")
                    else:
                        logging.warning(f"      No assets found in TeamDynamix database for serial number {serial_number}")

                    # Update the spreadsheet with ticket information
                    safe_write_sheet_data(range_name=cell, values=[[cell_value]], value_input_option="USER_ENTERED")
                    logging.info(f"      Updated spreadsheet cell {cell} with ticket #{ticket_number}")

                logging.info(f"  Successfully processed user {user_name} - Ticket #{ticket_number} created with {len(no_ticket)} computers")

                # Add progress tracking
                logging.info(f"Ticket Creation Progress: {tickets_created} tickets created, {assets_added} assets added")
            else:
                logging.info(f"  All computers for {user_name} already have tickets assigned")
        else:
            logging.warning(f"  No computers found for user {user_name}")

    except Exception as e:
        errors_encountered += 1
        logging.error(f"  Error processing user {user_name}: {str(e)}")
        import traceback
        logging.error(f"  Traceback: {traceback.format_exc()}")

# Final summary
logging.info("=" * 60)
logging.info("COMPLIANCE TICKET AUTOMATION COMPLETE")
logging.info("=" * 60)
logging.info(f"Environment: {environment}")
logging.info(f"Total users processed: {final_metadata_count}")
logging.info(f"Tickets created: {tickets_created}")
logging.info(f"Assets added to tickets: {assets_added}")
logging.info(f"Errors encountered: {errors_encountered}")
logging.info(f"Requestor notifications: {'ENABLED' if notify_requestors else 'DISABLED'}")

if args.dry_run:
    logging.info("*** NOTE: This was a dry run, no changes were made ***")
elif not notify_requestors and not is_sandbox:
    logging.info("*** NOTE: No notifications were sent to users ***")
elif notify_requestors:
    logging.warning("EMAIL NOTIFICATIONS WERE SENT TO USERS")
else:
    logging.info("All changes have been applied to TeamDynamix and Google Sheets")

logging.info("Compliance ticket automation finished successfully")
