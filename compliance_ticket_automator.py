from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
from urllib.parse import urljoin

current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%m/%d/%Y")
current_month = current_date.strftime("%B")

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
SHEET_NAME =  os.getenv('SHEET_NAME')


tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
sheet_adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
sheet = Sheet(sheet_adapter, SPREADSHEET_ID, SHEET_NAME, header_row=1)



# TICKET METADATA Fields
# # These variables represent the column header names in your google sheet
Region = "Support" # support region data, shortened version (eg. MLB, East Hall, Randall)
Dept = "Owning Dept" # Full department name and code
Owner = "Owner" # Full name of owner
Owner_Email = "Owner Email" # owner's umich email. unqinames may work too.
Dept_list = "TDX!H2:H" # list of all departments used by the computers. currently raw tdx data
# List of all departments noted from TDX import.
# Must be in format "TDX Sheet Name!Starting_Cell:Column"
# eg "TDX Database!H8:H" for sub-sheet TDX Database, with raw dept data in Column H, starting in cell H8
Computer_Name = "Hostname"
Serial_Number = "Serial"
Ticket = "Ticket"

# Issue/Fix variables generate the table of computers that need attended for the user.
Issue = "Fix"
Fix_is_Sheet = True
Fix = "FIX"
Fix_Header = 1 # Note, while cells are 1 indexed, header references are 0-indexed

the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[(the_list[Ticket] == "")] # ignore where ticket already exists
if Fix_is_Sheet: # build lookup from fix spreadsheet if fix and isue are not main sheet columns
    fixes = Sheet(sheet_adapter,SPREADSHEET_ID, Fix, header_row=Fix_Header)
    fixes = fixes.get_columns_as_dict("Issue","Fix") # Changes these if column headers change.
    the_list[Fix] = the_list[Issue].map(fixes)
ticket_column_letter = sheet.get_column_letter(Ticket) # used for writing ticket data to Google Sheet

dept_data = tdx_service.accounts.get_accounts() # get ALL department objects in TDX
departments = {item['Name']: item['ID'] for item in dept_data} # dictionary mapping dept name to dept's TDX ID

# All departments listed in TDX Database Sheet Owning Acct/Dept row.
regional_departments = sheet_adapter.fetch_data(SPREADSHEET_ID, range_name=Dept_list) # all tdx departments that appear in list.
regional_departments = [dept for dept_row in regional_departments for dept in dept_row] # convert to 1D list
regional_departments = list(set(dept for dept in regional_departments if dept != 'None')) # unique set of all departments in The Lists's TDX database
regional_departments = [departments.get(item, item) for item in regional_departments] # list of departmental tdx codes. Missing codes appear as dept name

region_respGUIDs = {
    'BSB': 370,
    'CHEM': 368,
    'MLB': 366,
    'Randall':365,
    'East Hall': 367,
    'LSA': 364,
    'Infrastructure': 371
} # UNUSED dictonary for region's ResponsibleGroup IDs

# Build ticket metadata required to create TDX Ticket.
## ticket_metadata is a dataframe representation of the list which uses TDX rather than human readable values
ticket_metadata = the_list[[Region,Dept,Owner,Owner_Email]].drop_duplicates(subset=Owner_Email, keep='first')

## Convert Dept & Region to respective TDX IDs
ticket_metadata[Dept] = ticket_metadata[Dept].map(departments)
ticket_metadata[Region] = ticket_metadata[Region].map(region_respGUIDs)
ticket_metadata['Uniqnames'] = ticket_metadata[Owner_Email].apply(lambda x: x.split('@')[0])
user_data = tdx_service.users.search_user({'AccountIDs': regional_departments}) # returns ALL users from departments in The_List
# lookup for username to Requestor UIDS
requestor_uids = {item['AuthenticationUserName']: item['UID'] for item in user_data}
first_names = {item['AuthenticationUserName']: item['FirstName'] for item in user_data}
ticket_metadata['RequestorUIDs'] = ticket_metadata['Uniqnames'].map(requestor_uids)
ticket_metadata['FirstName'] = ticket_metadata['Uniqnames'].map(first_names)
#print(f" number of NA ids {ticket_metadata['RequestorUIDs'].isna().sum()}")
#print(ticket_metadata[ticket_metadata['RequestorUIDs'].isna()])

###NOTE TDX User Search defaults to only active users while the list might contain inactive values.
# change isActive to True if you want to omit inactive users. use None for all.
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
#print(f" number of NA ids {ticket_metadata['RequestorUIDs'].isna().sum()}")
#print(ticket_metadata)
# Build Ticket description and json object to post
ticket_metadata = ticket_metadata.dropna(subset=[Owner,Owner_Email,Dept,Region]) # remove NA Values before loop
for index, row in ticket_metadata.iterrows(): # generate ticket description and metadata
    title = f"{current_month} Computer Compliance report for {row[Owner]}"
    ticket_email = row[Owner_Email]
    comment = f"""
    Hello {row['FirstName']},
    <br>
    <br>
    We understand that keeping technology up to date can sometimes be tedious work. To make this process a bit easier on you, LSA Technology Services Desktop Support team will be reaching out monthly with a list of your computers that need attention.<br><br>
    Below we’ve listed computer names, their issues and directions on how to fix them. Once you have applied the fix, it would be greatly appreciated if you could reply to this email letting us know. This allows us to verify everything is working as it should.
    If you have questions or need assistance with these issues you can also simply reply to this email.
    We appreciate your help keeping our computing environment secure!
    """
    table = the_list[the_list[Owner_Email] == row[Owner_Email]].copy()
    table = table.rename(columns={Computer_Name:"Name",Issue:"Issue(s)",Fix:"Fix(es)"})
    table_columns = ['Name','OS','Serial','Issue(s)', 'Fix(es)']
    table['Issue(s)'] = table['Issue(s)'].str.replace('\n', '<br>', regex=True)
    table['Fix(es)'] = table['Fix(es)'].str.replace('\n', '<br>', regex=True)
    table = table.to_html(index=False, columns=table_columns, render_links=True, escape=False)
    walk_in ="Need help finding your Local IT team? <a href=https://lsa.umich.edu/technology-services/help-support/walk-in-support.html>Click here</a> to find a walk-in location near you."
    comment = comment + table + '<br>' + walk_in + '<br><br>' 'Thank you,' + '<br>' + 'LSA Technology Services Desktop Support team'

    region = int(row[Region])
    dept = int(row[Dept])
    requestor = str(row['RequestorUIDs'])
    ticket_data = {
        "TypeID": 652, # Desktop and Mobile Computing
        "TypeCategoryID": 6, # Desktop and Mobile Computing
        "FormID": 107,
        "Title": title,
        "Description": comment,
        "isRichHtml": True,
        "AccountID": dept, # Dept
        "SourceID": 8,
        "StatusID": 620, # Awaiting Input
        "RequestorUid": requestor,
        "ResponsibleGroupID": 1678, # 1678 is code for LSA-TS-UnifiedListManagement, use Region variable to auto assign regionals.
        "ServiceID": 2325, # LSA-TS-Desktop-and-MobileDeviceSupport
        "ServiceOfferingID": 281, # LSA-TS-Desktop-OperatingSystemManagement
        "ServiceCategoryID": 307 # LSA-TS-Desktop-and-MobileComputing
    }
    # Find all computers (all other rows) that need fixing owned by the user.
    computers_to_fix, row_index = sheet.search_columns(ticket_email, columns=[Owner_Email])
    if computers_to_fix:
        ticket_cells = {}
        for i, computer in enumerate(computers_to_fix): # check any of the computers already have a ticket.
            row_dict = {}
            row_dict['user'] = computer[Owner]
            row_dict['computer'] = computer[Computer_Name]
            row_dict['ticket'] = computer[Ticket]
            row_dict['sn'] = computer[Serial_Number]
            ticket_cells[ticket_column_letter + row_index[i]] = row_dict
        no_ticket = {cell: entry for cell, entry in ticket_cells.items() if not entry['ticket']}
        if no_ticket: # Create a ticket
            ticket = tdx_service.tickets.create_ticket(ticket_data=ticket_data, notify_requestor=False,notify_responsible=False,allow_requestor_creation=False)
            ticket_number = ticket['ID']
            url = f"Apps/46/Tickets/TicketDet?TicketID={ticket['ID']}"
            print(url)
            url = urljoin(TDX_TICKET_DOMAIN, url)
            print(url)
            cell_value = f'=HYPERLINK(\"{url}\", {ticket_number})'
            for cell, entry in no_ticket.items(): # search and add all related assets that don't yet have a ticket.
                if entry['sn']:
                    assets = tdx_service.assets.search_asset({"SerialLike": entry['sn']})
                else:
                    assets = tdx_service.assets.search_asset({"SearchText": entry['computer']})
                    print(assets)
                if assets:
                    asset_id = ""
                    for asset in assets:
                        if asset["Name"].strip().lower() == entry["computer"].strip().lower():
                            asset_id = asset['ID']
                            break
                    if asset_id:
                        tdx_service.assets.add_asset(asset_id, ticket_number)
                        print(f"added asset {asset_id} to TDX#{ticket_number}")
                    else:
                        print(f"asset not found for s/n: {entry['sn']}, name: {entry['computer']}")
                else:
                    print(f"asset with serial {entry['sn']} not found in TDX. Please verify record.")
                entry['ticket'] = cell_value
                sheet.write_data(range_name=cell, values=[[cell_value]],value_InputOption="USER_ENTERED")
                print(f'Ticket number # {ticket_number} created for {entry['user']}\'s computer {entry['computer']}')
                print(url)






#print(f"ticket example: \n {tdx_service.tickets.get_ticket(7355612)}")
## TODO:
    # Build Create ticket in Ticket_API
    #    required fields:
        # TypeID
        # Title
        # AccountID
        # StatusID
        # PriorityID
        # RequestorUID
    # Test
    # build loop with Body

# generate ticket body
# create ticket api
# attach computers
#   create attach asset in ticket_api.
#     lookup assets
