from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from openai import OpenAI
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from openai import OpenAI
import json
import datetime
from urllib.parse import urljoin


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

LLM_BASE_URL = os.getenv('LLM_BASE_URL')
LLM_API_KEY = os.getenv('LLM_API_KEY')
client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
sheet_adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
sheet = Sheet(sheet_adapter, SPREADSHEET_ID, SHEET_NAME, header_row=1)

current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%m/%d/%Y")
current_month = current_date.strftime("%B")

the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[the_list['Delete'] == 'FALSE'] # Don't send email if slated to be deleted anyway'


dept_data = tdx_service.accounts.get_accounts() # get ALL department objects in TDX
departments = {item['Name']: item['ID'] for item in dept_data} # dictionary mapping dept name to dept's TDX ID

# All departments listed in TDX Database Sheet Owning Acct/Dept row.
regional_departments = sheet_adapter.fetch_data(SPREADSHEET_ID, range_name="TDX Database!H8:H") # all tdx departments that appear in list.
regional_departments = [dept for dept_row in regional_departments for dept in dept_row]
regional_departments = list(set(dept for dept in regional_departments if dept != 'None')) # unique set of all departments in The Lists's TDX database
regional_departments = [departments.get(item, item) for item in regional_departments] # list of departmental tdx codes

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
# ticket_metadata is a dataframe representation of the list which uses TDX rather than human readable values
ticket_metadata = the_list[['Region','Dept','Owner','Owner Email']].drop_duplicates(subset='Owner Email', keep='first')
## Convert Dept & Region to respective TDX IDs
ticket_metadata['Dept'] = ticket_metadata['Dept'].map(departments)
ticket_metadata['Region'] = ticket_metadata['Region'].map(region_respGUIDs)
ticket_metadata['Uniqnames'] = ticket_metadata['Owner Email'].apply(lambda x: x.split('@')[0])
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
for index, row in ticket_metadata.iterrows():
    title = f"{current_month} Computer Compliance report for {row['Owner']}"
    ticket_email = row['Owner Email']
    comment = f"""
    Hello {row['FirstName']},
    <br>
    <br>
    We understand that keeping technology up to date can sometimes be tedious work. To make this process a bit easier on you, LSA Technology Services Desktop Support team will be reaching out monthly with a list of your computers that need attention.<br><br>
    Below we’ve listed computer names, their issues and directions on how to fix them. Once you have applied the fix, it would be greatly appreciated if you could reply to this email letting us know. This allows us to verify everything is working as it should.
    If you have questions or need assistance with these issues you can also simply reply to this email.
    We appreciate your help keeping our computing environment secure!
    """
    table = the_list[the_list['Owner Email'] == row['Owner Email']]
    table = table.rename(columns={"Computer Name":"Name","Issue":"Issue(s)","Fix":"Fix(es)"})
    table_columns = ['Name','OS','Serial','Issue(s)', 'Fix(es)']
    table['Issue(s)'] = table['Issue(s)'].str.replace('\n', '<br>', regex=True)
    table['Fix(es)'] = table['Fix(es)'].str.replace('\n', '<br>', regex=True)
    table = table.to_html(index=False, columns=table_columns, render_links=True, escape=False)
    walk_in ="Need help finding your Local IT team? <a href=https://lsa.umich.edu/technology-services/help-support/walk-in-support.html>Click here</a> to find a walk-in location near you."
    comment = comment + table + '<br>' + walk_in + '<br><br>' 'Thank you,' + '<br>' + 'LSA Technology Services Desktop Support team'

    region = int(row['Region'])
    dept = int(row['Dept'])
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
        "ResponsibleGroupID": 1678, # code for LSA-TS-UnifiedListManagement, use Region variable to auto assign regionals.
        "ServiceID": 2325, # LSA-TS-Desktop-and-MobileDeviceSupport
        "ServiceOfferingID": 281, # LSA-TS-Desktop-OperatingSystemManagement
        "ServiceCategoryID": 307 # LSA-TS-Desktop-and-MobileComputing
    }

    computers_to_fix = sheet.search_columns(ticket_email, columns=['Owner Email'])
    if computers_to_fix:
        ticket_cells = {}
        for row in computers_to_fix[:-1]: # last list is list of rows
            row_dict = {}
            row_dict['user'] = row[8]
            row_dict['computer'] = row[11]
            row_dict['ticket'] = row[2]
            row_dict['sn'] = row[16]
            ticket_cells[f"C{sheet.data.index(row) + 1}"] = row_dict
        no_ticket = {cell: entry for cell, entry in ticket_cells.items() if not entry['ticket']}
        if no_ticket:
            ticket = tdx_service.tickets.create_ticket(ticket_data=ticket_data, notify_requestor=False,notify_responsible=False,allow_requestor_creation=False)
            ticket_number = ticket['ID']
            url = f"Apps/46/Tickets/TicketDet?TicketID={ticket['ID']}"
            print(url)
            url = urljoin(TDX_TICKET_DOMAIN, url)
            print(url)
            cell_value = f'=HYPERLINK(\"{url}\", {ticket_number})'
            for cell, entry in no_ticket.items():
                asset = tdx_service.assets.search_asset({"SerialLike": entry['sn']})
                if asset:
                    asset_id = asset[0]['ID']
                    tdx_service.assets.add_asset(asset_id, ticket_number)
                    print(f"added asset {asset_id} to TDX#{ticket_number}")
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
