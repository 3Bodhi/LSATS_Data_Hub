from re import I
from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from openai import OpenAI
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from openai import OpenAI
import json

load_dotenv()


TDX_BASE_URL = os.getenv('TDX_BASE_URL')
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


the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[the_list['Delete'] == 'FALSE'] # Don't send email if slated to be deleted anyway'

dept_data = tdx_service.accounts.get_accounts() # All department objects in TDX
departments = {item['Name']: item['ID'] for item in dept_data} # dictionary mapping dept name to TDX ID

# All departments listed in TDX Database Sheet Owning Acct/Dept row.
regional_departments = sheet_adapter.fetch_data(SPREADSHEET_ID, range_name="TDX Database!H8:H")
regional_departments = [dept for dept_row in regional_departments for dept in dept_row]
regional_departments = list(set(dept for dept in regional_departments if dept != 'None'))
regional_departments = [departments.get(item, item) for item in regional_departments]
print(regional_departments)

region_respGUIDs = {
    'BSB': 370,
    'CHEM': 368,
    'MLB': 366,
    'Randall':365,
    'East Hall': 367,
    'LSA':364
} # dictonary for region's ResponsibleGroup IDs

# Build ticket metadata required to create TDX Ticket.
ticket_metadata = the_list[['Region','Dept','Owner Email']].drop_duplicates(subset='Owner Email', keep='first')
## Convert Dept & Region to respective TDX IDs
ticket_metadata['Dept'] = ticket_metadata['Dept'].map(departments)
ticket_metadata['Region'] = ticket_metadata['Region'].map(region_respGUIDs)
ticket_metadata['Uniqnames'] = ticket_metadata['Owner Email'].apply(lambda x: x.split('@')[0])
user_data = tdx_service.users.search_user({'AccountIDs': regional_departments})
# lookup for username to Requestor UIDS
requestor_uids = {item['AuthenticationUserName']: item['UID'] for item in user_data}
ticket_metadata['RequestorUIDs'] = ticket_metadata['Uniqnames'].map(requestor_uids)
print(f" number of NA ids {ticket_metadata['RequestorUIDs'].isna().sum()}")
print(ticket_metadata[ticket_metadata['RequestorUIDs'].isna()])

###NOTE TDX User Search defualts to only active users while the list might contain inactive values.
# change isActive to True if you want to omit inactive users.
ticket_metadata['RequestorUIDs'] = ticket_metadata.apply(
    lambda x: tdx_service.users.get_user_attribute(uniqname=x['Uniqnames'], attribute='UID', isActive=None) \
    if pd.isna(x['RequestorUIDs']) else x['RequestorUIDs'],
    axis=1
)
print(f" number of NA ids {ticket_metadata['RequestorUIDs'].isna().sum()}")
print(ticket_metadata)
#print(tdx_service.users.get_user('jbardwel'))

#print(tdx_service.users.get_user('ava'))

#print(tdx_service.users.get_user('margoge'))
#print(tdx_service.get_dept_users(110))
#print(tdx_service.users.get_user("danweiss"))

# pull groups or use region to Id dictionary.
# generate ticket body
# create ticket
# attach computers
#     lookup assets
