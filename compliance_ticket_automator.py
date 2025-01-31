from re import I
from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from openai import OpenAI
from dotenv import load_dotenv
import os
import pandas as pd
from openai import OpenAI
import json
from teamdynamix.api.user_api import UserAPI
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
adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
sheet = Sheet(adapter, SPREADSHEET_ID, SHEET_NAME, header_row=1)

ticket_id = 5058401
ticket = tdx_service.tickets.get_ticket(ticket_id)
print(json.dumps(ticket, indent=4))
#ticket_email = ticket['RequestorEmail']
#column_names = sheet.get_column_names()
#print(column_names)
#print(ticket_email)
#search = sheet.search_columns(ticket_email,columns=['Owner Email'])
#print(search)
#results = pd.DataFrame(search[:-1],columns=column_names)
#print(results)
#row_numbers = search[-1]
#url = sheet.generate_url
comment = ""

#results_str = results.to_string()
prompt = '''You will be provided with a table related to computers
which need some sort of fix. Create a friendly email from a tech
to a user noting which computers have issues, their serial numbers
and the fix required. Only return the email. My name is Matthew Yodhes. Our team name is LSA Technology Services'''
'''
completion = client.chat.completions.create(
  #model="llama3.1-70b",
  model = 'deepseek-r1-32b?',
  messages=[
      {"role": "system", "content": prompt },
    {"role": "user", "content": results_str }
  ],
  temperature=0.7,
)
'''
'''
comment += str(completion.choices[0].message.content) + "\n\n"
for index, row in enumerate(row_numbers):
    comment += f"{results['Computer Name'].iloc[index]} - {sheet.generate_url(row)}" + "\n\n"
tdx_service.tickets.update_ticket(ticket_id,comments=comment,private=True,commrecord=False, rich=False)
'''


the_list = pd.DataFrame(sheet.data[1:], columns=sheet.data[1]).iloc[1:] # drop repeated column in dataset
the_list = the_list[the_list['Delete'] == 'FALSE'] # Don't send email if slated to be deleted anyway'

dept_data = tdx_service.accounts.get_accounts()
departments = {item['Name']: item['ID'] for item in dept_data} # dictionary mapping dept name to TDX ID

regions = {
    'BSB': 370,
    'CHEM': 368,
    'MLB': 366,
    'Randall':365,
    'East Hall': 367,
    'LSA':364 # dictonary for region's ResponsibleGroup IDs
}

# Build ticket metadata required to create TDX Ticket.
ticket_metadata = the_list[['Region','Dept','Owner Email']].drop_duplicates(subset='Owner Email', keep='first')
## Convert Dept & Region to respective TDX IDs
ticket_metadata['Dept'] = ticket_metadata['Dept'].map(departments)
ticket_metadata['Region'] = ticket_metadata['Region'].map(regions)
ticket_metadata['Uniqnames'] = ticket_metadata['Owner Email'].apply(lambda x: x.split('@')[0])
#ticket_metadata['RequestorUID'] = ticket_metadata.apply(
#    lambda x: tdx_service.users.get_user_attribute(x['Uniqnames'], 'UID'),
#    axis=1
#)
print(ticket_metadata)
print(tdx_service.groups.get_group(110))
print(tdx_service.groups.get_group_members(110))
#print(tdx_service.users.get_user("danweiss"))
# pull groups or use region to Id dictionary.
# generate ticket body
# create ticket
# attach computers
#     lookup assets
print(dept_data)
