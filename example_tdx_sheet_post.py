from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
from openai import OpenAI
from dotenv import load_dotenv
import os
import pandas as pd
from openai import OpenAI
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

ticket_id = 7713651
ticket = tdx_service.tickets.get_ticket(ticket_id)
ticket_email = ticket['RequestorEmail']
column_names = sheet.get_column_names()
print(column_names)
print(ticket_email)
search = sheet.search_columns(ticket_email,columns=['Owner Email'])
print(search)
results = pd.DataFrame(search[:-1],columns=column_names)
row_numbers = search[-1]
url = sheet.generate_url
comment = "test"
'''
results_str = results.to_string()
prompt = You will be provided with a table related to computers
which need some sort of fix. Create a friendly email from a tech
to a user noting which computers have issues, their serial numbers
and the fix required. Only return the email. My name is Matthew Yodhes. Our team name is LSA Technology Services

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
notify = [
        "myodhes@umich.edu",
        "matthewyodhes@gmail.com"
]
#comment += str(completion.choices[0].message.content) + "\n\n"
for index, row in enumerate(row_numbers):
    comment += f"{results['Computer Name'].iloc[index]} - {sheet.generate_url(row)}" + "\n\n"
tdx_service.tickets.update_ticket(ticket_id,comments=comment,private=False,commrecord=False, rich=False)
