import dotenv
from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
import os
from dotenv import load_dotenv
load_dotenv()

TDX_BASE_URL = os.getenv('TDX_BASE_URL')
TDX_APP_ID = os.getenv('TDX_APP_ID')
API_TOKEN = os.getenv('TDX_API_TOKEN')

tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)
lab = tdx_service.create_lab('tarazlee')
#tickets = tdx_service.get_user_tickets_by_uniqname('danweiss')
#print(tickets[0])
'''
for ticket in tickets:
    print(ticket['RequestorLastName'],ticket['Title'],ticket['StatusName'])
'''
#tdx_service.tickets.add_ticket_configuration_item(7151528,1205942)
#print(tdx_service.configuration_items.get_ci(1205942))
