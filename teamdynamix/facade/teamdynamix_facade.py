from ..api.teamdynamix_api import TeamDynamixAPI, create_headers
from ..api.asset_api import AssetAPI
from ..api.user_api import UserAPI
from ..api.account_api import AccountAPI
from ..api.configuration_item_api import ConfigurationItemAPI
from ..api.ticket_api import TicketAPI
from ..api.group_api import GroupAPI

class TeamDynamixFacade:
    def __init__(self, base_url, app_id, api_token):
        headers = create_headers(api_token)
        self.users = UserAPI(base_url, "", headers)
        self.assets = AssetAPI(base_url, app_id, headers)
        self.accounts = AccountAPI(base_url, "", headers)
        self.configuration_items = ConfigurationItemAPI(base_url, app_id, headers)
        self.tickets = TicketAPI(base_url, 46, headers)
        self.groups = GroupAPI(base_url, "", headers)

    def get_user_assets_by_uniqname(self, uniqname):
        user_id = self.users.get_user_attribute(uniqname,'UID')
        if user_id:
            return self.assets.get_assets([user_id])
        else:
            return None
    def get_user_tickets_by_uniqname(self, uniqname):
        user_id = self.users.get_user_attribute(uniqname,'UID')
        if user_id:
            return self.tickets.get_tickets([user_id])
        else:
            return None
    def get_dept_users(self, dept_id):
        self.users.search_users
    def create_lab(self, pi):
        def create_lab_CI(assets):
            lab = self.configuration_items.create_ci({
                'Name':f"{(pi).title()} Lab",
                'OwnerUID': assets[0]['OwningCustomerID'],
                'OwningDepartmentID': assets[0]['OwningDepartmentID'],
                'LocationID': assets[0]['LocationID']
            })
            print(f"{lab['Name']} created with ID {lab['ID']}" )
            return lab

        def add_assets(ci, assets):
            configurationIDs = [asset['ConfigurationItemID'] for asset in assets]
            for id in configurationIDs:
                self.configuration_items.add_asset(ci['ID'], id)
                print(f"Added asset {id} to {ci['Name']}")
            return ci

        def add_tickets(ci, tickets):
            if tickets:
                for ticket in tickets:
                    self.tickets.add_ticket_configuration_item(ticket['ID'], ci['ID'])
                    print(f"Added ticket '{ticket['Title']}' to {ci['Name']}")

        assets = self.get_user_assets_by_uniqname(pi)
        tickets = self.get_user_tickets_by_uniqname(pi)
        lab = create_lab_CI(assets)
        add_assets(lab, assets)
        add_tickets(lab, tickets)
