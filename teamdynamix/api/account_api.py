from .teamdynamix_api import TeamDynamixAPI
class AccountAPI(TeamDynamixAPI):
    def get_accounts(self): # returns all accounts
        return self.get('accounts')
