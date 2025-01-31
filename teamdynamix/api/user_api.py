from .teamdynamix_api import TeamDynamixAPI
class UserAPI(TeamDynamixAPI):
    def search_user(self, data): # data is dictionary/json object of all search options:
        return self.post('people/search', data)
    def search_users_by_uniqname(self, uniqname, isActive=True): # returns list of account objects
        data = {
            'UserName': f"{uniqname}@umich.edu",
            'isActive' : isActive
        }
        result = self.post('people/search', data)
        if result:
            return result
        else:
            data = {
                'AlternateID': uniqname,
                'isActive' : isActive
            }
            result = self.post('people/search', data)
            if result:
                return result
            else:
                data = {
                    'SearchText': uniqname,
                    'isActive' : isActive
                }
                result = self.post('people/search', data)
                if result:
                    return result
                else:
                    print(f"WARNING: no match found for {uniqname}")
    def get_user(self, uniqname=None,uid=None, isActive=True): # returns account object
        if uid:
            print(self.get(f"people/{uid}"))
            return self.get(f"people/{uid}")
        if uniqname:
            return self.search_users_by_uniqname(uniqname, isActive=isActive)


    def get_user_attribute(self, uniqname, attribute, isActive=True):
        user = self.get_user(uniqname, isActive=isActive)[0]
        return user[attribute]

    def get_user_list(self, isActive=True, isConfidential=False, isEmployee=False, userType=None):
        # NOTE: This action can only be performed by a special key-based administrative service account, using a token obtained from the api/auth/loginadmin endpoint.""
        return self.get(f"people/userlist?isActive={isActive}&isConfidential={isConfidential}&isEmployee={isEmployee}&userType={userType}")
