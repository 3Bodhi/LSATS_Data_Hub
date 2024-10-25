from .teamdynamix_api import TeamDynamixAPI
class UserAPI(TeamDynamixAPI):
    def search_users(self, uniqname): # returns account object
        data = {'SearchText': uniqname}
        return self.post('people/search', data)
    def get_user(self, uniqname=None,uid=None): # returns account object
        if uid:
            print(self.get(f"people/{uid}"))
            return self.get(f"people/{uid}")
        if uniqname:
            search = self.search_users(uniqname)
            if search:
                i = 0
                while not search[i]['AlternateID'] == uniqname:
                    i += 1
                return search[i]
            else:
                print(f"User {uniqname}{uid} not found.")
                return None

    def get_user_attribute(self, uniqname, attribute):
        user = self.get_user(uniqname)
        return user[attribute]
