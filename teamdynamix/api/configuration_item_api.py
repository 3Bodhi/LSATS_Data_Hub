import json
from .teamdynamix_api import TeamDynamixAPI
import copy

class ConfigurationItemAPI(TeamDynamixAPI):
    with open('teamdynamix/api/ci_defaults.json', 'r') as file:
        default_config = json.load(file)

    def search_ci(self, ci_name):
        data = {"NameLike": ci_name}
        return self.post('cmdb/search', data) #list of ci dictionary objects

    def get_ci(self, identifier):
        if str(identifier).isdigit():
            return self.get(f"cmdb/{identifier}") # 1 CI dictionary object
        search = self.search_ci(identifier)
        if not search:
            print(f"Bad identifier {identifier}")
            return None
        # NOTE: if multiple CIs have the same name, this will the match with the highest ID!
        def find_exact_match(search, identifier):
            return next((item for item in search if item['Name'] == identifier), None)
        def find_case_insensitive_match(search, identifier):
            return next((item for item in search if item['Name'].lower().strip() == identifier.lower().strip()), None)
        exact_match = find_exact_match(search, identifier)
        if exact_match:
            return exact_match
        case_insensitive_match = find_case_insensitive_match(search, identifier)
        if case_insensitive_match:
            return case_insensitive_match
        print(f"No exact matches to search text, returning {search[0]['Name']}.")
        return search[0]

    def edit_ci(self,fields,identifier=None):
        ci = self.get_ci(identifier)
        if ci:
            data = copy.deepcopy(self.default_config)
            if fields == {key: ci[key] for key in fields.keys() if key in ci}:
               print("Configuration Item already up to date!")
               return None
            if not identifier:
                identifier = (ci['ID'])
            data.update(fields)
            return self.put(f"cmdb/{identifier}", data)
        else:
            return None

    def create_ci(self,fields):
        data = copy.deepcopy(self.default_config)
        fields = fields
        data.update(fields)
        return self.post("/cmdb", data)

    def get_relationships(self, identifier):
        id = (self.get_ci(identifier)['ID']) if not str(identifier).isdigit() else identifier
        return self.get(f'cmdb/{id}/relationships')

    def add_relationship(self,ci_id,type_id,other_item_id,is_parent=True,remove_existing=False):
        #ConfigurationItemID is needed for assets
        data = None
        return self.put(f"cmdb/{ci_id}/relationships?typeId={type_id}&otherItemId={other_item_id}&isParent={is_parent}&removeExisting={remove_existing}", data)
    def add_asset(self,ci_id,asset_id):
        return self.add_relationship(ci_id,type_id=10012,other_item_id=asset_id)
