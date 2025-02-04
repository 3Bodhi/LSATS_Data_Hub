from .teamdynamix_api import TeamDynamixAPI
class AssetAPI(TeamDynamixAPI):
    def get_asset(self, asset_id):
        return self.get(f'assets/{asset_id}')
    def get_assets(self, search_item, search_by='owner'):
        if search_by == 'shortcode':
            attribute_id = 3513 # custom attribute # for shortcode
            data = {
                "CustomAttributes": [{
                   "ID": attribute_id, # shortcode
                   "Value": search_item
                   }]
            }
        else: # If Owner
            data = {"OwningCustomerIDs": search_item}
        return self.post(f'assets/search', data)
    def get_asset_attribute(self, asset_id, attribute, custom=False):
        asset = self.get_asset
        return NotImplemented
    def search_asset(self, data):
        return self.post('assets/search', data)
    def add_asset(self, asset_id, ticket_id):
        return self.post(f'/assets/{asset_id}/tickets/{ticket_id}')
