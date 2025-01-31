from .teamdynamix_api import TeamDynamixAPI
class GroupAPI(TeamDynamixAPI):
    def get_group(self, id):
        return self.get(f'groups/{id}')

    def get_group_members(self, id):
        return self.get(f'groups/{id}/members')
