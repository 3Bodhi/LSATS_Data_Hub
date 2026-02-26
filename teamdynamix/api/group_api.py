from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional

class GroupAPI(TeamDynamixAPI):
    def get_group(self, id: int) -> Dict[str, Any]:
        """
        Gets a group by ID.

        Args:
            id: The group ID.
        """
        return self.get(f'groups/{id}')

    def get_group_members(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the users belonging to a group.

        Args:
            id: The group identifier.
        """
        return self.get(f'groups/{id}/members')

    def create_group(self, group_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new group.

        Args:
            group_data: The group to be created.
        """
        return self.post('groups', group_data)

    def update_group(self, id: int, group_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits an existing group.

        Args:
            id: The group ID.
            group_data: The fields that the updated group should hold.
        """
        return self.put(f'groups/{id}', group_data)

    def get_group_applications(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the applications associated with the specified group.

        Args:
            id: The group ID.
        """
        return self.get(f'groups/{id}/applications')

    def add_applications_to_group(self, id: int, app_ids: List[int]) -> Dict[str, Any]:
        """
        Associates a collection of platform applications with a group.
        Existing application associations will not be affected.
        Only ticketing applications are able to be associated with a group.

        Args:
            id: The group ID.
            app_ids: The application IDs.
        """
        return self.post(f'groups/{id}/applications', app_ids)

    def remove_applications_from_group(self, id: int, app_ids: List[int]) -> Dict[str, Any]:
        """
        Unassociates a collection of platform applications from a group.

        Args:
            id: The group ID.
            app_ids: The application IDs.
        """
        return self.delete(f'groups/{id}/applications', app_ids)

    def add_members_to_group(self, id: int, user_uids: List[str],
                             is_primary: bool = False,
                             is_notified: bool = False,
                             is_manager: bool = False) -> Dict[str, Any]:
        """
        Adds a collection of users to a group. Users that did not exist in the group
        beforehand will have their settings set to the specified values.
        Existing users will not have their settings overwritten.

        Args:
            id: The group ID.
            user_uids: The user UIDs.
            is_primary: If True, new users will have this group set as their primary group.
            is_notified: If True, new users will be sent notifications for this group.
            is_manager: If True, new users will be set as a manager for this group.
        """
        url = f'groups/{id}/members?isPrimary={is_primary}&isNotified={is_notified}&isManager={is_manager}'
        return self.post(url, user_uids)

    def remove_members_from_group(self, id: int, user_uids: List[str]) -> Dict[str, Any]:
        """
        Removes a collection of users from a group.

        Args:
            id: The group ID.
            user_uids: The user UIDs.
        """
        return self.delete(f'groups/{id}/members', user_uids)

    def search_groups(self, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of groups based on search criteria.

        Args:
            search_params: The searching parameters to use.
        """
        return self.post('groups/search', search_params)
