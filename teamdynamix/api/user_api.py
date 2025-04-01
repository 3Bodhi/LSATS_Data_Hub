from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional, BinaryIO

class UserAPI(TeamDynamixAPI):
    def search_user(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of users. Will not return full user information.

        Args:
            data: Dictionary/JSON object of search options.
        """
        return self.post('people/search', data)

    def search_users_by_uniqname(self, uniqname: str, isActive: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Searches for users by uniqname in various fields. Returns list of account objects.

        Args:
            uniqname: The uniqname to search for.
            isActive: Whether to only return active users.
        """
        data = {
            'UserName': f"{uniqname}@umich.edu",
            'isActive': isActive
        }
        result = self.post('people/search', data)
        if result:
            return result
        else:
            data = {
                'AlternateID': uniqname,
                'isActive': isActive
            }
            result = self.post('people/search', data)
            if result:
                return result
            else:
                data = {
                    'SearchText': uniqname,
                    'isActive': isActive
                }
                result = self.post('people/search', data)
                if result:
                    return result
                else:
                    print(f"WARNING: no match found for {uniqname}")
                    return None

    def get_user(self, uniqname: Optional[str] = None, uid: Optional[str] = None, isActive: bool = True) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Gets user information by uniqname or UID.

        Args:
            uniqname: The uniqname to search for.
            uid: The user's unique identifier.
            isActive: Whether to only return active users.
        """
        if uid:
            return self.get(f"people/{uid}")
        if uniqname:
            return self.search_users_by_uniqname(uniqname, isActive=isActive)
        return None

    def get_user_attribute(self, uniqname: str, attribute: str, isActive: bool = True) -> Any:
        """
        Gets a specific attribute of a user.

        Args:
            uniqname: The uniqname to search for.
            attribute: The attribute to retrieve.
            isActive: Whether to only return active users.
        """
        user = self.get_user(uniqname, isActive=isActive)
        if user and isinstance(user, list) and len(user) > 0:
            return user[0][attribute]
        return None

    def get_user_list(self, isActive: Optional[bool] = True, isConfidential: Optional[bool] = False,
                     isEmployee: Optional[bool] = False, userType: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Gets a list of TeamDynamix people. Will not return full user information.

        Args:
            isActive: The active status to filter on.
            isConfidential: The confidential status to filter on.
            isEmployee: The employee status to filter on.
            userType: The type of user to filter on. None will return users of any type.
        """
        # NOTE: This action can only be performed by a special key-based administrative service account
        return self.get(f"people/userlist?isActive={isActive}&isConfidential={isConfidential}&isEmployee={isEmployee}&userType={userType}")

    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a user in the system and returns an object representing that person.

        Args:
            user_data: The user data to create.
        """
        return self.post('people', user_data)

    def get_user_by_uid(self, uid: str) -> Dict[str, Any]:
        """
        Gets a person from the system by their unique identifier.

        Args:
            uid: The user unique identifier.
        """
        return self.get(f'people/{uid}')

    def get_user_by_username(self, username: str) -> Dict[str, Any]:
        """
        Gets a person from the system by their username.

        Args:
            username: The username of the person.
        """
        return self.get(f'people/{username}')

    def get_uid_by_username(self, username: str) -> str:
        """
        Gets the GUID of a person from the system by their username.

        Args:
            username: The username of the person.
        """
        return self.get(f'people/getuid/{username}')

    def update_user(self, uid: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates a person entry for the user with the specified identifier.

        Args:
            uid: The person's unique identifier.
            user_data: New values for the user.
        """
        return self.post(f'people/{uid}', user_data)

    def patch_user(self, uid: str, patch_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Patches an existing person. This only supports patching the person itself,
        custom attributes, applications and organization applications.

        Args:
            uid: The person's unique identifier.
            patch_data: The patch document containing changes to apply to the person.
        """
        return self.patch(f'people/{uid}', patch_data)

    def get_user_functional_roles(self, uid: str) -> List[Dict[str, Any]]:
        """
        Gets all functional roles for a particular user.

        Args:
            uid: The UID of the user.
        """
        return self.get(f'people/{uid}/functionalroles')

    def add_user_to_functional_role(self, uid: str, role_id: int, is_primary: bool = False) -> Dict[str, Any]:
        """
        Adds the user to functional role if they are not already in that role.
        If they are in that role, this will update whether or not that role
        is the user's primary functional role.

        Args:
            uid: The UID of the user.
            role_id: The functional role ID.
            is_primary: Indicates whether to set this role as the user's primary functional role.
        """
        return self.put(f'people/{uid}/functionalroles/{role_id}?isPrimary={is_primary}', data={})

    def remove_user_from_functional_role(self, uid: str, role_id: int) -> Dict[str, Any]:
        """
        Removes the user from a functional role.

        Args:
            uid: The UID of the user.
            role_id: The functional role ID.
        """
        return self.delete(f'people/{uid}/functionalroles/{role_id}')

    def get_user_groups(self, uid: str) -> List[Dict[str, Any]]:
        """
        Gets all groups for a particular user.

        Args:
            uid: The UID of the user.
        """
        return self.get(f'people/{uid}/groups')

    def add_user_to_group(self, uid: str, group_id: int, is_primary: bool = False,
                          is_notified: bool = False, is_manager: bool = False) -> Dict[str, Any]:
        """
        Adds the user to a group if they are not already in that group.
        If they are in that group, this will update the user's settings for that group.

        Args:
            uid: The UID of the user.
            group_id: The ID of the group to add.
            is_primary: Whether this is the user's primary group.
            is_notified: Whether the user is notified along with this group.
            is_manager: Whether the user is a group manager.
        """
        url = f'people/{uid}/groups/{group_id}?isPrimary={is_primary}&isNotified={is_notified}&isManager={is_manager}'
        return self.put(url, data={})

    def remove_user_from_group(self, uid: str, group_id: int) -> Dict[str, Any]:
        """
        Removes the user from a group.

        Args:
            uid: The UID of the user.
            group_id: The group ID.
        """
        return self.delete(f'people/{uid}/groups/{group_id}')

    def add_user_to_groups(self, uid: str, group_ids: List[int], remove_other_groups: bool = False) -> Dict[str, Any]:
        """
        Adds a user to a collection of groups.

        Args:
            uid: The UID of the user.
            group_ids: The IDs of the groups to add the user to.
            remove_other_groups: If true, other groups that this user is a member of will be removed.
        """
        return self.post(f'people/{uid}/groups?removeOtherGroups={remove_other_groups}', group_ids)

    def set_user_active_status(self, uid: str, status: bool) -> Dict[str, Any]:
        """
        Updates the active status of the specified user.

        Args:
            uid: The GUID of the user to update.
            status: The new active status for the user.
        """
        return self.put(f'people/{uid}/isactive?status={status}', data={})

    def lookup_users(self, search_text: str = '', max_results: int = 50) -> List[Dict[str, Any]]:
        """
        Performs a restricted lookup of TeamDynamix people. Will not return full user information.

        Args:
            search_text: The searching text to use.
            max_results: The maximum number of results to return (1-100).
        """
        return self.get(f'people/lookup?searchText={search_text}&maxResults={max_results}')

    def import_users(self, file: BinaryIO, allow_active_changes: bool = False,
                    allow_security_role_changes: bool = False, allow_application_changes: bool = False,
                    notify_email_addresses: str = '') -> Dict[str, Any]:
        """
        Imports users from a file. The file must be in .xlsx format with a header row.

        Args:
            file: The Excel file containing user data.
            allow_active_changes: Whether to allow changes to user active status.
            allow_security_role_changes: Whether to allow changes to security roles.
            allow_application_changes: Whether to allow changes to applications.
            notify_email_addresses: Email addresses to notify about the import.
        """
        url = (f'people/import?AllowIsActiveChanges={allow_active_changes}'
              f'&AllowSecurityRoleChanges={allow_security_role_changes}'
              f'&AllowApplicationChanges={allow_application_changes}'
              f'&NotifyEmailAddresses={notify_email_addresses}')
        files = {'file': file}
        return self.post(url, files=files)
