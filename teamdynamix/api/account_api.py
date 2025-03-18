from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional

class AccountAPI(TeamDynamixAPI):
    def get_accounts(self) -> List[Dict[str, Any]]:
        """
        Gets a list of all active accounts/departments. Will not return full account/department information.

        Returns:
            A list of all active accounts/departments accessible to the current user.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
            Results will be limited to accounts/departments that the user is a member of,
            unless that user has the "View All Accounts/Departments" permission.
            The Attributes property will not be included in the results.
        """
        return self.get('accounts')

    def create_account(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new account.

        Args:
            account_data: The account data to be created. Should conform to TeamDynamix.Api.Accounts.Account structure.

        Returns:
            A dictionary containing the newly created account information.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
            This action requires the "Acct/Dept: Create Accts/Depts" permission.
        """
        return self.post('accounts', account_data)

    def get_account(self, id: int) -> Dict[str, Any]:
        """
        Gets an account with full information.

        Args:
            id: The account ID.

        Returns:
            A dictionary containing the full account information.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
            This action requires the user to be a member of the Account/Department requested,
            or have the "View All Accounts/Departments" permission.
        """
        return self.get(f'accounts/{id}')

    def edit_account(self, id: int, account_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits the account specified by the account ID.

        Args:
            id: The account ID.
            account_data: The fields that the updated account should hold. Should conform to
                          TeamDynamix.Api.Accounts.Account structure.

        Returns:
            A dictionary containing the updated account information.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
            This action requires the "Acct/Dept: Edit Accts/Depts" permission.
        """
        return self.put(f'accounts/{id}', account_data)

    def search_accounts(self, search_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of accounts/departments matching the specified criteria.
        Will not return full account/department information.

        Args:
            search_data: The searching parameters to use. Should conform to
                        TeamDynamix.Api.Accounts.AccountSearch structure.

        Returns:
            A list of accounts/departments matching the specified criteria.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
            Results will be limited to accounts/departments that the user is a member of,
            unless that user has the "View All Accounts/Departments" permission.
            The Attributes property will not be included in the results.
        """
        return self.post('accounts/search', search_data)
