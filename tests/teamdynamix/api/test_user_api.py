import unittest
from unittest.mock import patch, MagicMock
import json
from teamdynamix.api.user_api import UserAPI

class TestUserAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the UserAPI class.

    Tests cover:
    - Basic functionality for key methods
    - Error handling
    - Edge cases
    - Minimal parent class interactions to avoid redundancy
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock for the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.user_api.TeamDynamixAPI').start()

        # Create an instance of UserAPI with the mocked parent
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "12345"
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        self.api = UserAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    # ----- Search/Get User Tests -----

    def test_search_user(self):
        """Test successfully searching for users."""
        # Prepare test data
        search_data = {
            "UserName": "testuser@umich.edu",
            "isActive": True
        }

        # Mock the response from post method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_user(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('people/search', search_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_search_users_by_uniqname_with_direct_match(self):
        """Test searching users by uniqname with a direct match."""
        # Prepare test data
        uniqname = "testuser"
        
        # Set up the mock to return a result for the first search
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}
        ]
        self.api.post = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.search_users_by_uniqname(uniqname)
        
        # Assert post was called with the correct parameters for the first search
        self.api.post.assert_called_once_with('people/search', {"UserName": "testuser@umich.edu", "isActive": True})
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_search_users_by_uniqname_with_alternate_id_match(self):
        """Test searching users by uniqname with a match on alternate ID."""
        # Prepare test data
        uniqname = "testuser"
        
        # Set up the mock to return None for the first search, then a result for the second
        self.api.post = MagicMock()
        self.api.post.side_effect = [
            None,  # First search returns None
            [{"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}]  # Second search returns a result
        ]
        
        # Call the method being tested
        result = self.api.search_users_by_uniqname(uniqname)
        
        # Assert post was called with the correct parameters for both searches
        expected_calls = [
            unittest.mock.call('people/search', {"UserName": "testuser@umich.edu", "isActive": True}),
            unittest.mock.call('people/search', {"AlternateID": "testuser", "isActive": True})
        ]
        self.api.post.assert_has_calls(expected_calls)
        
        # Assert the result is as expected
        self.assertEqual(result, [{"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}])

    def test_search_users_by_uniqname_with_search_text_match(self):
        """Test searching users by uniqname with a match on search text."""
        # Prepare test data
        uniqname = "testuser"
        
        # Set up the mock to return None for the first two searches, then a result for the third
        self.api.post = MagicMock()
        self.api.post.side_effect = [
            None,  # First search returns None
            None,  # Second search returns None
            [{"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}]  # Third search returns a result
        ]
        
        # Call the method being tested
        result = self.api.search_users_by_uniqname(uniqname)
        
        # Assert post was called with the correct parameters for all three searches
        expected_calls = [
            unittest.mock.call('people/search', {"UserName": "testuser@umich.edu", "isActive": True}),
            unittest.mock.call('people/search', {"AlternateID": "testuser", "isActive": True}),
            unittest.mock.call('people/search', {"SearchText": "testuser", "isActive": True})
        ]
        self.api.post.assert_has_calls(expected_calls)
        
        # Assert the result is as expected
        self.assertEqual(result, [{"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}])

    def test_search_users_by_uniqname_no_match(self):
        """Test searching users by uniqname with no match."""
        # Prepare test data
        uniqname = "testuser"
        
        # Set up the mock to return None for all searches
        self.api.post = MagicMock(return_value=None)
        
        # Patch the print function to avoid actual console output during tests
        with patch('builtins.print') as mock_print:
            # Call the method being tested
            result = self.api.search_users_by_uniqname(uniqname)
            
            # Assert print was called with the warning message
            mock_print.assert_called_once_with(f"WARNING: no match found for {uniqname}")
        
        # Assert result is None
        self.assertIsNone(result)

    def test_get_user_by_uniqname(self):
        """Test getting a user by uniqname."""
        # Prepare test data
        uniqname = "testuser"
        
        # Mock the search_users_by_uniqname method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}
        ]
        self.api.search_users_by_uniqname = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_user(uniqname=uniqname)
        
        # Assert search_users_by_uniqname was called with the correct parameters
        self.api.search_users_by_uniqname.assert_called_once_with(uniqname, isActive=True)
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_user_by_uid(self):
        """Test getting a user by UID."""
        # Prepare test data
        uid = "user123"
        
        # Mock the get method
        mock_response = {
            "UID": "user123", 
            "UserName": "testuser@umich.edu", 
            "FirstName": "Test", 
            "LastName": "User"
        }
        self.api.get = MagicMock(return_value=mock_response)
        
        # Patch the print function to avoid actual console output during tests
        with patch('builtins.print'):
            # Call the method being tested
            result = self.api.get_user(uid=uid)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f"people/{uid}")
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_user_no_parameters(self):
        """Test getting a user with no parameters."""
        # Call the method being tested with no parameters
        result = self.api.get_user()
        
        # Assert the result is None
        self.assertIsNone(result)

    # ----- User Attribute Tests -----

    def test_get_user_attribute(self):
        """Test getting a specific attribute of a user."""
        # Prepare test data
        uniqname = "testuser"
        attribute = "UID"
        
        # Mock the get_user method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu", "FirstName": "Test", "LastName": "User"}
        ]
        self.api.get_user = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_user_attribute(uniqname, attribute)
        
        # Assert get_user was called with the correct parameters
        self.api.get_user.assert_called_once_with(uniqname, isActive=True)
        
        # Assert the result is as expected
        self.assertEqual(result, "user123")

    def test_get_user_attribute_no_user(self):
        """Test getting an attribute when the user is not found."""
        # Prepare test data
        uniqname = "testuser"
        attribute = "UID"
        
        # Mock the get_user method to return None
        self.api.get_user = MagicMock(return_value=None)
        
        # Call the method being tested
        result = self.api.get_user_attribute(uniqname, attribute)
        
        # Assert get_user was called with the correct parameters
        self.api.get_user.assert_called_once_with(uniqname, isActive=True)
        
        # Assert the result is None
        self.assertIsNone(result)

    def test_get_user_attribute_empty_list(self):
        """Test getting an attribute when the user result is an empty list."""
        # Prepare test data
        uniqname = "testuser"
        attribute = "UID"
        
        # Mock the get_user method to return an empty list
        self.api.get_user = MagicMock(return_value=[])
        
        # Call the method being tested
        result = self.api.get_user_attribute(uniqname, attribute)
        
        # Assert get_user was called with the correct parameters
        self.api.get_user.assert_called_once_with(uniqname, isActive=True)
        
        # Assert the result is None
        self.assertIsNone(result)

    # ----- User List Tests -----

    def test_get_user_list(self):
        """Test getting a list of TeamDynamix people."""
        # Mock the get method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu"},
            {"UID": "user456", "UserName": "anotheruser@umich.edu"}
        ]
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested with default parameters
        result = self.api.get_user_list()
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with("people/userlist?isActive=True&isConfidential=False&isEmployee=False&userType=None")
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_user_list_with_parameters(self):
        """Test getting a list of TeamDynamix people with custom parameters."""
        # Mock the get method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu"},
            {"UID": "user456", "UserName": "anotheruser@umich.edu"}
        ]
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested with custom parameters
        result = self.api.get_user_list(isActive=False, isConfidential=True, isEmployee=True, userType="Faculty")
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with("people/userlist?isActive=False&isConfidential=True&isEmployee=True&userType=Faculty")
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- User Creation/Update Tests -----

    def test_create_user(self):
        """Test creating a user in the system."""
        # Prepare test data
        user_data = {
            "UserName": "newuser@umich.edu",
            "FirstName": "New",
            "LastName": "User"
        }
        
        # Mock the post method
        mock_response = {
            "UID": "newuser123",
            "UserName": "newuser@umich.edu",
            "FirstName": "New",
            "LastName": "User"
        }
        self.api.post = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.create_user(user_data)
        
        # Assert post was called with the correct parameters
        self.api.post.assert_called_once_with('people', user_data)
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_user_by_uid_direct(self):
        """Test getting a user by their unique identifier directly."""
        # Prepare test data
        uid = "user123"
        
        # Mock the get method
        mock_response = {
            "UID": "user123",
            "UserName": "testuser@umich.edu",
            "FirstName": "Test",
            "LastName": "User"
        }
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_user_by_uid(uid)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f'people/{uid}')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_uid_by_username(self):
        """Test getting the GUID of a person by their username."""
        # Prepare test data
        username = "testuser@umich.edu"
        
        # Mock the get method
        mock_response = "user123"
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_uid_by_username(username)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f'people/getuid/{username}')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_update_user(self):
        """Test updating a person entry."""
        # Prepare test data
        uid = "user123"
        user_data = {
            "FirstName": "Updated",
            "LastName": "User"
        }
        
        # Mock the post method
        mock_response = {
            "UID": "user123",
            "UserName": "testuser@umich.edu",
            "FirstName": "Updated",
            "LastName": "User"
        }
        self.api.post = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.update_user(uid, user_data)
        
        # Assert post was called with the correct parameters
        self.api.post.assert_called_once_with(f'people/{uid}', user_data)
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_patch_user(self):
        """Test patching an existing person."""
        # Prepare test data
        uid = "user123"
        patch_data = [
            {
                "op": "replace",
                "path": "/FirstName",
                "value": "Patched"
            }
        ]
        
        # Mock the patch method
        mock_response = {
            "UID": "user123",
            "UserName": "testuser@umich.edu",
            "FirstName": "Patched",
            "LastName": "User"
        }
        self.api.patch = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.patch_user(uid, patch_data)
        
        # Assert patch was called with the correct parameters
        self.api.patch.assert_called_once_with(f'people/{uid}', patch_data)
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Functional Roles Tests -----

    def test_get_user_functional_roles(self):
        """Test getting all functional roles for a user."""
        # Prepare test data
        uid = "user123"
        
        # Mock the get method
        mock_response = [
            {"ID": 1, "Name": "Role A"},
            {"ID": 2, "Name": "Role B"}
        ]
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_user_functional_roles(uid)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f'people/{uid}/functionalroles')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_add_user_to_functional_role(self):
        """Test adding a user to a functional role."""
        # Prepare test data
        uid = "user123"
        role_id = 1
        is_primary = True
        
        # Mock the put method
        mock_response = {
            "Success": True
        }
        self.api.put = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.add_user_to_functional_role(uid, role_id, is_primary)
        
        # Assert put was called with the correct parameters
        self.api.put.assert_called_once_with(f'people/{uid}/functionalroles/{role_id}?isPrimary={is_primary}', data={})
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_remove_user_from_functional_role(self):
        """Test removing a user from a functional role."""
        # Prepare test data
        uid = "user123"
        role_id = 1
        
        # Mock the delete method
        mock_response = {
            "Success": True
        }
        self.api.delete = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.remove_user_from_functional_role(uid, role_id)
        
        # Assert delete was called with the correct parameters
        self.api.delete.assert_called_once_with(f'people/{uid}/functionalroles/{role_id}')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Group Management Tests -----

    def test_get_user_groups(self):
        """Test getting all groups for a user."""
        # Prepare test data
        uid = "user123"
        
        # Mock the get method
        mock_response = [
            {"ID": 1, "Name": "Group A"},
            {"ID": 2, "Name": "Group B"}
        ]
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.get_user_groups(uid)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f'people/{uid}/groups')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_add_user_to_group(self):
        """Test adding a user to a group."""
        # Prepare test data
        uid = "user123"
        group_id = 1
        is_primary = True
        is_notified = True
        is_manager = False
        
        # Mock the put method
        mock_response = {
            "Success": True
        }
        self.api.put = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.add_user_to_group(uid, group_id, is_primary, is_notified, is_manager)
        
        # Assert put was called with the correct parameters
        expected_url = f'people/{uid}/groups/{group_id}?isPrimary={is_primary}&isNotified={is_notified}&isManager={is_manager}'
        self.api.put.assert_called_once_with(expected_url, data={})
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_add_user_to_groups(self):
        """Test adding a user to multiple groups."""
        # Prepare test data
        uid = "user123"
        group_ids = [1, 2, 3]
        remove_other_groups = True
        
        # Mock the post method
        mock_response = {
            "Success": True
        }
        self.api.post = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.add_user_to_groups(uid, group_ids, remove_other_groups)
        
        # Assert post was called with the correct parameters
        self.api.post.assert_called_once_with(f'people/{uid}/groups?removeOtherGroups={remove_other_groups}', group_ids)
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Miscellaneous User Methods Tests -----

    def test_set_user_active_status(self):
        """Test updating the active status of a user."""
        # Prepare test data
        uid = "user123"
        status = True
        
        # Mock the put method
        mock_response = {
            "Success": True
        }
        self.api.put = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.set_user_active_status(uid, status)
        
        # Assert put was called with the correct parameters
        self.api.put.assert_called_once_with(f'people/{uid}/isactive?status={status}', data={})
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_lookup_users(self):
        """Test looking up users with search parameters."""
        # Prepare test data
        search_text = "test"
        max_results = 10
        
        # Mock the get method
        mock_response = [
            {"UID": "user123", "UserName": "testuser@umich.edu"}
        ]
        self.api.get = MagicMock(return_value=mock_response)
        
        # Call the method being tested with custom parameters
        result = self.api.lookup_users(search_text, max_results)
        
        # Assert get was called with the correct parameters
        self.api.get.assert_called_once_with(f'people/lookup?searchText={search_text}&maxResults={max_results}')
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_import_users(self):
        """Test importing users from a file."""
        # Prepare test data
        file_mock = MagicMock()
        allow_active_changes = True
        allow_security_role_changes = False
        allow_application_changes = True
        notify_email_addresses = "admin@umich.edu"
        
        # Mock the post method
        mock_response = {
            "SuccessCount": 2,
            "ErrorCount": 0
        }
        self.api.post = MagicMock(return_value=mock_response)
        
        # Call the method being tested
        result = self.api.import_users(
            file_mock,
            allow_active_changes,
            allow_security_role_changes,
            allow_application_changes,
            notify_email_addresses
        )
        
        # Assert post was called with the correct parameters
        expected_url = (
            'people/import?AllowIsActiveChanges=True'
            '&AllowSecurityRoleChanges=False'
            '&AllowApplicationChanges=True'
            '&NotifyEmailAddresses=admin@umich.edu'
        )
        self.api.post.assert_called_once_with(expected_url, files={'file': file_mock})
        
        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Error Handling Tests -----

    def test_error_handling_for_get_method(self):
        """Test error handling when the get method returns None."""
        # Prepare test data
        uid = "user123"
        
        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)
        
        # Call methods that use get and check results
        result1 = self.api.get_user_by_uid(uid)
        result2 = self.api.get_user_functional_roles(uid)
        
        # Assert get was called with the correct parameters
        expected_calls = [
            unittest.mock.call(f'people/{uid}'),
            unittest.mock.call(f'people/{uid}/functionalroles')
        ]
        self.api.get.assert_has_calls(expected_calls)
        
        # Assert the results are None
        self.assertIsNone(result1)
        self.assertIsNone(result2)

    def test_error_handling_for_post_method(self):
        """Test error handling when the post method returns None."""
        # Prepare test data
        search_data = {"UserName": "testuser@umich.edu"}
        
        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)
        
        # Call the search_user method
        result = self.api.search_user(search_data)
        
        # Assert post was called with the correct parameters
        self.api.post.assert_called_once_with('people/search', search_data)
        
        # Assert the result is None
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
