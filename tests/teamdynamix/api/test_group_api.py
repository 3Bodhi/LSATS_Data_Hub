import unittest
from unittest.mock import patch, MagicMock
from teamdynamix.api.group_api import GroupAPI

class TestGroupAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the GroupAPI class.

    These tests focus on the group-specific functionality without
    redundantly testing the underlying TeamDynamixAPI methods.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the parent class to avoid testing its functionality
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "12345"
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}

        # Create an instance of GroupAPI with the required parameters
        self.api = GroupAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        patch.stopall()

    # ----- Test get_group method -----

    def test_get_group_success(self):
        """Test successfully retrieving a group by ID."""
        # Prepare test data
        group_id = 42
        expected_response = {
            "ID": group_id,
            "Name": "Test Group",
            "Description": "This is a test group"
        }

        # Mock the get method
        self.api.get = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.get_group(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}')

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_get_group_error(self):
        """Test error handling when retrieving a group fails."""
        # Prepare test data
        group_id = 42

        # Mock the get method to simulate an error
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_group(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}')

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test get_group_members method -----

    def test_get_group_members_success(self):
        """Test successfully retrieving members of a group."""
        # Prepare test data
        group_id = 42
        expected_response = [
            {"UID": "user1", "FirstName": "John", "LastName": "Doe"},
            {"UID": "user2", "FirstName": "Jane", "LastName": "Smith"}
        ]

        # Mock the get method
        self.api.get = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.get_group_members(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}/members')

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_get_group_members_error(self):
        """Test error handling when retrieving group members fails."""
        # Prepare test data
        group_id = 42

        # Mock the get method to simulate an error
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_group_members(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}/members')

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test create_group method -----

    def test_create_group_success(self):
        """Test successfully creating a new group."""
        # Prepare test data
        group_data = {
            "Name": "New Test Group",
            "Description": "This is a newly created test group"
        }
        expected_response = {
            "ID": 42,
            "Name": "New Test Group",
            "Description": "This is a newly created test group"
        }

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.create_group(group_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('groups', group_data)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_create_group_error(self):
        """Test error handling when creating a group fails."""
        # Prepare test data
        group_data = {
            "Name": "New Test Group",
            "Description": "This is a newly created test group"
        }

        # Mock the post method to simulate an error
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.create_group(group_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('groups', group_data)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test update_group method -----

    def test_update_group_success(self):
        """Test successfully updating an existing group."""
        # Prepare test data
        group_id = 42
        group_data = {
            "Name": "Updated Test Group",
            "Description": "This is an updated test group"
        }
        expected_response = {
            "ID": group_id,
            "Name": "Updated Test Group",
            "Description": "This is an updated test group"
        }

        # Mock the put method
        self.api.put = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.update_group(group_id, group_data)

        # Assert the put method was called with the correct parameters
        self.api.put.assert_called_once_with(f'groups/{group_id}', group_data)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_update_group_error(self):
        """Test error handling when updating a group fails."""
        # Prepare test data
        group_id = 42
        group_data = {
            "Name": "Updated Test Group",
            "Description": "This is an updated test group"
        }

        # Mock the put method to simulate an error
        self.api.put = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.update_group(group_id, group_data)

        # Assert the put method was called with the correct parameters
        self.api.put.assert_called_once_with(f'groups/{group_id}', group_data)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test get_group_applications method -----

    def test_get_group_applications_success(self):
        """Test successfully retrieving applications associated with a group."""
        # Prepare test data
        group_id = 42
        expected_response = [
            {"ID": 1, "Name": "Application 1"},
            {"ID": 2, "Name": "Application 2"}
        ]

        # Mock the get method
        self.api.get = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.get_group_applications(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}/applications')

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_get_group_applications_error(self):
        """Test error handling when retrieving group applications fails."""
        # Prepare test data
        group_id = 42

        # Mock the get method to simulate an error
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_group_applications(group_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'groups/{group_id}/applications')

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test add_applications_to_group method -----

    def test_add_applications_to_group_success(self):
        """Test successfully adding applications to a group."""
        # Prepare test data
        group_id = 42
        app_ids = [1, 2, 3]
        expected_response = {"Success": True}

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.add_applications_to_group(group_id, app_ids)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(f'groups/{group_id}/applications', app_ids)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_add_applications_to_group_error(self):
        """Test error handling when adding applications to a group fails."""
        # Prepare test data
        group_id = 42
        app_ids = [1, 2, 3]

        # Mock the post method to simulate an error
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.add_applications_to_group(group_id, app_ids)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(f'groups/{group_id}/applications', app_ids)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test remove_applications_from_group method -----

    def test_remove_applications_from_group_success(self):
        """Test successfully removing applications from a group."""
        # Prepare test data
        group_id = 42
        app_ids = [1, 2, 3]
        expected_response = {"Success": True}

        # Mock the delete method
        self.api.delete = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.remove_applications_from_group(group_id, app_ids)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'groups/{group_id}/applications', app_ids)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_remove_applications_from_group_error(self):
        """Test error handling when removing applications from a group fails."""
        # Prepare test data
        group_id = 42
        app_ids = [1, 2, 3]

        # Mock the delete method to simulate an error
        self.api.delete = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.remove_applications_from_group(group_id, app_ids)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'groups/{group_id}/applications', app_ids)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test add_members_to_group method -----

    def test_add_members_to_group_success(self):
        """Test successfully adding members to a group with default values."""
        # Prepare test data
        group_id = 42
        user_uids = ["user1", "user2"]
        expected_response = {"Success": True}

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested with default parameters
        result = self.api.add_members_to_group(group_id, user_uids)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(
            f'groups/{group_id}/members?isPrimary=False&isNotified=False&isManager=False',
            user_uids
        )

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_add_members_to_group_with_options_success(self):
        """Test successfully adding members to a group with custom options."""
        # Prepare test data
        group_id = 42
        user_uids = ["user1", "user2"]
        is_primary = True
        is_notified = True
        is_manager = True
        expected_response = {"Success": True}

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested with custom parameters
        result = self.api.add_members_to_group(
            group_id,
            user_uids,
            is_primary=is_primary,
            is_notified=is_notified,
            is_manager=is_manager
        )

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(
            f'groups/{group_id}/members?isPrimary=True&isNotified=True&isManager=True',
            user_uids
        )

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_add_members_to_group_error(self):
        """Test error handling when adding members to a group fails."""
        # Prepare test data
        group_id = 42
        user_uids = ["user1", "user2"]

        # Mock the post method to simulate an error
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.add_members_to_group(group_id, user_uids)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(
            f'groups/{group_id}/members?isPrimary=False&isNotified=False&isManager=False',
            user_uids
        )

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test remove_members_from_group method -----

    def test_remove_members_from_group_success(self):
        """Test successfully removing members from a group."""
        # Prepare test data
        group_id = 42
        user_uids = ["user1", "user2"]
        expected_response = {"Success": True}

        # Mock the delete method
        self.api.delete = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.remove_members_from_group(group_id, user_uids)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'groups/{group_id}/members', user_uids)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_remove_members_from_group_error(self):
        """Test error handling when removing members from a group fails."""
        # Prepare test data
        group_id = 42
        user_uids = ["user1", "user2"]

        # Mock the delete method to simulate an error
        self.api.delete = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.remove_members_from_group(group_id, user_uids)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'groups/{group_id}/members', user_uids)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test search_groups method -----

    def test_search_groups_success(self):
        """Test successfully searching for groups."""
        # Prepare test data
        search_params = {
            "NameLike": "Test",
            "IsActive": True
        }
        expected_response = [
            {"ID": 1, "Name": "Test Group 1"},
            {"ID": 2, "Name": "Test Group 2"}
        ]

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.search_groups(search_params)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('groups/search', search_params)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_search_groups_error(self):
        """Test error handling when searching for groups fails."""
        # Prepare test data
        search_params = {
            "NameLike": "Test",
            "IsActive": True
        }

        # Mock the post method to simulate an error
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.search_groups(search_params)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('groups/search', search_params)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Test edge cases -----

    def test_search_groups_with_empty_params(self):
        """Test searching for groups with empty parameters."""
        # Prepare test data
        search_params = {}
        expected_response = []

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.search_groups(search_params)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('groups/search', search_params)

        # Assert the result is as expected
        self.assertEqual(result, expected_response)

    def test_add_members_to_group_with_empty_list(self):
        """Test adding an empty list of members to a group."""
        # Prepare test data
        group_id = 42
        user_uids = []
        expected_response = {"Success": True}

        # Mock the post method
        self.api.post = MagicMock(return_value=expected_response)

        # Call the method being tested
        result = self.api.add_members_to_group(group_id, user_uids)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(
            f'groups/{group_id}/members?isPrimary=False&isNotified=False&isManager=False',
            user_uids
        )

        # Assert the result is as expected
        self.assertEqual(result, expected_response)


if __name__ == '__main__':
    unittest.main()
