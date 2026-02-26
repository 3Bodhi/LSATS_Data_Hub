import unittest
from unittest.mock import patch, MagicMock
import json
import time
from datetime import datetime
from teamdynamix.api.account_api import AccountAPI
from teamdynamix.api.teamdynamix_api import TeamDynamixAPI

class TestAccountAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the AccountAPI class.

    Tests cover:
    - Basic functionality for all methods
    - Error handling
    - Edge cases
    - Parent class interactions
    - HTTP status code handling
    - Response parsing
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock for the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.account_api.TeamDynamixAPI').start()

        # Create an instance of AccountAPI with the mocked parent
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "12345"
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        self.api = AccountAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    # ----- Basic functionality tests -----

    def test_get_accounts(self):
        """Test successfully retrieving all accounts."""
        # Mock the response from get method
        mock_response = [
            {"ID": 1, "Name": "Department A"},
            {"ID": 2, "Name": "Department B"}
        ]
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_accounts()

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with('accounts')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_create_account(self):
        """Test successfully creating a new account."""
        # Prepare test data
        account_data = {
            "Name": "New Department",
            "Description": "A new department"
        }

        # Mock the response from post method
        mock_response = {
            "ID": 3,
            "Name": "New Department",
            "Description": "A new department"
        }
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.create_account(account_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts', account_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_account(self):
        """Test successfully retrieving a specific account."""
        # Prepare test data
        account_id = 1

        # Mock the response from get method
        mock_response = {
            "ID": 1,
            "Name": "Department A",
            "Description": "Department A description"
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_account(account_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'accounts/{account_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_edit_account(self):
        """Test successfully editing an existing account."""
        # Prepare test data
        account_id = 1
        account_data = {
            "Name": "Updated Department A",
            "Description": "Updated description"
        }

        # Mock the response from put method
        mock_response = {
            "ID": 1,
            "Name": "Updated Department A",
            "Description": "Updated description"
        }
        self.api.put = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.edit_account(account_id, account_data)

        # Assert the put method was called with the correct parameters
        self.api.put.assert_called_once_with(f'accounts/{account_id}', account_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_search_accounts(self):
        """Test successfully searching for accounts."""
        # Prepare test data
        search_data = {
            "NameLike": "Department",
            "IsActive": True
        }

        # Mock the response from post method
        mock_response = [
            {"ID": 1, "Name": "Department A"},
            {"ID": 2, "Name": "Department B"}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_accounts(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts/search', search_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Error handling tests -----

    def test_get_accounts_error(self):
        """Test handling errors when retrieving accounts."""
        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_accounts()

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with('accounts')

        # Assert the result is None
        self.assertIsNone(result)

    def test_create_account_error(self):
        """Test handling errors when creating an account."""
        # Prepare test data
        account_data = {
            "Name": "New Department",
            "Description": "A new department"
        }

        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.create_account(account_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts', account_data)

        # Assert the result is None
        self.assertIsNone(result)

    def test_get_account_error(self):
        """Test handling errors when retrieving a specific account."""
        # Prepare test data
        account_id = 1

        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_account(account_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'accounts/{account_id}')

        # Assert the result is None
        self.assertIsNone(result)

    def test_edit_account_error(self):
        """Test handling errors when editing an account."""
        # Prepare test data
        account_id = 1
        account_data = {
            "Name": "Updated Department A",
            "Description": "Updated description"
        }

        # Mock the put method to return None (simulating an error)
        self.api.put = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.edit_account(account_id, account_data)

        # Assert the put method was called with the correct parameters
        self.api.put.assert_called_once_with(f'accounts/{account_id}', account_data)

        # Assert the result is None
        self.assertIsNone(result)

    def test_search_accounts_error(self):
        """Test handling errors when searching for accounts."""
        # Prepare test data
        search_data = {
            "NameLike": "Department",
            "IsActive": True
        }

        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.search_accounts(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts/search', search_data)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Edge case tests -----

    def test_create_account_with_empty_data(self):
        """Test creating an account with empty data."""
        # Prepare empty account data
        account_data = {}

        # Mock the post method
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.create_account(account_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts', account_data)

        # Assert the result is None
        self.assertIsNone(result)

    def test_get_account_with_invalid_id(self):
        """Test retrieving an account with an invalid ID."""
        # Prepare invalid ID
        account_id = "invalid"

        # Mock the get method
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_account(account_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'accounts/{account_id}')

        # Assert the result is None
        self.assertIsNone(result)

    def test_edit_account_with_empty_data(self):
        """Test editing an account with empty data."""
        # Prepare test data
        account_id = 1
        account_data = {}

        # Mock the put method
        self.api.put = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.edit_account(account_id, account_data)

        # Assert the put method was called with the correct parameters
        self.api.put.assert_called_once_with(f'accounts/{account_id}', account_data)

        # Assert the result is None
        self.assertIsNone(result)

    def test_search_accounts_with_empty_data(self):
        """Test searching for accounts with empty search criteria."""
        # Prepare empty search data
        search_data = {}

        # Mock the post method
        mock_response = []
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_accounts(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('accounts/search', search_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Parent class interaction tests -----

    def test_parent_get_method_called(self):
        """Test interaction with parent class get method."""
        # Override the mock for TeamDynamixAPI to allow proper inheritance
        patch.stopall()

        # Create a new mock for the parent class that allows proper method calls
        with patch('teamdynamix.api.teamdynamix_api.TeamDynamixAPI.get') as mock_get:
            # Set up the mock to return a specific value
            mock_get.return_value = [{"ID": 1, "Name": "Department A"}]

            # Create a new instance of the API
            api = AccountAPI(self.base_url, self.app_id, self.headers)

            # Call the method that would use the parent's get method
            result = api.get_accounts()

            # Verify the parent's get method was called with the correct parameters
            mock_get.assert_called_once_with('accounts')

            # Verify the result matches what the parent's get method returned
            self.assertEqual(result, [{"ID": 1, "Name": "Department A"}])

    def test_parent_post_method_called(self):
        """Test interaction with parent class post method."""
        # Override the mock for TeamDynamixAPI to allow proper inheritance
        patch.stopall()

        # Create a new mock for the parent class that allows proper method calls
        with patch('teamdynamix.api.teamdynamix_api.TeamDynamixAPI.post') as mock_post:
            # Set up the mock to return a specific value
            mock_post.return_value = {"ID": 3, "Name": "New Department"}

            # Create a new instance of the API
            api = AccountAPI(self.base_url, self.app_id, self.headers)

            # Prepare test data
            account_data = {"Name": "New Department"}

            # Call the method that would use the parent's post method
            result = api.create_account(account_data)

            # Verify the parent's post method was called with the correct parameters
            mock_post.assert_called_once_with('accounts', account_data)

            # Verify the result matches what the parent's post method returned
            self.assertEqual(result, {"ID": 3, "Name": "New Department"})

    def test_parent_put_method_called(self):
        """Test interaction with parent class put method."""
        # Override the mock for TeamDynamixAPI to allow proper inheritance
        patch.stopall()

        # Create a new mock for the parent class that allows proper method calls
        with patch('teamdynamix.api.teamdynamix_api.TeamDynamixAPI.put') as mock_put:
            # Set up the mock to return a specific value
            mock_put.return_value = {"ID": 1, "Name": "Updated Department"}

            # Create a new instance of the API
            api = AccountAPI(self.base_url, self.app_id, self.headers)

            # Prepare test data
            account_id = 1
            account_data = {"Name": "Updated Department"}

            # Call the method that would use the parent's put method
            result = api.edit_account(account_id, account_data)

            # Verify the parent's put method was called with the correct parameters
            mock_put.assert_called_once_with(f'accounts/{account_id}', account_data)

            # Verify the result matches what the parent's put method returned
            self.assertEqual(result, {"ID": 1, "Name": "Updated Department"})

    # ----- URL formatting tests -----

    def test_url_formatting(self):
        """Test URL formatting for API calls."""
        # Mock the get method to verify URL construction
        self.api.get = MagicMock(return_value={"ID": 1, "Name": "Department A"})

        # Call the method with a complex ID that might need URL encoding
        account_id = "Department & IT"
        result = self.api.get_account(account_id)

        # Verify that get was called with the correctly formatted URL path
        self.api.get.assert_called_once_with(f'accounts/{account_id}')

if __name__ == '__main__':
    unittest.main()
