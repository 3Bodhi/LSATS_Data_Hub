import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import copy

from teamdynamix.api.configuration_item_api import ConfigurationItemAPI

class TestConfigurationItemAPI(unittest.TestCase):
    """Test cases for the ConfigurationItemAPI class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create patcher for the open function to mock loading the default config
        self.mock_open_patcher = patch('builtins.open', mock_open(read_data='{"ID": "", "Name": "", "TypeID": "10014", "TypeName": "Place"}'))
        self.mock_open = self.mock_open_patcher.start()

        # Create patcher for json.load
        self.mock_json_load_patcher = patch('json.load')
        self.mock_json_load = self.mock_json_load_patcher.start()
        self.mock_json_load.return_value = {"ID": "", "Name": "", "TypeID": "10014", "TypeName": "Place"}

        # Initialize API with mock values
        self.base_url = 'https://example.com/api'
        self.app_id = '123'
        self.headers = {'Authorization': 'Bearer test_token', 'Content-Type': 'application/json'}

        # Initialize the API
        self.api = ConfigurationItemAPI(self.base_url, self.app_id, self.headers)

        # Access the default_config directly
        self.api.default_config = {"ID": "", "Name": "", "TypeID": "10014", "TypeName": "Place"}

    def tearDown(self):
        """Clean up after each test."""
        self.mock_open_patcher.stop()
        self.mock_json_load_patcher.stop()
        patch.stopall()

    def test_search_ci(self):
        """Test searching for configuration items by name."""
        # Mock the post method to return test data
        self.api.post = MagicMock(return_value=[{"ID": 1, "Name": "Test CI"}])

        # Call the method
        result = self.api.search_ci("Test CI")

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('cmdb/search', {"NameLike": "Test CI"})

        # Verify result
        self.assertEqual(result, [{"ID": 1, "Name": "Test CI"}])

    def test_get_ci_by_id(self):
        """Test getting a configuration item by ID."""
        # Mock the get method to return test data
        self.api.get = MagicMock(return_value={"ID": 1, "Name": "Test CI"})

        # Call the method
        result = self.api.get_ci(1)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('cmdb/1')

        # Verify result
        self.assertEqual(result, {"ID": 1, "Name": "Test CI"})

    def test_get_ci_by_name_exact_match(self):
        """Test getting a configuration item by name with exact match."""
        # Setup mock search results
        search_result = [{"ID": 1, "Name": "Test CI"}, {"ID": 2, "Name": "Another CI"}]
        self.api.search_ci = MagicMock(return_value=search_result)

        # Call the method
        result = self.api.get_ci("Test CI")

        # Verify search_ci was called with the correct parameters
        self.api.search_ci.assert_called_once_with("Test CI")

        # Verify result is the exact match
        self.assertEqual(result, {"ID": 1, "Name": "Test CI"})

    def test_get_ci_by_name_case_insensitive_match(self):
        """Test getting a configuration item by name with case-insensitive match."""
        # Setup mock search results with different case
        search_result = [{"ID": 1, "Name": "Test CI"}, {"ID": 2, "Name": "Another CI"}]
        self.api.search_ci = MagicMock(return_value=search_result)

        # Call the method with different case
        result = self.api.get_ci("test ci")

        # Verify search_ci was called with the correct parameters
        self.api.search_ci.assert_called_once_with("test ci")

        # Verify result is the case-insensitive match
        self.assertEqual(result, {"ID": 1, "Name": "Test CI"})

    def test_get_ci_by_name_no_exact_match(self):
        """Test getting a configuration item by name with no exact match."""
        # Setup mock search results without any match
        search_result = [{"ID": 1, "Name": "Test CI"}, {"ID": 2, "Name": "Another CI"}]
        self.api.search_ci = MagicMock(return_value=search_result)

        # Call the method with a name that doesn't exist
        result = self.api.get_ci("Non-existent CI")

        # Verify search_ci was called with the correct parameters
        self.api.search_ci.assert_called_once_with("Non-existent CI")

        # Verify result returns the first item when no match found
        self.assertEqual(result, {"ID": 1, "Name": "Test CI"})

    def test_get_ci_by_name_empty_search_result(self):
        """Test getting a configuration item by name with empty search results."""
        # Setup mock search results to be empty
        self.api.search_ci = MagicMock(return_value=[])

        # Call the method
        result = self.api.get_ci("Non-existent CI")

        # Verify search_ci was called with the correct parameters
        self.api.search_ci.assert_called_once_with("Non-existent CI")

        # Verify result is None
        self.assertIsNone(result)

    def test_create_ci(self):
        """Test creating a configuration item."""
        # Mock the post method to return test data
        self.api.post = MagicMock(return_value={"ID": 1, "Name": "New CI"})

        # Test data
        fields = {"Name": "New CI", "OwnerUID": "user123"}

        # Call the method
        result = self.api.create_ci(fields)

        # Verify post was called with the correct parameters
        expected_data = copy.deepcopy(self.api.default_config)
        expected_data.update(fields)
        self.api.post.assert_called_once_with("/cmdb", expected_data)

        # Verify result
        self.assertEqual(result, {"ID": 1, "Name": "New CI"})

    def test_edit_ci_success(self):
        """Test editing a configuration item successfully."""
        # Mock the get_ci and put methods
        self.api.get_ci = MagicMock(return_value={"ID": 1, "Name": "Test CI", "Description": "Old description"})
        self.api.put = MagicMock(return_value={"ID": 1, "Name": "Test CI", "Description": "New description"})

        # Test data
        fields = {"Description": "New description"}

        # Call the method
        result = self.api.edit_ci(fields, identifier=1)

        # Verify get_ci and put were called with the correct parameters
        self.api.get_ci.assert_called_once_with(1)

        expected_data = copy.deepcopy(self.api.default_config)
        expected_data.update(fields)
        self.api.put.assert_called_once_with("cmdb/1", expected_data)

        # Verify result
        self.assertEqual(result, {"ID": 1, "Name": "Test CI", "Description": "New description"})

    def test_edit_ci_no_changes_needed(self):
        """Test editing a configuration item when no changes are needed."""
        # Mock the get_ci method
        ci = {"ID": 1, "Name": "Test CI", "Description": "Existing description"}
        self.api.get_ci = MagicMock(return_value=ci)

        # Test data (same as existing)
        fields = {"Description": "Existing description"}

        # Call the method
        result = self.api.edit_ci(fields, identifier=1)

        # Verify get_ci was called but put was not
        self.api.get_ci.assert_called_once_with(1)

        # Verify result is None (no update needed)
        self.assertIsNone(result)

    def test_edit_ci_ci_not_found(self):
        """Test editing a configuration item that doesn't exist."""
        # Mock the get_ci method to return None
        self.api.get_ci = MagicMock(return_value=None)

        # Test data
        fields = {"Description": "New description"}

        # Call the method
        result = self.api.edit_ci(fields, identifier=1)

        # Verify get_ci was called but put was not
        self.api.get_ci.assert_called_once_with(1)

        # Verify result is None
        self.assertIsNone(result)

    def test_get_relationships(self):
        """Test getting relationships for a configuration item."""
        # Mock the get_ci and get methods
        self.api.get_ci = MagicMock(return_value={"ID": 1, "Name": "Test CI"})
        self.api.get = MagicMock(return_value=[{"ID": 101, "RelatedCIID": 2}])

        # Call the method with name
        result = self.api.get_relationships("Test CI")

        # Verify get_ci and get were called with the correct parameters
        self.api.get_ci.assert_called_once_with("Test CI")
        self.api.get.assert_called_once_with('cmdb/1/relationships')

        # Verify result
        self.assertEqual(result, [{"ID": 101, "RelatedCIID": 2}])

    def test_add_relationship(self):
        """Test adding a relationship between configuration items."""
        # Mock the put method
        self.api.put = MagicMock(return_value={"ID": 101, "ParentID": 1, "ChildID": 2})

        # Call the method
        result = self.api.add_relationship(1, 10012, 2, True, False)

        # Verify put was called with the correct parameters
        self.api.put.assert_called_once_with("cmdb/1/relationships?typeId=10012&otherItemId=2&isParent=True&removeExisting=False", data={})

        # Verify result
        self.assertEqual(result, {"ID": 101, "ParentID": 1, "ChildID": 2})

    def test_add_asset(self):
        """Test adding an asset to a configuration item."""
        # Mock the add_relationship method
        self.api.add_relationship = MagicMock(return_value={"ID": 101, "ParentID": 1, "ChildID": 100})

        # Call the method
        result = self.api.add_asset(1, 100)

        # Verify add_relationship was called with the correct parameters
        self.api.add_relationship.assert_called_once_with(1, type_id=10012, other_item_id=100)

        # Verify result
        self.assertEqual(result, {"ID": 101, "ParentID": 1, "ChildID": 100})

    def test_delete_ci(self):
        """Test deleting a configuration item."""
        # Mock the delete method
        self.api.delete = MagicMock(return_value={"ID": 1, "Status": "Deleted"})

        # Call the method
        result = self.api.delete_ci(1)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with('cmdb/1')

        # Verify result
        self.assertEqual(result, {"ID": 1, "Status": "Deleted"})

    def test_get_ci_articles(self):
        """Test getting articles for a configuration item."""
        # Mock the get method
        self.api.get = MagicMock(return_value=[{"ID": 201, "Title": "Test Article"}])

        # Call the method
        result = self.api.get_ci_articles(1)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('cmdb/1/articles')

        # Verify result
        self.assertEqual(result, [{"ID": 201, "Title": "Test Article"}])

    def test_add_article_to_ci(self):
        """Test adding an article to a configuration item."""
        # Mock the post method
        self.api.post = MagicMock(return_value={"ID": 1, "Status": "Success"})

        # Call the method
        result = self.api.add_article_to_ci(1, 201)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('cmdb/1/articles/201', data={})

        # Verify result
        self.assertEqual(result, {"ID": 1, "Status": "Success"})

    def test_remove_article_from_ci(self):
        """Test removing an article from a configuration item."""
        # Mock the delete method
        self.api.delete = MagicMock(return_value={"ID": 1, "Status": "Success"})

        # Call the method
        result = self.api.remove_article_from_ci(1, 201)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with('cmdb/1/articles/201')

        # Verify result
        self.assertEqual(result, {"ID": 1, "Status": "Success"})

    def test_upload_ci_attachment(self):
        """Test uploading an attachment to a configuration item."""
        # Mock the post method
        self.api.post = MagicMock(return_value={"ID": 301, "FileName": "test.txt"})

        # Mock file
        mock_file = MagicMock()

        # Call the method
        result = self.api.upload_ci_attachment(1, mock_file, False)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('cmdb/1/attachments?showViewLink=False', files={'file': mock_file})

        # Verify result
        self.assertEqual(result, {"ID": 301, "FileName": "test.txt"})

    def test_get_ci_feed(self):
        """Test getting the feed for a configuration item."""
        # Mock the get method
        self.api.get = MagicMock(return_value=[{"ID": 401, "Comments": "Test comment"}])

        # Call the method
        result = self.api.get_ci_feed(1)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('cmdb/1/feed')

        # Verify result
        self.assertEqual(result, [{"ID": 401, "Comments": "Test comment"}])

    def test_add_comment_to_ci(self):
        """Test adding a comment to a configuration item's feed."""
        # Mock the post method
        self.api.post = MagicMock(return_value={"ID": 401, "Comments": "New comment"})

        # Test data
        comment_data = {"Comments": "New comment", "IsPrivate": False}

        # Call the method
        result = self.api.add_comment_to_ci(1, comment_data)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('cmdb/1/feed', comment_data)

        # Verify result
        self.assertEqual(result, {"ID": 401, "Comments": "New comment"})

    def test_remove_relationship(self):
        """Test removing a relationship from a configuration item."""
        # Mock the delete method
        self.api.delete = MagicMock(return_value={"ID": 101, "Status": "Deleted"})

        # Call the method
        result = self.api.remove_relationship(1, 101)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with('cmdb/1/relationships/101')

        # Verify result
        self.assertEqual(result, {"ID": 101, "Status": "Deleted"})

    def test_get_ci_tickets(self):
        """Test getting tickets for a configuration item."""
        # Mock the get method
        self.api.get = MagicMock(return_value=[{"ID": 501, "Title": "Test Ticket"}])

        # Call the method
        result = self.api.get_ci_tickets(1)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('cmdb/1/tickets')

        # Verify result
        self.assertEqual(result, [{"ID": 501, "Title": "Test Ticket"}])

    def test_get_ci_forms(self):
        """Test getting all active configuration item forms."""
        # Mock the get method
        self.api.get = MagicMock(return_value=[{"ID": 601, "Name": "Test Form"}])

        # Call the method
        result = self.api.get_ci_forms()

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('cmdb/forms')

        # Verify result
        self.assertEqual(result, [{"ID": 601, "Name": "Test Form"}])

    def test_search_ci_advanced(self):
        """Test advanced search for configuration items."""
        # Mock the post method
        self.api.post = MagicMock(return_value=[{"ID": 1, "Name": "Test CI"}])

        # Test data
        search_params = {"NameLike": "Test", "TypeIDs": [10014], "IsActive": True}

        # Call the method
        result = self.api.search_ci_advanced(search_params)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('cmdb/search', search_params)

        # Verify result
        self.assertEqual(result, [{"ID": 1, "Name": "Test CI"}])


if __name__ == '__main__':
    unittest.main()
