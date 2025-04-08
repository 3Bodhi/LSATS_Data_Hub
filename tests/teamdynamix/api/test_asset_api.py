import unittest
from unittest.mock import patch, MagicMock
import json
from teamdynamix.api.asset_api import AssetAPI


class TestAssetAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the AssetAPI class.

    These tests focus on the specific functionality of AssetAPI methods
    without redundant testing of the underlying TeamDynamixAPI functionality.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock for the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.asset_api.TeamDynamixAPI').start()

        # Create an instance of AssetAPI
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "48"  # Common app ID for assets
        self.headers = {"Authorization": "Bearer test_token", "Content-Type": "application/json"}
        self.api = AssetAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    # ----- Basic functionality tests -----

    def test_get_asset(self):
        """Test getting an asset by ID."""
        # Mock data
        asset_id = 12345
        mock_response = {"ID": asset_id, "Name": "Test Asset"}

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset(asset_id)

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with(f'assets/{asset_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_assets_by_owner(self):
        """Test getting assets by owner."""
        # Mock data
        owner_ids = ["user1", "user2"]
        mock_response = [
            {"ID": 1, "Name": "Asset 1", "OwningCustomerID": "user1"},
            {"ID": 2, "Name": "Asset 2", "OwningCustomerID": "user2"}
        ]

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_assets(owner_ids, search_by='owner')

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with(
            'assets/search',
            {"OwningCustomerIDs": owner_ids}
        )

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_assets_by_shortcode(self):
        """Test getting assets by shortcode."""
        # Mock data
        shortcodes = ["SC001", "SC002"]
        mock_response = [
            {"ID": 1, "Name": "Asset 1", "CustomAttributes": [{"ID": 3513, "Value": "SC001"}]},
            {"ID": 2, "Name": "Asset 2", "CustomAttributes": [{"ID": 3513, "Value": "SC002"}]}
        ]

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_assets(shortcodes, search_by='shortcode')

        # Verify correct endpoint and data were used
        expected_data = {
            "CustomAttributes": [{
                "ID": 3513,  # shortcode attribute ID
                "Value": shortcodes
            }]
        }
        self.api.post.assert_called_once_with('assets/search', expected_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_search_asset(self):
        """Test searching for assets with custom criteria."""
        # Mock data
        search_data = {
            "NameLike": "Test",
            "StatusIDs": [1, 2],
            "IsActive": True
        }
        mock_response = [
            {"ID": 1, "Name": "Test Asset 1"},
            {"ID": 2, "Name": "Test Asset 2"}
        ]

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.search_asset(search_data)

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with('assets/search', search_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_add_asset(self):
        """Test adding an asset to a ticket."""
        # Mock data
        asset_id = 123
        ticket_id = 456
        mock_response = {"ID": asset_id, "TicketID": ticket_id}

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.add_asset(asset_id, ticket_id)

        # Verify correct endpoint was called
        self.api.post.assert_called_once_with(f'/assets/{asset_id}/tickets/{ticket_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_create_asset(self):
        """Test creating a new asset."""
        # Mock data
        asset_data = {
            "Name": "New Asset",
            "SerialNumber": "SN12345",
            "StatusID": 1
        }
        mock_response = {
            "ID": 789,
            "Name": "New Asset",
            "SerialNumber": "SN12345",
            "StatusID": 1
        }

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.create_asset(asset_data)

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with('assets', asset_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_delete_asset(self):
        """Test deleting an asset."""
        # Mock data
        asset_id = this_will_be_deleted = 999
        mock_response = {"ID": asset_id, "IsDeleted": True}

        # Configure mock
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.delete_asset(asset_id)

        # Verify correct endpoint was called
        self.api.delete.assert_called_once_with(f'assets/{asset_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_update_asset(self):
        """Test updating an existing asset."""
        # Mock data
        asset_id = 123
        asset_data = {
            "Name": "Updated Asset",
            "SerialNumber": "SN12345-UPDATE",
            "StatusID": 2
        }
        mock_response = {
            "ID": asset_id,
            "Name": "Updated Asset",
            "SerialNumber": "SN12345-UPDATE",
            "StatusID": 2
        }

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.update_asset(asset_id, asset_data)

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with(f'assets/{asset_id}', asset_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_patch_asset(self):
        """Test patching an existing asset."""
        # Mock data
        asset_id = 123
        patch_data = [
            {
                "op": "replace",
                "path": "/Name",
                "value": "Patched Asset Name"
            }
        ]
        mock_response = {
            "ID": asset_id,
            "Name": "Patched Asset Name"
        }

        # Configure mock
        self.api.patch = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.patch_asset(asset_id, patch_data)

        # Verify correct endpoint and data were used
        self.api.patch.assert_called_once_with(f'assets/{asset_id}', patch_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_articles(self):
        """Test getting knowledge base articles associated with an asset."""
        # Mock data
        asset_id = 123
        mock_response = [
            {"ID": 1, "Title": "Article 1"},
            {"ID": 2, "Title": "Article 2"}
        ]

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_articles(asset_id)

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with(f'assets/{asset_id}/articles')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_add_article_to_asset(self):
        """Test adding an article relationship to an asset."""
        # Mock data
        asset_id = 123
        article_id = 456
        mock_response = {"AssetID": asset_id, "ArticleID": article_id}

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.add_article_to_asset(asset_id, article_id)

        # Verify correct endpoint was called
        self.api.post.assert_called_once_with(f'assets/{asset_id}/articles/{article_id}', data={})

        # Verify result
        self.assertEqual(result, mock_response)

    def test_remove_article_from_asset(self):
        """Test removing a relationship between an asset and a knowledge base article."""
        # Mock data
        asset_id = 123
        article_id = 456
        mock_response = {"Success": True}

        # Configure mock
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.remove_article_from_asset(asset_id, article_id)

        # Verify correct endpoint was called
        self.api.delete.assert_called_once_with(f'assets/{asset_id}/articles/{article_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_contracts(self):
        """Test getting contracts associated with an asset."""
        # Mock data
        asset_id = 123
        mock_response = [
            {"ID": 1, "Name": "Contract 1"},
            {"ID": 2, "Name": "Contract 2"}
        ]

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_contracts(asset_id)

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with(f'assets/{asset_id}/associatedcontracts')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_add_contract_to_asset(self):
        """Test associating a contract to an asset."""
        # Mock data
        asset_id = 123
        contract_id = 456
        mock_response = {"AssetID": asset_id, "ContractID": contract_id}

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.add_contract_to_asset(asset_id, contract_id)

        # Verify correct endpoint was called
        self.api.post.assert_called_once_with(f'assets/{asset_id}/associatedcontracts/{contract_id}', data={})

        # Verify result
        self.assertEqual(result, mock_response)

    def test_remove_contract_from_asset(self):
        """Test removing a contract from an asset."""
        # Mock data
        asset_id = 123
        contract_id = 456
        mock_response = {"Success": True}

        # Configure mock
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.remove_contract_from_asset(asset_id, contract_id)

        # Verify correct endpoint was called
        self.api.delete.assert_called_once_with(f'assets/{asset_id}/associatedcontracts/{contract_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_update_asset_contract(self):
        """Test editing an asset's contract dates."""
        # Mock data
        asset_id = 123
        contract_id = 456
        contract_data = {
            "StartDate": "2023-01-01T00:00:00Z",
            "EndDate": "2024-01-01T00:00:00Z"
        }
        mock_response = {
            "AssetID": asset_id,
            "ContractID": contract_id,
            "StartDate": "2023-01-01T00:00:00Z",
            "EndDate": "2024-01-01T00:00:00Z"
        }

        # Configure mock
        self.api.put = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.update_asset_contract(asset_id, contract_id, contract_data)

        # Verify correct endpoint and data were used
        self.api.put.assert_called_once_with(f'assets/{asset_id}/associatedcontracts/{contract_id}', contract_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_upload_asset_attachment(self):
        """Test uploading an attachment to an asset."""
        # Mock data
        asset_id = 123
        mock_file = MagicMock()
        show_view_link = True
        mock_response = {
            "ID": 789,
            "Name": "test_file.pdf",
            "ContentType": "application/pdf"
        }

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.upload_asset_attachment(asset_id, mock_file, show_view_link)

        # Verify correct endpoint and parameters were used
        expected_url = f'assets/{asset_id}/attachments?showViewLink={show_view_link}'
        expected_files = {'file': mock_file}
        self.api.post.assert_called_once_with(expected_url, files=expected_files)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_feed(self):
        """Test getting the feed entries for an asset."""
        # Mock data
        asset_id = 123
        mock_response = [
            {"ID": 1, "Comments": "Comment 1"},
            {"ID": 2, "Comments": "Comment 2"}
        ]

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_feed(asset_id)

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with(f'assets/{asset_id}/feed')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_add_comment_to_asset(self):
        """Test adding a comment to an asset."""
        # Mock data
        asset_id = 123
        comment_data = {
            "Comments": "Test comment",
            "IsRichHtml": True
        }
        mock_response = {
            "ID": 789,
            "Comments": "Test comment",
            "CreatedDate": "2023-01-01T00:00:00Z"
        }

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.add_comment_to_asset(asset_id, comment_data)

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with(f'assets/{asset_id}/feed', comment_data)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_remove_ticket_from_asset(self):
        """Test removing a ticket from an asset."""
        # Mock data
        asset_id = 123
        ticket_id = 456
        mock_response = {"Success": True}

        # Configure mock
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.remove_ticket_from_asset(asset_id, ticket_id)

        # Verify correct endpoint was called
        self.api.delete.assert_called_once_with(f'assets/{asset_id}/tickets/{ticket_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_users(self):
        """Test getting the asset resources (users)."""
        # Mock data
        asset_id = 123
        mock_response = [
            {"UID": "user1", "FullName": "User One"},
            {"UID": "user2", "FullName": "User Two"}
        ]

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_users(asset_id)

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with(f'assets/{asset_id}/users')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_add_user_to_asset(self):
        """Test adding a resource (user) to asset."""
        # Mock data
        asset_id = 123
        resource_id = "user1"
        mock_response = {"AssetID": asset_id, "UserUID": resource_id}

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.add_user_to_asset(asset_id, resource_id)

        # Verify correct endpoint was called
        self.api.post.assert_called_once_with(f'assets/{asset_id}/users/{resource_id}', data={})

        # Verify result
        self.assertEqual(result, mock_response)

    def test_remove_user_from_asset(self):
        """Test removing a resource (user) from an asset."""
        # Mock data
        asset_id = 123
        resource_id = "user1"
        mock_response = {"Success": True}

        # Configure mock
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.remove_user_from_asset(asset_id, resource_id)

        # Verify correct endpoint was called
        self.api.delete.assert_called_once_with(f'assets/{asset_id}/users/{resource_id}')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_feed_items(self):
        """Test getting feed items for an asset application feed."""
        # Mock data
        date_from = "2023-01-01T00:00:00Z"
        date_to = "2023-02-01T00:00:00Z"
        reply_count = 5
        return_count = 10
        mock_response = {
            "Items": [
                {"ID": 1, "Comments": "Feed Item 1"},
                {"ID": 2, "Comments": "Feed Item 2"}
            ],
            "TotalCount": 2
        }

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_feed_items(date_from, date_to, reply_count, return_count)

        # Verify correct endpoint was called
        expected_endpoint = f'assets/feed?DateFrom={date_from}&DateTo={date_to}&ReplyCount={reply_count}&ReturnCount={return_count}'
        self.api.get.assert_called_once_with(expected_endpoint)

        # Verify result
        self.assertEqual(result, mock_response)

    def test_get_asset_forms(self):
        """Test getting all active asset forms."""
        # Mock data
        mock_response = [
            {"ID": 1, "Name": "Form 1"},
            {"ID": 2, "Name": "Form 2"}
        ]

        # Configure mock
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.get_asset_forms()

        # Verify correct endpoint was called
        self.api.get.assert_called_once_with('assets/forms')

        # Verify result
        self.assertEqual(result, mock_response)

    def test_import_assets(self):
        """Test performing a bulk insert/update of assets."""
        # Mock data
        import_data = {
            "Items": [
                {"Name": "Asset 1", "SerialNumber": "SN001"},
                {"Name": "Asset 2", "SerialNumber": "SN002"}
            ],
            "UpdateExisting": True,
            "NotifyNewOwners": False
        }
        mock_response = [
            {"ID": 1, "Name": "Asset 1", "SerialNumber": "SN001", "IsNew": True},
            {"ID": 2, "Name": "Asset 2", "SerialNumber": "SN002", "IsNew": True}
        ]

        # Configure mock
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method
        result = self.api.import_assets(import_data)

        # Verify correct endpoint and data were used
        self.api.post.assert_called_once_with('assets/import', import_data)

        # Verify result
        self.assertEqual(result, mock_response)

    # ----- Error handling tests -----

    def test_get_asset_not_found(self):
        """Test getting an asset that doesn't exist."""
        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)

        # Call the method
        result = self.api.get_asset(999)

        # Verify the get method was called with the correct parameters
        self.api.get.assert_called_once_with('assets/999')

        # Verify the result is None
        self.assertIsNone(result)

    def test_create_asset_failure(self):
        """Test error handling when creating an asset fails."""
        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)

        # Test data
        asset_data = {"Name": "Failed Asset"}

        # Call the method
        result = self.api.create_asset(asset_data)

        # Verify the post method was called with the correct parameters
        self.api.post.assert_called_once_with('assets', asset_data)

        # Verify the result is None
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
