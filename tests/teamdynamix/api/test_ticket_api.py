import unittest
from unittest.mock import patch, MagicMock
import json
from teamdynamix.api.ticket_api import TicketAPI

class TestTicketAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the TicketAPI class.
    
    Tests cover:
    - Basic functionality for core methods
    - Error handling
    - Edge cases
    - Parameter construction and URL formatting
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create an instance of TicketAPI with mocked dependencies
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "46"  # Tickets app ID from the code
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        self.api = TicketAPI(self.base_url, self.app_id, self.headers)
        
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        pass

    # ----- Basic functionality tests -----

    def test_get_ticket(self):
        """Test retrieving a ticket by ID."""
        # Mock the response from get method
        ticket_id = 12345
        mock_response = {
            "ID": ticket_id,
            "Title": "Test Ticket",
            "Description": "This is a test ticket"
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_ticket(ticket_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'tickets/{ticket_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_tickets(self):
        """Test retrieving tickets for specified requestors."""
        # Mock the response from post method
        search_item = ["user1", "user2"]
        mock_response = [
            {"ID": 1, "Title": "Ticket 1", "Description": "Description 1"},
            {"ID": 2, "Title": "Ticket 2", "Description": "Description 2"}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_tickets(search_item)

        # Assert the post method was called with the correct parameters
        expected_data = {"RequestorUids": search_item}
        self.api.post.assert_called_once_with('tickets/search', expected_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_active_tickets(self):
        """Test retrieving active tickets for specified requestors."""
        # Mock the response from post method
        search_item = ["user1", "user2"]
        mock_response = [
            {"ID": 1, "Title": "Active Ticket 1", "StatusID": 115},
            {"ID": 2, "Title": "Active Ticket 2", "StatusID": 117}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_active_tickets(search_item)

        # Assert the post method was called with the correct parameters
        expected_data = {
            "RequestorUids": search_item,
            "StatusIDs": [115, 117, 121, 619, 620, 622]
        }
        self.api.post.assert_called_once_with('tickets/search', expected_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_move_ticket(self):
        """Test moving a ticket to a different application."""
        # Mock the response from post method
        ticket_id = 12345
        move_data = {"NewAppID": 48, "NotifyRequestor": True}
        mock_response = {"Success": True, "ID": ticket_id}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.move_ticket(ticket_id, move_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(f'tickets/{ticket_id}/application', move_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_ticket_assets(self):
        """Test retrieving assets associated with a ticket."""
        ticket_id = 12345
        mock_response = [
            {"ID": 101, "Name": "Asset 1"},
            {"ID": 102, "Name": "Asset 2"}
        ]
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_ticket_assets(ticket_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'tickets/{ticket_id}/assets')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_add_ticket_asset(self):
        """Test adding an asset to a ticket."""
        ticket_id = 12345
        asset_id = 101
        mock_response = {"Success": True}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.add_ticket_asset(ticket_id, asset_id)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with(f'tickets/{ticket_id}/assets/{asset_id}', data={})

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_remove_ticket_asset(self):
        """Test removing an asset from a ticket."""
        ticket_id = 12345
        asset_id = 101
        mock_response = {"Success": True}
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.remove_ticket_asset(ticket_id, asset_id)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'tickets/{ticket_id}/assets/{asset_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_update_ticket(self):
        """Test updating a ticket by adding a new feed entry."""
        # Mock the response from post method
        ticket_id = 12345
        comments = "Test comments"
        private = False
        commrecord = True
        status = 117
        cascade = False
        notify = ["user@example.com"]
        rich = True
        
        mock_response = {"Success": True, "ID": ticket_id}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.update_ticket(
            id=ticket_id,
            comments=comments,
            private=private,
            commrecord=commrecord,
            status=status,
            cascade=cascade,
            notify=notify,
            rich=rich
        )

        # Assert the post method was called with the correct parameters
        expected_data = {
            "NewStatusID": status,
            "CascadeStatus": cascade,
            "Comments": comments,
            "Notify": notify,
            "IsPrivate": private,
            "IsRichHTML": rich,
            "IsCommunication": commrecord
        }
        self.api.post.assert_called_once_with(f'tickets/{ticket_id}/feed', expected_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_ticket_feed(self):
        """Test retrieving feed entries for a ticket."""
        ticket_id = 12345
        mock_response = [
            {"ID": 201, "Comments": "Feed entry 1"},
            {"ID": 202, "Comments": "Feed entry 2"}
        ]
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_ticket_feed(ticket_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'tickets/{ticket_id}/feed')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_create_ticket(self):
        """Test creating a new ticket."""
        # Mock the response from post method
        ticket_data = {
            "TypeID": 652,
            "Title": "Test Ticket",
            "Description": "Test description"
        }
        notify_requestor = True
        notify_responsible = True
        allow_requestor_creation = False
        enable_notify_reviewer = True
        apply_defaults = True
        
        mock_response = {
            "ID": 12345,
            "Title": "Test Ticket"
        }
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.create_ticket(
            ticket_data=ticket_data,
            notify_requestor=notify_requestor,
            notify_responsible=notify_responsible,
            allow_requestor_creation=allow_requestor_creation,
            enable_notify_reviewer=enable_notify_reviewer,
            apply_defaults=apply_defaults
        )

        # Assert the post method was called with the correct parameters
        expected_url = f'tickets?EnableNotifyReviewer={enable_notify_reviewer}&NotifyRequestor={notify_requestor}&NotifyResponsible={notify_responsible}&AllowRequestorCreation={allow_requestor_creation}&applyDefaults={apply_defaults}'
        self.api.post.assert_called_once_with(expected_url, ticket_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_search_tickets(self):
        """Test searching for tickets."""
        # Mock the response from post method
        search_data = {
            "StatusIDs": [117, 118],
            "SearchText": "test"
        }
        mock_response = [
            {"ID": 1, "Title": "Matched Ticket 1"},
            {"ID": 2, "Title": "Matched Ticket 2"}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_tickets(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('tickets/search', search_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Error handling tests -----

    def test_get_ticket_not_found(self):
        """Test retrieving a ticket that doesn't exist."""
        ticket_id = 99999
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_ticket(ticket_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'tickets/{ticket_id}')

        # Assert the result is None
        self.assertIsNone(result)

    def test_create_ticket_error(self):
        """Test error handling when creating a ticket."""
        ticket_data = {
            "TypeID": 652,
            "Title": "Test Ticket",
            "Description": "Test description"
        }
        
        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.create_ticket(
            ticket_data=ticket_data,
            notify_requestor=True,
            notify_responsible=True,
            allow_requestor_creation=False
        )

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Edge case tests -----

    def test_update_ticket_with_default_values(self):
        """Test updating a ticket with default values."""
        ticket_id = 12345
        comments = "Test comments"
        private = False
        commrecord = True
        
        # Default values for other parameters
        status = 0
        cascade = False
        notify = ['null']
        rich = True
        
        mock_response = {"Success": True, "ID": ticket_id}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested with only required parameters
        result = self.api.update_ticket(
            id=ticket_id,
            comments=comments,
            private=private,
            commrecord=commrecord
        )

        # Assert the post method was called with the correct parameters
        expected_data = {
            "NewStatusID": status,
            "CascadeStatus": cascade,
            "Comments": comments,
            "Notify": notify,
            "IsPrivate": private,
            "IsRichHTML": rich,
            "IsCommunication": commrecord
        }
        self.api.post.assert_called_once_with(f'tickets/{ticket_id}/feed', expected_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_upload_ticket_attachment(self):
        """Test uploading an attachment to a ticket."""
        ticket_id = 12345
        mock_file = MagicMock()
        show_view_link = True
        
        mock_response = {"ID": 301, "FileName": "test.txt"}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.upload_ticket_attachment(ticket_id, mock_file, show_view_link)

        # Assert the post method was called with the correct parameters
        expected_url = f'tickets/{ticket_id}/attachments?showViewLink={show_view_link}'
        expected_files = {'file': mock_file}
        self.api.post.assert_called_once_with(expected_url, files=expected_files)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_ticket_configuration_items(self):
        """Test retrieving configuration items associated with a ticket."""
        ticket_id = 12345
        mock_response = [
            {"ID": 401, "Name": "Config Item 1"},
            {"ID": 402, "Name": "Config Item 2"}
        ]
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_ticket_configuration_items(ticket_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'tickets/{ticket_id}/configurationItems')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_add_ticket_configuration_item(self):
        """Test adding a configuration item to a ticket."""
        ticket_id = 12345
        ci_id = 401
        mock_response = {"Success": True}
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.add_ticket_configuration_item(ticket_id, ci_id)

        # Assert the post method was called with the correct parameters
        expected_data = {'configurationItemId': ci_id}
        self.api.post.assert_called_once_with(f'tickets/{ticket_id}/configurationItems/{ci_id}', data=expected_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_remove_ticket_configuration_item(self):
        """Test removing a configuration item from a ticket."""
        ticket_id = 12345
        ci_id = 401
        mock_response = {"Success": True}
        self.api.delete = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.remove_ticket_configuration_item(ticket_id, ci_id)

        # Assert the delete method was called with the correct parameters
        self.api.delete.assert_called_once_with(f'tickets/{ticket_id}/configurationItems/{ci_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_patch_ticket(self):
        """Test patching an existing ticket."""
        ticket_id = 12345
        patch_data = {
            "Title": "Updated Title",
            "Description": "Updated description"
        }
        notify_new_responsible = True
        
        mock_response = {
            "ID": ticket_id,
            "Title": "Updated Title",
            "Description": "Updated description"
        }
        self.api.patch = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.patch_ticket(ticket_id, patch_data, notify_new_responsible)

        # Assert the patch method was called with the correct parameters
        expected_url = f'tickets/{ticket_id}?notifyNewResponsible={notify_new_responsible}'
        self.api.patch.assert_called_once_with(expected_url, patch_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_edit_ticket(self):
        """Test editing an existing ticket."""
        ticket_id = 12345
        edit_data = {
            "Title": "Edited Title",
            "Description": "Edited description"
        }
        notify_new_responsible = True
        
        mock_response = {
            "ID": ticket_id,
            "Title": "Edited Title",
            "Description": "Edited description"
        }
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.edit_ticket(ticket_id, edit_data, notify_new_responsible)

        # Assert the post method was called with the correct parameters
        expected_url = f'tickets/{ticket_id}?notifyNewResponsible={notify_new_responsible}'
        self.api.post.assert_called_once_with(expected_url, edit_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

if __name__ == '__main__':
    unittest.main()
