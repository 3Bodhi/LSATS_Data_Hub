import unittest
from unittest.mock import patch, MagicMock
import datetime
from datetime import timezone
import json

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from teamdynamix.api.teamdynamix_api import create_headers


class TestTeamDynamixFacade(unittest.TestCase):
    """Test cases for the TeamDynamixFacade class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create patches for all API classes
        self.user_api_patch = patch('teamdynamix.facade.teamdynamix_facade.UserAPI')
        self.asset_api_patch = patch('teamdynamix.facade.teamdynamix_facade.AssetAPI')
        self.account_api_patch = patch('teamdynamix.facade.teamdynamix_facade.AccountAPI')
        self.configuration_item_api_patch = patch('teamdynamix.facade.teamdynamix_facade.ConfigurationItemAPI')
        self.ticket_api_patch = patch('teamdynamix.facade.teamdynamix_facade.TicketAPI')
        self.feed_api_patch = patch('teamdynamix.facade.teamdynamix_facade.FeedAPI')
        self.group_api_patch = patch('teamdynamix.facade.teamdynamix_facade.GroupAPI')
        self.knowledge_base_api_patch = patch('teamdynamix.facade.teamdynamix_facade.KnowledgeBaseAPI')
        self.report_api_patch = patch('teamdynamix.facade.teamdynamix_facade.ReportAPI')
        self.create_headers_patch = patch('teamdynamix.facade.teamdynamix_facade.create_headers')

        # Start all patches
        self.mock_user_api = self.user_api_patch.start()
        self.mock_asset_api = self.asset_api_patch.start()
        self.mock_account_api = self.account_api_patch.start()
        self.mock_configuration_item_api = self.configuration_item_api_patch.start()
        self.mock_ticket_api = self.ticket_api_patch.start()
        self.mock_feed_api = self.feed_api_patch.start()
        self.mock_group_api = self.group_api_patch.start()
        self.mock_knowledge_base_api = self.knowledge_base_api_patch.start()
        self.mock_report_api = self.report_api_patch.start()
        self.mock_create_headers = self.create_headers_patch.start()

        # Configure mock headers
        self.mock_headers = {'Authorization': 'Bearer test_token', 'Content-Type': 'application/json'}
        self.mock_create_headers.return_value = self.mock_headers

        # Common parameters for facade initialization
        self.base_url = 'https://example.com/api'
        self.app_id = '123'
        self.api_token = 'test_token'

        # Create instance of the facade with mocked dependencies
        self.facade = TeamDynamixFacade(self.base_url, self.app_id, self.api_token)

    def tearDown(self):
        """Clean up after each test."""
        patch.stopall()

    def test_initialization(self):
        """Test facade initialization properly instantiates API clients."""
        # Verify create_headers was called with the correct API token
        self.mock_create_headers.assert_called_once_with(self.api_token)

        # Verify each API client was instantiated with the correct parameters
        # auth=None for static token mode (no auto-refresh)
        self.mock_user_api.assert_called_once_with(self.base_url, "", self.mock_headers, auth=None)
        self.mock_asset_api.assert_called_once_with(self.base_url, self.app_id, self.mock_headers, auth=None)
        self.mock_account_api.assert_called_once_with(self.base_url, "", self.mock_headers, auth=None)
        self.mock_configuration_item_api.assert_called_once_with(self.base_url, self.app_id, self.mock_headers, auth=None)
        self.mock_ticket_api.assert_called_once_with(self.base_url, 46, self.mock_headers, auth=None)
        self.mock_feed_api.assert_called_once_with(self.base_url, "", self.mock_headers, auth=None)
        self.mock_group_api.assert_called_once_with(self.base_url, "", self.mock_headers, auth=None)
        self.mock_knowledge_base_api.assert_called_once_with(self.base_url, self.app_id, self.mock_headers, auth=None)
        self.mock_report_api.assert_called_once_with(self.base_url, "", self.mock_headers, auth=None)

    def test_get_user_assets_by_uniqname_success(self):
        """Test getting user assets by uniqname when the user exists."""
        # Mock data
        uniqname = 'testuser'
        user_id = 'user123'
        expected_assets = [{'ID': 'asset1', 'Name': 'Test Asset'}]

        # Configure mocks
        self.facade.users.get_user_attribute = MagicMock(return_value=user_id)
        self.facade.assets.get_assets = MagicMock(return_value=expected_assets)

        # Call the method
        result = self.facade.get_user_assets_by_uniqname(uniqname)

        # Verify the correct methods were called with expected parameters
        self.facade.users.get_user_attribute.assert_called_once_with(uniqname, 'UID')
        self.facade.assets.get_assets.assert_called_once_with([user_id])

        # Verify result
        self.assertEqual(result, expected_assets)

    def test_get_user_assets_by_uniqname_no_user(self):
        """Test getting user assets by uniqname when the user doesn't exist."""
        uniqname = 'nonexistentuser'

        # Configure mocks
        self.facade.users.get_user_attribute = MagicMock(return_value=None)

        # Call the method
        result = self.facade.get_user_assets_by_uniqname(uniqname)

        # Verify the correct methods were called with expected parameters
        self.facade.users.get_user_attribute.assert_called_once_with(uniqname, 'UID')
        self.facade.assets.get_assets.assert_not_called()

        # Verify result is None
        self.assertIsNone(result)

    def test_get_user_tickets_by_uniqname_success(self):
        """Test getting user tickets by uniqname when the user exists."""
        uniqname = 'testuser'
        user_id = 'user123'
        expected_tickets = [{'ID': 'ticket1', 'Title': 'Test Ticket'}]

        # Configure mocks
        self.facade.users.get_user_attribute = MagicMock(return_value=user_id)
        self.facade.tickets.get_tickets = MagicMock(return_value=expected_tickets)

        # Call the method
        result = self.facade.get_user_tickets_by_uniqname(uniqname)

        # Verify the correct methods were called with expected parameters
        self.facade.users.get_user_attribute.assert_called_once_with(uniqname, 'UID')
        self.facade.tickets.get_tickets.assert_called_once_with([user_id])

        # Verify result
        self.assertEqual(result, expected_tickets)

    def test_get_user_tickets_by_uniqname_no_user(self):
        """Test getting user tickets by uniqname when the user doesn't exist."""
        uniqname = 'nonexistentuser'

        # Configure mocks
        self.facade.users.get_user_attribute = MagicMock(return_value=None)

        # Call the method
        result = self.facade.get_user_tickets_by_uniqname(uniqname)

        # Verify the correct methods were called with expected parameters
        self.facade.users.get_user_attribute.assert_called_once_with(uniqname, 'UID')
        self.facade.tickets.get_tickets.assert_not_called()

        # Verify result is None
        self.assertIsNone(result)

    def test_get_dept_users(self):
        """Test getting department users."""
        dept_id = 'dept123'
        expected_data = {'AccountIDs': dept_id}

        # Configure mocks
        self.facade.users.search_user = MagicMock()

        # Call the method
        self.facade.get_dept_users(dept_id)

        # Verify the correct methods were called with expected parameters
        self.facade.users.search_user.assert_called_once_with(expected_data)

    def test_create_lab(self):
        """Test creating a lab configuration item."""
        pi = 'testpi'
        user_id = 'user123'
        assets = [
            {
                'OwningCustomerID': user_id,
                'OwningDepartmentID': 'dept123',
                'LocationID': 'loc123',
                'ConfigurationItemID': 'ci1'
            },
            {
                'ConfigurationItemID': 'ci2'
            }
        ]
        tickets = [
            {'ID': 'ticket1', 'Title': 'Ticket 1'},
            {'ID': 'ticket2', 'Title': 'Ticket 2'}
        ]
        lab_ci = {
            'ID': 'lab123',
            'Name': 'Testpi Lab'
        }

        # Configure mocks
        self.facade.get_user_assets_by_uniqname = MagicMock(return_value=assets)
        self.facade.get_user_tickets_by_uniqname = MagicMock(return_value=tickets)
        self.facade.configuration_items.create_ci = MagicMock(return_value=lab_ci)
        self.facade.configuration_items.add_asset = MagicMock()
        self.facade.tickets.add_ticket_configuration_item = MagicMock()

        # Call the method
        self.facade.create_lab(pi)

        # Verify correct methods were called with expected parameters
        self.facade.get_user_assets_by_uniqname.assert_called_once_with(pi)
        self.facade.get_user_tickets_by_uniqname.assert_called_once_with(pi)

        expected_ci_data = {
            'Name': f"{pi.title()} Lab",
            'OwnerUID': user_id,
            'OwningDepartmentID': 'dept123',
            'LocationID': 'loc123'
        }
        self.facade.configuration_items.create_ci.assert_called_once_with(expected_ci_data)

        # Verify assets were added
        self.assertEqual(self.facade.configuration_items.add_asset.call_count, 2)
        self.facade.configuration_items.add_asset.assert_any_call(lab_ci['ID'], 'ci1')
        self.facade.configuration_items.add_asset.assert_any_call(lab_ci['ID'], 'ci2')

        # Verify tickets were added
        self.assertEqual(self.facade.tickets.add_ticket_configuration_item.call_count, 2)
        self.facade.tickets.add_ticket_configuration_item.assert_any_call('ticket1', lab_ci['ID'])
        self.facade.tickets.add_ticket_configuration_item.assert_any_call('ticket2', lab_ci['ID'])

    def test_get_ticket_last_activity(self):
        """Test getting last activity timestamp for a ticket."""
        ticket_id = 'ticket123'
        modified_date = '2023-01-15T14:30:00Z'
        expected_datetime = datetime.datetime.fromisoformat('2023-01-15T14:30:00+00:00')
        ticket = {'ID': ticket_id, 'ModifiedDate': modified_date}
        feed = [
            {'ID': 'feed1', 'CreatedDate': '2023-01-10T10:00:00Z'},
            {'ID': 'feed2', 'CreatedDate': '2023-01-15T14:30:00Z'},
            {'ID': 'feed3', 'CreatedDate': '2023-01-12T09:15:00Z'}
        ]

        # Configure mocks
        self.facade.tickets.get_ticket = MagicMock(return_value=ticket)
        self.facade.tickets.get_ticket_feed = MagicMock(return_value=feed)

        # Call the method
        result = self.facade.get_ticket_last_activity(ticket_id)

        # Verify correct method calls
        self.facade.tickets.get_ticket.assert_called_once_with(ticket_id)
        self.facade.tickets.get_ticket_feed.assert_called_once_with(ticket_id)

        # Verify result
        self.assertEqual(result, expected_datetime)

    def test_get_ticket_last_activity_no_feed(self):
        """Test getting last activity timestamp when feed is empty."""
        ticket_id = 'ticket123'
        modified_date = '2023-01-15T14:30:00Z'
        expected_datetime = datetime.datetime.fromisoformat('2023-01-15T14:30:00+00:00')
        ticket = {'ID': ticket_id, 'ModifiedDate': modified_date}

        # Configure mocks
        self.facade.tickets.get_ticket = MagicMock(return_value=ticket)
        self.facade.tickets.get_ticket_feed = MagicMock(return_value=None)

        # Call the method
        result = self.facade.get_ticket_last_activity(ticket_id)

        # Verify method calls
        self.facade.tickets.get_ticket.assert_called_once_with(ticket_id)
        self.facade.tickets.get_ticket_feed.assert_called_once_with(ticket_id)

        # Verify result uses Modified Date from ticket when feed is empty
        self.assertEqual(result, expected_datetime)

    def test_get_last_requestor_response(self):
        """Test getting last requestor response timestamp."""
        ticket_id = 'ticket123'
        requestor_name = 'Test User'
        ticket = {'ID': ticket_id, 'RequestorName': requestor_name, 'ModifiedFullName': 'Other User', 'ModifiedDate': '2023-01-01T00:00:00Z'}
        feed = [
            {'ID': 'feed1', 'CreatedFullName': 'Other User', 'CreatedDate': '2023-01-10T10:00:00Z', 'LastUpdatedDate': '2023-01-10T10:00:00Z'},
            {'ID': 'feed2', 'CreatedFullName': requestor_name, 'CreatedDate': '2023-01-15T14:30:00Z', 'LastUpdatedDate': '2023-01-15T14:30:00Z'},
            {'ID': 'feed3', 'CreatedFullName': requestor_name, 'CreatedDate': '2023-01-12T09:15:00Z', 'LastUpdatedDate': '2023-01-13T09:15:00Z'},
            {'ID': 'feed4', 'CreatedFullName': 'Other User', 'CreatedDate': '2023-01-11T09:15:00Z', 'LastUpdatedDate': '2023-01-13T09:15:00Z'}
        ]
        expected_datetime = datetime.datetime.fromisoformat('2023-01-15T14:30:00+00:00')

        # Configure mocks
        self.facade.tickets.get_ticket = MagicMock(return_value=ticket)
        self.facade.tickets.get_ticket_feed = MagicMock(return_value=feed)

        # Mock the feed.get_feed_entry with side_effect for different entries
        self.facade.feed.get_feed_entry = MagicMock(side_effect=[
            {'ID': 'feed3', 'Replies': [
                {'CreatedFullName': 'Other User', 'CreatedDate': '2023-01-12T10:15:00Z'},
                {'CreatedFullName': requestor_name, 'CreatedDate': '2023-01-13T09:15:00Z'}
            ]},
            {'ID': 'feed4', 'Replies': [
                {'CreatedFullName': 'Another User', 'CreatedDate': '2023-01-12T10:15:00Z'}
            ]}
        ])

        # Call the method
        result = self.facade.get_last_requestor_response(ticket_id, requestor_name)

        # Verify the correct methods were called
        self.facade.tickets.get_ticket.assert_called_once_with(ticket_id)
        self.facade.tickets.get_ticket_feed.assert_called_once_with(ticket_id)
        self.facade.feed.get_feed_entry.assert_any_call('feed3')
        self.facade.feed.get_feed_entry.assert_any_call('feed4')

        # Verify result
        self.assertEqual(result, expected_datetime)

    def test_days_since_requestor_response(self):
        """Test calculating days since last requestor response."""
        ticket_id = 'ticket123'
        requestor_name = 'Test User'
        last_response = datetime.datetime(2023, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        expected_days = 5

        # Configure mocks
        self.facade.get_last_requestor_response = MagicMock(return_value=last_response)

        # Create a custom mock for datetime.datetime.now
        mock_now = MagicMock()
        mock_now.return_value = datetime.datetime(2023, 1, 20, 10, 0, 0, tzinfo=timezone.utc)

        # Patch the module's datetime usage through a more effective approach
        with patch('teamdynamix.facade.teamdynamix_facade.datetime') as mock_datetime:
            # Set up the datetime module to return our fixed date
            mock_datetime.datetime = MagicMock(spec=datetime.datetime)
            mock_datetime.datetime.now = mock_now
            mock_datetime.timezone = timezone

            # Call the method
            result = self.facade.days_since_requestor_response(ticket_id, requestor_name)

            # Verify method call
            self.facade.get_last_requestor_response.assert_called_once_with(ticket_id, requestor_name)

            # Verify result
            self.assertEqual(result, expected_days)

    def test_days_since_requestor_response_no_response(self):
        """Test calculating days since last requestor response when there's no response."""
        ticket_id = 'ticket123'
        requestor_name = 'Test User'

        # Configure mocks
        self.facade.get_last_requestor_response = MagicMock(return_value=None)

        # Call the method
        result = self.facade.days_since_requestor_response(ticket_id, requestor_name)

        # Verify method call
        self.facade.get_last_requestor_response.assert_called_once_with(ticket_id, requestor_name)

        # Verify result is infinity (no response ever)
        self.assertEqual(result, float('inf'))

    def test_days_since_any_activity(self):
        """Test calculating days since any activity on a ticket."""
        ticket_id = 'ticket123'
        last_activity = datetime.datetime(2023, 1, 19, 14, 30, 0, tzinfo=timezone.utc)
        fixed_today = datetime.datetime(2023, 1, 20, 10, 0, 0, tzinfo=timezone.utc)
        expected_days = 1  # Exactly one day difference

        # Configure mocks
        self.facade.get_ticket_last_activity = MagicMock(return_value=last_activity)

        # Create a mock for the datetime module used in the facade
        with patch('teamdynamix.facade.teamdynamix_facade.datetime') as mock_datetime:
            # Set up our controlled datetime environment
            mock_datetime.datetime = MagicMock()
            mock_datetime.datetime.now = MagicMock(return_value=fixed_today)

            # We need to preserve the real datetime.datetime functionality for date calculations
            mock_datetime.datetime.fromisoformat = datetime.datetime.fromisoformat

            # Ensure timezone works correctly
            mock_datetime.timezone = timezone
            #mock_datetime.timezone.utc = timezone.utc

            # Call the method being tested
            result = self.facade.days_since_any_activity(ticket_id)

            # Verify method call
            self.facade.get_ticket_last_activity.assert_called_once_with(ticket_id)

            # Verify result
            self.assertEqual(result, expected_days)

    def test_get_ticket_feed_by_user(self):
        """Test getting ticket feed entries from a specific user."""
        ticket_id = 'ticket123'
        user_name = 'Test User'
        feed = [
            {'ID': 'feed1', 'CreatedFullName': user_name, 'CreatedDate': '2023-01-10T10:00:00Z'},
            {'ID': 'feed2', 'CreatedFullName': 'Other User', 'CreatedDate': '2023-01-15T14:30:00Z'},
            {'ID': 'feed3', 'CreatedFullName': user_name, 'CreatedDate': '2023-01-12T09:15:00Z'}
        ]
        expected_entries = [
            {'ID': 'feed1', 'CreatedFullName': user_name, 'CreatedDate': '2023-01-10T10:00:00Z'},
            {'ID': 'feed3', 'CreatedFullName': user_name, 'CreatedDate': '2023-01-12T09:15:00Z'}
        ]

        # Configure mocks
        self.facade.tickets.get_ticket_feed = MagicMock(return_value=feed)

        # Call the method
        result = self.facade.get_ticket_feed_by_user(ticket_id, user_name)

        # Verify method call
        self.facade.tickets.get_ticket_feed.assert_called_once_with(ticket_id)

        # Verify result only contains entries from specified user
        self.assertEqual(result, expected_entries)

    def test_exception_handling_in_get_last_requestor_response(self):
        """Test exception handling in get_last_requestor_response method."""
        ticket_id = 'ticket123'
        requestor_name = 'Test User'
        ticket = {'ID': ticket_id, 'RequestorName': requestor_name}

        # Create feed with an entry that will trigger get_feed_entry call
        feed = [{
            'ID': 'feed1',
            'CreatedFullName': 'Other User',
            'CreatedDate': '2023-01-10T10:00:00Z',
            'LastUpdatedDate': '2023-01-13T09:15:00Z'  # Different from CreatedDate
        }]

        # Configure mocks
        self.facade.tickets.get_ticket = MagicMock(return_value=ticket)
        self.facade.tickets.get_ticket_feed = MagicMock(return_value=feed)
        # Mock get_feed_entry to raise an exception
        self.facade.feed.get_feed_entry = MagicMock(side_effect=Exception("Test exception"))

        # Call the method - should handle the exception
        result = self.facade.get_last_requestor_response(ticket_id, requestor_name)

        # Verify method calls
        self.facade.tickets.get_ticket.assert_called_once_with(ticket_id)
        self.facade.tickets.get_ticket_feed.assert_called_once_with(ticket_id)
        self.facade.feed.get_feed_entry.assert_called_once()

        # Verify result is None since we failed to find a requestor response
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
