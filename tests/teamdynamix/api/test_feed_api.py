import unittest
from unittest.mock import patch, MagicMock
from teamdynamix.api.feed_api import FeedAPI

class TestFeedAPI(unittest.TestCase):
    """
    Unit tests for the FeedAPI class.

    Tests cover:
    - Basic functionality of get_feed_entry
    - Error handling
    - Edge cases
    - Complex feed responses with replies and likes
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock for the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.feed_api.TeamDynamixAPI').start()

        # Create an instance of FeedAPI
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "12345"
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        self.api = FeedAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    def test_get_feed_entry_success(self):
        """Test successfully retrieving a feed entry."""
        # Mock the response from get method
        feed_id = 123
        mock_response = {
            "ID": feed_id,
            "Text": "Sample feed entry",
            "CreatedDate": "2025-04-01T12:00:00Z",
            "CreatedUID": "user123",
            "CreatedFullName": "Test User",
            "Replies": [],
            "Likes": []
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_feed_entry(feed_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'feed/{feed_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_feed_entry_error(self):
        """Test handling errors when retrieving a feed entry."""
        # Mock the get method to return None (simulating an error)
        feed_id = 123
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_feed_entry(feed_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'feed/{feed_id}')

        # Assert the result is None
        self.assertIsNone(result)

    def test_get_feed_entry_with_invalid_id(self):
        """Test retrieving a feed entry with an invalid ID."""
        # Prepare invalid ID formats to test
        invalid_ids = ["invalid_string", 0, -1]

        for feed_id in invalid_ids:
            with self.subTest(feed_id=feed_id):
                # Mock the get method for each subtest
                self.api.get = MagicMock(return_value=None)

                # Call the method being tested
                result = self.api.get_feed_entry(feed_id)

                # Assert the get method was called with the correct parameters
                self.api.get.assert_called_once_with(f'feed/{feed_id}')

                # Assert the result is None
                self.assertIsNone(result)

    def test_get_feed_entry_with_replies_and_likes(self):
        """Test retrieving a feed entry with replies and likes."""
        # Mock the response from get method
        feed_id = 123
        mock_response = {
            "ID": feed_id,
            "Text": "Sample feed entry",
            "CreatedDate": "2025-04-01T12:00:00Z",
            "CreatedUID": "user123",
            "CreatedFullName": "Test User",
            "Replies": [
                {
                    "ID": 456,
                    "Text": "Sample reply",
                    "CreatedDate": "2025-04-01T12:30:00Z",
                    "CreatedUID": "user456",
                    "CreatedFullName": "Reply User"
                }
            ],
            "Likes": [
                {
                    "UID": "user789",
                    "FullName": "Like User",
                    "CreatedDate": "2025-04-01T12:15:00Z"
                }
            ]
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_feed_entry(feed_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'feed/{feed_id}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

        # Additional assertions to verify the structure of the response
        self.assertEqual(len(result["Replies"]), 1)
        self.assertEqual(result["Replies"][0]["ID"], 456)
        self.assertEqual(len(result["Likes"]), 1)
        self.assertEqual(result["Likes"][0]["UID"], "user789")

    def test_parent_get_method_called(self):
        """Test interaction with parent class get method."""
        # Override the mock for TeamDynamixAPI to allow proper inheritance
        patch.stopall()

        # Create a new mock for the parent class that allows proper method calls
        with patch('teamdynamix.api.teamdynamix_api.TeamDynamixAPI.get') as mock_get:
            # Set up the mock to return a specific value
            feed_id = 123
            mock_response = {"ID": feed_id, "Text": "Sample feed entry"}
            mock_get.return_value = mock_response

            # Create a new instance of the API
            api = FeedAPI(self.base_url, self.app_id, self.headers)

            # Call the method that would use the parent's get method
            result = api.get_feed_entry(feed_id)

            # Verify the parent's get method was called with the correct parameters
            mock_get.assert_called_once_with(f'feed/{feed_id}')

            # Verify the result matches what the parent's get method returned
            self.assertEqual(result, mock_response)

if __name__ == '__main__':
    unittest.main()
