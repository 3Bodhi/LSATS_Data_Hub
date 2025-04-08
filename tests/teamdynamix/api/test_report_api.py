import unittest
from unittest.mock import patch, MagicMock
from teamdynamix.api.report_api import ReportAPI

class TestReportAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the ReportAPI class.

    These tests focus on the specific functionality of the ReportAPI class
    without redundantly testing the underlying TeamDynamixAPI methods.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.report_api.TeamDynamixAPI').start()

        # Create an instance of ReportAPI with the mocked parent
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "12345"
        self.headers = {"Authorization": "Bearer token123", "Content-Type": "application/json"}
        self.api = ReportAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    # ----- Basic functionality tests -----

    def test_get_reports(self):
        """Test successfully retrieving all reports."""
        # Mock the response from get method
        mock_response = [
            {"ID": 1, "Name": "Report A"},
            {"ID": 2, "Name": "Report B"}
        ]
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_reports()

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with('reports')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_report_without_data(self):
        """Test successfully retrieving a specific report without data."""
        # Prepare test data
        report_id = 1

        # Mock the response from get method
        mock_response = {
            "ID": 1,
            "Name": "Report A",
            "Description": "Report A description"
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_report(report_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'reports/{report_id}?withData=False&dataSortExpression=')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_get_report_with_data(self):
        """Test successfully retrieving a specific report with data."""
        # Prepare test data
        report_id = 1
        with_data = True
        sort_expression = "Name ASC"

        # Mock the response from get method
        mock_response = {
            "ID": 1,
            "Name": "Report A",
            "Description": "Report A description",
            "Data": [{"Row1": "Value1"}, {"Row2": "Value2"}]
        }
        self.api.get = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.get_report(report_id, with_data, sort_expression)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'reports/{report_id}?withData={with_data}&dataSortExpression={sort_expression}')

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    def test_search_reports(self):
        """Test successfully searching for reports."""
        # Prepare test data
        search_data = {
            "NameLike": "Report",
            "IsActive": True
        }

        # Mock the response from post method
        mock_response = [
            {"ID": 1, "Name": "Report A"},
            {"ID": 2, "Name": "Report B"}
        ]
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_reports(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('reports/search', search_data)

        # Assert the result is as expected
        self.assertEqual(result, mock_response)

    # ----- Error handling tests -----

    def test_get_reports_error(self):
        """Test handling errors when retrieving reports."""
        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_reports()

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with('reports')

        # Assert the result is None
        self.assertIsNone(result)

    def test_get_report_error(self):
        """Test handling errors when retrieving a specific report."""
        # Prepare test data
        report_id = 1

        # Mock the get method to return None (simulating an error)
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_report(report_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'reports/{report_id}?withData=False&dataSortExpression=')

        # Assert the result is None
        self.assertIsNone(result)

    def test_search_reports_error(self):
        """Test handling errors when searching for reports."""
        # Prepare test data
        search_data = {
            "NameLike": "Report",
            "IsActive": True
        }

        # Mock the post method to return None (simulating an error)
        self.api.post = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.search_reports(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('reports/search', search_data)

        # Assert the result is None
        self.assertIsNone(result)

    # ----- Edge case tests -----

    def test_get_report_with_invalid_id(self):
        """Test retrieving a report with an invalid ID."""
        # Prepare invalid ID
        report_id = "invalid"

        # Mock the get method
        self.api.get = MagicMock(return_value=None)

        # Call the method being tested
        result = self.api.get_report(report_id)

        # Assert the get method was called with the correct parameters
        self.api.get.assert_called_once_with(f'reports/{report_id}?withData=False&dataSortExpression=')

        # Assert the result is None
        self.assertIsNone(result)

    def test_search_reports_with_empty_data(self):
        """Test searching for reports with empty search criteria."""
        # Prepare empty search data
        search_data = {}

        # Mock the post method
        mock_response = []
        self.api.post = MagicMock(return_value=mock_response)

        # Call the method being tested
        result = self.api.search_reports(search_data)

        # Assert the post method was called with the correct parameters
        self.api.post.assert_called_once_with('reports/search', search_data)

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
            mock_get.return_value = [{"ID": 1, "Name": "Report A"}]

            # Create a new instance of the API
            api = ReportAPI(self.base_url, self.app_id, self.headers)  # Changed from ReportsAPI

            # Call the method that would use the parent's get method
            result = api.get_reports()

            # Verify the parent's get method was called with the correct parameters
            mock_get.assert_called_once_with('reports')

            # Verify the result matches what the parent's get method returned
            self.assertEqual(result, [{"ID": 1, "Name": "Report A"}])

    def test_parent_post_method_called(self):
        """Test interaction with parent class post method."""
        # Override the mock for TeamDynamixAPI to allow proper inheritance
        patch.stopall()

        # Create a new mock for the parent class that allows proper method calls
        with patch('teamdynamix.api.teamdynamix_api.TeamDynamixAPI.post') as mock_post:
            # Set up the mock to return a specific value
            mock_post.return_value = [{"ID": 1, "Name": "Report A"}]

            # Create a new instance of the API
            api = ReportAPI(self.base_url, self.app_id, self.headers)  # Changed from ReportsAPI

            # Prepare test data
            search_data = {"NameLike": "Report"}

            # Call the method that would use the parent's post method
            result = api.search_reports(search_data)

            # Verify the parent's post method was called with the correct parameters
            mock_post.assert_called_once_with('reports/search', search_data)

            # Verify the result matches what the parent's post method returned
            self.assertEqual(result, [{"ID": 1, "Name": "Report A"}])

if __name__ == '__main__':
    unittest.main()
