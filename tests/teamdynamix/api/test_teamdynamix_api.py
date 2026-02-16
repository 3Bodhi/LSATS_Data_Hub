import unittest
from unittest.mock import patch, Mock, MagicMock
import json
import requests
import logging
from datetime import datetime, timedelta, timezone

from teamdynamix.api.teamdynamix_api import TeamDynamixAPI, create_headers


class TestTeamDynamixAPI(unittest.TestCase):
    """Test cases for the TeamDynamixAPI base class."""

    def setUp(self):
        """Set up test environment before each test."""
        self.base_url = 'https://example.com/api'
        self.app_id = '123'
        self.headers = {'Authorization': 'Bearer test_token', 'Content-Type': 'application/json'}

        # Setup logging capture
        self.logger_mock = patch('teamdynamix.api.teamdynamix_api.logger').start()

        self.api = TeamDynamixAPI(self.base_url, self.app_id, self.headers)

    def tearDown(self):
        """Clean up after each test."""
        patch.stopall()

    def test_create_headers(self):
        """Test the create_headers function creates correct authorization headers."""
        api_token = 'test_token'
        expected_headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
        headers = create_headers(api_token)
        self.assertEqual(headers, expected_headers)

    def test_initialization(self):
        """Test the initialization of TeamDynamixAPI class."""
        self.assertEqual(self.api.base_url, self.base_url)
        self.assertEqual(self.api.app_id, self.app_id)
        self.assertEqual(self.api.headers, self.headers)

    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_get_success(self, mock_get):
        """Test successful GET request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test_data'}
        mock_get.return_value = mock_response

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify mock was called correctly
        mock_get.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'test_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.post')
    def test_post_success_with_data(self, mock_post):
        """Test successful POST request with data."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'data': 'created_data'}
        mock_post.return_value = mock_response

        # Test data
        data = {'test': 'value'}

        # Call the method and check result
        result = self.api.post('test_endpoint', data)

        # Verify mock was called correctly
        mock_post.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            json=data,
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'created_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.post')
    def test_post_success_without_data(self, mock_post):
        """Test successful POST request without data."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'data': 'created_data'}
        mock_post.return_value = mock_response

        # Call the method and check result
        result = self.api.post('test_endpoint')

        # Verify mock was called correctly
        mock_post.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            json=None,
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'created_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.put')
    def test_put_success(self, mock_put):
        """Test successful PUT request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'updated_data'}
        mock_put.return_value = mock_response

        # Test data
        data = {'test': 'update_value'}

        # Call the method and check result
        result = self.api.put('test_endpoint', data)

        # Verify mock was called correctly
        mock_put.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            json=data,
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'updated_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_get_no_content(self, mock_get):
        """Test GET request with 204 No Content response."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 204
        mock_get.return_value = mock_response

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify result is None
        self.assertIsNone(result)

    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_get_json_decode_error(self, mock_get):
        """Test GET request with JSON decode error."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError('Decode error', '', 0)
        mock_get.return_value = mock_response

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify result is None
        self.assertIsNone(result)

    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_get_failed_request(self, mock_get):
        """Test failed GET request (404)."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'
        mock_get.return_value = mock_response

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify result is None
        self.assertIsNone(result)

    @patch('teamdynamix.api.teamdynamix_api.time.sleep')
    @patch('teamdynamix.api.teamdynamix_api.datetime.datetime')
    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_rate_limit_retry(self, mock_get, mock_datetime, mock_sleep):
        """Test rate limit handling and retry."""
        # Setup datetime mocks
        current_time = datetime(2025, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        reset_time = current_time + timedelta(seconds=5)
        mock_datetime.now.return_value = current_time
        mock_datetime.strptime.return_value = reset_time

        # Setup UTC timezone
        mock_datetime.UTC = timezone.utc

        # Setup first response (rate limited)
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {
            'X-RateLimit-Reset': reset_time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        }
        rate_limited_response.request = Mock()
        rate_limited_response.request.method = 'get'
        rate_limited_response.request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        rate_limited_response.request.body = None
        rate_limited_response.request.headers = self.headers

        # Setup second response (success)
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'data': 'retry_success'}

        # Mock get to return rate_limited_response first, then success_response
        mock_get.side_effect = [rate_limited_response, success_response]

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify mocks were called correctly
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()

        # Verify result
        self.assertEqual(result, {'data': 'retry_success'})

    @patch('teamdynamix.api.teamdynamix_api.requests.post')
    def test_retry_post_request(self, mock_post):
        """Test retry for POST request."""
        # Setup request to retry
        request = Mock()
        request.method = 'post'
        request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        request.body = json.dumps({'test': 'retry_value'}).encode()
        request.headers = self.headers

        # Setup response
        response = Mock()
        response.status_code = 201
        response.json.return_value = {'data': 'retry_created'}
        mock_post.return_value = response

        # Call the method and check result
        result = self.api._retry_request(request)

        # Verify mock was called correctly
        mock_post.assert_called_once_with(
            request.url,
            data=request.body,
            headers=request.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'retry_created'})

    @patch('teamdynamix.api.teamdynamix_api.requests.put')
    def test_retry_put_request(self, mock_put):
        """Test retry for PUT request."""
        # Setup request to retry
        request = Mock()
        request.method = 'put'
        request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        request.body = json.dumps({'test': 'retry_update'}).encode()
        request.headers = self.headers

        # Setup response
        response = Mock()
        response.status_code = 200
        response.json.return_value = {'data': 'retry_updated'}
        mock_put.return_value = response

        # Call the method and check result
        result = self.api._retry_request(request)

        # Verify mock was called correctly
        mock_put.assert_called_once_with(
            request.url,
            data=request.body,
            headers=request.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'retry_updated'})

    @patch('teamdynamix.api.teamdynamix_api.requests.delete')
    def test_delete_success(self, mock_delete):
        """Test successful DELETE request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'deleted_data'}
        mock_delete.return_value = mock_response

        # Test data
        data = {'test': 'delete_value'}

        # Call the method and check result
        result = self.api.delete('test_endpoint', data)

        # Verify mock was called correctly
        mock_delete.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            json=data,
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'deleted_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.patch')
    def test_patch_success(self, mock_patch):
        """Test successful PATCH request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'patched_data'}
        mock_patch.return_value = mock_response

        # Test data
        data = {'test': 'patch_value'}

        # Call the method and check result
        result = self.api.patch('test_endpoint', data)

        # Verify mock was called correctly
        mock_patch.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            json=data,
            headers=self.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'patched_data'})

    @patch('teamdynamix.api.teamdynamix_api.requests.post')
    def test_post_with_files(self, mock_post):
        """Test POST request with file uploads."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'data': 'file_uploaded'}
        mock_post.return_value = mock_response

        # Test data and files
        data = {'test': 'value'}
        files = {'file': MagicMock()}

        # Call the method and check result
        result = self.api.post('test_endpoint', data=data, files=files)

        # Verify mock was called correctly
        # File uploads strip Content-Type to let requests set multipart/form-data
        expected_headers = {k: v for k, v in self.headers.items() if k.lower() != 'content-type'}
        mock_post.assert_called_once_with(
            f'{self.base_url}/{self.app_id}/test_endpoint',
            data=data,
            files=files,
            headers=expected_headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'file_uploaded'})

    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_exception_handling(self, mock_get):
        """Test exception handling during request processing."""
        # Setup mock to raise an exception
        mock_get.side_effect = Exception("Test exception")

        # Call the method and check result
        result = self.api.get('test_endpoint')

        # Verify result is None due to exception handling
        self.assertIsNone(result)

    @patch('teamdynamix.api.teamdynamix_api.datetime.datetime')
    @patch('teamdynamix.api.teamdynamix_api.time.sleep')
    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_rate_limit_negative_sleep_time(self, mock_get, mock_sleep, mock_datetime):
        """Test rate limit handling with negative sleep time calculation."""
        # Setup datetime mocks for negative sleep time scenario
        current_time = datetime(2025, 4, 1, 12, 0, 10, tzinfo=timezone.utc)  # Current time ahead of reset time
        reset_time = datetime(2025, 4, 1, 12, 0, 0, tzinfo=timezone.utc)    # Reset time in the past

        mock_datetime.now.return_value = current_time
        mock_datetime.strptime.return_value = reset_time

        # Setup UTC timezone
        mock_datetime.UTC = timezone.utc

        # Setup rate limited response
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {
            'X-RateLimit-Reset': reset_time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        }
        rate_limited_response.request = Mock()
        rate_limited_response.request.method = 'get'
        rate_limited_response.request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        rate_limited_response.request.body = None
        rate_limited_response.request.headers = self.headers

        # Setup success response for retry
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'data': 'success_after_negative_sleep'}

        mock_get.side_effect = [rate_limited_response, success_response]

        # Call the method
        result = self.api.get('test_endpoint')

        # Verify sleep was called with default value (5 seconds)
        mock_sleep.assert_called_once_with(5)

        # Verify result
        self.assertEqual(result, {'data': 'success_after_negative_sleep'})

    @patch('teamdynamix.api.teamdynamix_api.requests.delete')
    def test_retry_delete_request(self, mock_delete):
        """Test retry for DELETE request."""
        # Setup request to retry
        request = Mock()
        request.method = 'delete'
        request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        request.body = json.dumps({'test': 'retry_delete'}).encode()
        request.headers = self.headers

        # Setup response
        response = Mock()
        response.status_code = 200
        response.json.return_value = {'data': 'retry_deleted'}
        mock_delete.return_value = response

        # Call the method and check result
        result = self.api._retry_request(request)

        # Verify mock was called correctly
        mock_delete.assert_called_once_with(
            request.url,
            data=request.body,
            headers=request.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'retry_deleted'})

    @patch('teamdynamix.api.teamdynamix_api.requests.patch')
    def test_retry_patch_request(self, mock_patch):
        """Test retry for PATCH request."""
        # Setup request to retry
        request = Mock()
        request.method = 'patch'
        request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        request.body = json.dumps({'test': 'retry_patch'}).encode()
        request.headers = self.headers

        # Setup response
        response = Mock()
        response.status_code = 200
        response.json.return_value = {'data': 'retry_patched'}
        mock_patch.return_value = response

        # Call the method and check result
        result = self.api._retry_request(request)

        # Verify mock was called correctly
        mock_patch.assert_called_once_with(
            request.url,
            data=request.body,
            headers=request.headers
        )

        # Verify result
        self.assertEqual(result, {'data': 'retry_patched'})

    def test_retry_unsupported_method(self):
        """Test retry with unsupported HTTP method."""
        # Setup request with unsupported method
        request = Mock()
        request.method = 'head'  # Using HEAD since DELETE and PATCH are now supported
        request.url = f'{self.base_url}/{self.app_id}/test_endpoint'

        # Verify that ValueError is raised
        with self.assertRaises(ValueError):
            self.api._retry_request(request)

    @patch('teamdynamix.api.teamdynamix_api.time.sleep')
    @patch('teamdynamix.api.teamdynamix_api.requests.get')
    def test_rate_limit_no_reset_time(self, mock_get, mock_sleep):
        """Test rate limit handling when no reset time is provided."""
        # Setup rate limited response without reset time header
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {}  # No reset time header
        rate_limited_response.request = Mock()
        rate_limited_response.request.method = 'get'
        rate_limited_response.request.url = f'{self.base_url}/{self.app_id}/test_endpoint'
        rate_limited_response.request.body = None
        rate_limited_response.request.headers = self.headers

        # Setup success response for retry
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'data': 'success_default_backoff'}

        mock_get.side_effect = [rate_limited_response, success_response]

        # Call the method
        result = self.api.get('test_endpoint')

        # Verify sleep was called with default value (5 seconds)
        mock_sleep.assert_called_once_with(5)

        # Verify result
        self.assertEqual(result, {'data': 'success_default_backoff'})


if __name__ == '__main__':
    unittest.main()
