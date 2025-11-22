import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional, TypeVar, Union, cast

import requests
from requests.exceptions import ConnectionError, JSONDecodeError

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for generic return type
T = TypeVar("T")


def create_headers(api_token: str) -> Dict[str, str]:
    """
    Create HTTP headers for TeamDynamix API requests.

    Args:
        api_token (str): The API token for authentication.

    Returns:
        Dict[str, str]: A dictionary containing the required HTTP headers.
    """
    return {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}


class TeamDynamixAPI:
    """
    Base class for interacting with the TeamDynamix API.

    This class provides methods for making HTTP requests to the TeamDynamix API
    endpoints and handling responses.

    Attributes:
        base_url (str): The base URL for the TeamDynamix API.
        app_id (Union[int, str]): The application ID for the TeamDynamix instance.
        headers (Dict[str, str]): HTTP headers to use for API requests.
    """

    def __init__(self, base_url: str, app_id: Union[int, str], headers: Dict[str, str]):
        """
        Initialize the TeamDynamix API client.

        Args:
            base_url (str): The base URL for the TeamDynamix API.
            app_id (Union[int, str]): The application ID for the TeamDynamix instance.
            headers (Dict[str, str]): HTTP headers to use for API requests.
        """
        self.base_url = base_url
        self.app_id = app_id
        self.headers = headers

    def get(
        self, url_suffix: str, max_retries: int = 3
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a GET request to the specified TeamDynamix API endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            max_retries (int): Maximum number of retry attempts for transient errors (default: 3).

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.

        Notes:
            Automatically retries on connection errors (errno 54) with exponential backoff.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers)
                return self._handle_response(response)
            except (ConnectionError, ConnectionResetError) as e:
                # Check if this is a connection reset error
                is_connection_reset = (
                    isinstance(e, ConnectionResetError)
                    or "Connection reset by peer" in str(e)
                    or "Connection aborted" in str(e)
                )

                if is_connection_reset and attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    backoff_time = 2**attempt
                    logger.warning(
                        f"⚠️  Connection reset on attempt {attempt + 1}/{max_retries}. "
                        f"Retrying in {backoff_time}s... (URL: {url_suffix[:50]}...)"
                    )
                    time.sleep(backoff_time)
                    continue
                else:
                    # Final attempt failed or non-retriable error
                    logger.error(
                        f"❌ Connection error after {attempt + 1} attempts: {str(e)} "
                        f"(URL: {url_suffix[:50]}...)"
                    )
                    return None
            except Exception as e:
                logger.exception(f"Exception occurred during GET request: {str(e)}")
                return None

        # Should never reach here, but just in case
        return None

    def post(
        self,
        url_suffix: str,
        data: Optional[Any] = None,
        files: Optional[Dict[str, Any]] = None,
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a POST request to the specified TeamDynamix API endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Optional[Any]): Data to be sent in the request body, either as JSON or form data.
            files (Optional[Dict[str, Any]]): Files to be uploaded with the request.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"
        if files:
            # If files are provided, don't use json parameter
            response = requests.post(url, data=data, files=files, headers=self.headers)
        else:
            # If no files, use json parameter for JSON encoding
            response = requests.post(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def put(
        self, url_suffix: str, data: Any
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a PUT request to the specified TeamDynamix API endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Any): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"
        response = requests.put(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def delete(
        self, url_suffix: str, data: Optional[Any] = None
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a DELETE request to the specified TeamDynamix API endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Optional[Any]): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"
        response = requests.delete(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def patch(
        self, url_suffix: str, data: Any
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a PATCH request to the specified TeamDynamix API endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Any): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"
        response = requests.patch(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def _handle_response(
        self, response: requests.Response
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Handle the HTTP response from the TeamDynamix API.

        Args:
            response (requests.Response): The HTTP response object.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            if successful, None otherwise.
        """
        try:
            if response.status_code == 200:
                logger.debug(f"{response.status_code} | Successful Request!")
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    return None
            elif response.status_code == 201:
                logger.debug(f"{response.status_code} | Successful Post!")
                return response.json()
            elif response.status_code == 204:
                logger.debug(f"{response.status_code} | Successful Post!")
                return None
            elif response.status_code == 429:
                reset_time = response.headers.get("X-RateLimit-Reset")
                if reset_time:
                    # Parse the reset time from the header
                    reset_time_dt = datetime.datetime.strptime(
                        reset_time, "%a, %d %b %Y %H:%M:%S %Z"
                    )

                    # Make sure it's timezone-aware (UTC if not specified)
                    if reset_time_dt.tzinfo is None:
                        reset_time_dt = reset_time_dt.replace(
                            tzinfo=datetime.timezone.utc
                        )

                    # Calculate sleep time using aware datetime
                    current_time = datetime.datetime.now(datetime.UTC)
                    sleep_time = (reset_time_dt - current_time).total_seconds() + 5
                    # Add a safety check for negative sleep times (server time mismatch)
                    if sleep_time < 0:
                        logger.warning(
                            f"Calculated negative sleep time ({sleep_time}s). Using 5s instead."
                        )
                        sleep_time = 5

                    logger.info(
                        f"Rate limit exceeded. Sleeping for {sleep_time} seconds."
                    )
                    time.sleep(sleep_time)
                    return self._retry_request(response.request)
                else:
                    logger.warning("Rate limit exceeded but no reset time provided.")
                    # Consider adding a default backoff here
                    time.sleep(5)  # Simple default
                    return self._retry_request(response.request)
            else:
                logger.error(f"Request failed: {response.status_code}")
                logger.error(f"Response text: {response.text}")
                return None
        except Exception as e:
            logger.exception(f"Exception occurred during response handling: {str(e)}")
            return None

    def _retry_request(
        self, request: requests.PreparedRequest
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Retry a failed request.

        Args:
            request (requests.PreparedRequest): The original request to retry.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            if successful, None otherwise.

        Raises:
            ValueError: If the request method is not supported.
        """
        method = request.method.lower() if request.method else ""
        url = request.url
        data = request.body
        headers = request.headers

        logger.info(f"Retrying {method.upper()} request to {url}")

        if method == "get":
            response = requests.get(url, headers=headers)
        elif method == "post":
            response = requests.post(url, data=data, headers=headers)
        elif method == "put":
            response = requests.put(url, data=data, headers=headers)
        elif method == "delete":
            response = requests.delete(url, data=data, headers=headers)
        elif method == "patch":
            response = requests.patch(url, data=data, headers=headers)
        else:
            raise ValueError(f"Unsupported method: {method}")

        return self._handle_response(response)
