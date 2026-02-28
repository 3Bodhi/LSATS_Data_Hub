import base64
import datetime
import json
import logging
import threading
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


class TeamDynamixAuth:
    """
    Manages TeamDynamix authentication with automatic token refresh.

    Supports three authentication methods in priority order:
    1. Admin service account (BEID + WebServicesKey) via /auth/loginadmin
    2. Username/password login via /auth
    3. Static API token (legacy, no auto-refresh)

    Attributes:
        base_url (str): The base URL for the TeamDynamix API.
        headers (Dict[str, str]): Shared headers dict updated on token refresh.
    """

    def __init__(
        self,
        base_url: str,
        beid: Optional[str] = None,
        web_services_key: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        """
        Initialize authentication manager.

        Args:
            base_url: The base URL for the TeamDynamix API (e.g. https://host/TDWebApi/api).
            beid: Admin BEID (for loginadmin method).
            web_services_key: Admin WebServicesKey (for loginadmin method).
            username: Username (for login method).
            password: Password (for login method).
            api_token: Static API token (legacy, no auto-refresh).

        Raises:
            ValueError: If no valid credential combination is provided.
        """
        self.base_url = base_url
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._auth_method: Optional[str] = None

        # Determine auth method by priority
        if beid and web_services_key:
            self._auth_method = "loginadmin"
            self._credentials = {"BEID": beid, "WebServicesKey": web_services_key}
        elif username and password:
            self._auth_method = "login"
            self._credentials = {"UserName": username, "Password": password}
        elif api_token:
            self._auth_method = "static"
            self._token = api_token
        else:
            raise ValueError(
                "No valid credentials provided. Supply one of: "
                "beid+web_services_key, username+password, or api_token."
            )

        # Build initial headers (shared dict reference across all API adapters)
        if self._auth_method == "static":
            self.headers = create_headers(self._token)
        else:
            self.authenticate()
            self.headers = create_headers(self._token)

    def authenticate(self) -> str:
        """
        Authenticate with TeamDynamix and retrieve a new JWT.

        Returns:
            The JWT token string.

        Raises:
            RuntimeError: If authentication fails or static token mode is used.
        """
        if self._auth_method == "static":
            raise RuntimeError(
                "Cannot re-authenticate with a static API token. "
                "Provide username/password or BEID/WebServicesKey for auto-refresh."
            )

        with self._lock:
            if self._auth_method == "loginadmin":
                endpoint = f"{self.base_url}/auth/loginadmin"
            else:
                endpoint = f"{self.base_url}/auth"

            logger.info(f"🔑 Authenticating via {self._auth_method}...")

            try:
                response = requests.post(
                    endpoint,
                    json=self._credentials,
                    headers={"Content-Type": "application/json"},
                )

                if response.status_code == 200:
                    self._token = response.text.strip().strip('"')
                    logger.info("✅ Authentication successful.")
                    return self._token
                else:
                    error_msg = (
                        f"Authentication failed with status {response.status_code}: "
                        f"{response.text}"
                    )
                    logger.error(f"❌ {error_msg}")
                    raise RuntimeError(error_msg)
            except requests.RequestException as e:
                error_msg = f"Authentication request failed: {str(e)}"
                logger.error(f"❌ {error_msg}")
                raise RuntimeError(error_msg) from e

    def refresh_token(self) -> bool:
        """
        Refresh the JWT token and update the shared headers dict.

        Thread-safe: only one refresh will execute at a time. Concurrent callers
        will wait and then use the newly refreshed token.

        Returns:
            True if the token was successfully refreshed, False otherwise.
        """
        if self._auth_method == "static":
            logger.warning("⚠️  Cannot refresh a static API token.")
            return False

        try:
            self.authenticate()
            # Update the shared headers dict in-place so all API adapters see the change
            self.headers["Authorization"] = f"Bearer {self._token}"
            return True
        except RuntimeError:
            return False

    def is_token_expired(self) -> bool:
        """
        Check if the current JWT token is expired by decoding the exp claim.

        Uses a 60-second buffer to account for clock skew and network latency.

        Returns:
            True if the token is expired or cannot be decoded, False otherwise.
        """
        if not self._token:
            return True

        try:
            # JWT format: header.payload.signature
            parts = self._token.split(".")
            if len(parts) != 3:
                return True

            # Decode the payload (second part) with base64url
            payload_b64 = parts[1]
            # Add padding if needed
            padding = 4 - len(payload_b64) % 4
            if padding != 4:
                payload_b64 += "=" * padding

            payload_bytes = base64.urlsafe_b64decode(payload_b64)
            payload = json.loads(payload_bytes)

            exp = payload.get("exp")
            if exp is None:
                return True

            # Check with 60-second buffer
            now = time.time()
            return now >= (exp - 60)
        except Exception:
            # If we can't decode the token, assume expired
            logger.debug("Could not decode JWT to check expiration, assuming expired.")
            return True

    @property
    def can_refresh(self) -> bool:
        """Whether this auth instance supports token refresh."""
        return self._auth_method != "static"


class TeamDynamixAPI:
    """
    Base class for interacting with the TeamDynamix API.

    This class provides methods for making HTTP requests to the TeamDynamix API
    endpoints and handling responses.

    Attributes:
        base_url (str): The base URL for the TeamDynamix API.
        app_id (Union[int, str]): The application ID for the TeamDynamix instance.
        headers (Dict[str, str]): HTTP headers to use for API requests.
        auth (Optional[TeamDynamixAuth]): Auth manager for automatic token refresh.
    """

    def __init__(
        self,
        base_url: str,
        app_id: Union[int, str],
        headers: Dict[str, str],
        auth: Optional["TeamDynamixAuth"] = None,
    ):
        """
        Initialize the TeamDynamix API client.

        Args:
            base_url: The base URL for the TeamDynamix API.
            app_id: The application ID for the TeamDynamix instance.
            headers: HTTP headers to use for API requests.
            auth: Optional auth manager for automatic token refresh on 401 responses.
        """
        self.base_url = base_url
        self.app_id = app_id
        self.headers = headers
        self.auth = auth

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

        Notes:
            When files are provided, Content-Type header is removed to allow requests
            library to set multipart/form-data with proper boundary parameter.
        """
        url = f"{self.base_url}/{self.app_id}/{url_suffix}"
        if files:
            # For file uploads, remove Content-Type and let requests set multipart/form-data
            # The requests library will automatically set Content-Type to multipart/form-data
            # with the correct boundary parameter when files= is provided
            headers = {
                k: v for k, v in self.headers.items() if k.lower() != "content-type"
            }
            response = requests.post(url, data=data, files=files, headers=headers)
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
        self, response: requests.Response, _is_retry: bool = False
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Handle the HTTP response from the TeamDynamix API.

        Args:
            response: The HTTP response object.
            _is_retry: Internal flag to prevent infinite retry loops on 401.

        Returns:
            The JSON response if successful, None otherwise.
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
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    # Some endpoints return 201 with empty body
                    return None
            elif response.status_code == 204:
                logger.debug(f"{response.status_code} | Successful Post!")
                return None
            elif response.status_code == 401:
                return self._handle_unauthorized(response, _is_retry)
            elif response.status_code == 403:
                logger.error(
                    f"🚫 Permission denied (403 Forbidden): {response.request.url}"
                )
                logger.error(f"Response text: {response.text}")
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

    def _handle_unauthorized(
        self, response: requests.Response, _is_retry: bool
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Handle a 401 Unauthorized response by attempting token refresh and retry.

        On 401, checks if the auth manager supports refresh. If the token is expired,
        refreshes and retries. If the token appears valid (not expired), still attempts
        one refresh in case the token was invalidated server-side. Only retries once
        to avoid hammering the auth endpoint for genuinely unauthorized accounts.

        Args:
            response: The 401 HTTP response object.
            _is_retry: Whether this is already a retry after a refresh attempt.

        Returns:
            The JSON response if retry succeeds, None otherwise.
        """
        if _is_retry:
            logger.error(
                "❌ 401 Unauthorized."
                "Credentials may be invalid or account may lack access."
            )
            return None

        if not self.auth or not self.auth.can_refresh:
            logger.error(
                "❌ 401 Unauthorized. No credential-based auth configured for "
                "automatic token refresh. Check your API token or provide "
                "username/password credentials."
            )
            return None

        if self.auth.is_token_expired():
            logger.info("🔄 Token expired. Refreshing...")
        else:
            logger.info(
                "🔄 401 received but token not expired. "
                "Attempting refresh in case token was invalidated server-side..."
            )

        if self.auth.refresh_token():
            logger.info("🔄 Token refreshed. Retrying request...")
            return self._retry_request(response.request, _is_auth_retry=True)
        else:
            logger.error("❌ Token refresh failed. Cannot retry request.")
            return None

    def _retry_request(
        self,
        request: requests.PreparedRequest,
        _is_auth_retry: bool = False,
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Retry a failed request using current headers (picks up refreshed tokens).

        Args:
            request: The original request to retry.
            _is_auth_retry: If True, passes _is_retry=True to _handle_response
                to prevent infinite 401 retry loops.

        Returns:
            The JSON response if successful, None otherwise.

        Raises:
            ValueError: If the request method is not supported.
        """
        method = request.method.lower() if request.method else ""
        url = request.url
        data = request.body
        # Use current self.headers so retries pick up refreshed tokens
        headers = self.headers

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

        return self._handle_response(response, _is_retry=_is_auth_retry)
