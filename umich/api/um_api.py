import json
import logging
import requests
import time
import datetime
from typing import Dict, List, Union, Any, Optional, TypeVar, cast


from requests.exceptions import JSONDecodeError

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for generic return type
T = TypeVar('T')
# Global token cache to avoid unnecessary OAuth requests
_token_cache: Dict[str, Dict[str, Any]] = {}

def get_oauth_token(client_id: str, client_secret: str, scope: str,
                   oauth_url: str = "https://gw.api.it.umich.edu/um/oauth2/token") -> Optional[str]:
    """
    Obtain an OAuth2 access token using client credentials flow.

    Args:
        client_id (str): The OAuth2 client ID.
        client_secret (str): The OAuth2 client secret.
        scope (str): The requested scope for the token.
        oauth_url (str): The OAuth2 token endpoint URL.

    Returns:
        Optional[str]: The access token if successful, None otherwise.

    Raises:
        requests.RequestException: If the OAuth request fails.
        ValueError: If the response is missing required fields.
    """
    # Create cache key
    cache_key = f"{client_id}:{scope}"

    # Check if we have a valid cached token
    if cache_key in _token_cache:
        cached_token = _token_cache[cache_key]
        # Check if token is still valid (with 60 second buffer)
        if time.time() < cached_token['expires_at'] - 60:
            logger.debug("Using cached OAuth token")
            return cached_token['access_token']
        else:
            logger.debug("Cached OAuth token expired, requesting new token")

    try:
        # Prepare the request data
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': scope
        }

        headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }

        logger.debug(f"Requesting OAuth token for scope: {scope}")

        # Make the OAuth request
        response = requests.post(oauth_url, data=data, headers=headers)

        if response.status_code == 200:
            token_data = response.json()

            # Validate response contains required fields
            if 'access_token' not in token_data:
                raise ValueError("OAuth response missing access_token")
            if 'expires_in' not in token_data:
                raise ValueError("OAuth response missing expires_in")

            # Cache the token
            expires_at = time.time() + token_data['expires_in']
            _token_cache[cache_key] = {
                'access_token': token_data['access_token'],
                'expires_at': expires_at,
                'token_type': token_data.get('token_type', 'Bearer')
            }

            logger.debug(f"OAuth token obtained successfully, expires in {token_data['expires_in']} seconds")
            return token_data['access_token']
        else:
            logger.error(f"OAuth request failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None

    except requests.RequestException as e:
        logger.error(f"OAuth request failed with exception: {str(e)}")
        return None
    except (ValueError, KeyError) as e:
        logger.error(f"OAuth response parsing failed: {str(e)}")
        return None

def create_headers(client_id: Optional[str] = None, client_secret: Optional[str] = None,
                  scope: Optional[str] = None, api_token: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Create HTTP headers for UM API Gateway requests with OAuth2 or direct token authentication.

    Args:
        client_id (Optional[str]): OAuth2 client ID. Required if api_token is not provided.
        client_secret (Optional[str]): OAuth2 client secret. Required if api_token is not provided.
        scope (Optional[str]): OAuth2 scope. Required if api_token is not provided.
        api_token (Optional[str]): Pre-existing API token. If provided, OAuth parameters are ignored.

    Returns:
        Optional[Dict[str, str]]: A dictionary containing the required HTTP headers,
        or None if authentication fails.

    Raises:
        ValueError: If neither OAuth credentials nor api_token are provided.
    """
    if api_token:
        # Use provided token directly
        return {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
    elif client_id and client_secret and scope:
        # Use OAuth2 flow
        access_token = get_oauth_token(client_id, client_secret, scope)
        if access_token:
            return {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
        else:
            logger.error("Failed to obtain OAuth access token")
            return None
    else:
        raise ValueError("Either provide api_token or all OAuth2 credentials (client_id, client_secret, scope)")

def clear_token_cache() -> None:
    """
    Clear the OAuth token cache. Useful for testing or forcing token refresh.
    """
    global _token_cache
    _token_cache.clear()
    logger.debug("OAuth token cache cleared")

class UMichAPI:
    """
    Base class for interacting with the UM API Gateway.

    This class provides methods for making HTTP requests to the UM API Gateway
    endpoints and handling responses.

    Attributes:
        base_url (str): The base URL for the UM API Gateway.
        category_id (Union[int, str]): The category abbreviation that API is from (e.g. 'bf' for Business & Finance).
        headers (Dict[str, str]): HTTP headers to use for API requests.
    """

    def __init__(self, base_url: str, category_id: Union[int, str], headers: Dict[str, str]):
        """
        Initialize the UM API Gateway client.

        Args:
            base_url (str): The base URL for the UM API Gateway.
            category_id (Union[int, str]): The abbreviation of the category that API is from. e.g. 'bf' for Business & Finance.
            headers (Dict[str, str]): HTTP headers to use for API requests.
        """
        self.base_url = base_url
        self.category_id = category_id
        self.headers = headers

    def get(self, url_suffix: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a GET request to the specified UM API Gateway endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        try:
                url = f'{self.base_url}/{self.category_id}/{url_suffix}'
                response = requests.get(url, headers=self.headers)
                return self._handle_response(response)
        except Exception as e:
            logger.exception(f"Exception occurred during GET request: {str(e)}")
            return None


    def post(self, url_suffix: str, data: Optional[Any] = None, files: Optional[Dict[str, Any]] = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a POST request to the specified UM API Gateway endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Optional[Any]): Data to be sent in the request body, either as JSON or form data.
            files (Optional[Dict[str, Any]]): Files to be uploaded with the request.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f'{self.base_url}/{self.category_id}/{url_suffix}'
        if files:
            # If files are provided, don't use json parameter
            response = requests.post(url, data=data, files=files, headers=self.headers)
        else:
            # If no files, use json parameter for JSON encoding
            response = requests.post(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def put(self, url_suffix: str, data: Any) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a PUT request to the specified UM API Gateway endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Any): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f'{self.base_url}/{self.category_id}/{url_suffix}'
        response = requests.put(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def delete(self, url_suffix: str, data: Optional[Any] = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a DELETE request to the specified UM API Gateway endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Optional[Any]): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f'{self.base_url}/{self.category_id}/{url_suffix}'
        response = requests.delete(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def patch(self, url_suffix: str, data: Any) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Perform a PATCH request to the specified UM API Gateway endpoint.

        Args:
            url_suffix (str): The API endpoint path to append to the base URL.
            data (Any): Data to be sent in the request body as JSON.

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: The JSON response
            from the API if successful, None otherwise.
        """
        url = f'{self.base_url}/{self.category_id}/{url_suffix}'
        response = requests.patch(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def _handle_response(self, response: requests.Response) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Handle the HTTP response from the UM API Gateway.

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
                reset_time = response.headers.get('X-RateLimit-Reset')
                if reset_time:
                    # Parse the reset time from the header
                    reset_time_dt = datetime.datetime.strptime(reset_time, '%a, %d %b %Y %H:%M:%S %Z')

                    # Make sure it's timezone-aware (UTC if not specified)
                    if reset_time_dt.tzinfo is None:
                        reset_time_dt = reset_time_dt.replace(tzinfo=datetime.timezone.utc)

                    # Calculate sleep time using aware datetime
                    current_time = datetime.datetime.now(datetime.UTC)
                    sleep_time = (reset_time_dt - current_time).total_seconds() + 5
                    # Add a safety check for negative sleep times (server time mismatch)
                    if sleep_time < 0:
                        logger.warning(f"Calculated negative sleep time ({sleep_time}s). Using 5s instead.")
                        sleep_time = 5

                    logger.info(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
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

    def _retry_request(self, request: requests.PreparedRequest) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
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
        method = request.method.lower() if request.method else ''
        url = request.url
        data = request.body
        headers = request.headers

        logger.info(f"Retrying {method.upper()} request to {url}")

        if method == 'get':
            response = requests.get(url, headers=headers)
        elif method == 'post':
            response = requests.post(url, data=data, headers=headers)
        elif method == 'put':
            response = requests.put(url, data=data, headers=headers)
        elif method == 'delete':
            response = requests.delete(url, data=data, headers=headers)
        elif method == 'patch':
            response = requests.patch(url, data=data, headers=headers)
        else:
            raise ValueError(f"Unsupported method: {method}")

        return self._handle_response(response)
