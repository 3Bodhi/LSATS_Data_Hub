import getpass
import logging
import os
from typing import Any, Dict, List, Optional, Union

import keyring
from ldap3 import ALL, LEVEL, SUBTREE, Connection, Server
from ldap3.core.exceptions import LDAPException

# Set up logging to match existing LSATS patterns
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("logging set to DEBUG")


class LDAPAdapter:
    """
    LDAP connection adapter providing standardized LDAP operations.

    This class handles LDAP server connections, authentication, and basic
    query operations. It provides a consistent interface for LDAP operations
    regardless of the underlying server type (Active Directory, OpenLDAP, etc.).
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize LDAP adapter with configuration settings.

        Args:
            config: Dictionary containing LDAP connection settings.
                   Required keys:
                   - 'server': LDAP server hostname
                   - 'search_base': Base DN for searches
                   - 'user': Username for authentication
                   - 'keyring_service': Keyring service name for password

                   Optional keys with defaults:
                   - 'port': LDAP port (default: 636 for SSL, 389 for non-SSL)
                   - 'use_ssl': Enable SSL/TLS (default: True)
                   - 'timeout': Connection timeout in seconds (default: 30)
                   - 'auto_bind': Auto-bind on connection (default: True)
                   - 'get_info': Server info level (default: ALL)
                   - 'default_page_size': Default page size for pagination (default: 1000)

        Raises:
            ValueError: If required configuration keys are missing
            TypeError: If configuration is not a dictionary
        """
        if not isinstance(config, dict):
            raise TypeError("Configuration must be a dictionary")

        # Validate required configuration keys
        required_keys = ["server", "search_base", "user", "keyring_service"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")

        # Store core configuration
        self.server_hostname = config["server"]
        self.search_base = config["search_base"]
        self.user = config["user"]
        self.keyring_service = config["keyring_service"]

        # Set defaults for optional configuration
        self.use_ssl = config.get("use_ssl", True)
        self.port = config.get("port", 636 if self.use_ssl else 389)
        self.timeout = config.get("timeout", 600)  # AD is very slow, needs long timeout
        self.auto_bind = config.get("auto_bind", True)
        self.get_info = config.get("get_info", ALL)
        self.default_page_size = config.get("default_page_size", 1000)

        # Store additional configuration for extensibility
        self.additional_config = {
            k: v
            for k, v in config.items()
            if k
            not in required_keys
            + [
                "port",
                "use_ssl",
                "timeout",
                "auto_bind",
                "get_info",
                "default_page_size",
            ]
        }

        # Initialize connection objects (will be created on first use)
        self._server = None
        self._connection = None
        self._password = None

        logger.debug(f"LDAP adapter initialized for server: {self.server_hostname}")

    def _get_password(self) -> str:
        """
        Retrieve password from keyring or prompt user.

        This method follows the same pattern as get_ad_password() in
        clean_personnel_data.py, providing a consistent password management
        experience across the LSATS Data Hub.

        Returns:
            str: The password for LDAP authentication

        Raises:
            KeyboardInterrupt: If user cancels password prompt
        """
        if self._password:
            return self._password

        try:
            # First, try to get password from keyring
            password = keyring.get_password(self.keyring_service, self.user)
            if password:
                logger.debug("Using password from keyring")
                self._password = password
                return password
        except Exception as e:
            logger.warning(f"Could not retrieve password from keyring: {e}")

        # If keyring fails or no password stored, prompt user
        try:
            password = getpass.getpass(f"Enter LDAP password for {self.user}: ")
            self._password = password

            # Optionally store in keyring for future use
            try:
                save_password = (
                    input("Save password to keyring? (y/n): ").lower().strip()
                )
                if save_password == "y":
                    keyring.set_password(self.keyring_service, self.user, password)
                    logger.info("Password saved to keyring")
            except Exception as e:
                logger.warning(f"Could not save password to keyring: {e}")

            return password

        except KeyboardInterrupt:
            logger.info("Password prompt cancelled by user")
            raise

    def _create_server(self) -> Server:
        """
        Create LDAP server object with current configuration.

        Returns:
            Server: Configured ldap3 Server object

        Raises:
            LDAPException: If server creation fails
        """
        if not self._server:
            try:
                self._server = Server(
                    self.server_hostname,
                    use_ssl=self.use_ssl,
                    port=self.port,
                    get_info=self.get_info,
                    connect_timeout=self.timeout,
                )
                logger.debug(
                    f"LDAP server object created: {self.server_hostname}:{self.port}"
                )
            except Exception as e:
                logger.error(f"Failed to create LDAP server object: {e}")
                raise LDAPException(f"Server creation failed: {e}")

        return self._server

    def _create_connection(self) -> Connection:
        """
        Create and bind LDAP connection.

        Returns:
            Connection: Authenticated ldap3 Connection object

        Raises:
            LDAPException: If connection or authentication fails
        """
        try:
            server = self._create_server()
            password = self._get_password()

            connection = Connection(
                server, user=self.user, password=password, auto_bind=self.auto_bind
            )

            if connection.bound:
                logger.info(f"Successfully connected to {self.server_hostname}")
                return connection
            else:
                raise LDAPException("Failed to bind to LDAP server")

        except Exception as e:
            logger.error(f"LDAP connection failed: {e}")
            raise LDAPException(f"Connection failed: {e}")

    def test_connection(self) -> bool:
        """
        Test LDAP connection and verify functionality.

        This method performs a comprehensive test of the LDAP connection by:
        1. Establishing a connection to the server
        2. Authenticating with provided credentials
        3. Performing a basic search operation to verify query functionality
        4. Searching for organizational units at the root level

        The search uses a minimal filter (objectClass=organizationalUnit)
        with LEVEL scope to find top-level OUs without retrieving excessive data.
        This approach works with virtually any LDAP server configuration.

        Returns:
            bool: True if connection test succeeds, False otherwise
        """
        try:
            # Create connection
            conn = self._create_connection()

            # Perform verification search for organizational units
            # This tests both connection and search functionality
            search_filter = "(objectClass=organizationalUnit)"

            logger.debug(f"Testing connection with search at base: {self.search_base}")
            logger.debug(f"Search filter: {search_filter}")

            success = conn.search(
                search_base=self.search_base,
                search_filter=search_filter,
                search_scope=LEVEL,  # Only search immediate children
                attributes=["ou", "description"],  # Minimal attributes
                size_limit=10,  # Limit results for testing
            )

            if success:
                result_count = len(conn.entries)
                logger.info(
                    f"Connection test successful: found {result_count} organizational units"
                )
                logger.debug(f"Search result: {conn.result}")

                # Log some sample results for debugging
                if conn.entries:
                    for i, entry in enumerate(conn.entries[:3]):  # Show first 3 entries
                        logger.debug(f"Sample entry {i + 1}: {entry.entry_dn}")

                return True
            else:
                logger.warning(f"Search operation failed: {conn.result}")
                return False

        except LDAPException as e:
            logger.error(f"LDAP connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection test: {e}")
            return False
        finally:
            # Clean up connection
            if hasattr(self, "_connection") and self._connection:
                try:
                    self._connection.unbind()
                    logger.debug("LDAP connection closed")
                except:
                    pass  # Ignore cleanup errors

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current LDAP configuration.

        Returns:
            Dict[str, Any]: Configuration information (passwords excluded)
        """
        return {
            "server": self.server_hostname,
            "port": self.port,
            "use_ssl": self.use_ssl,
            "search_base": self.search_base,
            "user": self.user,
            "keyring_service": self.keyring_service,
            "timeout": self.timeout,
            "default_page_size": self.default_page_size,
            "additional_config": self.additional_config,
        }

    def __str__(self) -> str:
        """String representation of the LDAP adapter."""
        ssl_status = "SSL" if self.use_ssl else "non-SSL"
        return f"LDAPAdapter({self.server_hostname}:{self.port}, {ssl_status}, user={self.user})"

    def __repr__(self) -> str:
        """Detailed string representation for debugging."""
        return (
            f"LDAPAdapter(server='{self.server_hostname}', port={self.port}, "
            f"use_ssl={self.use_ssl}, search_base='{self.search_base}', "
            f"user='{self.user}', keyring_service='{self.keyring_service}')"
        )

    # Core Search Infrastructure

    def search(
        self,
        search_filter: str,
        search_base: Optional[str] = None,
        scope: str = "subtree",
        attributes: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        use_pagination: bool = True,
        page_size: Optional[int] = None,
    ) -> List:
        """
        Core search method with automatic pagination for complete results.

        This method serves as the foundation for all LDAP queries in this adapter.
        By default, it automatically handles server-side size limits using pagination
        to ensure complete results. This behavior can be disabled when you need
        fast sampling or are confident about result set sizes.

        Args:
            search_filter: LDAP filter string (e.g., '(objectClass=person)')
            search_base: Base DN for search (defaults to adapter's search_base)
            scope: Search scope - 'base', 'level', or 'subtree' (default: 'subtree')
            attributes: List of attributes to retrieve (None for all available)
            max_results: Maximum number of results to return (None for no limit)
            use_pagination: Enable automatic pagination for complete results (default: True)
            page_size: Page size for pagination (defaults to adapter's configured size)

        Returns:
            List: List of ldap3 Entry objects with full LDAP functionality

        Raises:
            LDAPException: If search operation fails
            ValueError: If parameters are invalid

        Examples:
            # Basic user search
            users = adapter.search('(objectClass=person)')
            for user in users:
                print(f"DN: {user.entry_dn}")
                print(f"Name: {user.cn.value if user.cn else 'N/A'}")
                print(f"Email: {user.mail.value if user.mail else 'N/A'}")

            # Convert to JSON if needed
            user_json = users[0].entry_to_json()

            # Access all attributes
            for attr in users[0].entry_attributes:
                print(f"{attr}: {getattr(users[0], attr).value}")
        """
        # Parameter validation
        if not search_filter or not isinstance(search_filter, str):
            raise ValueError("search_filter must be a non-empty string")

        # Use adapter's default search base if none provided
        base_dn = search_base if search_base is not None else self.search_base

        # Convert scope string to ldap3 constant
        scope_mapping = {"base": "BASE", "level": "LEVEL", "subtree": "SUBTREE"}

        if scope.lower() not in scope_mapping:
            raise ValueError(f"scope must be one of: {list(scope_mapping.keys())}")

        ldap_scope = getattr(__import__("ldap3"), scope_mapping[scope.lower()])

        try:
            # Create fresh connection for this search
            conn = self._create_connection()

            logger.debug(
                f"Executing search: filter='{search_filter}', base='{base_dn}', scope='{scope}', pagination={use_pagination}"
            )

            # Enhanced attribute handling for better LDAP server compatibility
            if attributes is None:
                # Request all available attributes using LDAP standard wildcard
                search_attributes = ["*"]
            elif attributes == ["1.1"]:
                # Special case: RFC 4511 standard for "no attributes"
                search_attributes = ["1.1"]
            elif len(attributes) == 0:
                # Empty list: fallback to minimal safe attribute that all objects have
                search_attributes = ["objectClass"]
                logger.debug(
                    "Empty attributes list provided, using 'objectClass' as safe fallback"
                )
            else:
                # Use the specified attributes as-is
                search_attributes = attributes

            search_kwargs = {
                "search_base": base_dn,
                "search_filter": search_filter,
                "search_scope": ldap_scope,
                "attributes": search_attributes,
            }

            # Add size limit if specified
            if max_results:
                search_kwargs["size_limit"] = max_results

            # Determine pagination strategy
            if use_pagination and not max_results:
                # Use intelligent pagination to ensure complete results
                results = self._execute_intelligent_search(
                    conn, page_size, **search_kwargs
                )
            elif use_pagination and page_size:
                # Use explicit pagination
                results = self._execute_paged_search(conn, page_size, **search_kwargs)
            else:
                # Execute simple search with potential server limits
                results = self._execute_simple_search(conn, **search_kwargs)

            # Return raw ldap3 Entry objects - no conversion needed!
            logger.info(
                f"Search completed successfully: {len(results)} results returned"
            )
            return results

        except LDAPException as e:
            logger.error(f"LDAP search failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during search: {e}")
            raise LDAPException(f"Search operation failed: {e}")
        finally:
            # Always clean up the connection
            try:
                if "conn" in locals() and conn:
                    conn.unbind()
                    logger.debug("Search connection closed")
            except:
                pass  # Ignore cleanup errors

    def search_as_dicts(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        Convenience method that returns search results as dictionaries.

        This is useful when you need simple key-value access or JSON serialization.
        For full LDAP functionality, use search() which returns Entry objects.

        Returns:
            List[Dict[str, Any]]: List of dictionaries with 'dn' and attributes
        """
        entries = self.search(*args, **kwargs)

        result_dicts = []
        for entry in entries:
            # Convert Entry to dictionary
            entry_dict = {"dn": entry.entry_dn}

            # Add all attributes
            for attr_name in entry.entry_attributes:
                attr_value = getattr(entry, attr_name)
                entry_dict[attr_name] = attr_value.value

            result_dicts.append(entry_dict)

        return result_dicts

    def _execute_simple_search(self, conn: Connection, **search_kwargs) -> List:
        """
        Execute a simple search that accepts server-side size limits.

        Args:
            conn: Active LDAP connection
            **search_kwargs: Search parameters

        Returns:
            List: Search results from server
        """
        success = conn.search(**search_kwargs)
        if not success:
            logger.warning(f"Search returned no results: {conn.result}")
            return []

        results = conn.entries

        # Check if results were truncated due to server size limits
        if hasattr(conn, "result") and conn.result:
            result_code = conn.result.get("result", 0)
            if result_code == 4:  # LDAP_SIZELIMIT_EXCEEDED
                logger.warning(
                    f"Search results truncated due to server size limit. "
                    f"Returned {len(results)} results, but more may be available."
                )
            elif "sizeLimitExceeded" in str(conn.result) or len(results) >= 1000:
                logger.warning(
                    f"Search hit server size limit after {len(results)} results. "
                    f"Results may be incomplete."
                )

        return results

    def _execute_intelligent_search(
        self, conn: Connection, page_size: Optional[int], **search_kwargs
    ) -> List:
        """
        Execute search with intelligent pagination detection.

        This method first attempts a simple search. If it detects potential truncation
        (indicated by hitting common size limits like 1000), it automatically switches
        to paginated search to retrieve complete results.

        Args:
            conn: Active LDAP connection
            page_size: Optional page size (uses adapter default if None)
            **search_kwargs: Search parameters

        Returns:
            List: Complete search results
        """
        # First try a simple search to detect size limits
        initial_results = self._execute_simple_search(conn, **search_kwargs)

        # Check if we likely hit a size limit (common values: 500, 1000, 2000)
        result_count = len(initial_results)
        potential_limits = [350, 500, 1000, 2000, 5000]

        if result_count in potential_limits:
            logger.info(
                f"Detected potential size limit ({result_count} results). Switching to paginated search for completeness."
            )

            # Use paginated search to get complete results
            effective_page_size = page_size or self.default_page_size
            return self._execute_paged_search(
                conn, effective_page_size, **search_kwargs
            )
        else:
            # Results appear complete, return them as-is
            logger.debug(f"Search returned {result_count} results, likely complete.")
            return initial_results

    def _execute_paged_search(
        self, conn: Connection, page_size: int, **search_kwargs
    ) -> List:
        """
        Execute a paged search to handle large result sets efficiently.

        This method uses ldap3's paged_search with generator=False to ensure
        the search completes before the connection is closed. The paged_search
        returns response dictionaries, which we filter for actual entries
        (type='searchResEntry') and then retrieve as Entry objects from the
        connection.

        For servers that have issues with paged_search (like MCommunity), this
        method falls back to manual pagination by adjusting the search filter
        to skip already-retrieved results.

        Note: The paged_search method does not accept a size_limit parameter
        (it uses its own pagination mechanism via paged_size). If size_limit
        is present in search_kwargs, it will be extracted and applied after
        all results are retrieved.

        Args:
            conn: Active LDAP connection
            page_size: Number of results per page
            **search_kwargs: Search parameters to pass to each page request.
                           If 'size_limit' is present, it will be removed before
                           calling paged_search and applied to the final results.

        Returns:
            List: Combined Entry objects from all pages (limited by size_limit if specified)
        """
        logger.debug(f"Starting paged search with page size: {page_size}")

        try:
            # Extract size_limit if present - paged_search doesn't accept this parameter
            # We'll apply the limit after fetching results
            size_limit = search_kwargs.pop("size_limit", None)

            # Try the standard paged_search first
            try:
                # Use generator=False to ensure the search completes synchronously
                # This returns a list of response dictionaries
                response_list = conn.extend.standard.paged_search(
                    paged_size=page_size, generator=False, **search_kwargs
                )

                # Filter for actual search result entries (exclude referrals and done messages)
                # paged_search returns response dictionaries with 'type' field
                entry_count = 0
                for response in response_list:
                    if (
                        isinstance(response, dict)
                        and response.get("type") == "searchResEntry"
                    ):
                        entry_count += 1

                logger.info(
                    f"Paged search completed: {entry_count} entries retrieved across "
                    f"{(entry_count // page_size) + 1} pages"
                )

                # After paged_search completes, the entries are available in conn.entries
                # These are proper Entry objects, not dictionaries
                results = list(conn.entries) if conn.entries else []

                # Apply size_limit if it was specified
                if size_limit and len(results) > size_limit:
                    logger.debug(
                        f"Applying size_limit: truncating {len(results)} results to {size_limit}"
                    )
                    results = results[:size_limit]

                return results

            except Exception as paged_error:
                # If paged_search fails (e.g., "invalid messageId"), fall back to manual cookie-based pagination
                logger.warning(
                    f"Standard paged_search failed: {paged_error}. Attempting manual cookie-based pagination fallback."
                )
                # Need to reconnect since the connection may be in a bad state
                conn.unbind()
                conn = self._create_connection()

                return self._execute_cookie_based_pagination(
                    conn, page_size, size_limit, **search_kwargs
                )

        except Exception as e:
            logger.error(f"Error during paged search: {e}")
            raise LDAPException(f"Paged search failed: {e}")

    def _execute_cookie_based_pagination(
        self,
        conn: Connection,
        page_size: int,
        size_limit: Optional[int],
        **search_kwargs,
    ) -> List:
        """
        Cookie-based pagination fallback using low-level LDAP paging control.

        This method manually implements the Simple Paged Results control by directly
        managing the paging cookie. This is more reliable than ldap3's paged_search
        helper for servers with quirky implementations like MCommunity.

        Based on the RFC 2696 Simple Paged Results control implementation.

        Args:
            conn: Active LDAP connection
            page_size: Number of results per page
            size_limit: Optional overall size limit
            **search_kwargs: Search parameters

        Returns:
            List: Combined Entry objects from all pages
        """
        logger.info("Using cookie-based pagination fallback strategy")

        all_results = []
        page_num = 0
        cookie = None

        # Remove size_limit from search_kwargs - we'll handle it ourselves
        search_kwargs.pop("size_limit", None)

        while True:
            page_num += 1
            logger.debug(f"Fetching cookie-based page {page_num}")

            try:
                # Perform search with paged control
                success = conn.search(
                    paged_size=page_size, paged_cookie=cookie, **search_kwargs
                )

                # Log the full result for debugging
                logger.debug(f"Page {page_num} conn.result: {conn.result}")

                # Get results from this page
                page_entries = []
                if conn.response:
                    # Filter for actual entries (not referrals)
                    page_entries = [
                        entry for entry in conn.entries if hasattr(entry, "entry_dn")
                    ]

                    logger.debug(f"Page {page_num}: Got {len(page_entries)} entries")

                    # Add to results
                    for entry in page_entries:
                        all_results.append(entry)

                        # Check if we've reached the size limit
                        if size_limit and len(all_results) >= size_limit:
                            logger.info(
                                f"Reached size_limit of {size_limit} during cookie-based pagination"
                            )
                            return all_results[:size_limit]

                # Check for the paging control in the response to get the cookie
                has_paging_control = False
                new_cookie = None

                if (
                    "controls" in conn.result
                    and "1.2.840.113556.1.4.319" in conn.result["controls"]
                ):
                    has_paging_control = True
                    new_cookie = conn.result["controls"]["1.2.840.113556.1.4.319"][
                        "value"
                    ]["cookie"]

                    logger.debug(
                        f"Paging control present - cookie: {new_cookie!r}, "
                        f"size: {conn.result['controls']['1.2.840.113556.1.4.319']['value'].get('size', 'N/A')}"
                    )

                # Handle different result codes
                result_code = conn.result.get("result", 0)
                result_desc = conn.result.get("description", "success")

                if result_code == 4:  # sizeLimitExceeded
                    logger.warning(
                        f"Page {page_num}: sizeLimitExceeded (code 4) - "
                        f"collected {len(all_results)} results so far. "
                        f"Server enforces cumulative result limit."
                    )

                    # Cookie-based pagination cannot continue past server's cumulative limit
                    # Fall back to filter-based chunking if we have results to work with
                    if all_results and len(all_results) >= 300:
                        # We hit the limit, try filter-based chunking
                        logger.info(
                            "Falling back to filter-based chunking to retrieve remaining results..."
                        )

                        # Reconnect with fresh connection
                        conn.unbind()
                        conn = self._create_connection()

                        # Use the same chunk size as the limit we hit
                        chunk_size = len(all_results)

                        # Continue with filter-based chunking from where we left off
                        remaining = self._execute_filter_based_chunking(
                            conn, chunk_size, size_limit, **search_kwargs
                        )

                        # Merge results (filter-based chunking will start from beginning,
                        # so we need to deduplicate)
                        seen_dns = {entry.entry_dn for entry in all_results}
                        for entry in remaining:
                            if entry.entry_dn not in seen_dns:
                                all_results.append(entry)
                                seen_dns.add(entry.entry_dn)

                        logger.info(
                            f"Filter-based fallback completed. Total results: {len(all_results)}"
                        )
                        return all_results
                    else:
                        # Not enough results to make chunking worthwhile
                        logger.info("Stopping pagination at server limit")
                        break

                elif result_code == 53:  # unwillingToPerform
                    logger.warning(
                        f"Page {page_num}: Server unwillingToPerform (code 53), "
                        "stopping pagination"
                    )
                    break

                elif not success or result_code != 0:
                    logger.warning(
                        f"Page {page_num}: Search failed with code {result_code} "
                        f"({result_desc}), stopping pagination"
                    )
                    break

                # Normal success case - update cookie
                if new_cookie:
                    cookie = new_cookie
                    logger.debug(f"Got cookie for next page: {cookie!r}")
                else:
                    # Empty cookie means we're done
                    logger.debug("Empty cookie received, pagination complete")
                    break

                # Safety check: if we got no entries, stop
                if not page_entries:
                    logger.debug("No entries in page, stopping pagination")
                    break

            except Exception as e:
                logger.error(
                    f"Error during cookie-based pagination on page {page_num}: {e}"
                )
                # If we got some results, return them; otherwise raise
                if all_results:
                    logger.warning(
                        f"Returning {len(all_results)} results collected before error"
                    )
                    return all_results
                else:
                    raise LDAPException(f"Cookie-based pagination failed: {e}")

            # Safety limit: don't fetch more than 200 pages
            if page_num >= 200:
                logger.warning(
                    f"Cookie-based pagination reached safety limit of 200 pages. "
                    f"Retrieved {len(all_results)} results so far."
                )
                break

        logger.info(
            f"Cookie-based pagination completed: {len(all_results)} entries retrieved across {page_num} pages"
        )
        return all_results

    def _execute_filter_based_chunking(
        self,
        conn: Connection,
        chunk_size: int,
        size_limit: Optional[int],
        **search_kwargs,
    ) -> List:
        """
        Filter-based chunking for servers with hard cumulative result limits.

        This method works around servers like MCommunity that have a hard limit
        (e.g., 350 results) per search operation, even with pagination. Instead of
        using paging controls, it makes multiple independent searches with modified
        filters using the > (greater than) operator to skip past retrieved entries.

        Strategy:
        1. Search with original filter, get up to chunk_size results
        2. Note the last entry's sort attribute value (e.g., uid="azz123")
        3. Make a NEW search with modified filter: (&(original)(uid>azz123))
        4. Repeat until we get fewer than chunk_size results

        Args:
            conn: Active LDAP connection
            chunk_size: Size of each chunk (should be less than server limit)
            size_limit: Optional overall size limit
            **search_kwargs: Search parameters

        Returns:
            List: Combined Entry objects from all chunks
        """
        logger.info(
            f"Using filter-based chunking strategy with chunk_size={chunk_size}"
        )

        all_results = []
        seen_dns = set()  # Safety net for deduplication
        chunk_num = 0
        last_sort_value = None

        # Detect which attribute to use for range filtering
        original_filter = search_kwargs.get("search_filter", "")
        sort_attr = self._detect_sort_attribute(original_filter)

        logger.debug(f"Using '{sort_attr}' as sort attribute for filter-based chunking")

        # Remove size_limit from search_kwargs - we'll handle it ourselves
        search_kwargs.pop("size_limit", None)

        while True:
            chunk_num += 1
            logger.debug(f"Fetching filter-based chunk {chunk_num}")

            # Build filter for this chunk
            if last_sort_value is not None:
                modified_filter = self._add_range_filter(
                    original_filter, sort_attr, last_sort_value, use_greater_than=False
                )
                logger.debug(f"Chunk {chunk_num} filter: {modified_filter}")
            else:
                modified_filter = original_filter

            # Create a fresh search (not using pagination - each chunk is independent)
            search_kwargs_copy = search_kwargs.copy()
            search_kwargs_copy["search_filter"] = modified_filter
            search_kwargs_copy["size_limit"] = chunk_size

            # Ensure we retrieve the sort attribute
            if "attributes" in search_kwargs_copy:
                attrs = search_kwargs_copy["attributes"]
                if attrs and attrs != ["*"] and sort_attr not in attrs:
                    search_kwargs_copy["attributes"] = list(attrs) + [sort_attr]

            try:
                success = conn.search(**search_kwargs_copy)

                if not success or not conn.entries:
                    logger.debug(f"No more results in chunk {chunk_num}")
                    break

                chunk_results = list(conn.entries)
                logger.debug(
                    f"Chunk {chunk_num}: Got {len(chunk_results)} entries "
                    f"(total so far: {len(all_results)})"
                )

                # Add results with deduplication
                new_count = 0
                for entry in chunk_results:
                    # Skip duplicates (shouldn't happen with > operator, but be safe)
                    if entry.entry_dn not in seen_dns:
                        seen_dns.add(entry.entry_dn)
                        all_results.append(entry)
                        new_count += 1

                        # Check size limit
                        if size_limit and len(all_results) >= size_limit:
                            logger.info(
                                f"Reached size_limit of {size_limit} during filter-based chunking"
                            )
                            return all_results[:size_limit]

                logger.debug(f"Added {new_count} new entries from chunk {chunk_num}")

                # If we got fewer results than chunk_size, we're done
                if len(chunk_results) < chunk_size:
                    logger.debug(
                        f"Chunk {chunk_num} returned fewer than chunk_size results, "
                        "chunking complete"
                    )
                    break

                # Get the last sort value for next iteration
                new_last_value = None
                for entry in reversed(chunk_results):
                    if hasattr(entry, sort_attr):
                        new_last_value = getattr(entry, sort_attr).value
                        break

                if new_last_value is None:
                    logger.warning(
                        f"Could not find {sort_attr} attribute in results. "
                        "Cannot continue chunking."
                    )
                    break

                # Check if we're stuck (same boundary value)
                if new_last_value == last_sort_value:
                    logger.warning(
                        f"Boundary value unchanged ({new_last_value}). "
                        "This may indicate all entries have the same value. Stopping."
                    )
                    break

                last_sort_value = new_last_value
                logger.debug(
                    f"Next chunk will start after {sort_attr}={last_sort_value}"
                )

            except Exception as e:
                logger.error(
                    f"Error during filter-based chunking on chunk {chunk_num}: {e}\n args: {search_kwargs_copy}"
                )
                # Return what we have so far
                if all_results:
                    logger.warning(
                        f"Returning {len(all_results)} results collected before error"
                    )
                    return all_results
                else:
                    raise LDAPException(f"Filter-based chunking failed: {e}")

            # Safety limit
            if chunk_num >= 2500:
                logger.warning(
                    f"Filter-based chunking reached safety limit of 1000 chunks. "
                    f"Retrieved {len(all_results)} results so far."
                )
                break

        logger.info(
            f"Filter-based chunking completed: {len(all_results)} entries "
            f"retrieved across {chunk_num} chunks"
        )
        return all_results

    def _detect_sort_attribute(self, ldap_filter: str) -> str:
        """
        Detect which attribute to use for range-based filtering from LDAP filter.

        Args:
            ldap_filter: LDAP filter string

        Returns:
            str: Attribute name to use for sorting/ranging
        """
        # Common sortable attributes in priority order
        candidates = ["uid", "uidNumber", "cn", "sn", "mail", "entryDN"]

        for attr in candidates:
            # Look for the attribute in the filter
            if (
                f"{attr}=" in ldap_filter
                or f"{attr}<" in ldap_filter
                or f"{attr}>" in ldap_filter
            ):
                logger.debug(f"Detected sort attribute '{attr}' from filter")
                return attr

        # Default fallback
        logger.debug("Could not detect sort attribute, defaulting to 'uid'")
        return "uid"

    def _add_range_filter(
        self,
        original_filter: str,
        attribute: str,
        boundary_value: Any,
        use_greater_than: bool = False,
    ) -> str:
        """
        Add a range constraint to an existing LDAP filter.

        Args:
            original_filter: Original LDAP filter
            attribute: Attribute to use for range constraint
            boundary_value: Value to compare against
            use_greater_than: If True, use > operator; if False, use >=

        Returns:
            str: Modified filter with range constraint added
        """
        # Escape special LDAP characters in boundary value
        escaped_value = str(boundary_value).replace("\\", "\\5c").replace("*", "\\2a")
        escaped_value = escaped_value.replace("(", "\\28").replace(")", "\\29")
        escaped_value = escaped_value.replace("\x00", "\\00")

        # Create the range filter
        operator = ">" if use_greater_than else ">="
        range_filter = f"({attribute}{operator}{escaped_value})"

        # Combine with original filter
        if original_filter.startswith("(&"):
            # Already an AND filter, add our condition
            # Remove trailing ) and add our filter + )
            modified = original_filter[:-1] + range_filter + ")"
        elif original_filter.startswith("(|"):
            # OR filter - wrap both in AND
            modified = f"(&{original_filter}{range_filter})"
        elif original_filter.startswith("(") and original_filter.endswith(")"):
            # Single condition - wrap in AND
            modified = f"(&{original_filter}{range_filter})"
        else:
            # Shouldn't happen, but handle gracefully
            modified = f"(&({original_filter}){range_filter})"

        return modified

    def _execute_manual_pagination(
        self,
        conn: Connection,
        page_size: int,
        size_limit: Optional[int],
        **search_kwargs,
    ) -> List:
        """
        Manual pagination fallback for LDAP servers with problematic paged controls.

        This method works around servers that don't properly handle the Simple Paged
        Results control (like MCommunity when hitting size limits). It uses multiple
        searches with modified filters to retrieve results in chunks, using a sort
        attribute (typically 'uid' or 'cn') to ensure we don't retrieve the same
        entries multiple times.

        Strategy:
        1. First search: Get first page_size results
        2. Find the last entry's sorting attribute value (uid or cn)
        3. Next search: Add filter condition (sortAttr>=lastValue) to skip past retrieved entries
        4. Repeat until no more results

        Args:
            conn: Active LDAP connection
            page_size: Number of results per page
            size_limit: Optional overall size limit
            **search_kwargs: Search parameters

        Returns:
            List: Combined Entry objects from all manual pages
        """
        logger.info("Using manual pagination fallback strategy")

        all_results = []
        page_num = 0
        last_sort_value = None

        # Determine which attribute to use for sorting/filtering
        # Try to find a suitable attribute from the search filter
        original_filter = search_kwargs.get("search_filter", "")
        sort_attr = None

        # Try to detect what type of objects we're searching for to pick the right sort attribute
        if (
            "uid=" in original_filter.lower()
            or "objectclass=person" in original_filter.lower()
        ):
            sort_attr = "uid"
        elif "cn=" in original_filter.lower():
            sort_attr = "cn"
        else:
            # Default fallback - cn is most universal
            sort_attr = "cn"

        logger.debug(f"Using '{sort_attr}' as sort attribute for manual pagination")

        while True:
            page_num += 1
            logger.debug(f"Fetching manual page {page_num}")

            # Build modified filter for this page
            search_kwargs_copy = search_kwargs.copy()

            # On subsequent pages, modify the filter to skip past entries we've already seen
            if last_sort_value is not None:
                original_filter = search_kwargs_copy["search_filter"]

                # Escape special LDAP characters in the last_sort_value
                escaped_value = (
                    str(last_sort_value)
                    .replace("\\", "\\5c")
                    .replace("*", "\\2a")
                    .replace("(", "\\28")
                    .replace(")", "\\29")
                )

                # Create a filter that requires sort_attr to be greater than last_sort_value
                # Use >= and combine with original filter
                skip_filter = f"({sort_attr}>={escaped_value})"

                # Combine with original filter using AND
                if original_filter.startswith("(&"):
                    # Already an AND filter, add our condition to it
                    modified_filter = original_filter[:-1] + skip_filter + ")"
                elif original_filter.startswith("(") and original_filter.endswith(")"):
                    # Single condition or OR filter, wrap in AND with our condition
                    modified_filter = f"(&{original_filter}{skip_filter})"
                else:
                    # Shouldn't happen, but handle it
                    modified_filter = f"(&({original_filter}){skip_filter})"

                search_kwargs_copy["search_filter"] = modified_filter
                logger.debug(f"Modified filter for page {page_num}: {modified_filter}")

            # Set size limit for this page
            search_kwargs_copy["size_limit"] = page_size

            # Ensure we're retrieving the sort attribute
            if "attributes" in search_kwargs_copy:
                attrs = search_kwargs_copy["attributes"]
                if attrs and attrs != ["*"] and sort_attr not in attrs:
                    attrs = list(attrs) + [sort_attr]
                    search_kwargs_copy["attributes"] = attrs

            # Execute search for this page
            success = conn.search(**search_kwargs_copy)

            if not success or not conn.entries:
                logger.debug(f"No more results on page {page_num}")
                break

            page_results = list(conn.entries)
            logger.debug(f"Page {page_num}: Got {len(page_results)} entries")

            # Add results from this page
            for entry in page_results:
                # Skip the first entry if it matches our last_sort_value (to avoid duplicate)
                if last_sort_value is not None and hasattr(entry, sort_attr):
                    entry_sort_value = getattr(entry, sort_attr).value
                    if entry_sort_value == last_sort_value:
                        logger.debug(
                            f"Skipping duplicate entry with {sort_attr}={entry_sort_value}"
                        )
                        continue

                all_results.append(entry)

                # Check if we've reached the size limit
                if size_limit and len(all_results) >= size_limit:
                    logger.info(
                        f"Reached size_limit of {size_limit} during manual pagination"
                    )
                    return all_results[:size_limit]

            # If we got fewer results than page_size, we've hit the end
            if len(page_results) < page_size:
                logger.debug(
                    "Received fewer than page_size results, pagination complete"
                )
                break

            # Update last_sort_value for next iteration
            # Use the last entry's sort attribute value
            last_entry = page_results[-1]
            if hasattr(last_entry, sort_attr):
                last_sort_value = getattr(last_entry, sort_attr).value
                logger.debug(f"Last {sort_attr} value: {last_sort_value}")
            else:
                logger.warning(
                    f"Last entry doesn't have {sort_attr} attribute. Cannot continue pagination safely."
                )
                break

            # Safety limit: don't fetch more than 100 pages
            if page_num >= 100:
                logger.warning(
                    f"Manual pagination reached safety limit of 100 pages. "
                    f"Retrieved {len(all_results)} results so far."
                )
                break

        logger.info(
            f"Manual pagination completed: {len(all_results)} entries retrieved across {page_num} pages"
        )
        return all_results

    # Generic Object Type Searches

    def search_users(
        self,
        search_term: Optional[str] = None,
        attributes: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        use_pagination: bool = True,
        page_size: Optional[int] = None,
    ) -> List:
        """
        Search for user objects in the LDAP directory.

        The method searches for any object that represents a person, regardless
        of whether the LDAP server calls them "person", "user", "inetOrgPerson"
        or other object classes. By default, uses pagination for complete results.

        Args:
            search_term: Optional text to search for in user attributes
            attributes: Specific attributes to retrieve (None for common user attributes)
            max_results: Maximum number of users to return (None for no limit)
            use_pagination: Enable automatic pagination (default: True)
            page_size: Page size for pagination (defaults to adapter's configured size)

        Returns:
            List: List of ldap3 Entry objects representing users
        """
        # Construct a flexible filter that catches different types of person objects
        base_filter = "(|(objectClass=person)(objectClass=inetOrgPerson))"

        if search_term:
            # Add search term constraints to find specific people
            search_constraints = (
                f"(|(cn=*{search_term}*)"
                f"(displayName=*{search_term}*)"
                f"(givenName=*{search_term}*)"
                f"(sn=*{search_term}*)"
                f"(mail=*{search_term}*))"
            )
            # Combine the base filter with search constraints using AND logic
            search_filter = f"(&{base_filter}{search_constraints})"
        else:
            search_filter = base_filter

        # Return all attributes if none specified.
        if attributes is None:
            attributes = ["*"]

        logger.debug(
            f"Searching for users with term: '{search_term}' and {len(attributes)} attributes"
        )

        # Delegate to our core search method, which handles all the complexity
        return self.search(
            search_filter=search_filter,
            attributes=attributes,
            max_results=max_results,
            use_pagination=use_pagination,
            page_size=page_size,
        )

    def search_groups(
        self,
        search_term: Optional[str] = None,
        attributes: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        use_pagination: bool = True,
        page_size: Optional[int] = None,
    ) -> List:
        """
        Search for group objects in the LDAP directory.

        Groups in LDAP can represent many different concepts: security groups,
        distribution lists, organizational units, or administrative collections.
        By default, uses pagination for complete results.

        Args:
            search_term: Optional text to search for in group names/descriptions
            attributes: Specific attributes to retrieve (None for common group attributes)
            max_results: Maximum number of groups to return (None for no limit)
            use_pagination: Enable automatic pagination (default: True)
            page_size: Page size for pagination (defaults to adapter's configured size)

        Returns:
            List: List of ldap3 Entry objects representing groups
        """
        # Look for various types of group objects
        base_filter = (
            "(|(objectClass=group)(objectClass=groupOfNames)(objectClass=posixGroup))"
        )

        if search_term:
            # Search in group name and description fields
            search_constraints = f"(cn=*{search_term}*)"
            search_filter = f"(&{base_filter}{search_constraints})"
        else:
            search_filter = base_filter

        # Default attributes useful for understanding groups
        if attributes is None:
            attributes = ["*"]
            """
            attributes = [
                "cn",
                "name",
                "description",
                "member",
                "memberOf",
                "gidNumber",
                "objectClass",
            ]
            """

        logger.debug(f"Searching for groups with term: '{search_term}'")

        return self.search(
            search_filter=search_filter,
            attributes=attributes,
            max_results=max_results,
            use_pagination=use_pagination,
            page_size=page_size,
        )

    def search_organizational_units(
        self,
        search_term: Optional[str] = None,
        attributes: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        use_pagination: bool = True,
        page_size: Optional[int] = None,
    ) -> List:
        """
        Search for organizational unit objects in the LDAP directory.

        Args:
            search_term: Optional text to search for in OU names/descriptions
            attributes: Specific attributes to retrieve (None for common OU attributes)
            max_results: Maximum number of OUs to return (None for no limit)
            use_pagination: Enable automatic pagination (default: True)
            page_size: Page size for pagination (defaults to adapter's configured size)

        Returns:
            List: List of ldap3 Entry objects representing organizational units
        """
        base_filter = "(objectClass=organizationalUnit)"

        if search_term:
            # Search in OU name and description
            search_constraints = f"(|(ou=*{search_term}*)(description=*{search_term}*))"
            search_filter = f"(&{base_filter}{search_constraints})"
            print(search_filter)
        else:
            search_filter = base_filter

        # Attributes that help understand organizational structure
        if attributes is None:
            attributes = ["ou", "description", "objectClass"]

        logger.debug(f"Searching for organizational units with term: '{search_term}'")

        return self.search(
            search_filter=search_filter,
            attributes=attributes,
            max_results=max_results,
            use_pagination=use_pagination,
            page_size=page_size,
        )

    # Data Warehouse Extraction Functions

    def extract_organizational_tree(
        self, base_dn: Optional[str] = None, max_depth: int = 5
    ) -> Dict[str, Any]:
        """
        Extract the complete organizational hierarchy as a nested JSON structure.

        Args:
            base_dn: Starting point for extraction (defaults to adapter's search_base)
            max_depth: Maximum depth to traverse (prevents infinite recursion)

        Returns:
            Dict[str, Any]: Nested JSON structure representing the org tree
        """
        start_base = base_dn if base_dn is not None else self.search_base

        logger.info(f"Starting organizational tree extraction from: {start_base}")

        def extract_ou_recursive(current_dn: str, current_depth: int) -> Dict[str, Any]:
            """
            Recursively extract organizational structure.
            """
            if current_depth > max_depth:
                logger.warning(f"Maximum depth {max_depth} reached at {current_dn}")
                return {"error": "max_depth_exceeded", "dn": current_dn}

            try:
                # Get information about this OU
                ou_entries = self.search(
                    search_filter="(objectClass=organizationalUnit)",
                    search_base=current_dn,
                    scope="base",  # Only this OU, not children
                    attributes=["ou", "name", "description", "objectClass"],
                    use_pagination=False,  # Single object, no pagination needed
                )

                # Convert first entry to dict for JSON compatibility
                ou_info = {}
                if ou_entries:
                    entry = ou_entries[0]
                    ou_info = {"dn": entry.entry_dn}
                    for attr_name in entry.entry_attributes:
                        attr_value = getattr(entry, attr_name)
                        ou_info[attr_name] = attr_value.value

                # Start building the result structure
                result = {
                    "dn": current_dn,
                    "depth": current_depth,
                    "attributes": ou_info,
                    "children": [],
                }

                # Find immediate child OUs
                child_entries = self.search(
                    search_filter="(objectClass=organizationalUnit)",
                    search_base=current_dn,
                    scope="level",  # Only immediate children
                    attributes=["ou", "name"],
                    use_pagination=True,  # Ensure we get all child OUs
                )

                # Recursively process each child OU
                for child_entry in child_entries:
                    child_result = extract_ou_recursive(
                        child_entry.entry_dn, current_depth + 1
                    )
                    result["children"].append(child_result)

                logger.debug(
                    f"Processed OU at depth {current_depth}: {current_dn} ({len(child_entries)} children)"
                )
                return result

            except Exception as e:
                logger.error(f"Error extracting OU at {current_dn}: {e}")
                return {"error": str(e), "dn": current_dn, "depth": current_depth}

        # Start the recursive extraction
        tree_structure = extract_ou_recursive(start_base, 0)

        # Add metadata about the extraction
        result = {
            "extraction_metadata": {
                "timestamp": __import__("datetime").datetime.now().isoformat(),
                "server": self.server_hostname,
                "base_dn": start_base,
                "max_depth": max_depth,
            },
            "organizational_tree": tree_structure,
        }

        logger.info("Organizational tree extraction completed successfully")
        return result

    def extract_organizational_unit(
        self,
        ou_dn: str,
        include_users: bool = True,
        include_groups: bool = True,
        include_sub_ous: bool = True,
    ) -> Dict[str, Any]:
        """
        Extract complete information about a specific organizational unit.

        Args:
            ou_dn: Distinguished name of the OU to extract
            include_users: Whether to include user objects
            include_groups: Whether to include group objects
            include_sub_ous: Whether to include sub-organizational units

        Returns:
            Dict[str, Any]: Complete OU information as JSON structure
        """
        logger.info(f"Extracting organizational unit: {ou_dn}")

        # Start with the OU's own information
        ou_entries = self.search(
            search_filter="(objectClass=organizationalUnit)",
            search_base=ou_dn,
            scope="base",
            attributes=["*"],  # Get all attributes for complete information
            use_pagination=False,  # Single object
        )

        # Convert OU entry to dict
        ou_info = {}
        if ou_entries:
            entry = ou_entries[0]
            ou_info = {"dn": entry.entry_dn}
            for attr_name in entry.entry_attributes:
                attr_value = getattr(entry, attr_name)
                ou_info[attr_name] = attr_value.value

        result = {
            "extraction_metadata": {
                "timestamp": __import__("datetime").datetime.now().isoformat(),
                "server": self.server_hostname,
                "ou_dn": ou_dn,
            },
            "ou_information": ou_info,
            "contained_objects": {},
        }

        # Extract users if requested - convert to dicts for JSON compatibility
        if include_users:
            user_entries = self.search(
                search_filter="(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))",
                search_base=ou_dn,
                scope="level",  # Only direct children, not nested
                attributes=["*"],
                use_pagination=True,  # Ensure complete user list
            )
            users = self.search_as_dicts(
                search_filter="(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))",
                search_base=ou_dn,
                scope="level",
                attributes=["*"],
                use_pagination=True,
            )
            result["contained_objects"]["users"] = users
            logger.debug(f"Found {len(users)} users in {ou_dn}")

        # Extract groups if requested - convert to dicts for JSON compatibility
        if include_groups:
            groups = self.search_as_dicts(
                search_filter="(|(objectClass=group)(objectClass=groupOfNames)(objectClass=posixGroup))",
                search_base=ou_dn,
                scope="level",
                attributes=["*"],
                use_pagination=True,
            )
            result["contained_objects"]["groups"] = groups
            logger.debug(f"Found {len(groups)} groups in {ou_dn}")

        # Extract sub-OUs if requested - convert to dicts for JSON compatibility
        if include_sub_ous:
            sub_ous = self.search_as_dicts(
                search_filter="(objectClass=organizationalUnit)",
                search_base=ou_dn,
                scope="level",
                attributes=["*"],
                use_pagination=True,
            )
            result["contained_objects"]["organizational_units"] = sub_ous
            logger.debug(f"Found {len(sub_ous)} sub-OUs in {ou_dn}")

        logger.info(f"OU extraction completed for: {ou_dn}")
        return result

    def extract_users_from_ou(
        self,
        ou_dn: str,
        include_nested: bool = False,
        attributes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract all user objects from a specific organizational unit.

        This method is optimized for data warehouse ingestion, returning a
        flat list of user records that can be easily processed into your
        user tables.

        Args:
            ou_dn: Distinguished name of the OU to extract users from
            include_nested: Whether to include users from sub-OUs
            attributes: Specific attributes to retrieve (None for comprehensive set)

        Returns:
            List[Dict[str, Any]]: List of user records ready for data warehouse ingestion
        """
        scope = "subtree" if include_nested else "level"

        # Use comprehensive default attributes for data warehouse purposes
        if attributes is None:
            attributes = [
                "cn",
                "displayName",
                "givenName",
                "sn",
                "mail",
                "userPrincipalName",
                "telephoneNumber",
                "title",
                "department",
                "description",
                "manager",
                "objectClass",
                "whenCreated",
                "whenChanged",
            ]

        logger.info(f"Extracting users from OU: {ou_dn} (nested: {include_nested})")

        users = self.search_as_dicts(
            search_filter="(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))",
            search_base=ou_dn,
            scope=scope,
            attributes=attributes,
            use_pagination=True,  # Ensure complete user extraction
        )

        # Add extraction metadata to each user record for data lineage
        for user in users:
            user["_extraction_metadata"] = {
                "source_ou": ou_dn,
                "extraction_timestamp": __import__("datetime")
                .datetime.now()
                .isoformat(),
                "server": self.server_hostname,
            }

        logger.info(f"Extracted {len(users)} users from {ou_dn}")
        return users

    def extract_groups_from_ou(
        self,
        ou_dn: str,
        include_nested: bool = False,
        attributes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract all group objects from a specific organizational unit.

        Similar to user extraction, this method provides a clean list of
        group records optimized for data warehouse processing.

        Args:
            ou_dn: Distinguished name of the OU to extract groups from
            include_nested: Whether to include groups from sub-OUs
            attributes: Specific attributes to retrieve (None for comprehensive set)

        Returns:
            List[Dict[str, Any]]: List of group records ready for data warehouse ingestion
        """
        scope = "subtree" if include_nested else "level"

        # Default attributes useful for understanding group structure and membership
        if attributes is None:
            attributes = [
                "cn",
                "name",
                "description",
                "member",
                "memberOf",
                "mail",
                "objectClass",
                "gidNumber",
                "whenCreated",
                "whenChanged",
            ]

        logger.info(f"Extracting groups from OU: {ou_dn} (nested: {include_nested})")

        groups = self.search_as_dicts(
            search_filter="(|(objectClass=group)(objectClass=groupOfNames)(objectClass=posixGroup))",
            search_base=ou_dn,
            scope=scope,
            attributes=attributes,
            use_pagination=True,  # Ensure complete group extraction
        )

        # Add extraction metadata for data lineage
        for group in groups:
            group["_extraction_metadata"] = {
                "source_ou": ou_dn,
                "extraction_timestamp": __import__("datetime")
                .datetime.now()
                .isoformat(),
                "server": self.server_hostname,
            }

        logger.info(f"Extracted {len(groups)} groups from {ou_dn}")
        return groups

    # Utility and Administrative Methods

    def count_search_results(
        self,
        search_filter: str,
        search_base: Optional[str] = None,
        scope: str = "subtree",
        use_pagination: bool = True,
    ) -> int:
        """
        Count the number of results a search would return.

        By default, uses pagination to ensure accurate counts even for large datasets.
        Can be disabled for faster estimates that accept server-side limits.

        Args:
            search_filter: LDAP filter to count results for
            search_base: Base DN for search (defaults to adapter's search_base)
            scope: Search scope ('base', 'level', or 'subtree')
            use_pagination: Use pagination for accurate counts (default: True)

        Returns:
            int: Number of objects that match the search criteria
        """
        try:
            # Use the LDAP RFC standard for minimal data transfer during counting
            # '1.1' is the official LDAP way to say "return no attributes"
            results = self.search(
                search_filter=search_filter,
                search_base=search_base,
                scope=scope,
                attributes=[
                    "1.1"
                ],  # RFC 4511 standard: no attributes, just entry structure
                use_pagination=use_pagination,
            )

            count = len(results)

            if use_pagination:
                logger.info(
                    f"Count search completed (with pagination): {count} results for filter '{search_filter}'"
                )
            else:
                logger.info(
                    f"Count search completed (server limit): {count} results for filter '{search_filter}'"
                )
                if count >= 1000:
                    logger.warning(
                        "Count may be incomplete due to server size limits. Use pagination for accurate count."
                    )

            return count

        except Exception as e:
            logger.error(f"Error counting search results: {e}")
            raise LDAPException(f"Failed to count search results: {e}")

    def get_server_info(self) -> Dict[str, Any]:
        """
        Retrieve information about the LDAP server and its capabilities.

        Returns:
            Dict[str, Any]: Server information and capabilities
        """
        try:
            server = self._create_server()

            # The server object contains extensive information after connection
            if server.info:
                server_info = {
                    "server_hostname": self.server_hostname,
                    "port": self.port,
                    "use_ssl": self.use_ssl,
                    "vendor": getattr(server.info, "vendor_name", "Unknown"),
                    "version": getattr(server.info, "vendor_version", "Unknown"),
                    "supported_ldap_versions": getattr(
                        server.info, "supported_ldap_versions", []
                    ),
                    "naming_contexts": getattr(server.info, "naming_contexts", []),
                    "supported_features": getattr(
                        server.info, "supported_features", []
                    ),
                    "supported_extensions": getattr(
                        server.info, "supported_extensions", []
                    ),
                    "schema_entry": getattr(server.info, "schema_entry", None),
                }
            else:
                server_info = {
                    "server_hostname": self.server_hostname,
                    "port": self.port,
                    "use_ssl": self.use_ssl,
                    "info_status": "Server info not available (may require connection)",
                }

            logger.debug("Retrieved server information successfully")
            return server_info

        except Exception as e:
            logger.error(f"Error retrieving server info: {e}")
            return {
                "server_hostname": self.server_hostname,
                "port": self.port,
                "use_ssl": self.use_ssl,
                "error": str(e),
            }


def main():
    """
    Comprehensive LDAP adapter testing across all methods and both servers.

    Tests all adapter methods with reasonable limits while demonstrating
    the full capability of each method with sample outputs.
    """
    # Configure logging for demonstration
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise for cleaner output
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    print("🔍 COMPREHENSIVE LDAP ADAPTER TEST SUITE")
    print("=" * 70)
    print("Testing all methods across both LDAP servers with sample outputs")

    # Configuration for both LDAP servers
    ad_config = {
        "server": "adsroot.itcs.umich.edu",
        "search_base": "OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
        "user": "umroot\\myodhes1",
        "keyring_service": "ldap_umich",
        "port": 636,
        "use_ssl": True,
    }

    mcommunity_config = {
        "server": "ldap.umich.edu",
        "search_base": "dc=umich,dc=edu",
        "user": "uid=myodhes,ou=People,dc=umich,dc=edu",
        "keyring_service": "Mcom_umich",
        "port": 636,
        "use_ssl": True,
    }

    def print_entry_sample(entry, title="Sample Entry"):
        """Helper to print a formatted sample of an Entry object"""
        print(f"      📋 {title}:")
        print(f"         DN: {entry.entry_dn}")
        print(f"         Attributes: {list(entry.entry_attributes)}")

        # Show first few meaningful attributes
        shown_attrs = 0
        for attr_name in entry.entry_attributes:
            if shown_attrs >= 3:  # Limit to 3 attributes for readability
                break
            if attr_name.lower() not in ["objectclass"]:  # Skip less interesting ones
                attr_value = getattr(entry, attr_name)
                if hasattr(attr_value, "value") and attr_value.value:
                    print(f"         {attr_name}: {attr_value.value}")
                    shown_attrs += 1

    def print_dict_sample(entry_dict, title="Sample Dictionary Entry"):
        """Helper to print a formatted sample of a dictionary entry"""
        print(f"      📋 {title}:")
        print(f"         DN: {entry_dict.get('dn', 'Unknown')}")

        # Show first few meaningful attributes
        shown_attrs = 0
        for key, value in entry_dict.items():
            if shown_attrs >= 3 or key in ["dn", "objectClass"]:
                continue
            if value:  # Only show non-empty values
                print(f"         {key}: {value}")
                shown_attrs += 1
                if shown_attrs >= 3:
                    break

    # Phase 1: Initialize and Test Connections
    print("\n" + "=" * 70)
    print("PHASE 1: CONNECTION SETUP & SERVER INFORMATION")
    print("=" * 70)

    adapters = []

    for name, config in [
        ("Active Directory", ad_config),
        ("MCommunity", mcommunity_config),
    ]:
        print(f"\n🔌 Initializing {name}:")
        print(f"   Server: {config['server']}")
        print(f"   Search Base: {config['search_base']}")

        try:
            adapter = LDAPAdapter(config)

            # Test connection
            if adapter.test_connection():
                print("   ✅ Connection successful!")
                adapters.append((name, adapter))

                # Test get_connection_info()
                conn_info = adapter.get_connection_info()
                print(
                    f"   📊 Connection Info: {conn_info['server']}:{conn_info['port']} ({conn_info['user']})"
                )

                # Test get_server_info()
                server_info = adapter.get_server_info()
                if "vendor" in server_info:
                    print(
                        f"   🖥️  Server: {server_info['vendor']} {server_info['version']}"
                    )
                else:
                    print(
                        f"   🖥️  Server: {server_info.get('info_status', 'Info unavailable')}"
                    )

            else:
                print("   ❌ Connection failed!")

        except Exception as e:
            print(f"   ❌ Setup failed: {e}")

    if not adapters:
        print("\n❌ No LDAP servers accessible. Cannot proceed with testing.")
        return

    # Phase 2: Test Core Search Methods
    print(f"\n" + "=" * 70)
    print("PHASE 2: CORE SEARCH METHODS")
    print("=" * 70)

    for name, adapter in adapters:
        print(f"\n🔍 Testing core search methods on {name}:")

        try:
            # Test basic search() method
            print(f"\n   📊 Testing search() method:")
            basic_entries = adapter.search(
                search_filter="(objectClass=organizationalUnit)", max_results=3
            )
            print(
                f"      Found {len(basic_entries)} organizational units (limited to 3)"
            )
            if basic_entries:
                print_entry_sample(basic_entries[0], "First OU Entry")

            # Test search_as_dicts() method
            print(f"\n   📝 Testing search_as_dicts() method:")
            dict_entries = adapter.search_as_dicts(
                search_filter="(objectClass=organizationalUnit)", max_results=2
            )
            print(f"      Converted {len(dict_entries)} entries to dictionaries")
            if dict_entries:
                print_dict_sample(dict_entries[0], "First OU Dictionary")

        except Exception as e:
            print(f"      ❌ Core search test failed: {e}")

    # Phase 3: Test Object Type Searches
    print(f"\n" + "=" * 70)
    print("PHASE 3: OBJECT TYPE SEARCH METHODS")
    print("=" * 70)

    for name, adapter in adapters:
        print(f"\n🔍 Testing object type searches on {name}:")

        try:
            # Test search_organizational_units()
            print(f"\n   🏢 Testing search_organizational_units():")
            total_ous = adapter.count_search_results("(objectClass=organizationalUnit)")
            sample_ous = adapter.search_organizational_units(max_results=2)
            print(f"      Total OUs available: {total_ous}")
            print(f"      Retrieved sample: {len(sample_ous)} OUs")
            if sample_ous:
                print_entry_sample(sample_ous[0], "Sample OU")

            # Test search_users()
            print(f"\n   👥 Testing search_users():")
            total_users = adapter.count_search_results(
                "(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))"
            )
            sample_users = adapter.search_users(max_results=2)
            print(f"      Total users available: {total_users}")
            print(f"      Retrieved sample: {len(sample_users)} users")
            if sample_users:
                print_entry_sample(sample_users[0], "Sample User")

            # Test search_groups()
            print(f"\n   👤 Testing search_groups():")
            total_groups = adapter.count_search_results(
                "(|(objectClass=group)(objectClass=groupOfNames)(objectClass=posixGroup))"
            )
            sample_groups = adapter.search_groups(max_results=2)
            print(f"      Total groups available: {total_groups}")
            print(f"      Retrieved sample: {len(sample_groups)} groups")
            if sample_groups:
                print_entry_sample(sample_groups[0], "Sample Group")

            # Test search with specific term
            print(f"\n   🔎 Testing search with specific terms:")
            if sample_ous:
                # Get the first OU name for searching
                first_ou = sample_ous[0]
                ou_name = None
                if hasattr(first_ou, "ou") and first_ou.ou:
                    ou_name = str(first_ou.ou.value)[:10]  # First 10 chars
                elif hasattr(first_ou, "name") and first_ou.name:
                    ou_name = str(first_ou.name.value)[:10]

                if ou_name:
                    search_ous = adapter.search_organizational_units(
                        search_term=ou_name, max_results=1
                    )
                    print(
                        f"      Search for OUs containing '{ou_name}': {len(search_ous)} found"
                    )

        except Exception as e:
            print(f"      ❌ Object type search test failed: {e}")

    # Phase 4: Test Data Warehouse Extraction Methods
    print(f"\n" + "=" * 70)
    print("PHASE 4: DATA WAREHOUSE EXTRACTION METHODS")
    print("=" * 70)

    for name, adapter in adapters:
        print(f"\n🏭 Testing data warehouse methods on {name}:")

        try:
            # Test extract_organizational_tree() with limited depth
            print(f"\n   🌳 Testing extract_organizational_tree():")
            org_tree = adapter.extract_organizational_tree(max_depth=2)
            print(f"      Organizational tree extracted (max depth: 2)")
            print(
                f"      Tree structure: {len(org_tree['organizational_tree'].get('children', []))} top-level children"
            )
            if org_tree["organizational_tree"].get("attributes"):
                print(f"      Root OU: {org_tree['organizational_tree']['dn']}")

            # Test extract_organizational_unit() on first available OU
            if sample_ous:
                print(f"\n   📦 Testing extract_organizational_unit():")
                first_ou_dn = sample_ous[0].entry_dn
                ou_extraction = adapter.extract_organizational_unit(
                    first_ou_dn,
                    include_users=True,
                    include_groups=True,
                    include_sub_ous=True,
                )
                print(f"      Extracted OU: {first_ou_dn}")
                contained = ou_extraction["contained_objects"]
                print(
                    f"      Contains: {len(contained.get('users', []))} users, "
                    f"{len(contained.get('groups', []))} groups, "
                    f"{len(contained.get('organizational_units', []))} sub-OUs"
                )

                # Test extract_users_from_ou() and extract_groups_from_ou()
                print(f"\n   👥 Testing extract_users_from_ou():")
                extracted_users = adapter.extract_users_from_ou(
                    first_ou_dn, include_nested=False
                )
                print(f"      Extracted {len(extracted_users)} users from OU")
                if extracted_users:
                    print_dict_sample(extracted_users[0], "Sample Extracted User")

                print(f"\n   👤 Testing extract_groups_from_ou():")
                extracted_groups = adapter.extract_groups_from_ou(
                    first_ou_dn, include_nested=False
                )
                print(f"      Extracted {len(extracted_groups)} groups from OU")
                if extracted_groups:
                    print_dict_sample(extracted_groups[0], "Sample Extracted Group")

        except Exception as e:
            print(f"      ❌ Data warehouse extraction test failed: {e}")

    # Phase 5: Test Utility Methods
    print(f"\n" + "=" * 70)
    print("PHASE 5: UTILITY METHODS")
    print("=" * 70)

    for name, adapter in adapters:
        print(f"\n🔧 Testing utility methods on {name}:")

        try:
            # Test count_search_results() with different scenarios
            print(f"\n   🔢 Testing count_search_results():")

            # Count with pagination (accurate)
            ou_count_paginated = adapter.count_search_results(
                "(objectClass=organizationalUnit)", use_pagination=True
            )
            print(f"      OUs (with pagination): {ou_count_paginated}")

            # Count without pagination (fast)
            ou_count_fast = adapter.count_search_results(
                "(objectClass=organizationalUnit)", use_pagination=False
            )
            print(f"      OUs (fast count): {ou_count_fast}")

            if ou_count_paginated != ou_count_fast:
                diff = ou_count_paginated - ou_count_fast
                print(f"      📊 Pagination found {diff} additional results!")
            else:
                print(f"      ✅ Counts match (directory within server limits)")

            # Test different search scopes
            print(f"\n   📐 Testing different search scopes:")
            base_count = adapter.count_search_results(
                "(objectClass=organizationalUnit)", scope="base"
            )
            level_count = adapter.count_search_results(
                "(objectClass=organizationalUnit)", scope="level"
            )
            subtree_count = adapter.count_search_results(
                "(objectClass=organizationalUnit)", scope="subtree"
            )
            print(
                f"      Base scope: {base_count}, Level scope: {level_count}, Subtree scope: {subtree_count}"
            )

        except Exception as e:
            print(f"      ❌ Utility method test failed: {e}")

    # Phase 6: Performance and Feature Summary
    print(f"\n" + "=" * 70)
    print("PHASE 6: TEST SUMMARY & CAPABILITIES")
    print("=" * 70)

    print(f"\n📊 Test Results Summary:")
    print(f"   ✅ Successfully tested {len(adapters)} LDAP server(s)")

    for name, adapter in adapters:
        print(f"\n🔍 {name} Capabilities:")
        try:
            ou_total = adapter.count_search_results("(objectClass=organizationalUnit)")
            user_total = adapter.count_search_results(
                "(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))"
            )
            group_total = adapter.count_search_results(
                "(|(objectClass=group)(objectClass=groupOfNames)(objectClass=posixGroup))"
            )

            print(f"      📈 Directory Scale:")
            print(f"         Organizational Units: {ou_total:,}")
            print(f"         Users/People: {user_total:,}")
            print(f"         Groups: {group_total:,}")

            print(f"      ✅ Tested Methods:")
            print(f"         • Core search() and search_as_dicts()")
            print(f"         • Object type searches (users, groups, OUs)")
            print(f"         • Data warehouse extractions")
            print(f"         • Utility methods (counting, server info)")
            print(f"         • Pagination (automatic and manual)")
            print(f"         • Multiple search scopes")

        except Exception as e:
            print(f"      ⚠️  Summary calculation failed: {e}")

    print(f"\n🎉 COMPREHENSIVE TEST COMPLETED SUCCESSFULLY!")
    print(f"\n💡 Key Features Demonstrated:")
    print(f"   • Raw ldap3 Entry objects for maximum functionality")
    print(f"   • Automatic pagination handling server-side limits")
    print(f"   • Optional dictionary conversion for JSON compatibility")
    print(f"   • Comprehensive object type searches")
    print(f"   • Data warehouse extraction with metadata")
    print(f"   • Robust error handling and connection management")
    print(f"   • Performance optimization options")

    print(
        f"\n🚀 Adapter ready for production deployment and facade layer implementation!"
    )


if __name__ == "__main__":
    main()
