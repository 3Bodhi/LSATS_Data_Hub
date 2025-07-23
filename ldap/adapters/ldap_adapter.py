"""
LDAP Connection Adapter for LSATS Data Hub

This module provides a flexible LDAP adapter that follows the established
adapter-facade-service pattern used throughout the LSATS Data Hub project.
"""

import os
import keyring
import getpass
from typing import Dict, Any, Optional, List, Union
from ldap3 import Server, Connection, ALL, SUBTREE, LEVEL
from ldap3.core.exceptions import LDAPException
import logging

# Set up logging to match existing LSATS patterns
logger = logging.getLogger(__name__)


class LDAPAdapter:
    """
    LDAP connection adapter providing standardized LDAP operations.

    This class handles LDAP server connections, authentication, and basic
    query operations.
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

        Raises:
            ValueError: If required configuration keys are missing
            TypeError: If configuration is not a dictionary
        """
        if not isinstance(config, dict):
            raise TypeError("Configuration must be a dictionary")

        # Validate required configuration keys
        required_keys = ['server', 'search_base', 'user', 'keyring_service']
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")

        # Store core configuration
        self.server_hostname = config['server']
        self.search_base = config['search_base']
        self.user = config['user']
        self.keyring_service = config['keyring_service']

        # Set defaults for optional configuration
        self.use_ssl = config.get('use_ssl', True)
        self.port = config.get('port', 636 if self.use_ssl else 389)
        self.timeout = config.get('timeout', 30)
        self.auto_bind = config.get('auto_bind', True)
        self.get_info = config.get('get_info', ALL)

        # Store additional configuration for extensibility
        self.additional_config = {k: v for k, v in config.items()
                                if k not in required_keys +
                                ['port', 'use_ssl', 'timeout', 'auto_bind', 'get_info']}

        # Initialize connection objects (will be created on first use)
        self._server = None
        self._connection = None
        self._password = None

        logger.debug(f"LDAP adapter initialized for server: {self.server_hostname}")

    def _get_password(self) -> str:
        """
        Retrieve password from keyring or prompt user.

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
                save_password = input("Save password to keyring? (y/n): ").lower().strip()
                if save_password == 'y':
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
                    connect_timeout=self.timeout
                )
                logger.debug(f"LDAP server object created: {self.server_hostname}:{self.port}")
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
                server,
                user=self.user,
                password=password,
                auto_bind=self.auto_bind
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
            search_filter = '(objectClass=organizationalUnit)'

            logger.debug(f"Testing connection with search at base: {self.search_base}")
            logger.debug(f"Search filter: {search_filter}")

            success = conn.search(
                search_base=self.search_base,
                search_filter=search_filter,
                search_scope=LEVEL,  # Only search immediate children
                attributes=['ou', 'description'],  # Minimal attributes
                size_limit=10  # Limit results for testing
            )

            if success:
                result_count = len(conn.entries)
                logger.info(f"Connection test successful: found {result_count} organizational units")
                for index, entry in enumerate(conn.entries):
                    logger.info(f"entry {index}:{entry}")
                logger.debug(f"Search result: {conn.result}")

                # Log some sample results for debugging
                if conn.entries:
                    for i, entry in enumerate(conn.entries[:3]):  # Show first 3 entries
                        logger.debug(f"Sample entry {i+1}: {entry.entry_dn}")

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
            if hasattr(self, '_connection') and self._connection:
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
            'server': self.server_hostname,
            'port': self.port,
            'use_ssl': self.use_ssl,
            'search_base': self.search_base,
            'user': self.user,
            'keyring_service': self.keyring_service,
            'timeout': self.timeout,
            'additional_config': self.additional_config
        }

    def __str__(self) -> str:
        """String representation of the LDAP adapter."""
        ssl_status = "SSL" if self.use_ssl else "non-SSL"
        return f"LDAPAdapter({self.server_hostname}:{self.port}, {ssl_status}, user={self.user})"

    def __repr__(self) -> str:
        """Detailed string representation for debugging."""
        return (f"LDAPAdapter(server='{self.server_hostname}', port={self.port}, "
                f"use_ssl={self.use_ssl}, search_base='{self.search_base}', "
                f"user='{self.user}', keyring_service='{self.keyring_service}')")


def main():
    """
    Main function to test LDAP adapter with both AD and MCommunity servers.

    This function demonstrates how to use the LDAPAdapter class by creating
    connections to both Active Directory and MCommunity LDAP servers, then
    verifying that both connections work properly.
    """
    # Configure logging for demonstration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("LDAP Adapter Connection Test")
    print("=" * 40)

    # Active Directory LDAP Configuration
    ad_config = {
        'server': 'adsroot.itcs.umich.edu',
        'search_base': 'OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu',
        'user': 'umroot\\myodhes1',
        'keyring_service': 'ldap_umich',
        'port': 636,
        'use_ssl': True
    }

    # MCommunity LDAP Configuration
    mcommunity_config = {
        'server': 'ldap.umich.edu',
        'search_base': 'dc=umich,dc=edu',
        'user': 'uid=myodhes,ou=People,dc=umich,dc=edu',
        'keyring_service': 'Mcom_umich',
        'port': 636,
        'use_ssl': True
    }

    # Test Active Directory connection
    print("\n1. Testing Active Directory Connection:")
    print(f"   Server: {ad_config['server']}")
    print(f"   Search Base: {ad_config['search_base']}")

    try:
        ad_adapter = LDAPAdapter(ad_config)
        print(f"   Adapter created: {ad_adapter}")

        if ad_adapter.test_connection():
            print("   ✅ Active Directory connection successful!")
        else:
            print("   ❌ Active Directory connection failed!")

    except Exception as e:
        print(f"   ❌ Active Directory setup failed: {e}")

    # Test MCommunity connection
    print("\n2. Testing MCommunity Connection:")
    print(f"   Server: {mcommunity_config['server']}")
    print(f"   Search Base: {mcommunity_config['search_base']}")

    try:
        mcommunity_adapter = LDAPAdapter(mcommunity_config)
        print(f"   Adapter created: {mcommunity_adapter}")

        if mcommunity_adapter.test_connection():
            print("   ✅ MCommunity connection successful!")
        else:
            print("   ❌ MCommunity connection failed!")

    except Exception as e:
        print(f"   ❌ MCommunity setup failed: {e}")

    print("\nConnection testing complete!")
    print("\nNext steps:")
    print("- Add query methods to the LDAPAdapter class")
    print("- Implement specific search operations for your use cases")
    print("- Integrate with existing LSATS Data Hub workflows")


if __name__ == "__main__":
    main()
