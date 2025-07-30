"""
LDAP Facade for LSATS Data Hub

This facade provides orchestrated access to multiple LDAP connections,
following the adapter-facade-service architecture pattern used throughout
the LSATS Data Hub project.
"""

from typing import Dict, Any, Optional
import logging
from ..adapters.ldap_adapter import LDAPAdapter

logger = logging.getLogger(__name__)


class LDAPFacade:
    """
    LDAP Facade providing orchestrated access to multiple LDAP connections.

    This facade manages both Active Directory and MCommunity LDAP connections,
    allowing users to:
    1. Access individual adapters directly via attributes
    2. Execute methods on both adapters simultaneously via the both() method

    The facade requires both connections to be active and will fail if either
    cannot be established, as it's designed for operations requiring both sources.
    """

    def __init__(self, ad_config: Dict[str, Any], mcommunity_config: Dict[str, Any]) -> None:
        """
        Initialize LDAP facade with both Active Directory and MCommunity connections.

        Args:
            ad_config (Dict[str, Any]): Configuration dictionary for Active Directory
            mcommunity_config (Dict[str, Any]): Configuration dictionary for MCommunity

        Raises:
            Exception: If either LDAP connection cannot be established
        """
        logger.info("Initializing LDAP Facade with dual connections")

        # Initialize both adapters as attributes (following TeamDynamix pattern)
        try:
            logger.debug("Creating Active Directory adapter")
            self.active_directory = LDAPAdapter(ad_config)

            logger.debug("Creating MCommunity adapter")
            self.mcommunity = LDAPAdapter(mcommunity_config)

            # Activate and test both connections - fail if either fails
            self._activate_connections()

            logger.info("✅ LDAP Facade successfully initialized with both connections active")

        except Exception as e:
            logger.error(f"❌ Failed to initialize LDAP Facade: {e}")
            raise

    def _activate_connections(self) -> None:
        """
        Test both LDAP connections and fail if either cannot connect.

        This method ensures both connections are working before the facade
        becomes available for use.

        Raises:
            Exception: If either connection test fails
        """
        logger.debug("Testing Active Directory connection...")
        if not self.active_directory.test_connection():
            raise Exception("Failed to establish Active Directory connection")
        logger.debug("✅ Active Directory connection successful")

        logger.debug("Testing MCommunity connection...")
        if not self.mcommunity.test_connection():
            raise Exception("Failed to establish MCommunity connection")
        logger.debug("✅ MCommunity connection successful")

    def both(self, method_name: str, *args, **kwargs) -> Dict[str, Any]:
        """
        Execute any adapter method on both LDAP connections simultaneously.

        This method provides orchestrated access to both LDAP adapters by calling
        the same method with the same arguments on both connections and returning
        both results separately.

        Args:
            method_name (str): Name of the adapter method to call
            *args: Positional arguments to pass to the method
            **kwargs: Keyword arguments to pass to the method

        Returns:
            Dict[str, Any]: Results from both connections with keys:
                - 'active_directory': Result from Active Directory adapter
                - 'mcommunity': Result from MCommunity adapter

        Raises:
            AttributeError: If the method doesn't exist on one or both adapters

        Examples:
            >>> facade.both('search', '(objectClass=user)')
            {
                'active_directory': [<AD user entries>],
                'mcommunity': [<MCommunity user entries>]
            }

            >>> facade.both('count_search_results', '(objectClass=organizationalUnit)')
            {
                'active_directory': 15,
                'mcommunity': 23
            }
        """
        logger.debug(f"Executing '{method_name}' on both LDAP adapters")

        # Validate method exists on both adapters
        ad_method = getattr(self.active_directory, method_name, None)
        mc_method = getattr(self.mcommunity, method_name, None)

        if ad_method is None:
            raise AttributeError(f"Method '{method_name}' not found on Active Directory adapter")
        if mc_method is None:
            raise AttributeError(f"Method '{method_name}' not found on MCommunity adapter")

        if not callable(ad_method) or not callable(mc_method):
            raise AttributeError(f"'{method_name}' is not a callable method on one or both adapters")

        results = {}

        # Execute on Active Directory adapter
        try:
            logger.debug(f"Calling {method_name} on Active Directory")
            results['active_directory'] = ad_method(*args, **kwargs)
            logger.debug(f"Active Directory {method_name} completed successfully")
        except Exception as e:
            logger.warning(f"Active Directory {method_name} failed: {e}")
            results['active_directory'] = {
                'error': f"Active Directory method failed: {str(e)}",
                'exception': e
            }

        # Execute on MCommunity adapter
        try:
            logger.debug(f"Calling {method_name} on MCommunity")
            results['mcommunity'] = mc_method(*args, **kwargs)
            logger.debug(f"MCommunity {method_name} completed successfully")
        except Exception as e:
            logger.warning(f"MCommunity {method_name} failed: {e}")
            results['mcommunity'] = {
                'error': f"MCommunity method failed: {str(e)}",
                'exception': e
            }

        return results

    def get_connection_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get connection information for both LDAP adapters.

        Returns:
            Dict[str, Dict[str, Any]]: Connection details for both adapters
        """
        return {
            'active_directory': {
                'server': self.active_directory.server_hostname,
                'port': self.active_directory.port,
                'search_base': self.active_directory.search_base,
                'user': self.active_directory.user,
                'use_ssl': self.active_directory.use_ssl
            },
            'mcommunity': {
                'server': self.mcommunity.server_hostname,
                'port': self.mcommunity.port,
                'search_base': self.mcommunity.search_base,
                'user': self.mcommunity.user,
                'use_ssl': self.mcommunity.use_ssl
            }
        }

    def close_connections(self) -> None:
        """
        Close both LDAP connections.

        This method should be called when the facade is no longer needed
        to properly clean up the connections.
        """
        logger.info("Closing LDAP Facade connections")

        try:
            if hasattr(self.active_directory, 'connection') and self.active_directory.connection:
                self.active_directory.connection.unbind()
                logger.debug("Active Directory connection closed")
        except Exception as e:
            logger.warning(f"Error closing Active Directory connection: {e}")

        try:
            if hasattr(self.mcommunity, 'connection') and self.mcommunity.connection:
                self.mcommunity.connection.unbind()
                logger.debug("MCommunity connection closed")
        except Exception as e:
            logger.warning(f"Error closing MCommunity connection: {e}")

        logger.info("LDAP Facade connections closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - closes connections."""
        self.close_connections()
