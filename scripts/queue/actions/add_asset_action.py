"""
Add Asset action for automatically associating assets with tickets.

Searches for computer identifiers in ticket title, description, and conversation,
then adds matching assets to the ticket. Falls back to requestor's single asset
if no specific identifiers are found.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

from .base_action import BaseAction

logger = logging.getLogger(__name__)


# Regex patterns for computer name/serial matching
COMPUTER_NAME_PATTERNS = [
    r"\b([A-Za-z]{2,10}[-_][A-Za-z0-9]{4,15}[-_]?[A-Za-z0-9]{0,8})\b",  # IC-EHLB760-F16, CHEM-SMALD1
    r"\b(L-[A-Z0-9]{10,15})\b",  # L-C02XJ0AXJGH5
    r"\b([A-Z]{2,6}-[A-Z0-9]{5,12})\b",  # LSAF-D30H6J3
    r"\b([a-z]{3,10}-[a-z0-9]{4,12}-?[a-z0-9]{0,6})\b",  # psyc-amiemgT01, eng-x-lec12
]

SERIAL_NUMBER_PATTERNS = [
    r"\b([A-Z0-9]{10,15})\b",  # Generic alphanumeric serial
    r"\b([CR][0-9][A-Z0-9]{8,12})\b",  # Apple-style serials (C02..., R8Y...)
]


class AddAssetAction(BaseAction):
    """
    Action that automatically adds assets to tickets.

    Search strategy (priority order):
    1. Extract identifiers from ticket title & description → search & verify → add all matches
    2. If no matches, extract from ticket conversation → search & verify → add all matches
    3. If no matches and ticket has no assets, get requestor's assets → add if exactly 1

    Configuration parameters:
    - add_summary_comment: Add summary to action_context for cumulative comment
    - max_assets_to_add: Safety limit on number of assets to add (default: 10)
    - skip_if_requestor_asset_exists: Skip Phase 3 if ticket already has assets
    - active_status_only: Only search for active assets (default: True)
    - computer_form_id: Filter to specific form ID (default: 2448 - Computer Form)
    - database_url: PostgreSQL connection string for bronze layer queries
    """

    def __init__(
        self,
        add_summary_comment: bool = True,
        max_assets_to_add: int = 10,
        skip_if_requestor_asset_exists: bool = True,
        active_status_only: bool = True,
        computer_form_id: int = 2448,
        database_url: Optional[str] = None,
        version: str = "v1",
        **kwargs,
    ):
        """
        Initialize add asset action.

        Args:
            add_summary_comment: If True, add summary to action_context
            max_assets_to_add: Maximum number of assets to add (safety limit)
            skip_if_requestor_asset_exists: Skip requestor asset if ticket has assets
            active_status_only: Only search active assets (StatusID filter)
            computer_form_id: Filter to specific form (2448 = Computer Form)
            database_url: Optional PostgreSQL URL for database queries
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)

        self.add_summary_comment = add_summary_comment
        self.max_assets_to_add = max_assets_to_add
        self.skip_if_requestor_asset_exists = skip_if_requestor_asset_exists
        self.active_status_only = active_status_only
        self.computer_form_id = computer_form_id
        self.database_url = database_url

        # Initialize database adapter if URL provided
        self.db_adapter = None
        if database_url:
            try:
                self.db_adapter = PostgresAdapter(database_url)
                logger.info("Database adapter initialized for asset search")
            except Exception as e:
                logger.warning(f"Failed to initialize database adapter: {e}")

    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "add_asset"

    def get_action_config(self) -> Dict[str, Any]:
        """Get configuration for content hashing."""
        return {
            "add_summary_comment": self.add_summary_comment,
            "max_assets_to_add": self.max_assets_to_add,
            "skip_if_requestor_asset_exists": self.skip_if_requestor_asset_exists,
            "active_status_only": self.active_status_only,
            "computer_form_id": self.computer_form_id,
            "has_database": self.database_url is not None,
        }

    def _extract_identifiers(self, text: str) -> Set[str]:
        """
        Extract computer names and serial numbers from text using regex.

        Args:
            text: Text to search for identifiers

        Returns:
            Set of unique identifiers found
        """
        if not text:
            return set()

        identifiers = set()

        # Apply all computer name patterns
        for pattern in COMPUTER_NAME_PATTERNS:
            matches = re.findall(pattern, text)
            identifiers.update(matches)

        # Apply all serial number patterns
        for pattern in SERIAL_NUMBER_PATTERNS:
            matches = re.findall(pattern, text)
            identifiers.update(matches)

        return identifiers

    def _search_computers_database(self, identifiers: Set[str]) -> List[Dict[str, Any]]:
        """
        Search bronze layer database for computers matching identifiers.

        Args:
            identifiers: Set of computer names/serials to search for

        Returns:
            List of matching computer records from database
        """
        if not self.db_adapter or not identifiers:
            return []

        try:
            # Normalize identifiers to lowercase for matching
            normalized_ids = [id.lower() for id in identifiers]

            # Build SQL query using OR conditions (SQLAlchemy compatible)
            # Create named parameters for each identifier
            name_conditions = " OR ".join(
                [
                    f"LOWER(raw_data->>'Name') = :id_{i}"
                    for i in range(len(normalized_ids))
                ]
            )
            serial_conditions = " OR ".join(
                [
                    f"LOWER(raw_data->>'SerialNumber') = :id_{i}"
                    for i in range(len(normalized_ids))
                ]
            )

            query = f"""
                SELECT
                    raw_data->>'ID' as id,
                    raw_data->>'Name' as name,
                    raw_data->>'SerialNumber' as serial_number,
                    raw_data
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                AND source_system = 'tdx'
                AND ({name_conditions} OR {serial_conditions})
            """

            # Build parameter dict: {id_0: 'psyc-gid08', id_1: 'mac-ward04', ...}
            params = {
                f"id_{i}": normalized_id
                for i, normalized_id in enumerate(normalized_ids)
            }

            df = self.db_adapter.query_to_dataframe(query, params)

            if df.empty:
                logger.debug("No computers found in database")
                return []

            # Convert to list of dicts
            results = df.to_dict("records")
            logger.info(f"Found {len(results)} computers in database")
            return results

        except Exception as e:
            logger.warning(f"Database search failed: {e}")
            return []

    def _search_computers_api(
        self, identifiers: Set[str], facade: TeamDynamixFacade
    ) -> List[Dict[str, Any]]:
        """
        Search TDX API for computers matching identifiers.

        Uses both SerialLike and SearchText parameters for comprehensive search.

        Args:
            identifiers: Set of computer names/serials to search for
            facade: TeamDynamixFacade for API access

        Returns:
            List of asset dictionaries from TDX API
        """
        if not identifiers:
            return []

        all_assets = []
        seen_ids = set()

        for identifier in identifiers:
            try:
                # Search using SerialLike (searches Serial and Tag fields)
                search_params = {"SerialLike": identifier}

                # Add status filter if configured
                if self.active_status_only:
                    search_params["StatusIDs"] = [38]  # 38 = Active status

                # Add form filter
                if self.computer_form_id:
                    search_params["FormIDs"] = [self.computer_form_id]

                logger.debug(f"Searching for '{identifier}' via SerialLike")
                serial_results = facade.assets.search_asset(search_params)

                # Also search using SearchText (full-text search)
                search_params["SearchText"] = identifier
                del search_params["SerialLike"]

                logger.debug(f"Searching for '{identifier}' via SearchText")
                text_results = facade.assets.search_asset(search_params)

                # Combine results, avoiding duplicates
                for asset in serial_results + text_results:
                    asset_id = asset.get("ID")
                    if asset_id and asset_id not in seen_ids:
                        all_assets.append(asset)
                        seen_ids.add(asset_id)

            except Exception as e:
                logger.warning(f"API search failed for '{identifier}': {e}")
                continue

        logger.info(f"Found {len(all_assets)} assets via API (before verification)")
        return all_assets

    def _verify_asset_match(
        self, asset: Dict[str, Any], search_terms: Set[str]
    ) -> bool:
        """
        Verify that asset Name or SerialNumber exactly matches a search term.

        This prevents false positives from fuzzy TDX searches.

        Args:
            asset: Asset dict from API/database
            search_terms: Set of identifiers we searched for

        Returns:
            True if asset name/serial matches a search term (normalized)
        """
        asset_name = (asset.get("Name") or "").strip().lower()
        asset_serial = (asset.get("SerialNumber") or "").strip().lower()
        asset_tag = (asset.get("Tag") or "").strip().lower()

        # Normalize search terms
        normalized_terms = {term.strip().lower() for term in search_terms}

        # Check for exact match
        if asset_name in normalized_terms:
            logger.debug(f"Asset {asset.get('ID')} verified by Name: {asset_name}")
            return True

        if asset_serial and asset_serial in normalized_terms:
            logger.debug(
                f"Asset {asset.get('ID')} verified by SerialNumber: {asset_serial}"
            )
            return True

        if asset_tag and asset_tag in normalized_terms:
            logger.debug(f"Asset {asset.get('ID')} verified by Tag: {asset_tag}")
            return True

        logger.debug(
            f"Asset {asset.get('ID')} ({asset_name}) does not match any search term"
        )
        return False

    def _add_assets_to_ticket(
        self,
        ticket_id: int,
        assets: List[Dict[str, Any]],
        facade: TeamDynamixFacade,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Add multiple assets to a ticket.

        Args:
            ticket_id: Ticket ID to add assets to
            assets: List of asset dicts to add
            facade: TeamDynamixFacade for API access
            dry_run: If True, simulate without making changes

        Returns:
            Dict with success status, added assets, and errors
        """
        added_assets = []
        failed_assets = []
        retryable_errors = []

        for asset in assets[: self.max_assets_to_add]:
            asset_id = asset.get("ID")
            asset_name = asset.get("Name", "Unknown")

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would add asset {asset_id} ({asset_name}) to ticket {ticket_id}"
                )
                added_assets.append(asset)
                continue

            try:
                logger.info(
                    f"Adding asset {asset_id} ({asset_name}) to ticket {ticket_id}"
                )
                result = facade.tickets.add_ticket_asset(
                    id=ticket_id, asset_id=asset_id
                )

                # TDX API sometimes returns None/empty on success
                # Verify by checking if asset is now on the ticket
                ticket_assets = facade.tickets.get_ticket_assets(ticket_id) or []

                # Assets on tickets are represented as CIs (Configuration Items)
                # The asset ID is in the BackingItemID field, not ID field
                backing_item_ids_on_ticket = {
                    a.get("BackingItemID")
                    for a in ticket_assets
                    if a.get("BackingItemType") == 27  # 27 = Asset type
                }

                if asset_id in backing_item_ids_on_ticket:
                    added_assets.append(asset)
                    logger.info(f"Successfully added asset {asset_id} (verified)")
                else:
                    # Asset not on ticket - likely a real failure
                    failed_assets.append(asset)
                    logger.warning(
                        f"Failed to add asset {asset_id} (not found on ticket after add)"
                    )

            except Exception as e:
                error_str = str(e).lower()

                # Check if error is retryable (5xx, timeout, network issues)
                if any(
                    x in error_str
                    for x in ["500", "502", "503", "504", "timeout", "network"]
                ):
                    logger.error(f"Retryable error adding asset {asset_id}: {e}")
                    retryable_errors.append((asset, str(e)))
                else:
                    # Non-retryable error (404, 400, etc.) - log and continue
                    logger.warning(f"Non-retryable error adding asset {asset_id}: {e}")
                    failed_assets.append(asset)

        return {
            "added": added_assets,
            "failed": failed_assets,
            "retryable_errors": retryable_errors,
        }

    def execute_action(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
        action_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the add asset action on a ticket.

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance for API operations
            dry_run: If True, simulate without making changes
            action_context: Optional context dict for cumulative summaries

        Returns:
            Execution result dictionary with success status and details
        """
        try:
            # Get ticket details
            ticket = facade.tickets.get_ticket(ticket_id)
            if not ticket:
                return {
                    "success": False,
                    "message": f"Ticket {ticket_id} not found",
                }

            ticket_title = ticket.get("Title", "")
            ticket_description = ticket.get("Description", "")
            requestor_uid = ticket.get("RequestorUid", "")

            logger.info(f"Processing ticket {ticket_id}: '{ticket_title}'")

            # Get existing assets on ticket
            existing_assets = facade.tickets.get_ticket_assets(ticket_id) or []
            existing_asset_ids = {asset.get("ID") for asset in existing_assets}

            logger.info(f"Ticket currently has {len(existing_assets)} asset(s)")

            # ============================================================
            # PHASE 1: Search Title & Description
            # ============================================================
            identifiers = self._extract_identifiers(
                f"{ticket_title} {ticket_description}"
            )

            if identifiers:
                logger.info(
                    f"Phase 1: Found {len(identifiers)} identifier(s) in title/description: {identifiers}"
                )

                # Try database search first
                db_results = self._search_computers_database(identifiers)

                # Fall back to API if database yields no results
                if not db_results:
                    api_results = self._search_computers_api(identifiers, facade)
                else:
                    # Convert database results to asset format
                    api_results = [record.get("raw_data", {}) for record in db_results]

                # Verify matches
                verified_assets = [
                    asset
                    for asset in api_results
                    if self._verify_asset_match(asset, identifiers)
                ]

                # Filter out already-added assets
                new_assets = [
                    asset
                    for asset in verified_assets
                    if asset.get("ID") not in existing_asset_ids
                ]

                if new_assets:
                    logger.info(
                        f"Phase 1: Verified {len(new_assets)} new asset(s) to add"
                    )

                    # Add assets to ticket
                    add_result = self._add_assets_to_ticket(
                        ticket_id, new_assets, facade, dry_run
                    )

                    # Check for retryable errors
                    if add_result["retryable_errors"]:
                        return {
                            "success": False,
                            "message": f"Retryable errors occurred, will retry next run",
                            "details": add_result,
                        }

                    # Generate summary
                    added_names = [
                        asset.get("Name", "Unknown") for asset in add_result["added"]
                    ]
                    found_ids = ", ".join(list(identifiers)[:3])
                    if len(identifiers) > 3:
                        found_ids += f" (and {len(identifiers) - 3} more)"

                    summary = (
                        f"Added {len(added_names)} asset(s): {', '.join(added_names)}. "
                        f"Reason: Computer identifier(s) found in ticket title/description ({found_ids})"
                    )

                    if add_result["failed"]:
                        summary += (
                            f" (Failed to add {len(add_result['failed'])} asset(s))"
                        )

                    return {
                        "success": True,
                        "message": f"Added {len(add_result['added'])} asset(s) from Phase 1",
                        "summary": summary if self.add_summary_comment else "",
                        "details": {
                            "phase": 1,
                            "identifiers_found": list(identifiers),
                            "assets_added": len(add_result["added"]),
                            "assets_failed": len(add_result["failed"]),
                            "asset_names": added_names,
                        },
                    }

            # ============================================================
            # PHASE 2: Search Conversation/Feed
            # ============================================================
            logger.info(
                "Phase 2: No verified matches in title/description, searching conversation"
            )

            try:
                conversation = facade.get_conversation(ticket_id, exclude_system=True)
                conversation_text = " ".join(
                    [msg.get("message", "") for msg in conversation]
                )

                conv_identifiers = self._extract_identifiers(conversation_text)

                if conv_identifiers:
                    logger.info(
                        f"Phase 2: Found {len(conv_identifiers)} identifier(s) in conversation: {conv_identifiers}"
                    )

                    # Search database/API
                    db_results = self._search_computers_database(conv_identifiers)
                    if not db_results:
                        api_results = self._search_computers_api(
                            conv_identifiers, facade
                        )
                    else:
                        api_results = [
                            record.get("raw_data", {}) for record in db_results
                        ]

                    # Verify and filter
                    verified_assets = [
                        asset
                        for asset in api_results
                        if self._verify_asset_match(asset, conv_identifiers)
                    ]

                    new_assets = [
                        asset
                        for asset in verified_assets
                        if asset.get("ID") not in existing_asset_ids
                    ]

                    if new_assets:
                        logger.info(
                            f"Phase 2: Verified {len(new_assets)} new asset(s) to add"
                        )

                        add_result = self._add_assets_to_ticket(
                            ticket_id, new_assets, facade, dry_run
                        )

                        if add_result["retryable_errors"]:
                            return {
                                "success": False,
                                "message": f"Retryable errors occurred, will retry next run",
                                "details": add_result,
                            }

                        added_names = [
                            asset.get("Name", "Unknown")
                            for asset in add_result["added"]
                        ]
                        found_ids = ", ".join(list(conv_identifiers)[:3])
                        if len(conv_identifiers) > 3:
                            found_ids += f" (and {len(conv_identifiers) - 3} more)"

                        summary = (
                            f"Added {len(added_names)} asset(s): {', '.join(added_names)}. "
                            f"Reason: Computer identifier(s) found in ticket conversation ({found_ids})"
                        )

                        if add_result["failed"]:
                            summary += (
                                f" (Failed to add {len(add_result['failed'])} asset(s))"
                            )

                        return {
                            "success": True,
                            "message": f"Added {len(add_result['added'])} asset(s) from Phase 2",
                            "summary": summary if self.add_summary_comment else "",
                            "details": {
                                "phase": 2,
                                "identifiers_found": list(conv_identifiers),
                                "assets_added": len(add_result["added"]),
                                "assets_failed": len(add_result["failed"]),
                                "asset_names": added_names,
                            },
                        }

            except Exception as e:
                logger.warning(f"Failed to search conversation: {e}")

            # ============================================================
            # PHASE 3: Requestor's Single Asset Fallback
            # ============================================================
            logger.info("Phase 3: No identifiers found, checking requestor's assets")

            # Skip if ticket already has assets
            if existing_assets and self.skip_if_requestor_asset_exists:
                summary = "No assets added. Reason: No computer identifiers found and ticket already has assets"
                logger.info(summary)
                return {
                    "success": True,
                    "message": "No action needed - ticket already has assets",
                    "summary": summary if self.add_summary_comment else "",
                    "details": {"phase": 3, "skipped": True},
                }

            # Get requestor's assets
            if not requestor_uid:
                summary = "No assets added. Reason: No computer identifiers found and no requestor UID available"
                logger.warning(summary)
                return {
                    "success": True,
                    "message": "No requestor UID available",
                    "summary": summary if self.add_summary_comment else "",
                    "details": {"phase": 3, "no_requestor": True},
                }

            try:
                requestor_assets = facade.assets.get_assets(
                    [requestor_uid], search_by="owner"
                )

                # Filter to computer form if configured
                if self.computer_form_id:
                    requestor_assets = [
                        asset
                        for asset in requestor_assets
                        if asset.get("FormID") == self.computer_form_id
                    ]

                logger.info(f"Requestor has {len(requestor_assets)} computer asset(s)")

                # Only add if exactly 1 asset
                if len(requestor_assets) == 1:
                    asset = requestor_assets[0]

                    # Check if already on ticket
                    if asset.get("ID") in existing_asset_ids:
                        summary = "No assets added. Reason: Requestor's only asset already on ticket"
                        logger.info(summary)
                        return {
                            "success": True,
                            "message": "Asset already on ticket",
                            "summary": summary if self.add_summary_comment else "",
                            "details": {"phase": 3, "already_added": True},
                        }

                    # Add the asset
                    add_result = self._add_assets_to_ticket(
                        ticket_id, [asset], facade, dry_run
                    )

                    if add_result["retryable_errors"]:
                        return {
                            "success": False,
                            "message": f"Retryable errors occurred, will retry next run",
                            "details": add_result,
                        }

                    if add_result["added"]:
                        asset_name = asset.get("Name", "Unknown")
                        summary = f"Added 1 asset: {asset_name}. Reason: User only has one computer"

                        return {
                            "success": True,
                            "message": f"Added requestor's only asset",
                            "summary": summary if self.add_summary_comment else "",
                            "details": {
                                "phase": 3,
                                "asset_name": asset_name,
                            },
                        }
                    else:
                        return {
                            "success": False,
                            "message": "Failed to add requestor's asset",
                            "details": add_result,
                        }

                else:
                    summary = f"No assets added. Reason: No computer identifiers found and user has {len(requestor_assets)} assets (expected exactly 1)"
                    logger.info(summary)
                    return {
                        "success": True,
                        "message": f"Requestor has {len(requestor_assets)} assets, not 1",
                        "summary": summary if self.add_summary_comment else "",
                        "details": {
                            "phase": 3,
                            "requestor_asset_count": len(requestor_assets),
                        },
                    }

            except Exception as e:
                logger.exception(f"Failed to get requestor assets: {e}")
                summary = f"No assets added. Reason: Error retrieving requestor's assets ({str(e)})"
                return {
                    "success": True,  # Don't fail the action, just log
                    "message": f"Error getting requestor assets: {str(e)}",
                    "summary": summary if self.add_summary_comment else "",
                    "details": {"phase": 3, "error": str(e)},
                }

        except Exception as e:
            logger.exception(f"Unexpected error in AddAssetAction: {e}")
            return {
                "success": False,
                "message": f"Unexpected error: {str(e)}",
                "details": {"error": str(e), "error_type": type(e).__name__},
            }

    def __repr__(self) -> str:
        """String representation of the add asset action."""
        return (
            f"AddAssetAction(action_id='{self.get_action_id()}', "
            f"max_assets={self.max_assets_to_add})"
        )
