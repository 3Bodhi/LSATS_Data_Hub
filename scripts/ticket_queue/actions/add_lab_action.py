"""
Add Lab action for automatically associating lab CIs with tickets.

Detects labs based on:
1. Requestor membership in labs
2. Computer assets belonging to labs

Associates the lab Configuration Item with the ticket for proper tracking.
"""

import logging
from typing import Any, Dict, List, Optional, Set

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

from .base_action import BaseAction

logger = logging.getLogger(__name__)


class AddLabAction(BaseAction):
    """
    Action that automatically adds lab CIs to tickets.

    Detection strategy (priority order):
    1. Check if ticket assets belong to labs → Add single lab (PRIORITY)
    2. Check if requestor is a lab member → Add single lab (FALLBACK)
    3. Skip if no labs detected or lab already exists

    IMPORTANT: Only ONE lab is added per ticket, based on priority order.

    Configuration parameters:
    - database_url: PostgreSQL connection string for lab lookups (required)
    - add_summary_comment: Add summary to action_context for cumulative comment
    - skip_if_lab_exists: Skip if ticket already has any lab CI
    - lab_selection_strategy: Strategy for selecting lab when multiple found
    """

    # Lab form ID in TDX (Research Lab form)
    LAB_FORM_ID = 3830

    def __init__(
        self,
        database_url: str,
        add_summary_comment: bool = True,
        skip_if_lab_exists: bool = True,
        lab_selection_strategy: str = "asset_first",
        version: str = "v2",
        **kwargs,
    ):
        """
        Initialize add lab action.

        Args:
            database_url: PostgreSQL connection string for lab queries
            add_summary_comment: If True, add summary to action_context
            skip_if_lab_exists: If True, skip if ticket already has lab CI
            lab_selection_strategy: Strategy for selecting lab when multiple found
                - "asset_first" (default): Prefer asset-based lab, fall back to requestor
                - "most_common": Choose lab that appears most frequently in results
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)

        self.database_url = database_url
        self.add_summary_comment = add_summary_comment
        self.skip_if_lab_exists = skip_if_lab_exists
        self.lab_selection_strategy = lab_selection_strategy

        # Initialize database adapter
        self.db_adapter = None
        if database_url:
            try:
                self.db_adapter = PostgresAdapter(database_url)
                logger.info("Database adapter initialized for lab lookup")
            except Exception as e:
                logger.warning(f"Failed to initialize database adapter: {e}")

    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "add_lab"

    def get_action_config(self) -> Dict[str, Any]:
        """Get configuration for content hashing."""
        return {
            "add_summary_comment": self.add_summary_comment,
            "skip_if_lab_exists": self.skip_if_lab_exists,
            "lab_selection_strategy": self.lab_selection_strategy,
            "has_database": self.database_url is not None,
        }

    def _get_existing_lab_cis(self, ticket_assets: List[Dict[str, Any]]) -> Set[int]:
        """
        Extract lab CI IDs already on ticket.

        Args:
            ticket_assets: List of asset/CI dicts from get_ticket_assets()

        Returns:
            Set of lab CI IDs currently on ticket
        """
        lab_ci_ids = set()

        for asset in ticket_assets:
            # Check if this is a lab CI (FormID 3830)
            if asset.get("FormID") == self.LAB_FORM_ID:
                lab_ci_id = asset.get("ID")
                if lab_ci_id:
                    lab_ci_ids.add(lab_ci_id)
                    logger.debug(
                        f"Found existing lab CI {lab_ci_id} ({asset.get('Name')})"
                    )

        return lab_ci_ids

    def _get_requestor_labs(self, requestor_uid: str) -> List[Dict[str, Any]]:
        """
        Get labs that the requestor is a member of.

        Args:
            requestor_uid: TDX User UID (from ticket.RequestorUid)

        Returns:
            List of lab info dicts: [{"lab_ci_id": 123, "lab_id": "joshea"}, ...]
        """
        if not self.db_adapter:
            logger.warning("Database adapter not available for requestor lab lookup")
            return []

        try:
            query = """
                SELECT DISTINCT
                    tdx_ci_id as lab_ci_id,
                    lab_id
                FROM silver.v_lab_members_all_tdx_reference
                WHERE tdx_user_uid = :requestor_uid
                AND tdx_ci_id IS NOT NULL
            """

            df = self.db_adapter.query_to_dataframe(
                query, {"requestor_uid": requestor_uid}
            )

            if df.empty:
                logger.debug(f"Requestor {requestor_uid} not found in any labs")
                return []

            labs = df.to_dict("records")
            logger.info(
                f"Found {len(labs)} lab(s) for requestor: {df['lab_id'].tolist()}"
            )
            return labs

        except Exception as e:
            logger.exception(f"Error querying requestor labs: {e}")
            return []

    def _get_asset_labs(self, asset_ci_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Get labs that own the specified computer assets.

        Args:
            asset_ci_ids: List of configuration item IDs from ticket assets

        Returns:
            List of lab info dicts: [{"lab_ci_id": 123, "lab_id": "joshea", "computer_ci_id": 456}, ...]
        """
        if not self.db_adapter or not asset_ci_ids:
            return []

        try:
            query = """
                SELECT DISTINCT
                    lab_department_tdx_id as lab_ci_id,
                    lab_id,
                    tdx_configuration_item_id as computer_ci_id
                FROM silver.v_lab_computers_tdx_reference
                WHERE tdx_configuration_item_id = ANY(:asset_ci_ids)
                AND lab_department_tdx_id IS NOT NULL
            """

            df = self.db_adapter.query_to_dataframe(
                query, {"asset_ci_ids": asset_ci_ids}
            )

            if df.empty:
                logger.debug(f"No lab computers found among {len(asset_ci_ids)} assets")
                return []

            labs = df.to_dict("records")
            logger.info(
                f"Found {len(labs)} lab computer(s) matching assets: {df['lab_id'].unique().tolist()}"
            )
            return labs

        except Exception as e:
            logger.exception(f"Error querying asset labs: {e}")
            return []

    def _get_lab_names(self, lab_ci_ids: List[int]) -> Dict[int, str]:
        """
        Get display names for lab CIs.

        Args:
            lab_ci_ids: List of lab CI IDs

        Returns:
            Dict mapping lab_ci_id → lab_name
        """
        if not self.db_adapter or not lab_ci_ids:
            return {}

        try:
            query = """
                SELECT
                    tdx_ci_id,
                    lab_name
                FROM silver.labs
                WHERE tdx_ci_id = ANY(:lab_ci_ids)
            """

            df = self.db_adapter.query_to_dataframe(query, {"lab_ci_ids": lab_ci_ids})

            if df.empty:
                logger.warning(f"No lab names found for CI IDs: {lab_ci_ids}")
                return {}

            # Return dict: {ci_id: name}
            return dict(zip(df["tdx_ci_id"], df["lab_name"]))

        except Exception as e:
            logger.exception(f"Error querying lab names: {e}")
            return {}

    def _add_lab_to_ticket(
        self,
        ticket_id: int,
        lab_ci_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
    ) -> bool:
        """
        Add a lab CI to a ticket.

        Args:
            ticket_id: Ticket ID
            lab_ci_id: Lab CI ID to add
            facade: TeamDynamixFacade for API access
            dry_run: If True, simulate without making changes

        Returns:
            True if successfully added, False otherwise
        """
        if dry_run:
            logger.info(f"[DRY RUN] Would add lab CI {lab_ci_id} to ticket {ticket_id}")
            return True

        try:
            logger.info(f"Adding lab CI {lab_ci_id} to ticket {ticket_id}")

            # Call API to add lab CI to ticket
            # Note: The API returns None/empty response on success (204 No Content)
            facade.configuration_items.add_ticket_to_ci(lab_ci_id, ticket_id)

            # If no exception was raised, consider it successful
            logger.info(
                f"✓ Successfully added lab CI {lab_ci_id} to ticket {ticket_id}"
            )
            return True

        except Exception as e:
            error_str = str(e).lower()

            # Check if error is retryable (5xx errors, timeouts, network issues)
            if any(
                x in error_str
                for x in ["500", "502", "503", "504", "timeout", "network"]
            ):
                logger.error(f"Retryable error adding lab CI {lab_ci_id}: {e}")
                raise  # Re-raise to trigger retry logic
            else:
                # Non-retryable error (404, 400, etc.) - log and continue
                logger.warning(f"Non-retryable error adding lab CI {lab_ci_id}: {e}")
                return False

    def execute_action(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
        action_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the add lab action on a ticket.

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

            ticket_title = ticket.get("Title", "Unknown")
            requestor_uid = ticket.get("RequestorUid")

            logger.info(f"Processing ticket {ticket_id}: '{ticket_title}'")

            # Get existing assets/CIs on ticket
            ticket_assets = facade.tickets.get_ticket_assets(ticket_id) or []
            existing_lab_cis = self._get_existing_lab_cis(ticket_assets)

            # Check if should skip (ticket already has lab CI)
            if existing_lab_cis and self.skip_if_lab_exists:
                logger.info(
                    f"Ticket already has {len(existing_lab_cis)} lab CI(s), skipping"
                )
                return {
                    "success": True,
                    "message": "No labs added - ticket already has lab CI(s)",
                    "summary": "",  # No summary for skipped action
                    "details": {
                        "labs_added": 0,
                        "existing_lab_count": len(existing_lab_cis),
                        "skipped": True,
                    },
                }

            # ============================================================
            # PHASE 1: Asset-Based Lab Detection (PRIORITY)
            # ============================================================
            lab_to_add = None  # Single lab to add: {"lab_ci_id": int, "lab_id": str, "source": str, ...}
            detection_method = None

            # Extract computer CI IDs from ticket assets
            asset_ci_ids = [
                asset.get("ID") for asset in ticket_assets if asset.get("ID")
            ]

            if asset_ci_ids:
                logger.info(
                    f"Phase 1: Checking if {len(asset_ci_ids)} asset(s) belong to labs"
                )
                asset_labs = self._get_asset_labs(asset_ci_ids)

                if asset_labs:
                    # Build mapping of computer CI ID → name for summary
                    asset_name_map = {
                        asset["ID"]: asset.get("Name", "Unknown")
                        for asset in ticket_assets
                    }

                    # Select single lab based on strategy
                    if self.lab_selection_strategy == "most_common":
                        # Count occurrences of each lab
                        from collections import Counter

                        lab_counts = Counter(lab["lab_ci_id"] for lab in asset_labs)
                        selected_lab_ci_id = lab_counts.most_common(1)[0][0]
                        selected_lab = next(
                            lab
                            for lab in asset_labs
                            if lab["lab_ci_id"] == selected_lab_ci_id
                        )
                    else:
                        # Default: take first lab found
                        selected_lab = asset_labs[0]

                    # Collect all asset names for this lab
                    lab_ci_id = selected_lab["lab_ci_id"]
                    asset_names = [
                        asset_name_map.get(lab["computer_ci_id"], "Unknown")
                        for lab in asset_labs
                        if lab["lab_ci_id"] == lab_ci_id
                    ]

                    lab_to_add = {
                        "lab_ci_id": lab_ci_id,
                        "lab_id": selected_lab["lab_id"],
                        "source": "assets",
                        "asset_names": asset_names,
                    }
                    detection_method = "assets"

                    logger.info(
                        f"→ Asset-based detection: Selected lab '{selected_lab['lab_id']}' "
                        f"(CI {lab_ci_id}) from {len(asset_labs)} asset(s)"
                    )
            else:
                logger.info("Phase 1: Skipped - ticket has no assets")

            # ============================================================
            # PHASE 2: Requestor-Based Lab Detection (FALLBACK)
            # ============================================================
            if not lab_to_add and requestor_uid:
                logger.info(
                    f"Phase 2: Checking if requestor {requestor_uid} is in any labs"
                )
                requestor_labs = self._get_requestor_labs(requestor_uid)

                if requestor_labs:
                    # Select single lab (first one or most common if multiple)
                    selected_lab = requestor_labs[0]

                    lab_to_add = {
                        "lab_ci_id": selected_lab["lab_ci_id"],
                        "lab_id": selected_lab["lab_id"],
                        "source": "requestor",
                    }
                    detection_method = "requestor"

                    logger.info(
                        f"→ Requestor-based detection: Selected lab '{selected_lab['lab_id']}' "
                        f"(CI {selected_lab['lab_ci_id']}) from {len(requestor_labs)} lab(s)"
                    )
            elif not lab_to_add:
                logger.info(
                    "Phase 2: Skipped - no lab found via assets, no requestor UID"
                )

            # ============================================================
            # PHASE 3: Add Single Lab to Ticket
            # ============================================================
            if not lab_to_add:
                logger.info("No labs detected for this ticket")
                return {
                    "success": True,
                    "message": "No labs detected for this ticket",
                    "summary": "",  # No summary
                    "details": {
                        "labs_added": 0,
                        "detection_method": "none",
                    },
                }

            # Get lab name for summary
            lab_ci_id = lab_to_add["lab_ci_id"]
            lab_names_map = self._get_lab_names([lab_ci_id])
            lab_name = lab_names_map.get(lab_ci_id, lab_to_add["lab_id"])

            # Add the single lab to ticket
            try:
                success = self._add_lab_to_ticket(ticket_id, lab_ci_id, facade, dry_run)

                if not success:
                    return {
                        "success": False,
                        "message": f"Failed to add lab {lab_name} (CI {lab_ci_id})",
                        "summary": "",
                        "details": {
                            "labs_added": 0,
                            "labs_failed": 1,
                            "detection_method": detection_method,
                        },
                    }

            except Exception as e:
                # Retryable error occurred (5xx, timeout, etc.)
                logger.error(f"Retryable error adding lab {lab_name}: {e}")
                return {
                    "success": False,
                    "message": "Retryable error occurred, will retry next run",
                    "details": {
                        "labs_added": 0,
                        "labs_failed": 1,
                        "retryable": True,
                        "error": str(e),
                    },
                }

            # ============================================================
            # PHASE 4: Generate Summary
            # ============================================================

            # Build summary message based on detection method
            if detection_method == "assets":
                # Asset-based detection
                asset_names = lab_to_add.get("asset_names", [])
                if asset_names:
                    # Show first 2 asset names
                    asset_names_preview = ", ".join(asset_names[:2])
                    if len(asset_names) > 2:
                        asset_names_preview += f" (+{len(asset_names) - 2} more)"
                    reason = f"Ticket contains lab computer(s) ({asset_names_preview})"
                else:
                    reason = "Ticket contains lab computer(s)"
            else:
                # Requestor-based detection
                requestor_uniqname = (
                    ticket.get("RequestorEmail", "").split("@")[0]
                    if ticket.get("RequestorEmail")
                    else "requestor"
                )
                reason = f"Requestor ({requestor_uniqname}) is a lab member"

            summary = f"Added lab: {lab_name} ({lab_to_add['lab_id'].upper()}). Reason: {reason}"

            return {
                "success": True,
                "message": f"Added 1 lab: {lab_name}",
                "summary": summary if self.add_summary_comment else "",
                "details": {
                    "labs_added": 1,
                    "labs_failed": 0,
                    "lab_ci_id": lab_ci_id,
                    "lab_id": lab_to_add["lab_id"],
                    "lab_name": lab_name,
                    "detection_method": detection_method,
                },
            }

        except Exception as e:
            logger.exception(f"Unexpected error in AddLabAction: {e}")
            return {
                "success": False,
                "message": f"Unexpected error: {str(e)}",
                "details": {"error": str(e), "error_type": type(e).__name__},
            }

    def __repr__(self) -> str:
        """String representation of the add lab action."""
        return (
            f"AddLabAction(action_id='{self.get_action_id()}', "
            f"skip_if_lab_exists={self.skip_if_lab_exists})"
        )
