"""
Find Active Tickets action for discovering related tickets.

Finds and reports all active tickets related to a ticket's requestor,
assets, and lab CI. Helps technicians see the full context of issues.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Set

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

from .base_action import BaseAction

logger = logging.getLogger(__name__)


class FindActiveTicketsAction(BaseAction):
    """
    Action that finds and reports active tickets related to current ticket.

    Searches for active tickets across three categories:
    1. Requestor tickets - Other tickets by the same user
    2. Asset tickets - Tickets for computer assets on this ticket
    3. Lab tickets - Tickets for the lab CI (if attached)

    Results are de-duplicated, sorted by most recent, and formatted as
    HTML tables for inclusion in the summary comment.

    Configuration parameters:
    - exclude_current_ticket: Don't include current ticket in results (default: True)
    - max_tickets_per_category: Limit results per category (default: 10)
    - active_status_ids: List of status IDs considered "active" (default: [115, 117, 121, 619, 620, 622])
    - lab_type_id: TypeID for Lab CIs (default: 10132)
    - show_partial_on_error: Show partial results if errors occur (default: True)
    """

    def __init__(
        self,
        exclude_current_ticket: bool = True,
        max_tickets_per_category: int = 10,
        active_status_ids: Optional[List[int]] = None,
        lab_type_id: int = 10132,
        show_partial_on_error: bool = True,
        version: str = "v1",
        **kwargs,
    ):
        """
        Initialize find active tickets action.

        Args:
            exclude_current_ticket: If True, exclude current ticket from results
            max_tickets_per_category: Maximum tickets to show per category
            active_status_ids: Status IDs considered "active"
            lab_type_id: TypeID for Lab configuration items
            show_partial_on_error: If True, show partial results on API errors
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)

        self.exclude_current_ticket = exclude_current_ticket
        self.max_tickets_per_category = max_tickets_per_category
        self.active_status_ids = active_status_ids or [115, 117, 121, 619, 620, 622]
        self.lab_type_id = lab_type_id
        self.show_partial_on_error = show_partial_on_error

    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "find_active_tickets"

    def get_action_config(self) -> Dict[str, Any]:
        """Get configuration for content hashing."""
        return {
            "exclude_current_ticket": self.exclude_current_ticket,
            "max_tickets_per_category": self.max_tickets_per_category,
            "active_status_ids": sorted(self.active_status_ids),
            "lab_type_id": self.lab_type_id,
            "show_partial_on_error": self.show_partial_on_error,
        }

    def _get_base_url(self) -> str:
        """
        Detect TDX base URL from environment and convert to web URL.

        Returns:
            Base URL for ticket links (sandbox or production)
        """
        tdx_base_url = os.getenv("TDX_BASE_URL", "")

        # Detect environment
        is_sandbox = "SB" in tdx_base_url or "sandbox" in tdx_base_url.lower()

        # Build web URL
        if is_sandbox:
            return "https://teamdynamix.umich.edu/SBTDNext"
        else:
            return "https://teamdynamix.umich.edu/TDNext"

    def _get_requestor_tickets(
        self, facade: TeamDynamixFacade, requestor_uid: str, current_ticket_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get active tickets for the requestor.

        Args:
            facade: TeamDynamixFacade instance
            requestor_uid: RequestorUID to search for
            current_ticket_id: Current ticket ID to exclude

        Returns:
            List of active ticket dictionaries
        """
        try:
            logger.debug(f"Fetching active tickets for requestor {requestor_uid}")

            # Use get_active_tickets which filters by active status IDs
            tickets = facade.tickets.get_active_tickets([requestor_uid])

            if not tickets:
                logger.debug(f"No active tickets found for requestor {requestor_uid}")
                return []

            # Exclude current ticket if configured
            if self.exclude_current_ticket:
                tickets = [t for t in tickets if t.get("ID") != current_ticket_id]

            logger.debug(
                f"Found {len(tickets)} active requestor tickets (excluding current)"
            )
            return tickets

        except Exception as e:
            logger.warning(f"Error fetching requestor tickets: {e}")
            if not self.show_partial_on_error:
                raise
            return []

    def _get_asset_tickets(
        self, facade: TeamDynamixFacade, ticket_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get active tickets for computer assets attached to this ticket.

        Args:
            facade: TeamDynamixFacade instance
            ticket_id: Current ticket ID

        Returns:
            List of active ticket dictionaries
        """
        try:
            logger.debug(f"Fetching asset tickets for ticket {ticket_id}")

            # Get all assets/CIs attached to ticket
            assets = facade.tickets.get_ticket_assets(ticket_id)

            if not assets:
                logger.debug(f"No assets found on ticket {ticket_id}")
                return []

            # Filter for computer assets (exclude lab CIs)
            computer_assets = [
                asset
                for asset in assets
                if asset.get("TypeID") != self.lab_type_id
                and asset.get("BackingItemType") == 27  # Asset type
            ]

            if not computer_assets:
                logger.debug(f"No computer assets found on ticket {ticket_id}")
                return []

            logger.debug(
                f"Found {len(computer_assets)} computer assets on ticket {ticket_id}"
            )

            # Search for tickets related to each computer asset's CI ID
            all_asset_tickets = []
            seen_ticket_ids: Set[int] = set()

            for asset in computer_assets:
                asset_ci_id = asset.get("ID")
                asset_name = asset.get("Name", "Unknown")

                try:
                    # Search for tickets with this configuration item
                    search_params = {
                        "ConfigurationItemIDs": [asset_ci_id],
                        "StatusIDs": self.active_status_ids,
                    }

                    asset_tickets = facade.tickets.search_tickets(search_params)

                    if asset_tickets:
                        # De-duplicate across assets
                        for ticket in asset_tickets:
                            ticket_id_val = ticket.get("ID")
                            if ticket_id_val and ticket_id_val not in seen_ticket_ids:
                                seen_ticket_ids.add(ticket_id_val)
                                all_asset_tickets.append(ticket)

                        logger.debug(
                            f"Found {len(asset_tickets)} tickets for asset {asset_name} (ID: {asset_ci_id})"
                        )

                except Exception as e:
                    logger.warning(
                        f"Error fetching tickets for asset {asset_name} (ID: {asset_ci_id}): {e}"
                    )
                    if not self.show_partial_on_error:
                        raise
                    continue

            # Exclude current ticket if configured
            if self.exclude_current_ticket:
                all_asset_tickets = [
                    t for t in all_asset_tickets if t.get("ID") != ticket_id
                ]

            logger.debug(
                f"Found {len(all_asset_tickets)} total asset tickets (excluding current)"
            )
            return all_asset_tickets

        except Exception as e:
            logger.warning(f"Error fetching asset tickets: {e}")
            if not self.show_partial_on_error:
                raise
            return []

    def _get_lab_tickets(
        self, facade: TeamDynamixFacade, ticket_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get active tickets for the lab CI attached to this ticket.

        Args:
            facade: TeamDynamixFacade instance
            ticket_id: Current ticket ID

        Returns:
            List of active ticket dictionaries
        """
        try:
            logger.debug(f"Fetching lab tickets for ticket {ticket_id}")

            # Get all assets/CIs attached to ticket
            assets = facade.tickets.get_ticket_assets(ticket_id)

            if not assets:
                logger.debug(f"No assets found on ticket {ticket_id}")
                return []

            # Find lab CI (TypeID = 10132)
            lab_cis = [
                asset for asset in assets if asset.get("TypeID") == self.lab_type_id
            ]

            if not lab_cis:
                logger.debug(f"No lab CI found on ticket {ticket_id}")
                return []

            # Use the first lab CI (there should only be one)
            lab_ci = lab_cis[0]
            lab_ci_id = lab_ci.get("BackingItemID")
            lab_name = lab_ci.get("Name", "Unknown")

            if not lab_ci_id:
                logger.warning(f"Lab CI {lab_name} has no BackingItemID")
                return []

            logger.debug(f"Found lab CI: {lab_name} (ID: {lab_ci_id})")

            # Get all tickets for this lab CI
            lab_tickets = facade.cis.get_ci_tickets(lab_ci_id)

            if not lab_tickets:
                logger.debug(f"No tickets found for lab {lab_name}")
                return []

            # Filter for active tickets only
            active_lab_tickets = [
                ticket
                for ticket in lab_tickets
                if ticket.get("StatusID") in self.active_status_ids
            ]

            # Exclude current ticket if configured
            if self.exclude_current_ticket:
                active_lab_tickets = [
                    t for t in active_lab_tickets if t.get("ID") != ticket_id
                ]

            logger.debug(
                f"Found {len(active_lab_tickets)} active lab tickets for {lab_name} (excluding current)"
            )
            return active_lab_tickets

        except Exception as e:
            logger.warning(f"Error fetching lab tickets: {e}")
            if not self.show_partial_on_error:
                raise
            return []

    def _deduplicate_tickets(
        self, tickets_dict: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        De-duplicate tickets across categories while tracking which categories they appear in.

        First occurrence wins. Order of processing: requestor → asset → lab

        Args:
            tickets_dict: Dictionary with keys 'requestor', 'asset', 'lab'

        Returns:
            Same structure with duplicates removed and sorted by ModifiedDate DESC
        """
        # Track seen ticket IDs globally
        seen_ids: Set[int] = set()
        result: Dict[str, List[Dict[str, Any]]] = {}

        # Process in order: requestor → asset → lab
        for category in ["requestor", "asset", "lab"]:
            tickets = tickets_dict.get(category, [])
            unique = []

            for ticket in tickets:
                ticket_id = ticket.get("ID")
                if ticket_id and ticket_id not in seen_ids:
                    seen_ids.add(ticket_id)
                    unique.append(ticket)

            # Sort by most recent ModifiedDate
            unique.sort(key=lambda t: t.get("ModifiedDate", ""), reverse=True)

            # Limit to max_tickets_per_category
            result[category] = unique[: self.max_tickets_per_category]

            if len(unique) > self.max_tickets_per_category:
                logger.debug(
                    f"Limited {category} tickets from {len(unique)} to {self.max_tickets_per_category}"
                )

        return result

    def _build_html_table(
        self, category: str, tickets: List[Dict[str, Any]], base_url: str
    ) -> str:
        """
        Build HTML table for a category of tickets.

        Args:
            category: Category name ('requestor', 'asset', 'lab')
            tickets: List of ticket dictionaries
            base_url: Base URL for ticket links

        Returns:
            HTML table string
        """
        if not tickets:
            return f"<p><em>No active {category} tickets found.</em></p>"

        # Table header with category title
        category_titles = {
            "requestor": "Requestor Tickets",
            "asset": "Asset Tickets",
            "lab": "Lab Tickets",
        }
        title = category_titles.get(category, f"{category.title()} Tickets")

        html = f"<h4>{title} ({len(tickets)})</h4>\n"
        html += '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">\n'
        html += "<tr><th>ID</th><th>Title</th><th>Status</th><th>Modified</th></tr>\n"

        # Table rows
        for ticket in tickets:
            ticket_id = ticket.get("ID")
            title_text = ticket.get("Title", "Unknown")
            status = ticket.get("StatusName", "Unknown")
            modified = ticket.get("ModifiedDate", "")

            # Format ticket ID as hyperlink
            ticket_url = f"{base_url}/Apps/46/Tickets/TicketDet?TicketID={ticket_id}"

            # Extract just the date part (first 10 chars: YYYY-MM-DD)
            modified_date = modified[:10] if modified else "Unknown"

            html += "<tr>"
            html += f'<td><a href="{ticket_url}">{ticket_id}</a></td>'
            html += f"<td>{title_text}</td>"
            html += f"<td>{status}</td>"
            html += f"<td>{modified_date}</td>"
            html += "</tr>\n"

        html += "</table>\n"

        return html

    def execute_action(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
        action_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the find active tickets action.

        Searches for related active tickets and adds HTML tables to summary.

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance for API operations
            dry_run: If True, simulate without actually adding comment
            action_context: Context dict for sharing data with other actions

        Returns:
            Execution result dictionary with success status and details
        """
        # Initialize action_context if not provided
        if action_context is None:
            action_context = {"summaries": []}

        errors: List[str] = []

        try:
            # Get ticket info
            ticket = facade.tickets.get_ticket(ticket_id)
            if not ticket:
                return {
                    "success": False,
                    "message": f"Ticket {ticket_id} not found",
                }

            ticket_title = ticket.get("Title", "Unknown")
            requestor_uid = ticket.get("RequestorUid")

            if not requestor_uid:
                logger.warning(
                    f"Ticket {ticket_id} has no RequestorUid (unexpected - skipping requestor tickets)"
                )
                errors.append("⚠️ No RequestorUID found (skipped requestor tickets)")

            logger.info(
                f"Finding active tickets related to ticket {ticket_id} ('{ticket_title}')"
            )

            # Collect tickets from all three categories
            tickets_dict: Dict[str, List[Dict[str, Any]]] = {
                "requestor": [],
                "asset": [],
                "lab": [],
            }

            # 1. Get requestor tickets
            if requestor_uid:
                try:
                    tickets_dict["requestor"] = self._get_requestor_tickets(
                        facade, requestor_uid, ticket_id
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch requestor tickets: {e}")
                    errors.append("⚠️ Could not fetch requestor tickets")
                    if not self.show_partial_on_error:
                        raise

            # 2. Get asset tickets
            try:
                tickets_dict["asset"] = self._get_asset_tickets(facade, ticket_id)
            except Exception as e:
                logger.warning(f"Failed to fetch asset tickets: {e}")
                errors.append("⚠️ Could not fetch asset tickets")
                if not self.show_partial_on_error:
                    raise

            # 3. Get lab tickets
            try:
                tickets_dict["lab"] = self._get_lab_tickets(facade, ticket_id)
            except Exception as e:
                logger.warning(f"Failed to fetch lab tickets: {e}")
                errors.append("⚠️ Could not fetch lab tickets")
                if not self.show_partial_on_error:
                    raise

            # De-duplicate and sort tickets
            deduplicated = self._deduplicate_tickets(tickets_dict)

            # Count total unique tickets found
            total_tickets = sum(len(tickets) for tickets in deduplicated.values())

            # Build summary text
            summary_parts = []
            for category in ["requestor", "asset", "lab"]:
                count = len(deduplicated[category])
                if count > 0:
                    summary_parts.append(f"{category.title()}: {count}")

            if summary_parts:
                summary = f"📋 Related Active Tickets: {', '.join(summary_parts)}"
            else:
                summary = "📋 No related active tickets found"

            # Build HTML tables
            base_url = self._get_base_url()
            html_tables = []

            for category in ["requestor", "asset", "lab"]:
                tickets = deduplicated[category]
                if tickets:  # Only build table if tickets exist
                    table_html = self._build_html_table(category, tickets, base_url)
                    html_tables.append(table_html)

            # Combine all HTML
            if html_tables:
                full_html = (
                    "<div><strong>📋 Related Active Tickets</strong><br/>\n"
                    + "\n".join(html_tables)
                    + "</div>"
                )
            else:
                full_html = "<p><em>No related active tickets found.</em></p>"

            # Add errors to HTML if any
            if errors:
                error_html = (
                    "<p><strong>Errors encountered:</strong><br/>\n"
                    + "<br/>\n".join(errors)
                    + "</p>"
                )
                full_html += "\n" + error_html

            # Add to action_context summaries
            if "summaries" in action_context:
                action_context["summaries"].append(summary)
                action_context["summaries"].append(full_html)

            # Log results
            if dry_run:
                logger.info(
                    f"[DRY RUN] Would add related tickets summary to ticket {ticket_id}: "
                    f"{total_tickets} total tickets found"
                )
                logger.debug(f"[DRY RUN] Summary: {summary}")
                logger.debug(
                    f"[DRY RUN] HTML preview (first 500 chars):\n{full_html[:500]}..."
                )
            else:
                logger.info(
                    f"Added related tickets summary to ticket {ticket_id}: "
                    f"{total_tickets} total tickets found"
                )

            return {
                "success": True,
                "message": f"Found {total_tickets} related active tickets",
                "summary": summary,
                "details": {
                    "ticket_id": ticket_id,
                    "ticket_title": ticket_title,
                    "requestor_tickets": len(deduplicated["requestor"]),
                    "asset_tickets": len(deduplicated["asset"]),
                    "lab_tickets": len(deduplicated["lab"]),
                    "total_tickets": total_tickets,
                    "errors": errors,
                    "dry_run": dry_run,
                },
            }

        except Exception as e:
            logger.exception(
                f"Unexpected error finding active tickets for ticket {ticket_id}"
            )
            return {
                "success": False,
                "message": f"Error finding active tickets: {str(e)}",
                "details": {
                    "ticket_id": ticket_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            }

    def __repr__(self) -> str:
        """String representation of the find active tickets action."""
        return (
            f"FindActiveTicketsAction(action_id='{self.get_action_id()}', "
            f"max_per_category={self.max_tickets_per_category}, "
            f"exclude_current={self.exclude_current_ticket})"
        )
