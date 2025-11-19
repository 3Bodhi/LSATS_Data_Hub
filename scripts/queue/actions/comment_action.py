"""
Comment action for adding feed entries to tickets.

Adds comments/feed entries to TeamDynamix tickets with configurable
message templates, privacy settings, and notification options.
"""

import logging
from typing import Any, Dict, List, Optional

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

from .base_action import BaseAction

logger = logging.getLogger(__name__)


class CommentAction(BaseAction):
    """
    Action that adds a comment/feed entry to a ticket.

    Configuration parameters:
    - comment_text: The comment message to add
    - is_private: Whether the comment should be private (default: False)
    - is_rich_html: Whether the comment contains HTML (default: False)
    - is_communication: Mark as communication record (default: False)
    - notify: List of email addresses to notify (default: ['null'])
    - new_status_id: Optional new status ID (0 = no change)
    - cascade_status: Cascade status change to children (default: False)
    """

    def __init__(
        self,
        comment_text: str,
        is_private: bool = False,
        is_rich_html: bool = False,
        is_communication: bool = False,
        notify: Optional[List[str]] = None,
        new_status_id: int = 0,
        cascade_status: bool = False,
        version: str = "v1",
        **kwargs,
    ):
        """
        Initialize comment action.

        Args:
            comment_text: The comment message to add to the ticket
            is_private: If True, comment will be private
            is_rich_html: If True, comment will be treated as HTML
            is_communication: If True, marked as communication record
            notify: List of email addresses/UIDs to notify (default: ['null'])
            new_status_id: Optional status ID to set (0 = no change)
            cascade_status: If True, cascade status change to child tickets
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)

        self.comment_text = comment_text
        self.is_private = is_private
        self.is_rich_html = is_rich_html
        self.is_communication = is_communication
        self.notify = notify if notify is not None else ["null"]
        self.new_status_id = new_status_id
        self.cascade_status = cascade_status

    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "comment"

    def get_action_config(self) -> Dict[str, Any]:
        """
        Get configuration for content hashing.

        If any of these parameters change, the action hash changes,
        allowing the action to execute again with the new configuration.
        """
        return {
            "comment_text": self.comment_text,
            "is_private": self.is_private,
            "is_rich_html": self.is_rich_html,
            "is_communication": self.is_communication,
            "notify": sorted(self.notify),  # Sort for consistent hashing
            "new_status_id": self.new_status_id,
            "cascade_status": self.cascade_status,
        }

    def execute_action(
        self, ticket_id: int, facade: TeamDynamixFacade, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the comment action on a ticket.

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance for API operations
            dry_run: If True, simulate without actually adding comment

        Returns:
            Execution result dictionary with success status and details
        """
        try:
            # Get ticket info for logging
            ticket = facade.tickets.get_ticket(ticket_id)
            if not ticket:
                return {
                    "success": False,
                    "message": f"Ticket {ticket_id} not found",
                    "details": {"ticket_id": ticket_id},
                }

            ticket_title = ticket.get("Title", "Unknown")

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would add comment to ticket {ticket_id} "
                    f"('{ticket_title}'): {self.comment_text[:50]}..."
                )
                return {
                    "success": True,
                    "message": f"[DRY RUN] Comment would be added to ticket {ticket_id}",
                    "details": {
                        "ticket_id": ticket_id,
                        "ticket_title": ticket_title,
                        "comment_preview": self.comment_text[:100],
                        "is_private": self.is_private,
                        "new_status_id": self.new_status_id,
                    },
                }

            # Add the comment via facade
            result = facade.tickets.update_ticket(
                id=ticket_id,
                comments=self.comment_text,
                private=self.is_private,
                commrecord=self.is_communication,
                status=self.new_status_id,
                cascade=self.cascade_status,
                notify=self.notify,
                rich=self.is_rich_html,
            )

            if result:
                logger.info(
                    f"Successfully added comment to ticket {ticket_id} ('{ticket_title}')"
                )
                return {
                    "success": True,
                    "message": f"Comment added to ticket {ticket_id}",
                    "details": {
                        "ticket_id": ticket_id,
                        "ticket_title": ticket_title,
                        "comment_length": len(self.comment_text),
                        "is_private": self.is_private,
                        "new_status_id": self.new_status_id,
                        "response": result,
                    },
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to add comment to ticket {ticket_id} (no response)",
                    "details": {"ticket_id": ticket_id, "ticket_title": ticket_title},
                }

        except Exception as e:
            logger.exception(f"Error adding comment to ticket {ticket_id}")
            return {
                "success": False,
                "message": f"Error adding comment: {str(e)}",
                "details": {
                    "ticket_id": ticket_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            }

    def __repr__(self) -> str:
        """String representation of the comment action."""
        preview = (
            self.comment_text[:30] + "..."
            if len(self.comment_text) > 30
            else self.comment_text
        )
        return (
            f"CommentAction(action_id='{self.get_action_id()}', "
            f"comment='{preview}', private={self.is_private})"
        )
