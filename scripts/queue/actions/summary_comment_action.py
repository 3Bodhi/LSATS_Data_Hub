"""
Summary Comment action for posting cumulative action summaries.

Collects summaries from all actions executed in a daemon run and posts
them as a single private comment to keep ticket feed organized.
"""

import logging
from typing import Any, Dict, List, Optional

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

from .base_action import BaseAction

logger = logging.getLogger(__name__)


class SummaryCommentAction(BaseAction):
    """
    Action that posts a cumulative summary comment from all executed actions.

    This action should typically be placed LAST in the action list. It collects
    all summaries added to action_context by previous actions and posts them as
    a single organized comment.

    Configuration parameters:
    - comment_prefix: Header text for the summary comment
    - is_private: Whether the summary comment should be private (default: True)
    - is_rich_html: Whether to format as HTML (default: False)
    - skip_if_empty: Don't post comment if no summaries exist (default: True)
    - separator: Line separator between summaries (default: newline)
    """

    def __init__(
        self,
        comment_prefix: str = "🤖 Automated Actions Summary",
        is_private: bool = True,
        is_rich_html: bool = False,
        skip_if_empty: bool = True,
        separator: str = "\n",
        version: str = "v1",
        **kwargs,
    ):
        """
        Initialize summary comment action.

        Args:
            comment_prefix: Header text for the summary (e.g., "Automated Actions Summary")
            is_private: If True, comment will be private
            is_rich_html: If True, comment will be treated as HTML
            skip_if_empty: If True, skip posting if no summaries exist
            separator: String to separate individual summaries (default: newline)
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)

        self.comment_prefix = comment_prefix
        self.is_private = is_private
        self.is_rich_html = is_rich_html
        self.skip_if_empty = skip_if_empty
        self.separator = separator

    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "summary_comment"

    def get_action_config(self) -> Dict[str, Any]:
        """Get configuration for content hashing."""
        return {
            "comment_prefix": self.comment_prefix,
            "is_private": self.is_private,
            "is_rich_html": self.is_rich_html,
            "skip_if_empty": self.skip_if_empty,
            "separator": self.separator,
        }

    def should_execute(
        self,
        ticket_id: int,
        state_tracker,
        facade: Optional[TeamDynamixFacade] = None,
    ) -> bool:
        """
        Summary comment should ALWAYS execute (no idempotency check).

        Unlike other actions, we want to post a summary comment for each
        daemon run where actions were executed, even if this action's
        config hasn't changed.

        Args:
            ticket_id: The TeamDynamix ticket ID
            state_tracker: StateTracker (not used)
            facade: Optional TeamDynamixFacade (not used)

        Returns:
            Always True
        """
        # Override base class - always execute
        return True

    def execute_action(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
        action_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the summary comment action.

        Collects all summaries from action_context and posts as single comment.

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance for API operations
            dry_run: If True, simulate without actually adding comment
            action_context: Context dict containing summaries from other actions

        Returns:
            Execution result dictionary with success status and details
        """
        # Initialize action_context if not provided
        if action_context is None:
            action_context = {"summaries": []}

        summaries = action_context.get("summaries", [])

        # Skip if no summaries and configured to do so
        if not summaries and self.skip_if_empty:
            logger.info(f"No action summaries to post for ticket {ticket_id}, skipping")
            return {
                "success": True,
                "message": "No summaries to post (skipped)",
                "details": {"skipped": True, "summary_count": 0},
            }

        # Build the comment text
        if summaries:
            # Add prefix and numbered list
            summary_lines = [
                f"{i + 1}. {summary}" for i, summary in enumerate(summaries)
            ]
            comment_text = f"{self.comment_prefix}\n\n" + self.separator.join(
                summary_lines
            )
        else:
            # Post even if empty (if skip_if_empty is False)
            comment_text = f"{self.comment_prefix}\n\nNo actions were executed."

        try:
            # Get ticket info for logging
            ticket = facade.tickets.get_ticket(ticket_id)
            if not ticket:
                return {
                    "success": False,
                    "message": f"Ticket {ticket_id} not found",
                }

            ticket_title = ticket.get("Title", "Unknown")

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would add summary comment to ticket {ticket_id} "
                    f"('{ticket_title}') with {len(summaries)} action(s)"
                )
                logger.debug(f"[DRY RUN] Comment preview:\n{comment_text}")
                return {
                    "success": True,
                    "message": f"[DRY RUN] Summary comment would be posted",
                    "details": {
                        "ticket_id": ticket_id,
                        "ticket_title": ticket_title,
                        "summary_count": len(summaries),
                        "comment_preview": comment_text[:200],
                    },
                }

            # Post the comment
            logger.info(
                f"Posting summary comment to ticket {ticket_id} with {len(summaries)} action(s)"
            )

            result = facade.tickets.update_ticket(
                id=ticket_id,
                comments=comment_text,
                private=self.is_private,
                commrecord=False,
                status=0,  # No status change
                cascade=False,
                notify=["null"],
                rich=self.is_rich_html,
            )

            if result:
                logger.info(
                    f"Successfully posted summary comment to ticket {ticket_id}"
                )
                return {
                    "success": True,
                    "message": f"Posted summary comment with {len(summaries)} action(s)",
                    "details": {
                        "ticket_id": ticket_id,
                        "ticket_title": ticket_title,
                        "summary_count": len(summaries),
                        "comment_length": len(comment_text),
                        "is_private": self.is_private,
                    },
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to post summary comment (no response)",
                    "details": {
                        "ticket_id": ticket_id,
                        "summary_count": len(summaries),
                    },
                }

        except Exception as e:
            logger.exception(f"Error posting summary comment to ticket {ticket_id}")
            return {
                "success": False,
                "message": f"Error posting summary comment: {str(e)}",
                "details": {
                    "ticket_id": ticket_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            }

    def __repr__(self) -> str:
        """String representation of the summary comment action."""
        return (
            f"SummaryCommentAction(action_id='{self.get_action_id()}', "
            f"prefix='{self.comment_prefix[:30]}...', private={self.is_private})"
        )
