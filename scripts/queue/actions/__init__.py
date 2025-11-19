"""
Action classes for ticket queue daemon.

Each action represents a specific operation that can be performed on a ticket.
Actions are idempotent and tracked in the database.
"""

from .base_action import BaseAction
from .comment_action import CommentAction

__all__ = ["BaseAction", "CommentAction"]
