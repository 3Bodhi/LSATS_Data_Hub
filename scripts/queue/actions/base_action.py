"""
Base action class for ticket queue daemon.

All daemon actions inherit from this abstract base class, which provides
the framework for idempotent, trackable actions on TeamDynamix tickets.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from scripts.queue.state.state_tracker import StateTracker
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

logger = logging.getLogger(__name__)


class BaseAction(ABC):
    """
    Abstract base class for all daemon actions.

    Each action must implement:
    - get_action_type(): Return action type string
    - get_action_config(): Return configuration dict for hashing
    - execute_action(): Perform the actual action

    The base class handles:
    - Action ID generation (type:hash:version)
    - Idempotency checking via StateTracker
    - Success/failure recording
    - Error handling and logging
    """

    def __init__(self, version: str = "v1", **kwargs):
        """
        Initialize base action.

        Args:
            version: Action version string (increment when behavior changes)
            **kwargs: Action-specific configuration parameters
        """
        self.version = version
        self.config = kwargs
        self._action_id = None
        self._action_hash = None

    @abstractmethod
    def get_action_type(self) -> str:
        """
        Get the action type identifier.

        Returns:
            Action type string (e.g., 'comment', 'status_change', 'assign')
        """
        pass

    @abstractmethod
    def get_action_config(self) -> Dict[str, Any]:
        """
        Get the action configuration for content hashing.

        This should return all parameters that define the action's behavior.
        If any of these parameters change, the hash changes, creating a new action.

        Returns:
            Dictionary of configuration parameters
        """
        pass

    @abstractmethod
    def execute_action(
        self, ticket_id: int, facade: TeamDynamixFacade, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the action on a ticket.

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance for API operations
            dry_run: If True, simulate action without making changes

        Returns:
            Dictionary with execution results:
            {
                'success': bool,
                'message': str,
                'details': dict (optional)
            }
        """
        pass

    def get_action_hash(self) -> str:
        """
        Generate SHA256 hash of action configuration.

        This hash is used for content-aware idempotency. If the action
        configuration changes (e.g., comment template modified), the hash
        changes and the action will execute again even if a previous version ran.

        Returns:
            Hexadecimal SHA256 hash string
        """
        if self._action_hash is None:
            config = self.get_action_config()
            config_str = str(sorted(config.items()))
            self._action_hash = hashlib.sha256(config_str.encode()).hexdigest()

        return self._action_hash

    def get_action_id(self) -> str:
        """
        Generate unique action identifier.

        Format: {action_type}:{content_hash}:{version}
        Example: comment:abc123def456:v1

        Returns:
            Unique action identifier string
        """
        if self._action_id is None:
            action_type = self.get_action_type()
            action_hash = self.get_action_hash()[:12]  # Use first 12 chars of hash
            self._action_id = f"{action_type}:{action_hash}:{self.version}"

        return self._action_id

    def should_execute(
        self,
        ticket_id: int,
        state_tracker: StateTracker,
        facade: Optional[TeamDynamixFacade] = None,
    ) -> bool:
        """
        Determine if this action should execute on the given ticket.

        Checks the state tracker to see if action has already been executed.
        Subclasses can override to add additional conditions (e.g., verify
        current ticket state).

        Args:
            ticket_id: The TeamDynamix ticket ID
            state_tracker: StateTracker for idempotency checking
            facade: Optional TeamDynamixFacade for state verification

        Returns:
            True if action should execute, False otherwise
        """
        action_id = self.get_action_id()

        # Check if already executed
        if state_tracker.has_executed(ticket_id, action_id):
            logger.debug(
                f"Action {action_id} already executed on ticket {ticket_id}, skipping"
            )
            return False

        return True

    def execute(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        state_tracker: StateTracker,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute the action with full idempotency and error handling.

        This is the main entry point for action execution. It:
        1. Checks if action should execute (idempotency)
        2. Executes the action
        3. Records the result in state tracker
        4. Handles errors gracefully

        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade instance
            state_tracker: StateTracker for recording execution
            dry_run: If True, simulate without making changes

        Returns:
            Dictionary with execution results:
            {
                'executed': bool,
                'success': bool,
                'status': str ('completed', 'failed', 'skipped'),
                'message': str,
                'action_id': str,
                'details': dict (optional)
            }
        """
        action_id = self.get_action_id()
        action_type = self.get_action_type()
        action_hash = self.get_action_hash()

        result = {
            "executed": False,
            "success": False,
            "status": "skipped",
            "message": "",
            "action_id": action_id,
            "ticket_id": ticket_id,
        }

        # Check if should execute
        if not self.should_execute(ticket_id, state_tracker, facade):
            result["message"] = (
                f"Action {action_id} already executed on ticket {ticket_id}"
            )
            logger.info(result["message"])
            return result

        # Execute the action
        try:
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}Executing action {action_id} "
                f"on ticket {ticket_id}"
            )

            exec_result = self.execute_action(ticket_id, facade, dry_run)

            result["executed"] = True
            result["success"] = exec_result.get("success", False)
            result["message"] = exec_result.get("message", "")
            result["details"] = exec_result.get("details", {})

            if result["success"]:
                result["status"] = "completed"

                # Record in state tracker (unless dry run)
                if not dry_run:
                    state_tracker.mark_completed(
                        ticket_id=ticket_id,
                        action_id=action_id,
                        action_type=action_type,
                        action_hash=action_hash,
                        status="completed",
                        metadata={
                            "details": result.get("details", {}),
                            "config": self.get_action_config(),
                        },
                    )

                logger.info(
                    f"{'[DRY RUN] ' if dry_run else ''}Action {action_id} "
                    f"completed successfully on ticket {ticket_id}"
                )
            else:
                result["status"] = "failed"
                error_msg = result.get("message", "Unknown error")

                # Record failure (unless dry run)
                if not dry_run:
                    state_tracker.mark_completed(
                        ticket_id=ticket_id,
                        action_id=action_id,
                        action_type=action_type,
                        action_hash=action_hash,
                        status="failed",
                        error_message=error_msg,
                        metadata={
                            "details": result.get("details", {}),
                            "config": self.get_action_config(),
                        },
                    )

                logger.error(
                    f"Action {action_id} failed on ticket {ticket_id}: {error_msg}"
                )

        except Exception as e:
            result["executed"] = True
            result["success"] = False
            result["status"] = "failed"
            result["message"] = f"Unexpected error: {str(e)}"

            # Record exception (unless dry run)
            if not dry_run:
                state_tracker.mark_completed(
                    ticket_id=ticket_id,
                    action_id=action_id,
                    action_type=action_type,
                    action_hash=action_hash,
                    status="failed",
                    error_message=str(e),
                    metadata={"config": self.get_action_config()},
                )

            logger.exception(
                f"Unexpected error executing action {action_id} on ticket {ticket_id}"
            )

        return result

    def __repr__(self) -> str:
        """String representation of the action."""
        return f"{self.__class__.__name__}(action_id='{self.get_action_id()}')"
