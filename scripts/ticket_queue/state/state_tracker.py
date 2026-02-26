"""
State tracker for daemon action idempotency.

Tracks which actions have been executed on which tickets to prevent
duplicate processing. Uses PostgreSQL meta.daemon_action_log table.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from database.adapters.postgres_adapter import PostgresAdapter

logger = logging.getLogger(__name__)


class StateTracker:
    """
    Tracks action execution state for idempotent daemon operations.

    Uses the meta.daemon_action_log table to record which actions have been
    performed on which tickets, preventing duplicate execution.
    """

    def __init__(self, db_adapter: PostgresAdapter):
        """
        Initialize state tracker with database adapter.

        Args:
            db_adapter: PostgresAdapter instance for database operations
        """
        self.db = db_adapter
        logger.debug("StateTracker initialized")

    def has_executed(self, ticket_id: int, action_id: str) -> bool:
        """
        Check if an action has already been executed on a ticket.

        Args:
            ticket_id: The TeamDynamix ticket ID
            action_id: Unique action identifier (format: {type}:{hash}:{version})

        Returns:
            bool: True if action has been executed, False otherwise
        """
        try:
            query = text("""
                SELECT EXISTS(
                    SELECT 1
                    FROM meta.daemon_action_log
                    WHERE ticket_id = :ticket_id
                    AND action_id = :action_id
                    AND status IN ('completed', 'skipped')
                ) AS executed
            """)

            with self.db.engine.connect() as conn:
                result = conn.execute(
                    query, {"ticket_id": ticket_id, "action_id": action_id}
                )
                row = result.fetchone()
                executed = row[0] if row else False

                if executed:
                    logger.debug(
                        f"Action already executed: ticket={ticket_id}, action={action_id}"
                    )

                return executed

        except SQLAlchemyError as e:
            logger.error(f"Error checking execution state: {e}")
            # Conservative approach: if we can't check state, assume not executed
            # This may result in duplicate attempts but won't skip actions
            return False

    def mark_completed(
        self,
        ticket_id: int,
        action_id: str,
        action_type: str,
        action_hash: str,
        status: str = "completed",
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Mark an action as completed in the state log.

        Args:
            ticket_id: The TeamDynamix ticket ID
            action_id: Unique action identifier
            action_type: Type of action (e.g., 'comment', 'status_change')
            action_hash: SHA256 hash of action configuration
            status: Execution status ('completed', 'failed', 'skipped')
            error_message: Error message if status is 'failed'
            metadata: Additional metadata about the action execution

        Returns:
            bool: True if successfully recorded, False otherwise
        """
        try:
            insert_query = text("""
                INSERT INTO meta.daemon_action_log (
                    ticket_id,
                    action_id,
                    action_type,
                    action_hash,
                    status,
                    error_message,
                    metadata,
                    executed_at
                ) VALUES (
                    :ticket_id,
                    :action_id,
                    :action_type,
                    :action_hash,
                    :status,
                    :error_message,
                    :metadata,
                    :executed_at
                )
                ON CONFLICT (ticket_id, action_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    metadata = EXCLUDED.metadata,
                    executed_at = EXCLUDED.executed_at
            """)

            with self.db.engine.connect() as conn:
                conn.execute(
                    insert_query,
                    {
                        "ticket_id": ticket_id,
                        "action_id": action_id,
                        "action_type": action_type,
                        "action_hash": action_hash,
                        "status": status,
                        "error_message": error_message,
                        "metadata": json.dumps(metadata or {}),
                        "executed_at": datetime.utcnow(),
                    },
                )
                conn.commit()

            logger.debug(
                f"Action marked as {status}: ticket={ticket_id}, "
                f"action={action_id}, type={action_type}"
            )
            return True

        except IntegrityError as e:
            logger.warning(
                f"Action already exists (concurrent execution?): "
                f"ticket={ticket_id}, action={action_id}"
            )
            return True  # Already recorded, so idempotency is maintained

        except SQLAlchemyError as e:
            logger.error(f"Error recording action execution: {e}")
            return False

    def get_ticket_actions(self, ticket_id: int) -> List[Dict[str, Any]]:
        """
        Get all actions executed on a ticket.

        Args:
            ticket_id: The TeamDynamix ticket ID

        Returns:
            List of action records as dictionaries
        """
        try:
            query = text("""
                SELECT
                    log_id,
                    ticket_id,
                    action_type,
                    action_id,
                    action_hash,
                    status,
                    error_message,
                    metadata,
                    executed_at
                FROM meta.daemon_action_log
                WHERE ticket_id = :ticket_id
                ORDER BY executed_at DESC
            """)

            with self.db.engine.connect() as conn:
                result = conn.execute(query, {"ticket_id": ticket_id})
                rows = result.fetchall()

                actions = []
                for row in rows:
                    actions.append(
                        {
                            "log_id": str(row[0]),
                            "ticket_id": row[1],
                            "action_type": row[2],
                            "action_id": row[3],
                            "action_hash": row[4],
                            "status": row[5],
                            "error_message": row[6],
                            "metadata": row[7],
                            "executed_at": row[8],
                        }
                    )

                return actions

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving ticket actions: {e}")
            return []

    def get_action_stats(self, action_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about action execution.

        Args:
            action_type: Optional filter by action type

        Returns:
            Dictionary with action statistics
        """
        try:
            if action_type:
                query = text("""
                    SELECT
                        action_type,
                        status,
                        COUNT(*) as count,
                        MAX(executed_at) as last_executed,
                        MIN(executed_at) as first_executed
                    FROM meta.daemon_action_log
                    WHERE action_type = :action_type
                    GROUP BY action_type, status
                """)
                params = {"action_type": action_type}
            else:
                query = text("""
                    SELECT
                        action_type,
                        status,
                        COUNT(*) as count,
                        MAX(executed_at) as last_executed,
                        MIN(executed_at) as first_executed
                    FROM meta.daemon_action_log
                    GROUP BY action_type, status
                """)
                params = {}

            with self.db.engine.connect() as conn:
                result = conn.execute(query, params)
                rows = result.fetchall()

                stats = {
                    "total_actions": 0,
                    "by_type": {},
                    "by_status": {"completed": 0, "failed": 0, "skipped": 0},
                }

                for row in rows:
                    act_type, status, count, last_exec, first_exec = row

                    stats["total_actions"] += count
                    stats["by_status"][status] = (
                        stats["by_status"].get(status, 0) + count
                    )

                    if act_type not in stats["by_type"]:
                        stats["by_type"][act_type] = {
                            "total": 0,
                            "completed": 0,
                            "failed": 0,
                            "skipped": 0,
                            "last_executed": None,
                            "first_executed": None,
                        }

                    stats["by_type"][act_type]["total"] += count
                    stats["by_type"][act_type][status] = count
                    stats["by_type"][act_type]["last_executed"] = last_exec
                    stats["by_type"][act_type]["first_executed"] = first_exec

                return stats

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving action statistics: {e}")
            return {
                "total_actions": 0,
                "by_type": {},
                "by_status": {"completed": 0, "failed": 0, "skipped": 0},
            }

    def clear_failed_actions(self, ticket_id: Optional[int] = None) -> int:
        """
        Clear failed action records to allow retry.

        Args:
            ticket_id: Optional ticket ID to clear failed actions for.
                      If None, clears all failed actions.

        Returns:
            Number of records cleared
        """
        try:
            if ticket_id:
                query = text("""
                    DELETE FROM meta.daemon_action_log
                    WHERE ticket_id = :ticket_id AND status = 'failed'
                """)
                params = {"ticket_id": ticket_id}
            else:
                query = text("""
                    DELETE FROM meta.daemon_action_log
                    WHERE status = 'failed'
                """)
                params = {}

            with self.db.engine.connect() as conn:
                result = conn.execute(query, params)
                deleted_count = result.rowcount
                conn.commit()

            logger.info(f"Cleared {deleted_count} failed action records")
            return deleted_count

        except SQLAlchemyError as e:
            logger.error(f"Error clearing failed actions: {e}")
            return 0
