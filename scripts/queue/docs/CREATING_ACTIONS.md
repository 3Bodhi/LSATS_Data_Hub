# Creating Custom Actions for Ticket Queue Daemon

A comprehensive guide to designing and implementing new actions for the TeamDynamix Ticket Queue Daemon.

## Table of Contents

- [Overview](#overview)
- [Action Design Principles](#action-design-principles)
- [Action Anatomy](#action-anatomy)
- [Step-by-Step Implementation](#step-by-step-implementation)
- [Action Patterns & Examples](#action-patterns--examples)
- [Testing Actions](#testing-actions)
- [Best Practices](#best-practices)
- [Common Pitfalls](#common-pitfalls)

## Overview

Actions are self-contained, idempotent operations that execute on TeamDynamix tickets. They inherit from `BaseAction` and implement three core methods:

1. **`get_action_type()`**: Return action type identifier string
2. **`get_action_config()`**: Return configuration dict for content hashing
3. **`execute_action()`**: Perform the actual operation

The `BaseAction` framework handles:
- Idempotency checking (has this action already run?)
- State tracking (record execution in database)
- Error handling (catch exceptions, log errors)
- Context sharing (communicate with other actions)

## Action Design Principles

### 1. Idempotency

**Actions must be safe to execute multiple times.**

**Good** (idempotent):
```python
# Add asset to ticket (TDX prevents duplicates)
facade.tickets.add_ticket_asset(ticket_id, asset_id)

# Set ticket status (setting same status twice is safe)
facade.tickets.update_ticket(id=ticket_id, status=45)
```

**Bad** (not idempotent):
```python
# Increment a counter (would increment on every retry)
current_count = get_custom_field(ticket_id, "CounterField")
set_custom_field(ticket_id, "CounterField", current_count + 1)

# Append to description (would append multiple times)
ticket = facade.tickets.get_ticket(ticket_id)
new_desc = ticket['Description'] + "\nProcessed by automation"
facade.tickets.update_ticket(id=ticket_id, description=new_desc)
```

**How to fix non-idempotent operations**:
- Use database state tracking to record what you've done
- Check current state before making changes
- Use TDX's built-in deduplication (assets, responsible users, etc.)

### 2. Single Responsibility

**Each action should do one thing well.**

**Good**:
```python
AddAssetAction       # Only adds assets
CommentAction        # Only adds comments
ChangeStatusAction   # Only changes status
```

**Bad**:
```python
CompleteWorkflowAction  # Adds assets, posts comment, changes status, assigns ticket
                        # (should be 4 separate actions)
```

**Benefits of single responsibility**:
- Easier to test and debug
- Reusable in different workflows
- Clear execution logs (one line per action)
- Granular idempotency (each operation tracked separately)

### 3. Configuration Over Hard-Coding

**Make actions configurable, not specialized.**

**Good**:
```python
class CommentAction(BaseAction):
    def __init__(self, comment_text, is_private=False, **kwargs):
        self.comment_text = comment_text
        self.is_private = is_private
        
# Usage: Different comments for different workflows
actions = [
    CommentAction(comment_text="Welcome!", is_private=False),
    CommentAction(comment_text="Internal note", is_private=True),
]
```

**Bad**:
```python
class WelcomeCommentAction(BaseAction):
    def execute_action(self, ...):
        comment = "Welcome!"  # Hard-coded
        
class InternalNoteAction(BaseAction):
    def execute_action(self, ...):
        comment = "Internal note"  # Hard-coded
```

### 4. Context Awareness

**Use `action_context` to share data between actions.**

Actions execute in a pipeline. Later actions can read data written by earlier actions:

```python
# Action 1: Collects data
class AnalyzeTicketAction(BaseAction):
    def execute_action(self, ticket_id, facade, dry_run, action_context):
        # Analyze ticket...
        priority = self._calculate_priority(ticket)
        
        # Write to context for later actions
        action_context['calculated_priority'] = priority
        action_context['summaries'].append(f"Priority calculated: {priority}")
        
        return {"success": True, "summary": f"Calculated priority: {priority}"}

# Action 2: Uses data from Action 1
class AssignByPriorityAction(BaseAction):
    def execute_action(self, ticket_id, facade, dry_run, action_context):
        # Read from context
        priority = action_context.get('calculated_priority', 'normal')
        
        if priority == 'high':
            assign_to = "senior-team"
        else:
            assign_to = "general-queue"
        
        # Assign ticket...
        return {"success": True, "summary": f"Assigned to {assign_to}"}
```

## Action Anatomy

### Basic Structure

```python
"""
Brief description of what this action does.
"""

import logging
from typing import Any, Dict, Optional

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from .base_action import BaseAction

logger = logging.getLogger(__name__)


class YourAction(BaseAction):
    """
    Detailed description of action behavior.
    
    Configuration parameters:
    - param1: Description of param1
    - param2: Description of param2
    """
    
    def __init__(
        self,
        param1: str,
        param2: int = 10,
        version: str = "v1",
        **kwargs
    ):
        """
        Initialize action.
        
        Args:
            param1: Description
            param2: Description (default: 10)
            version: Action version (increment when behavior changes)
            **kwargs: Additional parameters passed to base class
        """
        super().__init__(version=version, **kwargs)
        
        self.param1 = param1
        self.param2 = param2
    
    def get_action_type(self) -> str:
        """Get action type identifier."""
        return "your_action"  # Lowercase, underscores
    
    def get_action_config(self) -> Dict[str, Any]:
        """Get configuration for content hashing."""
        return {
            "param1": self.param1,
            "param2": self.param2,
        }
    
    def execute_action(
        self,
        ticket_id: int,
        facade: TeamDynamixFacade,
        dry_run: bool = False,
        action_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the action on a ticket.
        
        Args:
            ticket_id: The TeamDynamix ticket ID
            facade: TeamDynamixFacade for API operations
            dry_run: If True, simulate without making changes
            action_context: Shared context dict for cross-action communication
        
        Returns:
            Dictionary with execution results:
            {
                'success': bool,
                'message': str,
                'summary': str (optional - added to action_context['summaries']),
                'details': dict (optional),
            }
        """
        try:
            # Your implementation here
            
            if dry_run:
                logger.info(f"[DRY RUN] Would execute action on ticket {ticket_id}")
                return {
                    "success": True,
                    "message": "Dry run - no changes made",
                }
            
            # Actual execution
            result = self._do_work(ticket_id, facade)
            
            return {
                "success": True,
                "message": "Action completed successfully",
                "summary": "Summary for cumulative comment",  # Optional
                "details": {"key": "value"},  # Optional
            }
            
        except Exception as e:
            logger.exception(f"Error executing action on ticket {ticket_id}")
            return {
                "success": False,
                "message": f"Error: {str(e)}",
                "details": {"error": str(e), "error_type": type(e).__name__},
            }
    
    def _do_work(self, ticket_id: int, facade: TeamDynamixFacade):
        """Private helper method for actual work."""
        # Implementation...
        pass
    
    def __repr__(self) -> str:
        """String representation for logging."""
        return (
            f"YourAction(action_id='{self.get_action_id()}', "
            f"param1='{self.param1}')"
        )
```

### Required Methods

#### 1. `get_action_type() -> str`

Returns a unique identifier for this action type.

**Requirements**:
- Lowercase, underscores only (e.g., `"add_asset"`, `"change_status"`)
- Must be unique across all action types
- Should be descriptive (avoid generic names like `"action1"`)

**Examples**:
```python
def get_action_type(self) -> str:
    return "add_asset"          # Good
    return "comment"            # Good
    return "assign_by_priority" # Good
    
    return "AddAsset"           # Bad - uppercase
    return "add-asset"          # Bad - hyphens
    return "process"            # Bad - too generic
```

#### 2. `get_action_config() -> Dict[str, Any]`

Returns all configuration parameters that define the action's behavior.

**Purpose**: Used for content hashing to detect configuration changes.

**Requirements**:
- Include ALL parameters that affect action behavior
- Values must be JSON-serializable (str, int, bool, list, dict)
- Use sorted lists/dicts for consistent hashing
- Don't include runtime state or database connections

**Examples**:
```python
def get_action_config(self) -> Dict[str, Any]:
    return {
        "comment_text": self.comment_text,       # String parameter
        "is_private": self.is_private,           # Boolean parameter
        "max_retries": self.max_retries,         # Integer parameter
        "notify": sorted(self.notify),           # List (sorted for consistency)
        "has_database": self.db_url is not None, # Boolean flag (not the URL itself)
    }
```

**What NOT to include**:
```python
def get_action_config(self) -> Dict[str, Any]:
    return {
        "database_url": self.database_url,  # Bad - credentials
        "db_adapter": self.db_adapter,      # Bad - runtime object
        "facade": self.facade,              # Bad - API client
        "last_run_time": datetime.now(),    # Bad - runtime state
    }
```

#### 3. `execute_action(...) -> Dict[str, Any]`

Performs the actual action on a ticket.

**Return Value Structure**:
```python
{
    # Required
    "success": bool,        # True if action succeeded, False otherwise
    "message": str,         # Human-readable result message
    
    # Optional
    "summary": str,         # One-line summary for cumulative comment
    "details": dict,        # Additional structured data (logged to database)
}
```

**Example Return Values**:
```python
# Success with summary
{
    "success": True,
    "message": "Added 3 assets to ticket 12345",
    "summary": "Added 3 assets: MAC-01, MAC-02, MAC-03",
    "details": {
        "asset_ids": [101, 102, 103],
        "asset_names": ["MAC-01", "MAC-02", "MAC-03"],
    }
}

# Success without summary
{
    "success": True,
    "message": "Ticket status unchanged (already In Progress)",
}

# Failure
{
    "success": False,
    "message": "Failed to add asset: Asset ID 999 not found",
    "details": {
        "error": "Asset not found",
        "asset_id": 999,
    }
}
```

## Step-by-Step Implementation

### Step 1: Plan Your Action

**Questions to answer**:

1. **What does this action do?** (Single responsibility)
   - "Adds computer assets to tickets based on regex-extracted identifiers"
   - "Posts a welcome comment to new tickets"
   - "Assigns tickets to teams based on custom field values"

2. **What are the configuration parameters?**
   - Comment text, privacy setting, HTML flag
   - Status ID, cascade setting
   - Search patterns, limits, filters

3. **Is it idempotent?**
   - Will it cause problems if run twice?
   - Does TDX API prevent duplicates, or do I need to check?

4. **Does it need to share data with other actions?**
   - Does it produce summaries for SummaryCommentAction?
   - Does it need data from previous actions?

5. **What errors can occur?**
   - Network/API errors (retryable)
   - Invalid configuration (non-retryable)
   - Ticket not found (non-retryable)

### Step 2: Create the File

Create `scripts/queue/actions/your_action.py`:

```python
"""
Brief description of your action.
"""

import logging
from typing import Any, Dict, Optional

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from .base_action import BaseAction

logger = logging.getLogger(__name__)


class YourAction(BaseAction):
    """Action class docstring."""
    
    # Implementation...
```

### Step 3: Implement `__init__`

Define configuration parameters:

```python
def __init__(
    self,
    required_param: str,
    optional_param: int = 10,
    flag_param: bool = False,
    version: str = "v1",
    **kwargs
):
    """
    Initialize action.
    
    Args:
        required_param: Description of required parameter
        optional_param: Description with default (default: 10)
        flag_param: Boolean flag (default: False)
        version: Action version (increment when behavior changes)
        **kwargs: Additional parameters passed to base class
    """
    super().__init__(version=version, **kwargs)
    
    # Store all configuration parameters as instance variables
    self.required_param = required_param
    self.optional_param = optional_param
    self.flag_param = flag_param
    
    # Initialize helper objects if needed
    if some_condition:
        self.helper = SomeHelper()
```

### Step 4: Implement Required Methods

```python
def get_action_type(self) -> str:
    """Get action type identifier."""
    return "your_action"  # Lowercase, underscores

def get_action_config(self) -> Dict[str, Any]:
    """Get configuration for content hashing."""
    return {
        "required_param": self.required_param,
        "optional_param": self.optional_param,
        "flag_param": self.flag_param,
    }

def execute_action(
    self,
    ticket_id: int,
    facade: TeamDynamixFacade,
    dry_run: bool = False,
    action_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute the action."""
    try:
        # Get ticket details
        ticket = facade.tickets.get_ticket(ticket_id)
        if not ticket:
            return {
                "success": False,
                "message": f"Ticket {ticket_id} not found",
            }
        
        # Dry run check
        if dry_run:
            logger.info(f"[DRY RUN] Would execute action on ticket {ticket_id}")
            return {
                "success": True,
                "message": "Dry run - no changes made",
            }
        
        # Actual execution
        result = self._do_your_work(ticket_id, facade, action_context)
        
        return {
            "success": True,
            "message": "Action completed",
            "summary": "One-line summary for cumulative comment",
            "details": result,
        }
        
    except Exception as e:
        logger.exception(f"Error executing action on ticket {ticket_id}")
        return {
            "success": False,
            "message": f"Error: {str(e)}",
        }
```

### Step 5: Add Helper Methods

Break complex logic into private methods:

```python
def _do_your_work(
    self,
    ticket_id: int,
    facade: TeamDynamixFacade,
    action_context: Dict[str, Any]
) -> Dict[str, Any]:
    """Main work logic."""
    # Step 1: Gather data
    data = self._gather_data(ticket_id, facade)
    
    # Step 2: Process data
    processed = self._process_data(data)
    
    # Step 3: Apply changes
    result = self._apply_changes(ticket_id, facade, processed)
    
    return result

def _gather_data(self, ticket_id: int, facade: TeamDynamixFacade) -> Dict[str, Any]:
    """Gather necessary data from ticket/API."""
    # Implementation...
    pass

def _process_data(self, data: Dict[str, Any]) -> Any:
    """Process/transform data."""
    # Implementation...
    pass

def _apply_changes(
    self,
    ticket_id: int,
    facade: TeamDynamixFacade,
    changes: Any
) -> Dict[str, Any]:
    """Apply changes to ticket."""
    # Implementation...
    pass
```

### Step 6: Update `__init__.py`

Add your action to `scripts/queue/actions/__init__.py`:

```python
from .base_action import BaseAction
from .add_asset_action import AddAssetAction
from .comment_action import CommentAction
from .summary_comment_action import SummaryCommentAction
from .your_action import YourAction  # Add this line

__all__ = [
    "BaseAction",
    "AddAssetAction",
    "CommentAction",
    "SummaryCommentAction",
    "YourAction",  # Add this line
]
```

### Step 7: Configure in Daemon

Add to `ticket_queue_daemon.py:main()`:

```python
from scripts.queue.actions import (
    AddAssetAction,
    CommentAction,
    SummaryCommentAction,
    YourAction,  # Import your action
)

# Configure actions
actions = [
    YourAction(
        required_param="value",
        optional_param=20,
        flag_param=True,
        version="v1",
    ),
    # ... other actions
]
```

## Action Patterns & Examples

### Pattern 1: Simple API Call Action

**Use case**: Single TDX API operation with minimal logic.

**Example**: Change ticket status

```python
class ChangeStatusAction(BaseAction):
    """Changes ticket status."""
    
    def __init__(self, new_status_id: int, cascade: bool = False, version: str = "v1", **kwargs):
        super().__init__(version=version, **kwargs)
        self.new_status_id = new_status_id
        self.cascade = cascade
    
    def get_action_type(self) -> str:
        return "change_status"
    
    def get_action_config(self) -> Dict[str, Any]:
        return {
            "new_status_id": self.new_status_id,
            "cascade": self.cascade,
        }
    
    def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
        try:
            if dry_run:
                return {
                    "success": True,
                    "message": f"[DRY RUN] Would change status to {self.new_status_id}",
                }
            
            result = facade.tickets.update_ticket(
                id=ticket_id,
                status=self.new_status_id,
                cascade=self.cascade,
                comments="",  # No comment
                notify=["null"],
            )
            
            return {
                "success": True,
                "message": f"Changed status to {self.new_status_id}",
                "summary": f"Status changed to {self.new_status_id}",
            }
            
        except Exception as e:
            return {"success": False, "message": f"Error: {str(e)}"}
```

### Pattern 2: Conditional Logic Action

**Use case**: Different behavior based on ticket state.

**Example**: Assign ticket based on priority

```python
class AssignByPriorityAction(BaseAction):
    """Assigns tickets to teams based on priority."""
    
    def __init__(
        self,
        high_priority_group_id: int,
        normal_priority_group_id: int,
        version: str = "v1",
        **kwargs
    ):
        super().__init__(version=version, **kwargs)
        self.high_priority_group_id = high_priority_group_id
        self.normal_priority_group_id = normal_priority_group_id
    
    def get_action_type(self) -> str:
        return "assign_by_priority"
    
    def get_action_config(self) -> Dict[str, Any]:
        return {
            "high_priority_group_id": self.high_priority_group_id,
            "normal_priority_group_id": self.normal_priority_group_id,
        }
    
    def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
        try:
            # Get ticket details
            ticket = facade.tickets.get_ticket(ticket_id)
            if not ticket:
                return {"success": False, "message": "Ticket not found"}
            
            # Determine priority
            priority_id = ticket.get("PriorityID", 0)
            
            # Select group based on priority
            if priority_id >= 4:  # High or Critical
                group_id = self.high_priority_group_id
                team_name = "Senior Team"
            else:
                group_id = self.normal_priority_group_id
                team_name = "General Team"
            
            if dry_run:
                return {
                    "success": True,
                    "message": f"[DRY RUN] Would assign to {team_name} (group {group_id})",
                }
            
            # Assign ticket
            result = facade.tickets.update_ticket(
                id=ticket_id,
                responsible_group_id=group_id,
                comments="",
                notify=["null"],
            )
            
            return {
                "success": True,
                "message": f"Assigned to {team_name}",
                "summary": f"Assigned to {team_name} based on priority",
            }
            
        except Exception as e:
            return {"success": False, "message": f"Error: {str(e)}"}
```

### Pattern 3: Multi-Step Action with Database

**Use case**: Complex logic requiring external data.

**Example**: Add related tickets as references

```python
class AddRelatedTicketsAction(BaseAction):
    """Finds and links related tickets based on shared assets."""
    
    def __init__(
        self,
        database_url: str,
        max_related: int = 5,
        version: str = "v1",
        **kwargs
    ):
        super().__init__(version=version, **kwargs)
        self.database_url = database_url
        self.max_related = max_related
        
        # Initialize database adapter
        from database.adapters.postgres_adapter import PostgresAdapter
        self.db = PostgresAdapter(database_url)
    
    def get_action_type(self) -> str:
        return "add_related_tickets"
    
    def get_action_config(self) -> Dict[str, Any]:
        return {
            "max_related": self.max_related,
            "has_database": True,  # Don't include URL
        }
    
    def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
        try:
            # Step 1: Get ticket assets
            ticket_assets = facade.tickets.get_ticket_assets(ticket_id) or []
            asset_ids = [a.get("ID") for a in ticket_assets]
            
            if not asset_ids:
                return {
                    "success": True,
                    "message": "No assets on ticket, cannot find related tickets",
                }
            
            # Step 2: Find related tickets from database
            related_tickets = self._find_related_tickets(ticket_id, asset_ids)
            
            if not related_tickets:
                return {
                    "success": True,
                    "message": "No related tickets found",
                }
            
            # Step 3: Add references
            added_count = 0
            for related_id in related_tickets[: self.max_related]:
                if dry_run:
                    logger.info(f"[DRY RUN] Would link ticket {related_id}")
                    added_count += 1
                else:
                    # Add ticket reference (implementation depends on TDX API)
                    # facade.tickets.add_related_ticket(ticket_id, related_id)
                    added_count += 1
            
            return {
                "success": True,
                "message": f"Added {added_count} related ticket reference(s)",
                "summary": f"Linked {added_count} related tickets with shared assets",
            }
            
        except Exception as e:
            logger.exception("Error adding related tickets")
            return {"success": False, "message": f"Error: {str(e)}"}
    
    def _find_related_tickets(self, ticket_id: int, asset_ids: list) -> list:
        """Query database for tickets with same assets."""
        query = """
            SELECT DISTINCT
                (raw_data->>'TicketID')::int as ticket_id
            FROM bronze.raw_entities
            WHERE entity_type = 'ticket_asset'
            AND source_system = 'tdx'
            AND (raw_data->>'AssetID')::int = ANY(:asset_ids)
            AND (raw_data->>'TicketID')::int != :ticket_id
            ORDER BY ticket_id DESC
            LIMIT :max_limit
        """
        
        df = self.db.query_to_dataframe(query, {
            "asset_ids": asset_ids,
            "ticket_id": ticket_id,
            "max_limit": self.max_related * 2,  # Get extras for filtering
        })
        
        return df["ticket_id"].tolist() if not df.empty else []
```

### Pattern 4: Context-Dependent Action

**Use case**: Action behavior depends on previous actions.

**Example**: Post different comments based on what assets were added

```python
class AssetAddedCommentAction(BaseAction):
    """Posts a comment based on assets added by previous action."""
    
    def __init__(
        self,
        template_multiple: str = "We added {count} computers to your ticket: {names}",
        template_single: str = "We added your computer ({name}) to this ticket.",
        template_none: str = "We could not automatically identify your computer.",
        version: str = "v1",
        **kwargs
    ):
        super().__init__(version=version, **kwargs)
        self.template_multiple = template_multiple
        self.template_single = template_single
        self.template_none = template_none
    
    def get_action_type(self) -> str:
        return "asset_added_comment"
    
    def get_action_config(self) -> Dict[str, Any]:
        return {
            "template_multiple": self.template_multiple,
            "template_single": self.template_single,
            "template_none": self.template_none,
        }
    
    def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
        try:
            # Read context from previous AddAssetAction
            if action_context is None:
                action_context = {}
            
            # Look for asset information in context
            asset_names = []
            for summary in action_context.get("summaries", []):
                # Parse summaries for asset names
                # (This is simplified - real implementation would be more robust)
                if "Added" in summary and "asset" in summary:
                    # Extract asset names from summary
                    pass
            
            # Alternative: AddAssetAction could write structured data to context
            assets_added = action_context.get("assets_added", [])
            asset_count = len(assets_added)
            
            # Build comment based on count
            if asset_count == 0:
                comment = self.template_none
            elif asset_count == 1:
                comment = self.template_single.format(name=assets_added[0])
            else:
                names = ", ".join(assets_added[:3])
                if asset_count > 3:
                    names += f" (and {asset_count - 3} more)"
                comment = self.template_multiple.format(count=asset_count, names=names)
            
            if dry_run:
                return {
                    "success": True,
                    "message": f"[DRY RUN] Would post: {comment}",
                }
            
            # Post comment
            result = facade.tickets.update_ticket(
                id=ticket_id,
                comments=comment,
                private=False,
                notify=["null"],
            )
            
            return {
                "success": True,
                "message": "Posted asset comment",
            }
            
        except Exception as e:
            return {"success": False, "message": f"Error: {str(e)}"}
```

## Testing Actions

### Unit Testing

Create `tests/queue/test_your_action.py`:

```python
import pytest
from unittest.mock import Mock, MagicMock
from scripts.queue.actions.your_action import YourAction


class TestYourAction:
    """Tests for YourAction."""
    
    def test_action_type(self):
        """Test action type identifier."""
        action = YourAction(required_param="test")
        assert action.get_action_type() == "your_action"
    
    def test_action_config(self):
        """Test configuration hashing."""
        action = YourAction(
            required_param="test",
            optional_param=20,
            flag_param=True,
        )
        
        config = action.get_action_config()
        assert config["required_param"] == "test"
        assert config["optional_param"] == 20
        assert config["flag_param"] is True
    
    def test_action_id_changes_with_config(self):
        """Test that action ID changes when config changes."""
        action1 = YourAction(required_param="test1")
        action2 = YourAction(required_param="test2")
        
        assert action1.get_action_id() != action2.get_action_id()
    
    def test_action_id_same_for_same_config(self):
        """Test that action ID is stable for same config."""
        action1 = YourAction(required_param="test")
        action2 = YourAction(required_param="test")
        
        assert action1.get_action_id() == action2.get_action_id()
    
    def test_execute_dry_run(self):
        """Test dry run mode."""
        action = YourAction(required_param="test")
        facade_mock = Mock()
        
        result = action.execute_action(
            ticket_id=12345,
            facade=facade_mock,
            dry_run=True,
        )
        
        assert result["success"] is True
        assert "DRY RUN" in result["message"]
        # Verify no API calls were made
        facade_mock.tickets.update_ticket.assert_not_called()
    
    def test_execute_success(self):
        """Test successful execution."""
        action = YourAction(required_param="test")
        
        # Mock facade
        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
        }
        facade_mock.tickets.update_ticket.return_value = {"success": True}
        
        result = action.execute_action(
            ticket_id=12345,
            facade=facade_mock,
            dry_run=False,
        )
        
        assert result["success"] is True
        facade_mock.tickets.update_ticket.assert_called_once()
    
    def test_execute_ticket_not_found(self):
        """Test behavior when ticket doesn't exist."""
        action = YourAction(required_param="test")
        
        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = None
        
        result = action.execute_action(
            ticket_id=99999,
            facade=facade_mock,
            dry_run=False,
        )
        
        assert result["success"] is False
        assert "not found" in result["message"].lower()
    
    def test_execute_api_error(self):
        """Test error handling."""
        action = YourAction(required_param="test")
        
        facade_mock = Mock()
        facade_mock.tickets.get_ticket.side_effect = Exception("API Error")
        
        result = action.execute_action(
            ticket_id=12345,
            facade=facade_mock,
            dry_run=False,
        )
        
        assert result["success"] is False
        assert "error" in result["message"].lower()
```

### Integration Testing

Test with actual TDX sandbox instance:

```python
"""
Integration tests for YourAction.

Requires:
- TDX sandbox instance configured in .env
- Test ticket ID
"""

import os
import pytest
from dotenv import load_dotenv
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from scripts.queue.actions.your_action import YourAction

load_dotenv()


@pytest.fixture
def facade():
    """Create TDX facade for testing."""
    return TeamDynamixFacade(
        os.getenv("TDX_BASE_URL"),
        os.getenv("TDX_APP_ID"),
        os.getenv("TDX_API_TOKEN"),
    )


@pytest.fixture
def test_ticket_id():
    """Test ticket ID (set this to a real ticket in your sandbox)."""
    return int(os.getenv("TEST_TICKET_ID", "12345"))


def test_your_action_integration(facade, test_ticket_id):
    """Test action on real TDX instance."""
    action = YourAction(required_param="integration_test")
    
    # Execute action
    result = action.execute_action(
        ticket_id=test_ticket_id,
        facade=facade,
        dry_run=False,
    )
    
    # Verify result
    assert result["success"] is True
    
    # Verify changes in TDX
    ticket = facade.tickets.get_ticket(test_ticket_id)
    # Add assertions based on expected changes
```

### Manual Testing

Test interactively in Python REPL:

```python
# Start Python in project directory
python

from dotenv import load_dotenv
import os
load_dotenv()

from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from scripts.queue.actions.your_action import YourAction

# Initialize facade
facade = TeamDynamixFacade(
    os.getenv("TDX_BASE_URL"),
    os.getenv("TDX_APP_ID"),
    os.getenv("TDX_API_TOKEN"),
)

# Create action
action = YourAction(required_param="manual_test")

# Test on a ticket (dry run first)
result = action.execute_action(
    ticket_id=12345,
    facade=facade,
    dry_run=True,
)

print(result)

# If dry run looks good, try for real
result = action.execute_action(
    ticket_id=12345,
    facade=facade,
    dry_run=False,
)

print(result)
```

## Best Practices

### 1. Always Support Dry Run

```python
def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
    # Check dry_run BEFORE making any changes
    if dry_run:
        logger.info(f"[DRY RUN] Would do X, Y, Z on ticket {ticket_id}")
        return {
            "success": True,
            "message": "Dry run - preview of what would happen",
            "details": {"preview": "..."},
        }
    
    # Actual execution only happens if dry_run=False
    result = facade.tickets.update_ticket(...)
```

### 2. Log Extensively

```python
def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
    logger.info(f"Starting action on ticket {ticket_id}")
    
    try:
        ticket = facade.tickets.get_ticket(ticket_id)
        logger.debug(f"Ticket title: {ticket.get('Title')}")
        
        # ... do work
        
        logger.info(f"Successfully completed action on ticket {ticket_id}")
        return {"success": True, "message": "..."}
        
    except Exception as e:
        logger.exception(f"Error on ticket {ticket_id}")
        return {"success": False, "message": str(e)}
```

### 3. Handle Missing Data Gracefully

```python
# Good - defensive programming
ticket = facade.tickets.get_ticket(ticket_id)
if not ticket:
    return {"success": False, "message": "Ticket not found"}

title = ticket.get("Title", "Unknown")  # Provide default
requestor = ticket.get("RequestorUid")  # May be None

if not requestor:
    logger.warning(f"Ticket {ticket_id} has no requestor")
    return {"success": True, "message": "Skipped - no requestor"}
```

### 4. Provide Detailed Return Values

```python
# Good - detailed result
return {
    "success": True,
    "message": "Added 3 assets to ticket 12345",
    "summary": "Added 3 assets: MAC-01, MAC-02, MAC-03",  # For cumulative comment
    "details": {
        "asset_ids": [101, 102, 103],
        "asset_names": ["MAC-01", "MAC-02", "MAC-03"],
        "phase": 1,  # Which discovery phase succeeded
        "identifiers_found": ["mac-01", "mac-02", "mac-03"],
    }
}

# Bad - minimal result
return {"success": True, "message": "Done"}
```

### 5. Use Type Hints

```python
from typing import Any, Dict, List, Optional

def execute_action(
    self,
    ticket_id: int,
    facade: TeamDynamixFacade,
    dry_run: bool = False,
    action_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute action with full type hints."""
    pass

def _helper_method(
    self,
    items: List[str],
    threshold: int,
) -> Optional[Dict[str, Any]]:
    """Helper method with type hints."""
    pass
```

### 6. Document Configuration

```python
class YourAction(BaseAction):
    """
    One-line description.
    
    Detailed description of what this action does and when to use it.
    
    Configuration parameters:
    - param1: Description (required)
    - param2: Description (default: 10)
    - flag: Description (default: False)
    
    Example:
        action = YourAction(
            param1="value",
            param2=20,
            flag=True,
            version="v1",
        )
    """
```

### 7. Version Strategically

**Increment version when**:
- Fixing bugs that should only apply to future tickets
- Changing behavior in non-backwards-compatible ways
- Adding features you don't want applied retroactively

**Don't increment version when**:
- Fixing bugs that affected all tickets (let it re-execute)
- Improving performance without changing behavior
- Adding optional parameters with defaults

## Common Pitfalls

### 1. Non-Idempotent Operations

**Problem**:
```python
# This will add duplicate comments if action runs twice
facade.tickets.update_ticket(
    id=ticket_id,
    comments="Automated message",
)
```

**Solution**: Rely on daemon's idempotency tracking (actions only run once per ticket+action_id)

---

### 2. Including Runtime State in Config Hash

**Problem**:
```python
def get_action_config(self):
    return {
        "database_url": self.database_url,  # Changes between dev/prod
        "current_time": datetime.now(),     # Always different
    }
```

**Solution**:
```python
def get_action_config(self):
    return {
        "has_database": self.database_url is not None,  # Boolean flag
        "max_retries": self.max_retries,  # Configuration, not state
    }
```

---

### 3. Forgetting Dry Run

**Problem**:
```python
def execute_action(self, ticket_id, facade, dry_run=False, action_context=None):
    # Missing dry_run check!
    result = facade.tickets.update_ticket(...)
```

**Solution**: Always check `dry_run` before making changes

---

### 4. Not Handling API Errors

**Problem**:
```python
def execute_action(self, ...):
    # No try/except - exceptions propagate
    result = facade.tickets.update_ticket(...)
```

**Solution**: Wrap in try/except and return error dict

---

### 5. Hardcoding Values

**Problem**:
```python
class WelcomeCommentAction(BaseAction):
    def execute_action(self, ...):
        comment = "Welcome to our ticketing system!"  # Hardcoded
```

**Solution**: Make it configurable
```python
class CommentAction(BaseAction):
    def __init__(self, comment_text, **kwargs):
        self.comment_text = comment_text
```

---

### 6. Overwriting action_context

**Problem**:
```python
def execute_action(self, ..., action_context=None):
    action_context = {}  # Overwrites shared context!
```

**Solution**:
```python
def execute_action(self, ..., action_context=None):
    if action_context is None:
        action_context = {}  # Only initialize if None
    
    # Now safe to read/write
    action_context["my_data"] = "value"
```

---

## Summary

Creating actions is straightforward when you follow these principles:

1. **Inherit from BaseAction** - Get idempotency, error handling, and context sharing for free
2. **Implement 3 methods** - `get_action_type()`, `get_action_config()`, `execute_action()`
3. **Make it configurable** - Parameters, not hard-coded values
4. **Support dry run** - Always check before making changes
5. **Log extensively** - Help future you debug issues
6. **Return detailed results** - Success/failure, messages, summaries, details
7. **Test thoroughly** - Unit tests, integration tests, manual tests
8. **Version strategically** - Increment when behavior changes

With these patterns and examples, you should be able to create robust, maintainable actions for any ticket automation workflow.

For questions or issues, consult the existing actions in `scripts/queue/actions/` for reference implementations.
