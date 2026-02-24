# TeamDynamix Ticket Queue Daemon

A robust, idempotent automation system for processing TeamDynamix tickets through configurable action pipelines.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Available Actions](#available-actions)
- [Usage Examples](#usage-examples)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Advanced Topics](#advanced-topics)

## Overview

The Ticket Queue Daemon monitors a TeamDynamix report and automatically executes configurable actions on tickets. It's designed for:

- **Computer compliance automation**: Automatically add computer assets to tickets
- **Ticket enrichment**: Add comments, change status, assign tickets
- **Workflow automation**: Multi-step ticket processing with dependencies
- **Audit compliance**: Complete execution history and idempotency guarantees

### What Makes This Different?

Unlike simple scripts that process tickets once, this daemon:

1. **Never duplicates work**: Idempotency tracking ensures actions only execute once per ticket
2. **Handles configuration changes**: Content-aware hashing detects when action config changes
3. **Recovers from failures**: Retryable errors automatically retry on next run
4. **Supports complex workflows**: Actions can share context and build cumulative results
5. **Provides full audit trail**: Complete execution history in PostgreSQL

## Key Features

- **Content-Aware Idempotency**: Actions use SHA256 hashing of configuration to detect changes
- **Action Pipeline**: Chain multiple actions together with shared context
- **Database State Tracking**: PostgreSQL-backed execution log (`meta.daemon_action_log`)
- **Dry Run Mode**: Preview actions without making changes
- **Daemon Mode**: Continuous polling with configurable intervals
- **Graceful Error Handling**: Retryable vs non-retryable error detection
- **Rich Logging**: Detailed execution logs with emoji indicators
- **Statistics & Monitoring**: Built-in metrics for actions executed/succeeded/failed

## Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────────┐
│                  Ticket Queue Daemon                         │
│  (Orchestrator - Fetches tickets, runs action pipeline)     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ├─────────────────────────────────────┐
                      │                                     │
         ┌────────────▼────────────┐         ┌─────────────▼──────────┐
         │    State Tracker        │         │   Action Framework      │
         │  (Idempotency via DB)   │         │  (Pluggable Actions)    │
         │                         │         │                         │
         │ - has_executed()        │         │ - BaseAction (abstract) │
         │ - mark_completed()      │         │ - AddAssetAction        │
         │ - get_action_stats()    │         │ - CommentAction         │
         │                         │         │ - SummaryCommentAction  │
         └─────────────────────────┘         └─────────────────────────┘
                      │
                      │
         ┌────────────▼────────────┐
         │ PostgreSQL Database     │
         │ meta.daemon_action_log  │
         └─────────────────────────┘
```

### Action Execution Flow

```
For each ticket in report:
  ┌──────────────────────────────────────────────────┐
  │ 1. Initialize action_context = {                 │
  │      "summaries": [],                            │
  │      "ticket_id": ticket_id,                     │
  │      "ticket_data": {...}                        │
  │    }                                             │
  └──────────────────┬───────────────────────────────┘
                     │
  ┌──────────────────▼───────────────────────────────┐
  │ 2. For each action in pipeline:                  │
  │    a. Generate action_id (type:hash:version)     │
  │    b. Check: has_executed(ticket_id, action_id)? │
  │       ├─ Yes → Skip (return "skipped")           │
  │       └─ No  → Continue                          │
  │    c. Execute action.execute_action()            │
  │    d. Add summary to action_context["summaries"] │
  │    e. Record in database (completed/failed)      │
  └──────────────────┬───────────────────────────────┘
                     │
  ┌──────────────────▼───────────────────────────────┐
  │ 3. Log results and statistics                    │
  └──────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

1. **Environment Variables** (in `.env`):
   ```bash
   # TeamDynamix Configuration
   TDX_BASE_URL=https://yourinstance.teamdynamix.com/TDWebApi
   TDX_APP_ID=12345
   TDX_API_TOKEN=your_token_here
   
   # Database Configuration
   DATABASE_URL=postgresql://lsats_user:password@localhost:5432/lsats_db
   
   # Daemon Configuration
   DAEMON_REPORT_ID=67890  # Optional: Default report ID
   ```

2. **Database Schema**: Ensure `meta.daemon_action_log` table exists (run migrations)

3. **Virtual Environment**:
   ```bash
   source venv/bin/activate
   ```

### Basic Usage

**Test with dry run** (recommended first step):
```bash
python scripts/queue/ticket_queue_daemon.py \
  --report-id 12345 \
  --dry-run \
  --log test_run.log \
  --log-level INFO
```

**Single execution** (process report once):
```bash
python scripts/queue/ticket_queue_daemon.py \
  --report-id 12345 \
  --log daemon.log
```

**Continuous daemon mode** (poll every 5 minutes):
```bash
python scripts/queue/ticket_queue_daemon.py \
  --report-id 12345 \
  --daemon \
  --interval 300 \
  --log daemon.log
```

### Production Deployment

For production use, run as a background service:

```bash
# Using nohup
nohup python scripts/queue/ticket_queue_daemon.py \
  --report-id 12345 \
  --daemon \
  --interval 300 \
  --log /var/log/tdx_daemon.log \
  --log-level INFO &

# Or using systemd (recommended)
# See deployment documentation for systemd unit file
```

## Configuration

### Command-Line Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--report-id` | Yes* | `DAEMON_REPORT_ID` env var | TeamDynamix report ID to monitor |
| `--daemon` | No | False | Run continuously as daemon |
| `--interval` | No | 300 | Polling interval in seconds (daemon mode only) |
| `--dry-run` | No | False | Simulate actions without making changes |
| `--log` | No | stdout only | Enable logging to file (specify path) |
| `--log-level` | No | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |

*Either `--report-id` or `DAEMON_REPORT_ID` environment variable must be set.

### Configuring Actions

Actions are configured in `ticket_queue_daemon.py:main()`:

```python
actions = [
    # Phase 1: Add computer assets to tickets
    AddAssetAction(
        add_summary_comment=True,          # Add summary for cumulative comment
        max_assets_to_add=10,              # Safety limit
        skip_if_requestor_asset_exists=True,  # Skip requestor fallback if assets exist
        active_status_only=True,           # Only active assets
        computer_form_id=2448,             # Computer Form ID
        database_url=DATABASE_URL,         # Use bronze layer for fast searches
        version="v2",
    ),
    
    # Phase 2: Post cumulative summary of all actions
    SummaryCommentAction(
        comment_prefix="🤖 Automated Actions Summary",
        is_private=True,                   # Private comment
        skip_if_empty=True,                # Only post if actions executed
        version="v2",
    ),
]
```

**Important**: If you change action configuration (e.g., modify `comment_prefix`), the action hash changes and the action will re-execute on all tickets. Increment `version` to prevent re-execution on old tickets.

## Available Actions

### 1. AddAssetAction

Automatically adds computer assets to tickets using intelligent discovery.

**Configuration Parameters**:
```python
AddAssetAction(
    add_summary_comment=True,          # Add summary to action_context
    max_assets_to_add=10,              # Maximum assets to add (safety limit)
    skip_if_requestor_asset_exists=True,  # Skip Phase 3 if ticket has assets
    active_status_only=True,           # Only search active assets
    computer_form_id=2448,             # Filter to Computer Form (None = all forms)
    database_url=DATABASE_URL,         # PostgreSQL URL for bronze layer queries
    version="v1",
)
```

**Logic Flow**:

```
Phase 1: Title & Description Analysis
  ├─ Extract identifiers using regex patterns:
  │    - Computer names: IC-EHLB760-F16, CHEM-SMALD1, psyc-gid08
  │    - Serial numbers: C02XJ0AXJGH5, R8Y123456789
  │
  ├─ Search bronze.raw_entities (fast database query)
  │    - Match on Name (case-insensitive)
  │    - Match on SerialNumber (case-insensitive)
  │
  ├─ Fallback to TDX API search if database empty
  │    - Search SerialLike field
  │    - Search SearchText field
  │
  ├─ Verify exact match (prevent fuzzy search false positives)
  │    - Asset Name must exactly match an identifier
  │    - OR SerialNumber must exactly match
  │    - OR Tag must exactly match
  │
  ├─ Filter out already-added assets
  │
  ├─ Add verified assets to ticket (up to max_assets_to_add)
  │
  └─ If assets added → SUCCESS (skip Phase 2 & 3)
      If no matches → Continue to Phase 2

Phase 2: Conversation Analysis (if Phase 1 found nothing)
  ├─ Fetch ticket conversation/feed (exclude system messages)
  │
  ├─ Extract identifiers from all conversation messages
  │
  ├─ Search, verify, add (same logic as Phase 1)
  │
  └─ If assets added → SUCCESS (skip Phase 3)
      If no matches → Continue to Phase 3

Phase 3: Requestor Fallback (if Phases 1 & 2 found nothing)
  ├─ Skip if ticket already has assets (configurable)
  │
  ├─ Get requestor's computer assets
  │
  ├─ If requestor has exactly 1 computer:
  │    └─ Add it to the ticket
  │
  └─ If 0 or 2+ computers → Skip (ambiguous)
```

**Output Summary Examples**:
- `"Added 2 assets: MAC-WARD04, PSYC-GID08. Reason: Computer identifier(s) found in ticket title/description (mac-ward04, psyc-gid08)"`
- `"Added 1 asset: IC-EHLB760-F16. Reason: User only has one computer"`
- `"No assets added. Reason: No computer identifiers found and user has 3 assets (expected exactly 1)"`

**Error Handling**:
- **Retryable errors** (5xx, timeout, network): Action stays "not executed", will retry next run
- **Non-retryable errors** (404, 400): Logs warning, continues with other assets
- **Individual asset failures**: Doesn't fail entire action (partial success allowed)

---

### 2. CommentAction

Adds a comment/feed entry to a ticket.

**Configuration Parameters**:
```python
CommentAction(
    comment_text="This is the comment message",
    is_private=False,               # Public or private comment
    is_rich_html=False,             # Plain text or HTML
    is_communication=False,         # Mark as communication record
    notify=["null"],                # Email addresses to notify (["null"] = default)
    new_status_id=0,                # Change status (0 = no change)
    cascade_status=False,           # Cascade status to child tickets
    version="v1",
)
```

**Logic Flow**:
```
1. Check idempotency (has this exact comment been posted?)
2. If already posted → Skip
3. If not posted:
   a. Get ticket details for logging
   b. Call facade.tickets.update_ticket() with comment
   c. Record execution in database
```

**Use Cases**:
- Standard response messages
- Status update notifications
- Automated follow-ups

**Example**:
```python
CommentAction(
    comment_text="We have automatically added your computer to this ticket. "
                 "Our team will review your request shortly.",
    is_private=False,
    version="v1",
)
```

---

### 3. SummaryCommentAction

Posts a cumulative summary of all actions executed in the current run.

**Configuration Parameters**:
```python
SummaryCommentAction(
    comment_prefix="🤖 Automated Actions Summary",
    is_private=True,                # Private or public comment
    is_rich_html=False,             # Plain text or HTML
    skip_if_empty=True,             # Skip if no actions have summaries
    separator="\n",                 # Separator between summaries
    version="v1",
)
```

**Logic Flow**:
```
1. Collect all summaries from action_context["summaries"]
   (Previous actions must set add_summary_comment=True)

2. If no summaries and skip_if_empty=True → Skip

3. Build comment:
   {comment_prefix}
   
   1. {summary_from_action_1}
   2. {summary_from_action_2}
   ...

4. Post comment to ticket

5. Mark as completed (but always re-execute on next run)
```

**Special Behavior**:
- **Always executes** (overrides default idempotency)
- Should be placed **last** in the action pipeline
- Only posts if previous actions added summaries

**Example Output**:
```
🤖 Automated Actions Summary

1. Added 2 assets: MAC-WARD04, PSYC-GID08. Reason: Computer identifier(s) found in ticket title/description (mac-ward04, psyc-gid08)
2. Changed ticket status to In Progress
```

---

## Usage Examples

### Example 1: Simple Asset Addition

Add assets to tickets in a compliance report:

```python
actions = [
    AddAssetAction(
        add_summary_comment=False,  # Don't need summary
        max_assets_to_add=5,
        database_url=DATABASE_URL,
        version="v1",
    ),
]
```

```bash
python scripts/queue/ticket_queue_daemon.py --report-id 12345 --dry-run
```

---

### Example 2: Asset Addition + Summary Comment

Add assets and post a summary:

```python
actions = [
    AddAssetAction(
        add_summary_comment=True,   # Generate summary for cumulative comment
        max_assets_to_add=10,
        database_url=DATABASE_URL,
        version="v2",
    ),
    SummaryCommentAction(
        comment_prefix="🤖 Automated Actions Summary",
        is_private=True,
        skip_if_empty=True,
        version="v2",
    ),
]
```

---

### Example 3: Multi-Step Workflow

Add assets, post a welcome comment, and change status:

```python
actions = [
    # Step 1: Add assets
    AddAssetAction(
        add_summary_comment=True,
        max_assets_to_add=10,
        database_url=DATABASE_URL,
        version="v1",
    ),
    
    # Step 2: Welcome message
    CommentAction(
        comment_text="Thank you for your request. We have automatically "
                     "added your computer(s) to this ticket.",
        is_private=False,
        version="v1",
    ),
    
    # Step 3: Change status to "In Progress"
    CommentAction(
        comment_text="",  # No comment text
        new_status_id=45,  # Status ID for "In Progress"
        version="v1",
    ),
    
    # Step 4: Post summary (private)
    SummaryCommentAction(
        comment_prefix="🤖 Internal Actions Log",
        is_private=True,
        version="v1",
    ),
]
```

---

### Example 4: Daemon Mode with Monitoring

Run continuously with detailed logging:

```bash
python scripts/queue/ticket_queue_daemon.py \
  --report-id 12345 \
  --daemon \
  --interval 300 \
  --log /var/log/tdx_daemon.log \
  --log-level DEBUG
```

**Monitor logs**:
```bash
tail -f /var/log/tdx_daemon.log
```

**Check statistics**:
```python
from database.adapters.postgres_adapter import PostgresAdapter
from scripts.queue.state.state_tracker import StateTracker

db = PostgresAdapter(DATABASE_URL)
tracker = StateTracker(db)

stats = tracker.get_action_stats()
print(f"Total actions: {stats['total_actions']}")
print(f"By status: {stats['by_status']}")
print(f"By type: {stats['by_type']}")
```

---

## Monitoring & Troubleshooting

### Database Queries

**View recent executions**:
```sql
SELECT 
    ticket_id,
    action_type,
    action_id,
    status,
    executed_at,
    error_message
FROM meta.daemon_action_log
ORDER BY executed_at DESC
LIMIT 50;
```

**Count actions by status**:
```sql
SELECT 
    action_type,
    status,
    COUNT(*) as count,
    MAX(executed_at) as last_executed
FROM meta.daemon_action_log
GROUP BY action_type, status
ORDER BY action_type, status;
```

**Find failed actions**:
```sql
SELECT 
    ticket_id,
    action_type,
    error_message,
    executed_at
FROM meta.daemon_action_log
WHERE status = 'failed'
ORDER BY executed_at DESC;
```

### Clearing Failed Actions for Retry

If you've fixed an issue and want to retry failed actions:

```python
from database.adapters.postgres_adapter import PostgresAdapter
from scripts.queue.state.state_tracker import StateTracker

db = PostgresAdapter(DATABASE_URL)
tracker = StateTracker(db)

# Clear all failed actions
cleared = tracker.clear_failed_actions()
print(f"Cleared {cleared} failed actions")

# Or clear for specific ticket
cleared = tracker.clear_failed_actions(ticket_id=12345)
print(f"Cleared {cleared} failed actions for ticket 12345")
```

### Common Issues

**Issue**: Actions re-execute on all tickets after config change

**Solution**: Increment the `version` parameter when changing action configuration:
```python
# Before
AddAssetAction(max_assets_to_add=10, version="v1")

# After (prevents re-execution on old tickets)
AddAssetAction(max_assets_to_add=15, version="v2")
```

---

**Issue**: Daemon not finding tickets

**Solution**: Verify report ID and permissions:
```bash
# Test report access
python -c "
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
import os
from dotenv import load_dotenv

load_dotenv()
facade = TeamDynamixFacade(
    os.getenv('TDX_BASE_URL'),
    os.getenv('TDX_APP_ID'),
    os.getenv('TDX_API_TOKEN')
)

report = facade.reports.get_report(id=12345, withData=True)
print(f'Report: {report.get(\"Name\")}')
print(f'Tickets: {len(report.get(\"DataRows\", []))}')
"
```

---

**Issue**: Database connection errors

**Solution**: Check PostgreSQL connection and schema:
```bash
# Test database connection
docker ps --filter "name=lsats-database" --format "{{.ID}}"

# Verify schema exists
docker exec -i <container_id> psql -U lsats_user -d lsats_db -c "
  SELECT table_name 
  FROM information_schema.tables 
  WHERE table_schema = 'meta' 
  AND table_name = 'daemon_action_log';
"
```

---

## Advanced Topics

### Idempotency Guarantees

The daemon provides two levels of idempotency:

1. **Configuration-based** (default):
   - Action ID includes content hash: `add_asset:abc123def456:v1`
   - If configuration changes (e.g., `max_assets_to_add` changes), hash changes
   - New action ID → Action re-executes on all tickets

2. **Version-based** (explicit):
   - Increment `version` parameter when changing behavior
   - Same configuration + new version = new action ID
   - Prevents re-execution on tickets processed by old version

**When to increment version**:
- Changing action logic/behavior
- Changing default parameters
- Bug fixes that should only apply to new tickets

**When NOT to increment version**:
- Fixing a bug that affected all tickets (let it re-execute)
- Adding new features you want applied retroactively

### Action Context Sharing

Actions communicate through `action_context` dict:

```python
# Custom action that reads context
class ConditionalCommentAction(BaseAction):
    def execute_action(self, ticket_id, facade, dry_run, action_context):
        # Read data from previous actions
        assets_added = action_context.get("assets_added", 0)
        
        if assets_added > 0:
            comment = f"We found and added {assets_added} computers to your ticket."
        else:
            comment = "Please provide your computer name or serial number."
        
        # Post comment...
        
        # Write data for next actions
        action_context["comment_posted"] = True
        
        return {"success": True, "message": "Posted comment"}
```

### Custom Action Development

See `CREATING_ACTIONS.md` for detailed guide on implementing custom actions.

### Performance Tuning

**Database-first searches** (recommended):
- AddAssetAction queries `bronze.raw_entities` before hitting TDX API
- 10-100x faster for large asset databases
- Requires bronze layer ingestion scripts running

**Batch processing**:
- Daemon processes all tickets in report sequentially
- For very large reports (1000+ tickets), consider:
  - Splitting into multiple smaller reports
  - Running multiple daemon instances with different reports
  - Increasing polling interval to reduce load

**API rate limiting**:
- TDX APIs have rate limits (varies by instance)
- Daemon includes exponential backoff for retries
- Consider adding delays between tickets for high-volume processing

### Security Considerations

**Private comments**:
- Use `is_private=True` for internal automation messages
- Public comments are visible to requestors and responsibles

**Database credentials**:
- Store `DATABASE_URL` in `.env` (not committed to git)
- Use read-only database user if possible (daemon doesn't modify bronze/silver)

**API token security**:
- `TDX_API_TOKEN` has full permissions of the owning user
- Use service account with minimal required permissions
- Rotate tokens periodically

### Extending the Framework

The action framework is designed for extensibility:

1. **New action types**: Inherit from `BaseAction`
2. **New state backends**: Implement `StateTracker` interface (e.g., Redis, filesystem)
3. **New orchestrators**: Use `BaseAction` with custom execution logic
4. **Action plugins**: Dynamically load actions from external modules

See `CREATING_ACTIONS.md` for implementation guides.

---

## Support & Contributing

For issues, questions, or feature requests, contact the LSATS Data Hub team.

**Related Documentation**:
- `CREATING_ACTIONS.md`: Guide for implementing custom actions
- `CLAUDE.md`: Overall project documentation
- `.claude/database_script_standards.md`: Database script patterns
