# TeamDynamix Ticket Queue Daemon

A robust, idempotent daemon service for monitoring TeamDynamix reports and executing automated actions on tickets. Built with database-backed state tracking for production reliability.

## Features

- **Idempotent Actions**: Actions execute exactly once per ticket using PostgreSQL state tracking
- **Content-Aware**: Action configuration changes trigger re-execution automatically
- **Automated Asset Association**: Intelligently adds computer assets by detecting identifiers in tickets
- **Cumulative Summaries**: Posts organized summaries of all actions in single comment
- **Flexible Deployment**: Run once or continuously as a daemon
- **Extensible Actions**: Easy to add new action types (status changes, assignments, etc.)
- **Production-Ready**: Error handling, logging, dry-run mode, and comprehensive monitoring
- **Database Integration**: Leverages existing medallion architecture (bronze layer queries + state tracking)

## Architecture

```
scripts/queue/
├── ticket_queue_daemon.py          # Main daemon entry point
├── ADD_ASSET_ACTION.md             # Detailed asset action documentation
├── IMPLEMENTATION_SUMMARY.md       # Original MVP implementation summary
├── QUICKSTART.md                   # Quick start guide
├── actions/
│   ├── base_action.py              # Abstract base class for actions
│   ├── comment_action.py           # Comment/feed entry action
│   ├── add_asset_action.py         # Automated asset association (NEW)
│   └── summary_comment_action.py   # Cumulative summary posting (NEW)
└── state/
    └── state_tracker.py            # PostgreSQL state tracking
```

### Database Schema

The daemon uses `meta.daemon_action_log` table:

```sql
CREATE TABLE meta.daemon_action_log (
    log_id UUID PRIMARY KEY,
    ticket_id INTEGER NOT NULL,
    action_type VARCHAR(100),
    action_id VARCHAR(255),        -- Format: {type}:{hash}:{version}
    action_hash VARCHAR(64),        -- SHA256 of action config
    status VARCHAR(20),             -- 'completed', 'failed', 'skipped'
    executed_at TIMESTAMP,
    error_message TEXT,
    metadata JSONB,
    UNIQUE (ticket_id, action_id)
);
```

## Installation

### 1. Run Database Migration

```bash
# Apply the daemon action log schema
docker exec -i $(docker ps --filter "name=lsats-database" --format "{{.ID}}") \
  psql -U lsats_user -d lsats_db -f /docker-entrypoint-initdb.d/migrations/008_add_daemon_action_log.sql
```

Or manually execute `/docker/postgres/migrations/008_add_daemon_action_log.sql`.

### 2. Install Python Dependencies

```bash
# If using development mode install
pip install -e .

# Or install with database dependencies
pip install -e .[database]

# Or install everything
pip install -e .[all]
```

### 3. Configure Environment

Add to your `.env` file:

```bash
# Required
DAEMON_REPORT_ID=1344
DATABASE_URL=postgresql://lsats_user:password@localhost:5432/lsats_db
TDX_BASE_URL=https://teamdynamix.umich.edu/TDWebApi/api
TDX_APP_ID=46
TDX_API_TOKEN=your_token_here

# Optional
DAEMON_INTERVAL=300              # Polling interval (seconds)
DAEMON_DRY_RUN=false
```

## Usage

### Quick Start

```bash
# Run once in dry-run mode (safe testing)
ticket-queue-daemon --report-id 1344 --dry-run --log daemon.log

# Run once for real
ticket-queue-daemon --report-id 1344 --log daemon.log

# Run as continuous daemon (poll every 5 minutes)
ticket-queue-daemon --report-id 1344 --daemon --interval 300 --log daemon.log
```

### Command Line Options

```bash
ticket-queue-daemon [OPTIONS]

Options:
  --report-id ID         TDX report ID to monitor (or set DAEMON_REPORT_ID)
  --daemon               Run continuously (default: run once)
  --interval SECONDS     Polling interval for daemon mode (default: 300)
  --dry-run              Simulate actions without making changes
  --log [FILE]           Enable logging to file (default: ticket_queue_daemon.log)
  --log-level LEVEL      Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)
  -h, --help             Show help message
```

### Configuration from Environment

Instead of command-line flags, use environment variables:

```bash
export DAEMON_REPORT_ID=1344
export DAEMON_INTERVAL=300
ticket-queue-daemon --daemon --log daemon.log
```

## Action Configuration

### Current Production Actions

The daemon is currently configured with a **multi-action workflow**:

1. **AddAssetAction** - Automatically associates computer assets with tickets
2. **SummaryCommentAction** - Posts cumulative summary of all actions

#### AddAssetAction

Intelligently adds computer assets to tickets using a three-phase search strategy:

**Phase 1:** Extract computer names/serials from ticket title & description → Search & verify → Add matches  
**Phase 2:** If no matches, search ticket conversation/feed → Search & verify → Add matches  
**Phase 3:** If no identifiers found, add requestor's asset (only if they have exactly 1)

**Configuration:**
```python
from scripts.queue.actions import AddAssetAction

action = AddAssetAction(
    add_summary_comment=True,           # Add to cumulative summary
    max_assets_to_add=10,               # Safety limit
    skip_if_requestor_asset_exists=True,# Skip Phase 3 if assets exist
    active_status_only=True,            # Only active assets
    computer_form_id=2448,              # Computer Form
    database_url=DATABASE_URL,          # Use bronze layer queries
    version='v1'
)
```

**Supported Computer Naming Patterns:**
- `IC-EHLB760-F16` (Department-Location-Identifier)
- `L-C02XJ0AXJGH5` (L-Serial)
- `LSAF-D30H6J3` (Department-Serial)
- `psyc-amiemgT01` (lowercase variants)
- `C02XJ0AXJGH5` (Apple serials)
- `R8YWA0LX6ZE` (Generic serials)

**See [ADD_ASSET_ACTION.md](ADD_ASSET_ACTION.md) for detailed documentation.**

#### SummaryCommentAction

Posts a cumulative summary comment with all actions executed in a daemon run.

**Configuration:**
```python
from scripts.queue.actions import SummaryCommentAction

action = SummaryCommentAction(
    comment_prefix="🤖 Automated Actions Summary",
    is_private=True,                    # Private comment
    skip_if_empty=True,                 # Only post if actions executed
    separator="\n",
    version='v1'
)
```

**Example Output:**
```
🤖 Automated Actions Summary

1. Added 2 assets: IC-EHLB760-F16, CHEM-SMALD1. Reason: Computer names found in ticket title
2. Changed status to In Progress
```

**Note:** SummaryCommentAction should always be placed **last** in the action list.

#### CommentAction

Adds a comment/feed entry to tickets (legacy/testing action).

**Configuration:**
```python
from scripts.queue.actions import CommentAction

action = CommentAction(
    comment_text="Hello World",
    is_private=False,
    is_rich_html=False,
    is_communication=False,
    notify=['null'],
    new_status_id=0,           # 0 = no status change
    cascade_status=False,
    version='v1'
)
```

### Action Idempotency

**Action ID format:** `{type}:{content_hash}:{version}`

Example: `add_asset:7a8b9c0d1e2f:v1`

- If action config changes (e.g., `max_assets_to_add` modified), hash changes → new action executes
- If `version` increments → new action executes
- Otherwise → action skipped (already executed)

### Adding New Actions

1. Create new class in `scripts/queue/actions/`:

```python
from .base_action import BaseAction

class StatusChangeAction(BaseAction):
    def __init__(self, new_status_id: int, version: str = 'v1'):
        super().__init__(version=version)
        self.new_status_id = new_status_id
    
    def get_action_type(self) -> str:
        return 'status_change'
    
    def get_action_config(self) -> Dict[str, Any]:
        return {'new_status_id': self.new_status_id}
    
    def execute_action(self, ticket_id, facade, dry_run=False):
        # Implementation here
        pass
```

2. Import in `actions/__init__.py`:

```python
from .status_change_action import StatusChangeAction
__all__ = ['BaseAction', 'CommentAction', 'StatusChangeAction']
```

3. Configure in `ticket_queue_daemon.py`:

```python
actions = [
    CommentAction(comment_text="Hello"),
    StatusChangeAction(new_status_id=117)
]
```

## Idempotency Explained

### How It Works

1. **Action ID Generation**: Each action generates a unique ID:
   - Format: `{type}:{content_hash}:{version}`
   - Example: `comment:abc123def456:v1`

2. **Pre-Execution Check**: Before executing, checks database:
   ```sql
   SELECT EXISTS(
       SELECT 1 FROM meta.daemon_action_log 
       WHERE ticket_id = 9291804 
       AND action_id = 'comment:abc123:v1'
   )
   ```

3. **Execution & Recording**: If not found, executes and records:
   ```sql
   INSERT INTO meta.daemon_action_log (
       ticket_id, action_id, action_type, 
       action_hash, status, executed_at
   ) VALUES (...)
   ```

4. **Skip on Retry**: Next run finds existing record → skips action

### Content-Aware Re-Execution

If you change action configuration:

```python
# Version 1
CommentAction(comment_text="Hello", version='v1')
# → action_id: comment:abc123:v1

# Change text
CommentAction(comment_text="Hello World", version='v1')
# → action_id: comment:def456:v1  (different hash!)
# → Executes again because action_id is different
```

### Manual Version Control

Force re-execution by incrementing version:

```python
CommentAction(comment_text="Hello", version='v2')
# → action_id: comment:abc123:v2
# → Executes even if v1 already ran
```

## Monitoring & Debugging

### View Recent Activity

```sql
-- Recent actions (last 24 hours)
SELECT * FROM meta.daemon_recent_activity;

-- Action summary by type
SELECT * FROM meta.daemon_activity_summary;
```

### Check Specific Ticket

```python
from database.adapters.postgres_adapter import PostgresAdapter
from scripts.queue.state.state_tracker import StateTracker

db = PostgresAdapter(database_url)
tracker = StateTracker(db)

# Get all actions performed on ticket
actions = tracker.get_ticket_actions(9291804)
print(actions)

# Get daemon statistics
stats = tracker.get_action_stats()
print(f"Total actions: {stats['total_actions']}")
print(f"By status: {stats['by_status']}")
```

### Clear Failed Actions for Retry

```python
# Clear failed actions for specific ticket
tracker.clear_failed_actions(ticket_id=9291804)

# Clear all failed actions
tracker.clear_failed_actions()
```

## Production Deployment

### Systemd Service (Linux)

Create `/etc/systemd/system/tdx-ticket-daemon.service`:

```ini
[Unit]
Description=TeamDynamix Ticket Queue Daemon
After=network.target postgresql.service

[Service]
Type=simple
User=lsats
WorkingDirectory=/opt/lsats-data-hub
Environment="PATH=/opt/lsats-data-hub/venv/bin"
ExecStart=/opt/lsats-data-hub/venv/bin/ticket-queue-daemon \
    --daemon \
    --interval 300 \
    --log /var/log/lsats/ticket_daemon.log
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable tdx-ticket-daemon
sudo systemctl start tdx-ticket-daemon
sudo systemctl status tdx-ticket-daemon
```

### Cron Job (Run Hourly)

```bash
# Edit crontab
crontab -e

# Add entry (runs every hour)
0 * * * * /usr/local/bin/ticket-queue-daemon --report-id 1344 --log /var/log/ticket_daemon.log
```

### Docker Container (Future)

```bash
# Build container
docker build -t lsats-ticket-daemon .

# Run daemon
docker run -d \
  --name ticket-daemon \
  --env-file .env \
  lsats-ticket-daemon \
  ticket-queue-daemon --daemon --interval 300
```

## Troubleshooting

### Common Issues

**Issue**: `Missing required environment variables`
- **Fix**: Ensure `.env` contains `TDX_BASE_URL`, `TDX_API_TOKEN`, `DATABASE_URL`, `DAEMON_REPORT_ID`

**Issue**: `Database connection test failed`
- **Fix**: Verify PostgreSQL is running: `docker ps | grep lsats-database`
- **Fix**: Check `DATABASE_URL` format: `postgresql://user:pass@host:port/dbname`

**Issue**: `Failed to fetch report 1344`
- **Fix**: Verify report ID exists in TeamDynamix
- **Fix**: Ensure API token has permission to view report
- **Fix**: Check TDX_BASE_URL matches environment (sandbox vs production)

**Issue**: Actions executing multiple times
- **Fix**: Check database migration was applied: `SELECT * FROM meta.daemon_action_log LIMIT 1;`
- **Fix**: Verify unique constraint exists: `\d meta.daemon_action_log` in psql

### Debug Mode

Run with verbose logging:

```bash
ticket-queue-daemon \
  --report-id 1344 \
  --dry-run \
  --log-level DEBUG \
  --log debug.log
```

### Database Queries for Debugging

```sql
-- Check if migration applied
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'meta' AND table_name = 'daemon_action_log';

-- View all actions (limit 10)
SELECT ticket_id, action_type, action_id, status, executed_at 
FROM meta.daemon_action_log 
ORDER BY executed_at DESC 
LIMIT 10;

-- Find failed actions
SELECT ticket_id, action_type, error_message, executed_at
FROM meta.daemon_action_log
WHERE status = 'failed'
ORDER BY executed_at DESC;

-- Count actions by status
SELECT status, COUNT(*) 
FROM meta.daemon_action_log 
GROUP BY status;
```

## Examples

### Example 1: Simple Comment

```bash
# Add "Hello World" to all tickets in report 1344
ticket-queue-daemon --report-id 1344 --dry-run
# Review output, then run for real:
ticket-queue-daemon --report-id 1344
```

### Example 2: Continuous Monitoring

```bash
# Monitor report every 5 minutes
ticket-queue-daemon --report-id 1344 --daemon --interval 300 --log daemon.log
```

### Example 3: Multiple Actions (Future)

```python
# Edit ticket_queue_daemon.py:
actions = [
    CommentAction(comment_text="Initial message", version='v1'),
    CommentAction(comment_text="Follow-up reminder", version='v1'),
    StatusChangeAction(new_status_id=117, version='v1')
]
```

## Future Enhancements

- [ ] JSON configuration file support
- [ ] Conditional actions (execute only if ticket matches criteria)
- [ ] Time-based actions (execute X days after ticket creation)
- [ ] Status change action
- [ ] Assignment action
- [ ] Tag/label action
- [ ] Custom field update action
- [ ] Notification action (add contacts)
- [ ] Web UI for configuration
- [ ] Metrics dashboard
- [ ] Slack/email alerting

## Contributing

To add new action types:

1. Inherit from `BaseAction`
2. Implement required methods: `get_action_type()`, `get_action_config()`, `execute_action()`
3. Add to `actions/__init__.py`
4. Test with `--dry-run` first
5. Submit pull request

## License

Part of LSATS Data Hub - Internal LSA Technology Services tool.
