# Quick Start Guide - Ticket Queue Daemon

Get the daemon running in 5 minutes!

## Prerequisites

- ✅ PostgreSQL database running (`docker ps | grep lsats-database`)
- ✅ Environment variables configured in `.env`
- ✅ Python virtual environment activated

## Step 1: Apply Database Migration (One-Time)

```bash
# Get database container ID
docker ps --filter "name=lsats-database" --format "{{.ID}}"

# Apply migration (replace CONTAINER_ID)
docker exec -i CONTAINER_ID psql -U lsats_user -d lsats_db \
  < docker/postgres/migrations/008_add_daemon_action_log.sql

# Verify table created
docker exec -i CONTAINER_ID psql -U lsats_user -d lsats_db \
  -c "\d meta.daemon_action_log"
```

## Step 2: Configure Environment

Add to your `.env` file:

```bash
# Required
DAEMON_REPORT_ID=1344
DATABASE_URL=postgresql://lsats_user:password@localhost:5432/lsats_db
TDX_BASE_URL=https://teamdynamix.umich.edu/SBTDWebApi/api
TDX_APP_ID=46
TDX_API_TOKEN=your_token_here
```

## Step 3: Test with Dry Run

```bash
# Activate virtual environment
source venv/bin/activate

# Test without making changes
python -m scripts.queue.ticket_queue_daemon --report-id 1344 --dry-run

# Expected output:
# ✓ Fetched report successfully
# ✓ [DRY RUN] Would add comment to ticket
# ✓ Actions: 1 executed (1 succeeded, 0 failed, 0 skipped)
```

## Step 4: Run for Real

```bash
# Single run
python -m scripts.queue.ticket_queue_daemon --report-id 1344 --log daemon.log

# Expected output:
# ✓ Comment added to ticket 9291804
# ✓ Actions: 1 executed (1 succeeded, 0 failed, 0 skipped)
```

## Step 5: Verify Idempotency

```bash
# Run again immediately
python -m scripts.queue.ticket_queue_daemon --report-id 1344

# Expected output:
# ✓ Action already executed on ticket 9291804
# ✓ Actions: 0 executed (0 succeeded, 0 failed, 1 skipped)
```

## Step 6: Verify Database State

```bash
# Check action log
docker exec -i CONTAINER_ID psql -U lsats_user -d lsats_db -c \
  "SELECT ticket_id, action_type, status, executed_at 
   FROM meta.daemon_action_log 
   ORDER BY executed_at DESC 
   LIMIT 5;"

# Check activity summary
docker exec -i CONTAINER_ID psql -U lsats_user -d lsats_db -c \
  "SELECT * FROM meta.daemon_activity_summary;"
```

## Step 7: Continuous Daemon Mode

```bash
# Run every 5 minutes (press Ctrl+C to stop)
python -m scripts.queue.ticket_queue_daemon \
  --report-id 1344 \
  --daemon \
  --interval 300 \
  --log daemon.log
```

## Common Commands

```bash
# Help
python -m scripts.queue.ticket_queue_daemon --help

# Debug mode
python -m scripts.queue.ticket_queue_daemon --report-id 1344 --log-level DEBUG --dry-run

# Production run (every 5 minutes, logged)
python -m scripts.queue.ticket_queue_daemon \
  --report-id 1344 \
  --daemon \
  --interval 300 \
  --log /var/log/ticket_daemon.log
```

## Troubleshooting

### Error: "Missing required environment variables"
**Fix:** Check `.env` file contains all required variables

### Error: "Database connection test failed"
**Fix:** Verify PostgreSQL container is running:
```bash
docker ps | grep lsats-database
```

### Error: "Failed to fetch report 1344"
**Fix:** 
- Verify report ID exists in TeamDynamix
- Check TDX_API_TOKEN is valid
- Ensure API token has permission to view report

### Actions executing multiple times
**Fix:** Verify migration was applied:
```bash
docker exec -i CONTAINER_ID psql -U lsats_user -d lsats_db -c \
  "SELECT COUNT(*) FROM meta.daemon_action_log;"
```

## Next Steps

1. **Customize Action:** Edit `ticket_queue_daemon.py` to change comment text
2. **Add More Actions:** See `scripts/queue/actions/` for examples
3. **Production Deployment:** See `README.md` for systemd/cron setup
4. **Monitoring:** Use SQL views for activity tracking

## Support

- **Full Documentation:** `scripts/queue/README.md`
- **Implementation Details:** `scripts/queue/IMPLEMENTATION_SUMMARY.md`
- **Project Instructions:** `CLAUDE.md`

---

**That's it!** You now have a production-ready ticket queue daemon with idempotent action execution.
