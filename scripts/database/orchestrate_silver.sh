#!/bin/bash
# orchestrate_silver.sh
# Runs all silver transformation scripts in tier order.
# Takes a pre-silver snapshot before running transforms for rollback safety.
# Used by the lsats-silver.timer for scheduled transformation runs.
#
# Tier order:
#   Tier 1 (001-009): Source-specific transforms (independent of each other)
#   Tier 2 (010-013): Consolidated transforms (depend on Tier 1)
#   Tier 3 (013-017): Composite/aggregate transforms (depend on Tier 2)
#
# Suggested cadence: weekly, after bronze completes (timer set to run after bronze window)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_silver.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/silver"
LOG_DIR="/var/log/lsats/silver"
LOG="${LOG_DIR}/orchestrate_silver_$(date +%Y%m%d_%H%M%S).log"
SNAPSHOT_DIR="/var/backups/lsats"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Silver Transformation Started: $(date) ==="

# --- Pre-silver snapshot ---
# Capture state after bronze ingestion, before silver transforms.
# If any transform fails: restore this snapshot, fix the script, re-run silver.
# Avoids re-running the full bronze ingestion pipeline on failure.
mkdir -p "$SNAPSHOT_DIR"
SNAPSHOT_FILE="${SNAPSHOT_DIR}/pre_silver_$(date +%Y%m%d_%H%M%S).dump"
echo "Taking pre-silver snapshot: ${SNAPSHOT_FILE}"
pg_dump -U lsats_user -h localhost lsats_db \
    --format=custom --compress=9 \
    --file="${SNAPSHOT_FILE}"
echo "Snapshot complete ($(du -h "${SNAPSHOT_FILE}" | cut -f1))"

# Keep only the 2 most recent pre-silver snapshots
ls -t "${SNAPSHOT_DIR}"/pre_silver_*.dump 2>/dev/null | tail -n +3 | xargs -r rm --

# Run all silver scripts in numbered order
for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Silver Transformation Complete: $(date) ==="
