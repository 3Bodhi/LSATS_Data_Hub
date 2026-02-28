#!/bin/bash
# backup_database.sh
# Daily pg_dump backup of lsats_db with compression and retention management.
# Runs via lsats-backup.timer at 1am daily.
#
# Backup location: /var/backups/lsats/
# Retention: keeps 14 daily backups
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/backup_database.sh
set -euo pipefail

BACKUP_DIR="/var/backups/lsats"
LOG_DIR="/var/log/lsats"
LOG="${LOG_DIR}/backup_$(date +%Y%m%d_%H%M%S).log"
BACKUP_FILE="${BACKUP_DIR}/lsats_db_$(date +%Y%m%d_%H%M%S).dump"
RETAIN_DAYS=14
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$BACKUP_DIR" "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Database Backup Started: $(date) ==="
echo "Destination: ${BACKUP_FILE}"

pg_dump -U lsats_user -h localhost lsats_db \
    --format=custom --compress=9 \
    --file="${BACKUP_FILE}"

echo "Backup complete ($(du -h "${BACKUP_FILE}" | cut -f1))"

# Prune backups older than RETAIN_DAYS (excludes pre_silver_* snapshots)
echo "Pruning daily backups older than ${RETAIN_DAYS} days..."
find "$BACKUP_DIR" -maxdepth 1 -name "lsats_db_*.dump" -mtime +${RETAIN_DAYS} -delete

echo "Remaining backups:"
ls -lh "${BACKUP_DIR}"/lsats_db_*.dump 2>/dev/null || echo "  (none)"

echo "=== Database Backup Complete: $(date) ==="
