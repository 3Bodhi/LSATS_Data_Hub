#!/bin/bash
# orchestrate_bronze_umapi.sh
# Runs UMich API bronze ingestion scripts in numbered order.
#
# Suggested cadence: weekly (UMich API organizational data changes infrequently)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze_umapi.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/bronze/umapi"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_umapi_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze UMich API Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze UMich API Ingestion Complete: $(date) ==="
