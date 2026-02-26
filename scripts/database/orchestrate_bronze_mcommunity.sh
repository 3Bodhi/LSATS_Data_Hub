#!/bin/bash
# orchestrate_bronze_mcommunity.sh
# Runs MCommunity LDAP bronze ingestion scripts in numbered order.
#
# Suggested cadence: weekly (MCommunity group/user data is relatively stable)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze_mcommunity.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/bronze/mcommunity"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_mcommunity_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze MCommunity Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze MCommunity Ingestion Complete: $(date) ==="
