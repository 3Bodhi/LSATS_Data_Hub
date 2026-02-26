#!/bin/bash
# orchestrate_bronze_ad.sh
# Runs Active Directory LDAP bronze ingestion scripts in numbered order.
#
# Suggested cadence: weekly (AD group/user/computer data changes at moderate frequency)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze_ad.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/bronze/ad"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_ad_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze Active Directory Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze Active Directory Ingestion Complete: $(date) ==="
