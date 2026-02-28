#!/bin/bash
# orchestrate_bronze_document.sh
# Runs document/file-based bronze ingestion scripts in numbered order.
# Expects lab_awards*.csv and keyconfigure_computers*.xlsx to be present in DATA_PATH.
#
# Suggested cadence: monthly (lab awards from sponsored programs, KeyConfigure exports)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze_document.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/bronze/document"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_document_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze Document Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze Document Ingestion Complete: $(date) ==="
