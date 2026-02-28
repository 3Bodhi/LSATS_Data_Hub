#!/bin/bash
# orchestrate_bronze_tdx.sh
# Runs TeamDynamix bronze ingestion and enrichment scripts in numbered order.
# Scripts are order-dependent: departments (001) must complete before users (002),
# and all ingest scripts must complete before enrich scripts (010+).
#
# Suggested cadence: weekly (TDX ticket/asset/user data updates frequently)
# Note: Enrichment scripts (010+) make per-record API calls and are slow.
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze_tdx.sh
set -euo pipefail

PYTHON="/opt/LSATS_Data_Hub/venv/bin/python"
SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database/bronze/tdx"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_tdx_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Bronze TDX Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze TDX Ingestion Complete: $(date) ==="
