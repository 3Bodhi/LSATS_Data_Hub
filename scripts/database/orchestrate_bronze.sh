#!/bin/bash
# orchestrate_bronze.sh
# Master bronze orchestrator: runs all source groups in series.
# Used by the lsats-bronze.timer for full nightly ingestion.
# Individual groups can be triggered directly via their own service units.
#
# Run order: umapi → mcommunity → ad → document → tdx
# (Groups are independent of each other; tdx runs last since it's slowest)
# To run manually: sudo -u lsats /bin/bash /opt/LSATS_Data_Hub/scripts/database/orchestrate_bronze.sh
set -euo pipefail

SCRIPT_DIR="/opt/LSATS_Data_Hub/scripts/database"
LOG_DIR="/var/log/lsats/bronze"
LOG="${LOG_DIR}/orchestrate_bronze_$(date +%Y%m%d_%H%M%S).log"
export PGPASSFILE="/opt/LSATS_Data_Hub/.pgpass"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "=== Full Bronze Ingestion Started: $(date) ==="

/bin/bash "${SCRIPT_DIR}/orchestrate_bronze_umapi.sh"
/bin/bash "${SCRIPT_DIR}/orchestrate_bronze_mcommunity.sh"
/bin/bash "${SCRIPT_DIR}/orchestrate_bronze_ad.sh"
/bin/bash "${SCRIPT_DIR}/orchestrate_bronze_document.sh"
/bin/bash "${SCRIPT_DIR}/orchestrate_bronze_tdx.sh"

echo "=== Full Bronze Ingestion Complete: $(date) ==="
