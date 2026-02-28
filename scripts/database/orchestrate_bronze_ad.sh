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

# ---------------------------------------------------------------------------
# Credential injection (production only)
# When running under systemd with LoadCredential=, systemd decrypts the
# credential file and sets CREDENTIALS_DIRECTORY to a tmpfs path.
# We read that file and export it as an env var so the Python scripts
# can pick it up via os.getenv().  On a developer workstation
# CREDENTIALS_DIRECTORY is unset, so this block is skipped and the scripts
# fall back to AD_PASSWORD from the .env file instead.
# ---------------------------------------------------------------------------
if [[ -n "${CREDENTIALS_DIRECTORY:-}" ]]; then
    export AD_PASSWORD
    AD_PASSWORD=$(cat "$CREDENTIALS_DIRECTORY/ad_password")
fi

echo "=== Bronze Active Directory Ingestion Started: $(date) ==="

for script in $(ls "$SCRIPT_DIR"/*.py | sort); do
    echo "--- Running: $(basename "$script") ---"
    "$PYTHON" "$script"
done

echo "=== Bronze Active Directory Ingestion Complete: $(date) ==="
