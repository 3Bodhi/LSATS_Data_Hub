# systemd Unit Files

These unit files live in the repo and are copied to `/etc/systemd/system/` during deployment.

## Install

```bash
# Copy all unit files
sudo cp /opt/LSATS_Data_Hub/scripts/systemd/*.service /etc/systemd/system/
sudo cp /opt/LSATS_Data_Hub/scripts/systemd/*.timer /etc/systemd/system/

# Make orchestrator scripts executable
chmod +x /opt/LSATS_Data_Hub/scripts/database/*.sh

# Reload systemd
sudo systemctl daemon-reload
```

## Timers

| Timer | Schedule | Description |
|---|---|---|
| `lsats-backup.timer` | Daily 1am | pg_dump of lsats_db, 14-day retention |
| `lsats-bronze-umapi.timer` | Weekly (Sun 2am) | UMich API departments + employees |
| `lsats-bronze-mcommunity.timer` | Weekly (Sun 2am) | MCommunity users + groups |
| `lsats-bronze-ad.timer` | Weekly (Sun 2am) | AD users, groups, OUs, computers |
| `lsats-bronze-document.timer` | Weekly (Sun 2am) | Lab awards + KeyConfigure CSVs |
| `lsats-bronze-tdx.timer` | Weekly (Sun 2am) | TDX ingest + enrich (slow) |
| `lsats-silver.timer` | Weekly (Sun 4am) | All silver transforms, post-bronze |

## Enable

```bash
# Enable backup (daily, always on)
sudo systemctl enable --now lsats-backup.timer

# Enable bronze groups
sudo systemctl enable --now lsats-bronze-umapi.timer
sudo systemctl enable --now lsats-bronze-mcommunity.timer
sudo systemctl enable --now lsats-bronze-ad.timer
sudo systemctl enable --now lsats-bronze-document.timer
sudo systemctl enable --now lsats-bronze-tdx.timer

# Enable silver
sudo systemctl enable --now lsats-silver.timer

# Enable queue daemon (long-running service, no timer)
sudo systemctl enable --now lsats-queue-daemon
```

## Manual Trigger (First Ingestion / On-Demand)

```bash
# Run a single bronze source group
sudo systemctl start lsats-bronze-umapi
sudo systemctl start lsats-bronze-mcommunity
sudo systemctl start lsats-bronze-ad
sudo systemctl start lsats-bronze-document
sudo systemctl start lsats-bronze-tdx

# Run silver after bronze completes
sudo systemctl start lsats-silver

# Watch logs in real time
journalctl -fu lsats-bronze-tdx
journalctl -fu lsats-silver
```

## Verify

```bash
sudo systemctl list-timers --all | grep lsats
sudo systemctl status lsats-bronze-tdx
sudo systemctl status lsats-queue-daemon
```

## Update After Code Changes

```bash
sudo -u lsats git -C /opt/LSATS_Data_Hub pull
sudo -u lsats /opt/LSATS_Data_Hub/venv/bin/pip install -e '.[all]'
sudo systemctl restart lsats-queue-daemon
# Bronze/silver timers pick up new code automatically on next run
```
