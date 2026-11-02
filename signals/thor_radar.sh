#!/bin/bash
# Wrapper: радар боли для постов @Thor_INV → личка админа @frameadminbot (signals/thor_radar.py).
#   0 6 * * * /bin/bash /opt/frame/signals/thor_radar.sh >> /opt/frame/logs/thor_radar.log 2>&1
# Ручной прогон без отправки: --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
mkdir -p /opt/frame/data/scan_state
exec flock -w 240 /opt/frame/data/scan_state/thor_radar.lock \
    /opt/frame/signals/.venv/bin/python -m signals.thor_radar "$@"
