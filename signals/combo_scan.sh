#!/bin/bash
# Wrapper: связки → кандидаты завода постов (signals/combo_scan.py).
#   40 7 * * 2-6      /bin/bash /opt/frame/signals/combo_scan.sh --mode data >> /opt/frame/logs/combo_scan.log 2>&1
#   */20 6-17 * * 1-5 /bin/bash /opt/frame/signals/combo_scan.sh --mode news >> /opt/frame/logs/combo_scan.log 2>&1
# Ручной прогон без записи: /opt/frame/signals/combo_scan.sh --mode data --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
exec /opt/frame/signals/.venv/bin/python -m signals.combo_scan "$@"
