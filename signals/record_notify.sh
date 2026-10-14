#!/bin/bash
# Wrapper: рекорды позиций физлиц → @framesignalbot (signals/record_notify.py). Два крона, общий замок — у режимов
# общее состояние отправленного:
#   */5 6-20 * * * /bin/bash /opt/frame/signals/record_notify.sh --live >> /opt/frame/logs/record_notify.log 2>&1
#   45 7-17 * * * /bin/bash /opt/frame/signals/record_notify.sh >> /opt/frame/logs/record_notify.log 2>&1
# Ручной прогон без отправки: --dry-run (по закрытию) или --live --dry-run; по закрытию сейчас: --force
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 240 /opt/frame/data/scan_state/record_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.record_notify "$@"
