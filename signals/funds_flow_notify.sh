#!/bin/bash
# Wrapper: рекордные притоки и оттоки «Денег в фондах» → @framesignalbot (signals/funds_flow_notify.py). Фонды
# обновляются в 09:30/14:30/18:00/19:10 МСК — прогон через полчаса после каждого (крон в UTC), замок от наложения:
#   0 7,12 * * *    /bin/bash /opt/frame/signals/funds_flow_notify.sh >> /opt/frame/logs/funds_flow_notify.log 2>&1
#   30 15 * * *    /bin/bash /opt/frame/signals/funds_flow_notify.sh >> /opt/frame/logs/funds_flow_notify.log 2>&1
#   40 16 * * *    /bin/bash /opt/frame/signals/funds_flow_notify.sh >> /opt/frame/logs/funds_flow_notify.log 2>&1
# Ручной прогон без отправки: --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 240 /opt/frame/data/scan_state/funds_flow_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.funds_flow_notify "$@"
