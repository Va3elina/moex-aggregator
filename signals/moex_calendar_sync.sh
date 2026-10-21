#!/bin/bash
# Календарь торгов МосБиржи из ISS → moex_trading_calendar, раз в день (см. moex_calendar_sync.py).
#   15 4 * * *  /bin/bash /opt/frame/signals/moex_calendar_sync.sh >> /opt/frame/logs/moex_calendar_sync.log 2>&1
set -eu
cd /opt/frame
exec flock -n /tmp/moex_calendar_sync.lock /opt/frame/signals/.venv/bin/python -m signals.moex_calendar_sync "$@"
