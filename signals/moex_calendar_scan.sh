#!/bin/bash
# Wrapper: MOEX-календарь закрытий реестра → content_candidates. Раз в сутки —
# данные не меняются внутри дня (host venv, DB_URL @db→127.0.0.1 как остальные):
#   0 6 * * * /opt/frame/signals/moex_calendar_scan.sh >> /opt/frame/logs/moex_calendar_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.moex_calendar_scan "$@"
