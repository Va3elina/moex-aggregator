#!/bin/bash
# Wrapper: лента раскрытия FinanceMarker → disclosure_events → кандидаты в посты.
#   */15 * * * * /opt/frame/signals/fm_disclosure_scan.sh >> /opt/frame/logs/fm_disclosure_scan.log 2>&1
# Нужен пакет quickjs в signals/.venv (исполняет window.__NUXT__ страницы).
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
exec /opt/frame/signals/.venv/bin/python -m signals.fm_disclosure_scan "$@"
