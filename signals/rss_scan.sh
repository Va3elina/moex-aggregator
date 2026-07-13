#!/bin/bash
# Wrapper: RSS-сборщик + кросс-издательская конвергенция → content_candidates.
# Каждые 10 минут (host venv, DB_URL @db→127.0.0.1 как остальные):
#   */10 * * * * /opt/frame/signals/rss_scan.sh >> /opt/frame/logs/rss_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.rss_scan "$@"
