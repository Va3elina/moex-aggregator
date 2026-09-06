#!/bin/bash
# Wrapper: Интерфакс (раздел бизнес) → news_archive + кандидаты в посты.
#   */5 * * * * /opt/frame/signals/interfax_scan.sh >> /opt/frame/logs/interfax_scan.log 2>&1
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
exec /opt/frame/signals/.venv/bin/python -m signals.interfax_scan "$@"
