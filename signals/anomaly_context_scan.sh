#!/bin/bash
# Wrapper: Шаг Б′ content-пайплайна — «аномалия сначала» (обратное направление).
# Раз в час, в паре с content_match.py:
#   20 * * * * /opt/frame/signals/anomaly_context_scan.sh >> /opt/frame/logs/anomaly_context_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.anomaly_context_scan "$@"
