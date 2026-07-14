#!/bin/bash
# Wrapper: Шаг А/В content-пайплайна — дёргает Claude Routine через /fire.
# Routine-сессии не мгновенные (1-5 мин), поэтому раз в 15-20 минут:
#   */15 * * * * /opt/frame/signals/content_ai.sh >> /opt/frame/logs/content_ai.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.content_ai "$@"
