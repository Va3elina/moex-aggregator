#!/bin/bash
# Wrapper: скан «вышли новые данные» (SCHA-составы фондов, потоки ОРФР ЦБ) →
# Telegram-рассылка + лента сайта. Host-cron (как alerts_run: БД на 127.0.0.1,
# pg_notify для SSE, Telegram с хоста):
#   17 * * * * /opt/frame/signals/data_release_scan.sh >> /opt/frame/logs/data_release_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.data_release_scan "$@"
