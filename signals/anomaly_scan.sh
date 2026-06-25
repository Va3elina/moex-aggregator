#!/bin/bash
# Wrapper: маркет-вайд скан публичных аномалий (лента сайта) с host venv. Один
# проход — запускается cron'ом (как alerts_run, с хоста: нужен доступ к БД на
# 127.0.0.1 и pg_notify; Telegram не трогает):
#   5 * * * * /opt/frame/signals/anomaly_scan.sh >> /opt/frame/logs/anomaly_scan.log 2>&1
#
# DB_URL @db→127.0.0.1 (как alerts_run.sh); прочее из .env самим anomaly_scan.py.
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.anomaly_scan "$@"
