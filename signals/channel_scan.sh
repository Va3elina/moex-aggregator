#!/bin/bash
# Wrapper: ридер постов Telegram-каналов (секция «Новости каналов») с host venv.
# Cron на хосте (t.me/s достижим напрямую, без релея):
#   */30 * * * * /bin/bash /opt/frame/signals/channel_scan.sh >> /opt/frame/logs/channel_scan.log 2>&1
#
# DB_URL @db→127.0.0.1 (как anomaly_scan.sh); прочее из .env самим channel_scan.py.
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.channel_scan "$@"
