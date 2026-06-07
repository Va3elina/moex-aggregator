#!/bin/bash
# Wrapper: alert eval-loop (Phase 3) с host venv. Один проход — запускается cron'ом:
#   */10 * * * * /opt/frame/signals/alerts_run.sh >> /opt/frame/logs/alerts.log 2>&1
#
# С хоста (не docker) — РКН блокирует IPv4 Telegram, хост идёт через IPv6.
# DB_URL @db→127.0.0.1 (как signals/run.sh); ALERT_BOT_TOKEN берётся из .env
# самим alerts_run.py (load_dotenv).
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.alerts_run "$@"
