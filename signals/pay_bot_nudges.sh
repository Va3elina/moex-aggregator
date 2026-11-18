#!/bin/bash
# Wrapper: дожимы бота оплаты @FrameDataPayBot (signals/pay_bot_nudges.py, docs/tg-pay-bot.md).
#   */15 * * * * /bin/bash /opt/frame/signals/pay_bot_nudges.sh >> /opt/frame/logs/pay_bot_nudges.log 2>&1
# Ручной прогон без отправки: --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
mkdir -p /opt/frame/data/scan_state
exec flock -w 60 /opt/frame/data/scan_state/pay_bot_nudges.lock \
    /opt/frame/signals/.venv/bin/python -m signals.pay_bot_nudges "$@"
