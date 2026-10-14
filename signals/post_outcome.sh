#!/bin/bash
# Wrapper: итоги постов канала (ход цены через неделю/месяц/3 месяца) → @framesignalbot (signals/post_outcome.py).
#   20 7 * * * /bin/bash /opt/frame/signals/post_outcome.sh >> /opt/frame/logs/post_outcome.log 2>&1
# Ручной прогон без отправки: --dry-run; таблица по последним постам: --report 40
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
mkdir -p /opt/frame/data/scan_state
exec flock -w 240 /opt/frame/data/scan_state/post_outcome.lock \
    /opt/frame/signals/.venv/bin/python -m signals.post_outcome "$@"
