#!/bin/bash
# Wrapper: рекорды движка находок → сводка в @framesignalbot (signals/record_notify.py).
# Крон каждый час; сканер работает раз на торговый день, как только его дневные позиции в базе:
#   45 7-17 * * * /bin/bash /opt/frame/signals/record_notify.sh >> /opt/frame/logs/record_notify.log 2>&1
# Ручной прогон без отправки: /opt/frame/signals/record_notify.sh --dry-run; с отправкой, не дожидаясь: --force
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
exec /opt/frame/signals/.venv/bin/python -m signals.record_notify "$@"
