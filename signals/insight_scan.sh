#!/bin/bash
# Wrapper: движок находок → кандидаты завода постов (signals/insight_scan.py).
# Крон каждый час; сканер работает раз на торговый день, как только его дневные позиции в базе
# (пятница приходит в субботу, signals/insights/fresh.py):
#   30 7-17 * * * /bin/bash /opt/frame/signals/insight_scan.sh >> /opt/frame/logs/insight_scan.log 2>&1
# Ручной прогон без записи: /opt/frame/signals/insight_scan.sh --dry-run; с записью, не дожидаясь: --force
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
exec /opt/frame/signals/.venv/bin/python -m signals.insight_scan "$@"
