#!/bin/bash
# Wrapper: движок находок → кандидаты завода постов (signals/insight_scan.py).
# Раз в день по будням, на данных прошлого торгового дня:
#   30 7 * * 2-6 /bin/bash /opt/frame/signals/insight_scan.sh >> /opt/frame/logs/insight_scan.log 2>&1
# Ручной прогон без записи: /opt/frame/signals/insight_scan.sh --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
exec /opt/frame/signals/.venv/bin/python -m signals.insight_scan "$@"
