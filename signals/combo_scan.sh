#!/bin/bash
# Wrapper: связки → кандидаты завода постов (signals/combo_scan.py).
#   40 7-17 * * *     /bin/bash /opt/frame/signals/combo_scan.sh --mode data >> /opt/frame/logs/combo_scan.log 2>&1
#                     (каждый час, но прогон раз на торговый день, когда его позиции в базе — signals/insights/fresh.py)
#   */20 6-17 * * 1-5 /bin/bash /opt/frame/signals/combo_scan.sh --mode news >> /opt/frame/logs/combo_scan.log 2>&1
# Ручной прогон без записи: /opt/frame/signals/combo_scan.sh --mode data --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
# Оба режима стартуют в :40 одной минутой. Без замка каждый читал «недавние сюжеты» до вставки соседа,
# и 02.10 одна связка «Минфин и валюта» пришла дважды (#3518 data, #3519 news). Второй ждёт первого
# и видит его тему в _RECENT.
exec flock -w 900 /tmp/combo_scan.lock /opt/frame/signals/.venv/bin/python -m signals.combo_scan "$@"
