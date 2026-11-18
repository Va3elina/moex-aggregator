#!/bin/bash
# Wrapper: Потоки капитала (ОРФР ЦБ): новый месяц в базе → карусель как пост 205. Крон раз в час (данные заливаются вручную):
#   25 * * * * /bin/bash /opt/frame/signals/cbr_flows_notify.sh >> /opt/frame/logs/cbr_flows_notify.log 2>&1
# Ручной прогон без отправки: --dry-run (CARD_DIR=/tmp/x — сохранить картинки)
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 120 /opt/frame/data/scan_state/cbr_flows_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.cbr_flows_notify "$@"
