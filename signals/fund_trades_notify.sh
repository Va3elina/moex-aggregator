#!/bin/bash
# Wrapper: Сделки фондов: фонд отчитался + итог раздела (карусель как пост 220). Крон каждые 30 минут (после конвейера справок СЧА):
#   */30 * * * * /bin/bash /opt/frame/signals/fund_trades_notify.sh >> /opt/frame/logs/fund_trades_notify.log 2>&1
# Ручной прогон без отправки: --dry-run (CARD_DIR=/tmp/x — сохранить картинки)
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 120 /opt/frame/data/scan_state/fund_trades_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.fund_trades_notify "$@"
