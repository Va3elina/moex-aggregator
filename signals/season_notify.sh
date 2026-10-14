#!/bin/bash
# Wrapper: сильные сезонные периоды → уведомление в @framesignalbot в день начала (signals/season_notify.py).
# Крон раз в день, 10:00 МСК:
#   0 7 * * * /bin/bash /opt/frame/signals/season_notify.sh >> /opt/frame/logs/season_notify.log 2>&1
# Ручной прогон без отправки: --dry-run; все периоды года: --list
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export MPLBACKEND=Agg
exec /opt/frame/signals/.venv/bin/python -m signals.season_notify "$@"
