#!/bin/bash
# Wrapper: Индикатор Баффетта (Кап/ВВП) → @framesignalbot (signals/buffett_notify.py). Разбирает закрытый месяц один
# раз, крон раз в день, 09:45 МСК:
#   45 6 * * * /bin/bash /opt/frame/signals/buffett_notify.sh >> /opt/frame/logs/buffett_notify.log 2>&1
# Ручной прогон без отправки: --dry-run (CARD_DIR=/tmp/x — сохранить картинки); история событий: --backtest
set -eu

cd /opt/frame

export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 120 /opt/frame/data/scan_state/buffett_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.buffett_notify "$@"
