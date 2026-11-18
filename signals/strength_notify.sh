#!/bin/bash
# Wrapper: «Сила рынка» (доля акций индекса выше EMA200) → @framesignalbot (signals/strength_notify.py). Разбирает
# закрытые дни (вчера и раньше), крон раз в день, 09:40 МСК:
#   40 6 * * * /bin/bash /opt/frame/signals/strength_notify.sh >> /opt/frame/logs/strength_notify.log 2>&1
# Ручной прогон без отправки: --dry-run (CARD_DIR=/tmp/x — сохранить картинки); история событий: --backtest
set -eu

cd /opt/frame

export MPLBACKEND=Agg
mkdir -p /opt/frame/data/scan_state
exec flock -w 120 /opt/frame/data/scan_state/strength_notify.lock \
    /opt/frame/signals/.venv/bin/python -m signals.strength_notify "$@"
