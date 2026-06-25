#!/bin/bash
# Wrapper: ежемесячный промо канала Фрейм в ленту аномалий, с host venv.
#   0 12 1 * *  /bin/bash /opt/frame/signals/promo_push.sh >> /opt/frame/logs/promo_push.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.promo_push "$@"
