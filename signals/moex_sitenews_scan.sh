#!/bin/bash
# Wrapper: лента объявлений МосБиржи (ISS sitenews) → moex_sitenews → кандидаты.
#   */15 * * * * /bin/bash /opt/frame/signals/moex_sitenews_scan.sh >> /opt/frame/logs/moex_sitenews_scan.log 2>&1
# Токена не нужно: /iss/sitenews публичный. Тела новостей качаются только для
# рубрик «индекс» и «листинг» — остальное в ленте служебное.
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
exec timeout -k 30 300 /opt/frame/signals/.venv/bin/python -m signals.moex_sitenews_scan "$@"
