#!/bin/bash
# Wrapper: Этап 6 content-пайплайна — репост-хайп @markettwits (MTProto).
# Раз в 15 минут (чекпоинты +15/+90 мин дискретные, см. modul docstring):
#   */15 * * * * /opt/frame/signals/marketwits_scan.sh >> /opt/frame/logs/marketwits_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.marketwits_scan "$@"
