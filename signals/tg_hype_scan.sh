#!/bin/bash
# Wrapper: Этап 6 content-пайплайна — репост-хайп TG-каналов (MTProto),
# см. config.TG_HYPE_CHANNELS (markettwits, newssmartlab).
# Раз в 5 минут (чаще, чем раньше — нужно поймать узкое окно измерительного
# чекпоинта +3мин; чекпоинты дискретные, см. модульную докстроку):
#   */5 * * * * /opt/frame/signals/tg_hype_scan.sh >> /opt/frame/logs/tg_hype_scan.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.tg_hype_scan "$@"
