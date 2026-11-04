#!/bin/bash
# Wrapper: подборка дня, топ репостов отобранных новостей за 24 часа → FRAME · Редакция (signals/hype_digest.py).
#   0 14 * * * /bin/bash /opt/frame/signals/hype_digest.sh >> /opt/frame/logs/hype_digest.log 2>&1   (17:00 МСК)
# Ручной прогон без отправки: --dry-run
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

# Та же MTProto-сессия, что у tg_hype_scan: два процесса на одном session-файле ломают его, поэтому ждём
# его замок (тик хайп-бота на эту минуту пропустится, у него flock -n). Потолок как у хайп-бота: Telethon
# умеет висеть на мёртвом соединении.
exec flock -w 300 /tmp/tg_hype_scan.lock \
    timeout -k 30 600 /opt/frame/signals/.venv/bin/python -m signals.hype_digest "$@"
