#!/bin/bash
# Wrapper для запуска signals/run.py с host venv.
# Запускается cron'ом раз в час из /etc/crontab или crontab -e:
#   0 * * * * /opt/frame/signals/run.sh >> /opt/frame/logs/signals.log 2>&1
#
# Почему с хоста, а не из docker — РКН блокирует IPv4 Telegram;
# хост подключается через IPv6, docker network — IPv4-only (тёмная тема Docker).
set -eu

cd /opt/frame

# DB_URL в .env указывает на 'db:5432' (docker network).
# С хоста нужен 127.0.0.1:5432 (port exposed через docker-compose).
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.run "$@"
