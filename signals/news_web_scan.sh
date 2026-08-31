#!/bin/bash
# Wrapper: живой сбор новостей через веб-превью Telegram в архив news_archive.
# MTProto с сервера мёртв (проверено 31.08.2026), t.me по HTTPS доступен.
#   */5 * * * * /opt/frame/signals/news_web_scan.sh >> /opt/frame/logs/news_web_scan.log 2>&1
#
# DB_URL @db→127.0.0.1 (как anomaly_scan.sh); прочее из .env самим скриптом.
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.news_web_scan "$@"
