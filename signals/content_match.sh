#!/bin/bash
# Wrapper: Шаг Б content-пайплайна — сверка pending-кандидатов с anomalies.
# Раз в 5 минут (было 15 мин, до этого час) — сверка уже готовых сигналов
# (host venv, DB_URL @db→127.0.0.1):
#   */5 * * * * /opt/frame/signals/content_match.sh >> /opt/frame/logs/content_match.log 2>&1
set -eu

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.content_match "$@"
