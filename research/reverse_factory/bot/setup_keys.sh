#!/usr/bin/env bash
# Сохранить ключ Bybit для бота. Запускать у себя в терминале (ключ никуда, кроме этого файла, не уходит):
#   ssh -t root@103.88.243.232 /opt/kamaz-bot/bot/setup_keys.sh demo      — ключ демо-счёта
#   ssh -t root@103.88.243.232 /opt/kamaz-bot/bot/setup_keys.sh live      — ключ реального субаккаунта
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="${1:-demo}"
[ "$MODE" = "demo" ] || [ "$MODE" = "live" ] || { echo "укажи demo или live"; exit 1; }
read -r -p "API key: " KEY
read -r -s -p "API secret (при вводе не виден): " SECRET; echo
[ -n "$KEY" ] && [ -n "$SECRET" ] || { echo "пусто — ничего не сохранено"; exit 1; }
umask 077
printf 'BYBIT_API_KEY=%s\nBYBIT_API_SECRET=%s\nBYBIT_KEY_MODE=%s\n' "$KEY" "$SECRET" "$MODE" > "$DIR/.env"
chown kamaz:kamaz "$DIR/.env" 2>/dev/null || true
chmod 600 "$DIR/.env"
echo "сохранено. Проверяю ключ…"
sudo -u kamaz /opt/kamaz-bot/venv/bin/python "$DIR/run.py" check --mode "$MODE" || true
