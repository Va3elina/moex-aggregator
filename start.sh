#!/usr/bin/env bash
set -euo pipefail

# Запуск Фрейм на macOS/Linux:
# - backend (FastAPI) на 127.0.0.1:8000
# - frontend (Vite) на 127.0.0.1:5173
# - опционально cloudflared tunnel (если установлен)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$ROOT_DIR/frontend"
VENV_PY="$ROOT_DIR/.venv/bin/python"
PID_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs"
TUNNEL_LOG="$ROOT_DIR/tunnel.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
TUNNEL_PID_FILE="$PID_DIR/tunnel.pid"

kill_port() {
  local port="$1"
  local pids
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    echo "Освобождаю порт $port (PID: $pids)..."
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
  fi
}

kill_port 8000
kill_port 5173
sleep 0.4

if [[ ! -x "$VENV_PY" ]]; then
  echo "❌ Не найден Python из venv: $VENV_PY"
  echo "Создай окружение: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

if [[ ! -f "$FRONTEND_DIR/package.json" ]]; then
  echo "❌ Не найден frontend/package.json"
  exit 1
fi

echo ""
echo "Запускаю backend (127.0.0.1:8000)..."
nohup "$VENV_PY" -m uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload \
  > "$LOG_DIR/backend.log" 2>&1 &
echo $! > "$BACKEND_PID_FILE"

sleep 1.5

echo "Запускаю frontend (127.0.0.1:5173)..."
nohup bash -lc "cd '$FRONTEND_DIR' && npm run dev -- --host 127.0.0.1 --port 5173" \
  > "$LOG_DIR/frontend.log" 2>&1 &
echo $! > "$FRONTEND_PID_FILE"

sleep 1.5

TUNNEL_URL=""
if command -v cloudflared >/dev/null 2>&1; then
  echo "Запускаю Cloudflare Tunnel..."
  : > "$TUNNEL_LOG"
  nohup cloudflared tunnel --url http://127.0.0.1:5173 --no-autoupdate \
    > "$TUNNEL_LOG" 2>&1 &
  echo $! > "$TUNNEL_PID_FILE"

  # Ждём URL до ~15 сек
  for _ in $(seq 1 30); do
    sleep 0.5
    if [[ -f "$TUNNEL_LOG" ]]; then
      TUNNEL_URL="$(grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' "$TUNNEL_LOG" | head -n 1 || true)"
      [[ -n "$TUNNEL_URL" ]] && break
    fi
  done
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Локально:  http://127.0.0.1:5173"
if [[ -n "$TUNNEL_URL" ]]; then
  echo "  Публично:  $TUNNEL_URL"
  if command -v pbcopy >/dev/null 2>&1; then
    printf '%s' "$TUNNEL_URL" | pbcopy
    echo "  ✓ URL скопирован в буфер обмена"
  fi
elif [[ -f "$TUNNEL_PID_FILE" ]]; then
  echo "  Туннель запущен, URL пока не найден — смотри tunnel.log"
else
  echo "  cloudflared не найден — публичный URL недоступен"
fi
echo "  API docs:  http://127.0.0.1:8000/api/docs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Для остановки: ./stop.sh"
