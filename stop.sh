#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$ROOT_DIR/.run"
TUNNEL_LOG="$ROOT_DIR/tunnel.log"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
TUNNEL_PID_FILE="$PID_DIR/tunnel.pid"

kill_by_pid_file() {
  local file="$1"
  local name="$2"
  if [[ -f "$file" ]]; then
    local pid
    pid="$(cat "$file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Останавливаю $name (PID $pid)..."
      kill "$pid" 2>/dev/null || true
      sleep 0.2
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$file"
  fi
}

kill_by_pid_file "$BACKEND_PID_FILE" "backend"
kill_by_pid_file "$FRONTEND_PID_FILE" "frontend"
kill_by_pid_file "$TUNNEL_PID_FILE" "cloudflared"

# Подчистка по портам (на случай, если PID-файлы потерялись)
for port in 8000 5173; do
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    echo "Освобождаю порт $port (PID: $pids)..."
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
  fi
done

# Подчистка cloudflared по имени
pkill -f "cloudflared tunnel --url http://127.0.0.1:5173" 2>/dev/null || true

rm -f "$TUNNEL_LOG"
echo "Готово."
