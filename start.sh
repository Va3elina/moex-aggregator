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
# Подчищаем зависшие процессы dev-сервера/туннеля
pkill -f "vite --host 127.0.0.1 --port 5173" 2>/dev/null || true
pkill -f "cloudflared tunnel --url http://127.0.0.1:5173" 2>/dev/null || true
sleep 0.6

# ── PostgreSQL ───────────────────────────────────────────────────
PG_DATA="/opt/homebrew/var/postgresql@18"

start_pg() {
  # Сначала stop (сбрасывает error-состояние в brew services)
  brew services stop postgresql@18 >/dev/null 2>&1 || true
  sleep 0.3

  # Удаляем stale postmaster.pid если процесс уже не postgres
  if [[ -f "$PG_DATA/postmaster.pid" ]]; then
    local old_pid
    old_pid="$(head -1 "$PG_DATA/postmaster.pid" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]]; then
      local cmd
      cmd="$(ps -p "$old_pid" -o comm= 2>/dev/null || true)"
      if [[ "$cmd" != *postgres* ]]; then
        echo "  Удаляю stale postmaster.pid (PID $old_pid → $cmd)"
        rm -f "$PG_DATA/postmaster.pid"
      fi
    fi
  fi

  brew services start postgresql@18 2>/dev/null \
    || brew services start postgresql 2>/dev/null \
    || return 1
}

echo "Проверяю PostgreSQL..."
if ! lsof -tiTCP:5432 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "PostgreSQL не запущен, запускаю..."
  start_pg || { echo "❌ Не удалось запустить PostgreSQL"; exit 1; }

  # Ждём готовности до ~10 сек
  PG_READY=false
  for _ in $(seq 1 20); do
    sleep 0.5
    if lsof -tiTCP:5432 -sTCP:LISTEN >/dev/null 2>&1; then
      PG_READY=true
      break
    fi
  done

  if [[ "$PG_READY" != "true" ]]; then
    echo "❌ PostgreSQL не запустился за 10 секунд. Проверь: brew services list"
    exit 1
  fi
  echo "✓ PostgreSQL запущен"
else
  echo "✓ PostgreSQL уже работает"
fi

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
nohup bash -lc "cd '$FRONTEND_DIR' && npm run dev -- --host 127.0.0.1 --port 5173 --strictPort" \
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
