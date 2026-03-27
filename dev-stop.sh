#!/usr/bin/env bash
# ============================================================
#  dev-stop.sh — останавливает всё и освобождает порты
#  PostgreSQL 18 + FastAPI backend + Vite frontend
# ============================================================
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$ROOT/.dev-pids"

# ── Цвета ──
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# ── 1. Фронтенд (порт 5173) ──
if [ -f "$PID_DIR/frontend.pid" ]; then
    PID=$(cat "$PID_DIR/frontend.pid")
    kill "$PID" 2>/dev/null && ok "Фронтенд остановлен (PID $PID)" || warn "Фронтенд (PID $PID) уже не работал"
    rm -f "$PID_DIR/frontend.pid"
fi
# Подчистить всё что слушает 5173
PIDS_5173=$(lsof -ti:5173 2>/dev/null || true)
if [ -n "$PIDS_5173" ]; then
    echo "$PIDS_5173" | xargs kill -9 2>/dev/null
    ok "Порт 5173 освобождён"
fi

# ── 2. Бэкенд (порт 8000) ──
if [ -f "$PID_DIR/backend.pid" ]; then
    PID=$(cat "$PID_DIR/backend.pid")
    kill "$PID" 2>/dev/null && ok "Бэкенд остановлен (PID $PID)" || warn "Бэкенд (PID $PID) уже не работал"
    rm -f "$PID_DIR/backend.pid"
fi
# Убить uvicorn и все его воркеры на 8000
PIDS_8000=$(lsof -ti:8000 2>/dev/null || true)
if [ -n "$PIDS_8000" ]; then
    echo "$PIDS_8000" | xargs kill -9 2>/dev/null
    ok "Порт 8000 освобождён"
fi

# ── 3. PostgreSQL ──
PG_CTL="/opt/homebrew/opt/postgresql@18/bin/pg_ctl"
PG_DATA="/opt/homebrew/var/postgresql@18"

if pg_isready -q 2>/dev/null; then
    "$PG_CTL" -D "$PG_DATA" stop -m fast \
        && ok "PostgreSQL остановлен" \
        || warn "Не удалось остановить PostgreSQL"
else
    ok "PostgreSQL уже был остановлен"
fi

# ── Очистка логов (опционально) ──
rm -f "$PID_DIR"/*.log 2>/dev/null

echo ""
echo -e "${GREEN}Всё остановлено, порты 5173 / 8000 / 5432 свободны.${NC}"
