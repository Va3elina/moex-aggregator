#!/usr/bin/env bash
# ============================================================
#  dev-start.sh — поднимает локальную среду разработки
#  PostgreSQL 18 + FastAPI backend + Vite frontend
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$ROOT/.dev-pids"
mkdir -p "$PID_DIR"

# ── Цвета ──
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# ── 1. PostgreSQL ──
PG_CTL="/opt/homebrew/opt/postgresql@18/bin/pg_ctl"
PG_DATA="/opt/homebrew/var/postgresql@18"
export LC_ALL=en_US.UTF-8

if pg_isready -q 2>/dev/null; then
    ok "PostgreSQL уже запущен"
else
    echo "Запускаю PostgreSQL..."
    "$PG_CTL" -D "$PG_DATA" -l "$PID_DIR/postgres.log" start \
        && ok "PostgreSQL запущен" \
        || fail "Не удалось запустить PostgreSQL. Лог: $PID_DIR/postgres.log"
    sleep 1
fi

# Проверяем что БД доступна
if psql -U postgres -d moex_db -c "SELECT 1" &>/dev/null; then
    ok "БД moex_db доступна"
else
    warn "БД moex_db не найдена или недоступна — бэкенд может не работать"
fi

# ── 2. FastAPI бэкенд (uvicorn) ──
if lsof -ti:8000 &>/dev/null; then
    warn "Порт 8000 уже занят — пропускаю запуск бэкенда"
else
    echo "Запускаю бэкенд (uvicorn :8000)..."
    cd "$ROOT"
    source .venv/bin/activate
    nohup python -m uvicorn api.main:app \
        --host 127.0.0.1 --port 8000 --reload \
        > "$PID_DIR/backend.log" 2>&1 &
    echo $! > "$PID_DIR/backend.pid"
    deactivate 2>/dev/null || true
    sleep 2

    if lsof -ti:8000 &>/dev/null; then
        ok "Бэкенд запущен (PID $(cat "$PID_DIR/backend.pid"))"
    else
        warn "Бэкенд стартует... проверь лог: $PID_DIR/backend.log"
    fi
fi

# ── 3. Vite фронтенд ──
if lsof -ti:5173 &>/dev/null; then
    warn "Порт 5173 уже занят — пропускаю запуск фронтенда"
else
    echo "Запускаю фронтенд (vite :5173)..."
    cd "$ROOT/frontend"
    nohup npm run dev > "$PID_DIR/frontend.log" 2>&1 &
    echo $! > "$PID_DIR/frontend.pid"
    sleep 2

    if lsof -ti:5173 &>/dev/null; then
        ok "Фронтенд запущен (PID $(cat "$PID_DIR/frontend.pid"))"
    else
        warn "Фронтенд стартует... проверь лог: $PID_DIR/frontend.log"
    fi
fi

# ── Итог ──
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Фронтенд:  http://127.0.0.1:5173${NC}"
echo -e "${GREEN}  Бэкенд:    http://127.0.0.1:8000${NC}"
echo -e "${GREEN}  API docs:  http://127.0.0.1:8000/docs${NC}"
echo -e "${GREEN}========================================${NC}"
