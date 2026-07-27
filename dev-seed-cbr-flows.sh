#!/usr/bin/env bash
# ============================================================
#  dev-seed-cbr-flows.sh — сид таблицы cbr_flows с ПРОДА (read-only)
#
#  Единственная таблица без узкого локального фетчера: CBR/fetch_orfr_flows.py
#  тянет только целый XLSX-архив с cbr.ru, без нарезки по дате/категории —
#  а вручную скачивать XLSX ради локальной вёрстки избыточно. Таблица сама
#  по себе маленькая (long-format, ~2 года помесячно/поквартально × ~19
#  категорий) — копируем целиком, фильтровать по дате смысла нет.
#
#  Отдельный от dev-seed-db.sh скрипт НАМЕРЕННО — это единственный шаг здесь,
#  который трогает прод (по SSH, только SELECT/COPY TO STDOUT, ничего не
#  пишет и не меняет на сервере). Держим осознанным отдельным вызовом.
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
ok() { echo -e "${GREEN}[OK]${NC} $1"; }

if ! psql -U postgres -d moex_db -c "SELECT 1" &>/dev/null; then
    echo -e "${RED}[FAIL]${NC} moex_db недоступна — сначала ./dev-start.sh"
    exit 1
fi

echo "Накатываю схему cbr_flows (если ещё не применена)…"
psql -U postgres -d moex_db -v ON_ERROR_STOP=1 -f db/cbr_flows.sql

echo "Тяну cbr_flows с прода (read-only COPY, целиком — таблица маленькая)…"
# TRUNCATE — чтобы скрипт был безопасно перезапускаемым (иначе повторный
# запуск упадёт на конфликте PK с уже сидированными строками).
psql -U postgres -d moex_db -c "TRUNCATE cbr_flows"
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
    'docker exec frame-db-1 psql -U postgres -d moex_db -c "COPY cbr_flows TO STDOUT"' \
  | psql -U postgres -d moex_db -c "COPY cbr_flows FROM STDIN"

ROWS="$(psql -U postgres -d moex_db -tAc 'SELECT count(*) FROM cbr_flows')"
ok "cbr_flows: $ROWS строк"
