#!/usr/bin/env bash
# ============================================================
#  dev-seed-db.sh — маленький сид/рефреш локальной БД под UI-разработку
#
#  Локальная moex_db — НЕ пустая: это старый частичный снепшот прода,
#  замороженный на некоторую дату (у большинства таблиц данные ЕСТЬ, просто
#  устарели на несколько месяцев) + не хватает ~16 таблиц из более новых
#  миграций (напр. cbr_flows). Полное зеркалирование прода — десятки ГБ,
#  этот скрипт вместо этого:
#    1. докатывает недостающие миграции схемы (безопасно — IF NOT EXISTS);
#    2. дотягивает свежие данные до сегодня по узкому набору
#       тикеров/фьючерса, которых хватает для UI/вёрстки (SBER, GAZP,
#       индексы, SR-фьючерс + его открытый интерес).
#
#  НЕ трогает прод. Реальные сетевые запросы — только к публичному ISS
#  MOEX (без ключей). Для cbr_flows (нет узкого локального фетчера —
#  CBR/fetch_orfr_flows.py тянет только целый XLSX) см. отдельный
#  dev-seed-cbr-flows.sh — он ходит на прод по SSH (read-only), поэтому
#  вынесен отдельно как осознанный шаг.
#
#  Запускать после dev-start.sh (нужен поднятый локальный Postgres).
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
step() { echo -e "\n${GREEN}── $1 ──${NC}"; }

if ! psql -U postgres -d moex_db -c "SELECT 1" &>/dev/null; then
    echo -e "${RED}[FAIL]${NC} moex_db недоступна — сначала ./dev-start.sh"
    exit 1
fi
ok "moex_db доступна"

# Часть фетчеров (futures/OI-скрипты) не делает load_dotenv сама — читает
# DB_URL из окружения процесса напрямую, поэтому экспортируем .env вручную.
set -a
source .env
set +a

step "1/2: докатываем недостающие миграции схемы"
# Миграций нет таблицы-трекера (alembic_version-аналога в проекте нет) —
# накатываем ВСЕ файлы по порядку, "already exists" на уже применённых
# считаем ожидаемым и не падаем на нём; падаем только на ДРУГИХ ошибках.
MIGRATION_LOG="$(mktemp)"
for f in db/migrations/*.sql db/cbr_flows.sql db/analytics_events.sql db/ab_experiments.sql; do
    [ -f "$f" ] || continue
    if ! psql -U postgres -d moex_db -v ON_ERROR_STOP=1 -f "$f" > "$MIGRATION_LOG" 2>&1; then
        if grep -qi "already exists" "$MIGRATION_LOG"; then
            : # ожидаемо — миграция уже применена раньше
        else
            warn "$f — ошибка (не 'already exists'):"
            cat "$MIGRATION_LOG"
        fi
    fi
done
rm -f "$MIGRATION_LOG"
ok "миграции применены (существовавшие — пропущены)"

step "2/2: дотягиваем свежие данные (SBER / GAZP / индексы / SR-фьючерс) до сегодня"
source .venv/bin/activate

echo "  → SBER (акции, последние ~6 мес.)"
python Candles/backfill_daily_history.py --ticker SBER --years 0.5

echo "  → GAZP (акции, последние ~6 мес.)"
python Candles/backfill_daily_history.py --ticker GAZP --years 0.5

echo "  → индексы (IMOEX и др. — таблица маленькая, тянем всю историю разом)"
# ВАЖНО: --once обязателен — без него скрипт уходит в daemon-режим
# (бесконечный цикл с проверкой каждые 5 минут, никогда не завершается).
python Funds/fetch_indices_realtime.py --once --force

echo "  → SR-фьючерс (Сбербанк) — контракты + свечи, текущий+соседний год"
CUR_YEAR="$(date +%Y)"
python Candles/backfill_futures_history.py --sectypes SR --from-year "$CUR_YEAR" --to-year "$((CUR_YEAR + 1))"

echo "  → открытый интерес SR (iss_code=SBRF, с начала года по сегодня)"
python OI/backfill_oi_range.py SR SBRF "${CUR_YEAR}-01-01" "$(date +%Y-%m-%d)"

deactivate 2>/dev/null || true
ok "готово"

echo -e "\n${GREEN}Локальная БД дотянута.${NC} Не сидировалось: cbr_flows (см. ./dev-seed-cbr-flows.sh)"
echo "и всё остальное вне SBER/GAZP/индексов/SR — при нужде добавь свои вызовы"
echo "backfill_daily_history.py/--sectypes по тому же образцу."
