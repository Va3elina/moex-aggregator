---
name: moex-local-dev-seed
description: Дотянуть/сидировать локальную dev-БД (moex_db) когда индикаторы на localhost показывают «Ошибка загрузки» / 500. Use when user says «локально ошибка загрузки», «нет данных на localhost», «БД пустая локально», «почему на деве не грузится», or when Claude itself hits empty/error chart data while verifying a fix in the local dev environment (frontend/backend via dev-start.sh) and needs real-ish data to test against. NOT for production data issues (see moex-data-triage) — this is purely about the local Homebrew Postgres dev instance.
---

# Локальный dev — сид/рефреш БД

## Ключевой факт (неочевидный)

Локальная `moex_db` (Homebrew Postgres, поднимается `dev-start.sh`) — **это НЕ
пустая БД**. Это старый частичный снепшот прода: у большинства таблиц (`candles`,
`open_interest`, `instruments`, ...) данные ЕСТЬ, но заморожены на дату
снепшота (были на несколько месяцев позади текущей даты — проверь
`SELECT max(begin_time) FROM candles`). Плюс локальная БД отстаёт от прода
примерно на **16 миграций схемы** — нет части таблиц целиком, в т.ч.
**`cbr_flows`** (отсюда ошибка `relation "cbr_flows" does not exist`).
`futures_contracts` существует, но локально **пустая** (0 строк) — таблица
календаря контрактов появилась позже снепшота.

Полностью зеркалировать прод локально — десятки ГБ, избыточно. Вместо этого:
маленький точечный сид/рефреш по узкому набору тикеров, которых хватает для
UI/вёрстки/раскладки — НЕ для проверки точности цифр.

## Что делать при «Ошибка загрузки» на localhost

1. Сначала проверь, это вообще про данные, а не про твой код: `read_console_messages`
   / `read_network_requests` в Browser pane, или бэкенд-логи (`preview_logs`) —
   ищи `relation "X" does not exist` (таблицы нет) или пустой `SELECT` (таблица
   есть, но нет строк под текущий тикер/дату).
2. Обычный (безопасный, прод не трогает) сид:
   ```bash
   ./dev-seed-db.sh
   ```
   Делает: докатывает недостающие `db/migrations/*.sql` (+ `db/cbr_flows.sql`,
   `db/analytics_events.sql`, `db/ab_experiments.sql`) — безопасно, `IF NOT EXISTS`,
   ошибки `already exists` игнорируются; затем дотягивает свежие данные до
   сегодня по SBER, GAZP, индексам (IMOEX и др.) и SR-фьючерсу (Сбербанк,
   `iss_code=SBRF`, контракты + свечи + открытый интерес). Все сетевые запросы —
   публичный ISS MOEX, без ключей.
3. Если не хватает `cbr_flows` (индикатор «Поток капитала») — отдельно:
   ```bash
   ./dev-seed-cbr-flows.sh
   ```
   Это единственный шаг, который трогает ПРОД (SSH + `docker exec frame-db-1
   psql ... COPY ... TO STDOUT`, только чтение) — таблица маленькая (long-format,
   ~2 года помесячно/поквартально), копируется целиком, без нарезки по дате.
   Держится отдельным скриптом намеренно — не бандлится молча в обычный сид.

Оба скрипта лежат в корне репо, рядом с `dev-start.sh`/`dev-stop.sh`, и
безопасно перезапускаемы (upsert/truncate+copy — повторный запуск не сломает
данные).

## Если нужен тикер/индикатор, которого нет в списке выше

Сиды покрывают только то, что реально понадобилось в разработке до сих пор
(SBER/GAZP/индексы/SR). Для нового тикера — добавь свой вызов по тому же
образцу, что уже есть в `dev-seed-db.sh`:

- Акции: `python Candles/backfill_daily_history.py --ticker <TICKER> --years 0.5`
  (тикер должен уже быть в `instruments` с `type='stock'` — если нет, сначала
  добавь строку туда: `sec_id, sectype, name, type, group, iss_code`, см.
  `api/models/instrument.py`).
- Фьючерсы: `python Candles/backfill_futures_history.py --sectypes <SECTYPE> --from-year Y --to-year Y+1`
  (сам обнаруживает контракты — не нужно знать точный код с годом заранее).
- Открытый интерес: `python OI/backfill_oi_range.py <SECTYPE> <ISS_CODE> <FROM> <TO>`
  — `ISS_CODE` НЕ всегда равен `SECTYPE` (проверь `SELECT iss_code FROM
  instruments WHERE sectype='<SECTYPE>'` — для SR это `SBRF`).
- Индексы (IMOEX/RGBITR/RUSFAR3M/GLDRUB_TOM) — только через
  `Funds/fetch_indices_realtime.py --once --force` (⚠️ `--once` обязателен — без
  него уходит в daemon-режим, бесконечный цикл каждые 5 минут), без выбора
  конкретного индекса
  (всегда все 4 разом; таблица маленькая, не страшно).

⚠️ `Candles/backfill_futures_history.py`, `Candles/backfill_futures_candles.py`,
`OI/backfill_oi_range.py` **не делают `load_dotenv` сами** — читают `DB_URL`
из окружения процесса напрямую. Если вызываешь их не через `dev-seed-db.sh`
(который уже экспортирует `.env`), сначала: `set -a; source .env; set +a`.

## Не относится к этому скиллу

- Диагностика **прод**-данных («данные встали», «почему на сайте старые цифры») →
  `moex-data-triage`.
- Ручной ингест `cbr_flows` из свежего XLSX ЦБ (не для дев-сида, а по-настоящему,
  новый отчёт вышел) → `moex-cbr-flows`.
- Запросы к прод-БД вообще → `moex-db-query`.
