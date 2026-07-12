---
name: moex-data-triage
description: Диагностика «данные по X встали / не обновляются» на Фрейме — диспетчер по всем источникам. Use when user says «данные встали», «X не обновляется», «почему нет свежих данных», «график застрял на дате», «данные старые», «пропали данные по индикатору». Определяет источник (MOEX/Росстат/ЦБ/Cbonds/Yahoo), проверяет свежесть в БД + пульс пайплайна, ловит «зелёный пайплайн, а данные старые», направляет в узкий рунбук.
---

# Триаж «данные встали»

Диспетчер: за 2-3 запроса понять ЧТО встало, ПОЧЕМУ и куда копать. Ключевой
принцип — **свежесть данных в БД и статус пайплайна это РАЗНЫЕ вещи**: пайплайн
может быть `ok`, а данные старые (тихое зависание фетча-под-задачей).

## Шаг 1. Свежесть по всем источникам (одна SSH-сессия)
```bash
cat <<'SQL' | ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
-- MOEX-ряды
SELECT 'candles_spot' src, MAX(begin_time)::date d FROM candles WHERE secid='SBER' AND interval=24
UNION ALL SELECT 'oi_daily', MAX(tradedate) FROM open_interest
UNION ALL SELECT 'indices', MAX(trade_date) FROM index_data
-- Макро (Росстат/ЦБ)
UNION ALL SELECT 'gdp', MAX(period_date) FROM macro_data WHERE indicator='GDP_QUARTERLY'
UNION ALL SELECT 'm2', MAX(period_date) FROM macro_data WHERE indicator='M2_MONTHLY'
-- Фонды (Cbonds)
UNION ALL SELECT 'funds', MAX(trade_date) FROM fund_data
ORDER BY 1;
SELECT pipeline, last_run_at::date, last_status FROM pipeline_runs ORDER BY last_run_at DESC;
SQL
```
NB колонки дат: `fund_data.trade_date`, `candles.begin_time`, `open_interest.tradedate`,
`index_data.trade_date`, `macro_data.period_date`. (Полная карта — [[ingestion_map]].)

## Шаг 2. Определить источник → узкий рунбук
| Что встало | Источник | Куда |
|---|---|---|
| OI 5м/часовики, свечи, индексы, календарь — ВСЁ разом с одной даты | MOEX | `/moex-moex-ban` — почти всегда бан IP / сетевой блэкхол, транзиент на часы |
| Только ВВП (квартал) или M2 (месяц) | Росстат/ЦБ | `/moex-macro-refresh` — обычно auto-discover Росстата сломался |
| /cbr-flows (потоки участников) | ОРФР ЦБ | `/moex-cbr-flows` — РУЧНОЙ ингест, ждём файл от Вадима |
| Фонды/СЧА (/funds, /fund-trades) | Cbonds / УК | funds gap-safe (доедет); СЧА-фонды — `/moex-fund-scha-backfill` |
| Сырьё/золото | Yahoo | commodity_daily лог; Yahoo флапает, обычно самозалечивается |

## Шаг 3. «Зелёный пайплайн, а данные старые» — частая ловушка
Пайплайн `ok`, но `MAX(date)` отстаёт → фетч конкретного источника упал ТИХО
(его сбой некритичен для пайплайна). Известные случаи:
- **ВВП**: `macro_daily=ok`, ВВП завис — auto-discover Росстата (см. `/moex-macro-refresh`).
- **Праздник**: демоны фетчат только в торговый день. Проверь `moex_calendar`. Gap-safe
  фетчеры (funds/indices/candles) доедут на след. торговый день — это НЕ баг.
- **Разновременный сеттл ISS**: сектор-индексы/FX сеттлятся ПОЗЖЕ флагманов (>19:10 МСК),
  дневной прогон их пропускает, доедут завтра. Проверь напрямую:
  `iss.moex.com/iss/history/engines/stock/markets/index/securities/<secid>.json?from=<дата>`.

## Шаг 4. Что НЕ делать
- НЕ паниковать и НЕ менять IP при MOEX-зависании вслепую — сначала `/moex-ip-ban` (транзиент).
- НЕ строить новый cron/автофетч — пайплайны уже ежедневные. УК-раскрытие = ручное
  by design ([[feedback_no_data_autofetch]]).
- Логи оркестратора ЭФЕМЕРНЫ (per-day на writable-слое, теряются при recreate) → для
  форензики опирайся на insert-таймстемпы в БД (`created_at`/`systime`/`updated_at`).

Связано: [[ingestion_map]], [[moex_ip_ban]], [[monitoring_system]], [[cbr_flows]].
