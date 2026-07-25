-- Фикс реальной коллизии: у Eurex daily (с 2022-08-19) и monthly (2003-2026)
-- писали ОДНУ И ТУ ЖЕ ячейку (exchange, asset_code, category, trade_date) на
-- датах конца месяца — но это два разных числа (разная методология снятия
-- OI), не дубликат. Кто побеждал при upsert — зависело от порядка запуска
-- фоновых фетчеров, чистая случайность (проверено вживую: FDAX 2022-08-31
-- daily=90452 vs monthly=92085, в базе оказался daily). granularity делает
-- обе точки адресуемыми одновременно, без взаимной перезаписи.
--
-- Существующие строки размечаются по источнику:
--   CFTC   → 'weekly'  (COT-отчёт по вторникам)
--   NSE    → 'daily'   (bhavcopy, торговый день)
--   EUREX  → 'monthly' ВРЕМЕННО как safety-default; реальные daily-строки
--            переlivём заново после миграции (fetch_eurex.py теперь пишет
--            granularity корректно) — просто дефолт на случай, если чего-то
--            не долили.
--
-- Применение:
--   cat db/migrations/042_open_interest_intl_granularity.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE open_interest_intl ADD COLUMN IF NOT EXISTS granularity VARCHAR(10);

UPDATE open_interest_intl SET granularity = 'weekly' WHERE exchange = 'CFTC' AND granularity IS NULL;
UPDATE open_interest_intl SET granularity = 'daily' WHERE exchange = 'NSE' AND granularity IS NULL;
UPDATE open_interest_intl SET granularity = 'monthly' WHERE exchange = 'EUREX' AND granularity IS NULL;
UPDATE open_interest_intl SET granularity = 'daily' WHERE granularity IS NULL;  -- будущие биржи, safety net

ALTER TABLE open_interest_intl ALTER COLUMN granularity SET NOT NULL;

ALTER TABLE open_interest_intl DROP CONSTRAINT IF EXISTS open_interest_intl_pkey;
ALTER TABLE open_interest_intl ADD PRIMARY KEY (exchange, asset_code, category, trade_date, granularity);

DROP INDEX IF EXISTS idx_oi_intl_lookup;
CREATE INDEX IF NOT EXISTS idx_oi_intl_lookup ON open_interest_intl (exchange, asset_code, granularity, trade_date);
