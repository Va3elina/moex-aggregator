-- Два режима breadth на страну (Вадим, 2026-07-25) — точная копия RF-паттерна
-- (breadth_history: universe 'imoex' vs 'all'): 'index' = кураторский список
-- голубых фишек (BLUE_CHIPS в фетчерах), 'all' = все тикеры биржи, что есть
-- в candles_intl (для NSE — реально ВСЕ ~210 FUTSTK, для TAIFEX пока
-- совпадает с 'index' — нет надёжного полного списка single-stock-futures
-- тикеров биржи, см. комментарий в Candles/compute_price_breadth_intl.py).
--
-- Применение:
--   cat db/migrations/046_price_breadth_universe.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE price_breadth_intl_history ADD COLUMN IF NOT EXISTS universe VARCHAR(10) NOT NULL DEFAULT 'index';

ALTER TABLE price_breadth_intl_history DROP CONSTRAINT IF EXISTS price_breadth_intl_history_pkey;
ALTER TABLE price_breadth_intl_history ADD PRIMARY KEY (exchange, universe, ema_period, trade_date);
