-- OI-based «сила рынка» признана методологически нежизнеспособной (2026-07-25,
-- Вадим: "сила рынка по ОИ чушь") — нельзя мешать компании (NSE/TAIFEX) и
-- commodities/currencies/индексы (CFTC/Eurex) в одну breadth-метрику.
-- Заменена price-based версией (price_breadth_intl_history, 044).
--
-- Применение:
--   cat db/migrations/045_drop_oi_intl_strength.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

DROP TABLE IF EXISTS oi_intl_strength_history;
