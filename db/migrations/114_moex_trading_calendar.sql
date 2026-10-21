-- Календарь торгов МосБиржи из ISS (apim.moex.com/iss/calendars.json, off_days) — 21.10.2026.
-- Пополняется раз в день (signals/moex_calendar_sync.py); moex_calendar.py читает отсюда, а при сбое —
-- запасной блок в коде. ISS отдаёт только ОСОБЫЕ дни (праздники и сессии выходного дня); обычный день
-- в таблице не значится. Год перезаписывается целиком, чтобы снятый биржей праздник исчез.
--   cat db/migrations/114_moex_trading_calendar.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
CREATE TABLE IF NOT EXISTS moex_trading_calendar (
    tradedate                    DATE PRIMARY KEY,
    stock_workday                SMALLINT,
    futures_workday              SMALLINT,
    currency_workday             SMALLINT,
    stock_reason                 VARCHAR(4),      -- H праздник, W сессия выходного дня, T торговый выходной
    futures_reason               VARCHAR(4),
    currency_reason              VARCHAR(4),
    stock_trade_session_date     DATE,
    futures_trade_session_date   DATE,
    currency_trade_session_date  DATE,
    raw                          JSONB,
    updated_at                   TIMESTAMPTZ DEFAULT now()
);
