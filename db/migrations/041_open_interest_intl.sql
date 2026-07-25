-- Международный Open Interest (CFTC/NSE/Eurex/B3) — admin-only фича, отдельно
-- от open_interest (та таблица целиком заточена под MOEX/FORTS, без понятия
-- страны/биржи). Каждый фетчер ВСЕГДА пишет строку category='TOTAL' — общий
-- OI по активу напрямую из поля источника (у Eurex это единственное число,
-- у CFTC/NSE/B3 берётся из итоговой строки/поля отчёта). Отдельные
-- holder-категории (commercial/noncommercial, client/dii/fii/pro, PF/PJF/...)
-- пишутся дополнительными строками для детального чарта, но "сила рынка"
-- всегда считается по category='TOTAL' — метрика не зависит от того, сколько
-- holder-категорий у конкретной биржи.
--
-- Применение:
--   cat db/migrations/041_open_interest_intl.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS open_interest_intl (
    exchange     VARCHAR(10) NOT NULL,   -- 'CFTC' | 'NSE' | 'EUREX' | 'B3'
    country      VARCHAR(5)  NOT NULL,   -- 'US' | 'IN' | 'DE' | 'BR'
    asset_code   VARCHAR(30) NOT NULL,   -- нативный код источника
    asset_name   VARCHAR(80) NOT NULL,   -- человекочитаемое имя для UI
    category     VARCHAR(20) NOT NULL,   -- 'TOTAL' + source-specific holder-type
    trade_date   DATE NOT NULL,
    oi_long      BIGINT,
    oi_short     BIGINT,
    oi_total     BIGINT NOT NULL,
    systime      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, asset_code, category, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_oi_intl_lookup ON open_interest_intl (exchange, asset_code, trade_date);

CREATE TABLE IF NOT EXISTS oi_intl_strength_history (
    exchange      VARCHAR(10) NOT NULL,  -- конкретная биржа, либо 'ALL' — по всем странам сразу
    ema_period    SMALLINT NOT NULL,
    trade_date    DATE NOT NULL,
    percent_above REAL NOT NULL,
    count_above   SMALLINT NOT NULL,
    count_total   SMALLINT NOT NULL,
    PRIMARY KEY (exchange, ema_period, trade_date)
);
