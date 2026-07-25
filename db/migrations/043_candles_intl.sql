-- Свечи (цена) для международных активов — отдельно от open_interest_intl,
-- как и в остальном проекте candles vs open_interest — две разные таблицы,
-- не смешиваем цену и OI в одних строках.
--
-- Источник для NSE/TAIFEX — те же файлы (bhavcopy / TAIFEX daily archive),
-- что уже качаются для OI: колонки OHLC лежат в тех же строках, просто не
-- читались раньше. Один тикер может иметь несколько одновременно торгуемых
-- контрактных месяцев — здесь хранится ТОЛЬКО передний/самый ликвидный месяц
-- (макс. open_interest среди фьючерсных контрактов на дату), не сумма и не
-- среднее — цену контрактов складывать нельзя, в отличие от OI.
--
-- Применение:
--   cat db/migrations/043_candles_intl.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS candles_intl (
    exchange         VARCHAR(10) NOT NULL,   -- 'NSE' | 'TAIFEX' (пока — источники без цены не пишут сюда)
    country          VARCHAR(5)  NOT NULL,
    asset_code       VARCHAR(30) NOT NULL,   -- тот же код, что в open_interest_intl — для JOIN по (exchange, asset_code, trade_date)
    asset_name       VARCHAR(80) NOT NULL,
    trade_date       DATE NOT NULL,
    granularity      VARCHAR(10) NOT NULL,   -- 'daily' — оба источника только EOD
    open             NUMERIC,
    high             NUMERIC,
    low              NUMERIC,
    close            NUMERIC,
    settlement_price NUMERIC,
    systime          TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, asset_code, trade_date, granularity)
);

CREATE INDEX IF NOT EXISTS idx_candles_intl_lookup ON candles_intl (exchange, asset_code, trade_date);
