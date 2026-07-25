-- «Сила рынка» по ЦЕНЕ (не ОИ) для NSE/TAIFEX голубых фишек — точный аналог
-- Frame'овского breadth_history (Candles/compute_breadth_history.py, IMOEX),
-- просто на международных данных из candles_intl. Сознательно ОТДЕЛЬНАЯ
-- таблица от oi_intl_strength_history — та методология про ОИ, эта про цену,
-- смешивать в одну таблицу нельзя (разные числа при одинаковой форме строки).
--
-- РФ-плечо в это "3 рынка сразу" сравнение НЕ дублируется сюда — читается
-- напрямую из существующего breadth_history (universe='imoex') на API-слое,
-- чтобы не рассинхронизировать копию с оригиналом.
--
-- Применение:
--   cat db/migrations/044_price_breadth_intl.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS price_breadth_intl_history (
    exchange      VARCHAR(10) NOT NULL,  -- 'NSE' | 'TAIFEX'
    ema_period    SMALLINT NOT NULL,
    trade_date    DATE NOT NULL,
    percent_above REAL NOT NULL,
    count_above   SMALLINT NOT NULL,
    count_total   SMALLINT NOT NULL,
    PRIMARY KEY (exchange, ema_period, trade_date)
);
