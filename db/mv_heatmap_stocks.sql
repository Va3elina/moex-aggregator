-- Materialized View: карта рынка акций
-- Обновляется каждые 5 минут через оркестратор (refresh_materialized_views)
--
-- change_1d: текущая цена vs close последнего будня (PREVPRICE MOEX)
-- change_1w/1m/1y: snapshot-to-snapshot — current price vs split-adjusted historical close
--   Используем ТОЛЬКО daily candles (interval=24) — 5-мин историю в БД не
--   пересчитывают при split'ах (например T 2026-04-02), поэтому 5-мин
--   pre-split close может остаться "raw" (3200₽ вместо 320₽) и испортит расчёт.
--
-- known_splits — retroactive split adjustment для pre-split цен.
-- Если known_splits.split_date IS NOT NULL и begin_time < split_date,
-- то historical close делится на ratio чтобы привести к post-split equivalent.
-- Добавлять новые сплиты при появлении (детектировать аномалии в daily change).
--
-- ВАЖНО: дневные свечи (interval=24) должны быть из ISS API (не Algopack),
-- иначе close-цены будут неполными на буднях.

DROP MATERIALIZED VIEW IF EXISTS mv_heatmap_stocks;

CREATE MATERIALIZED VIEW mv_heatmap_stocks AS
WITH known_splits AS (
    -- secid, split_date, ratio (сколько новых акций на 1 старую)
    -- Применяется ТОЛЬКО к close < split_date (retroactive adjustment)
    SELECT 'T'::varchar     AS secid, '2026-04-02'::date AS split_date, 10.0::numeric AS ratio
    UNION ALL
    SELECT 'SFIN'::varchar, '2025-12-25'::date, 1.93::numeric
),
ranked_daily AS (
    SELECT secid, open, close, begin_time,
           ROW_NUMBER() OVER (PARTITION BY secid ORDER BY begin_time DESC) AS rn
    FROM candles
    WHERE type = 'stock' AND interval = 24
),
latest_daily AS (
    SELECT secid, open AS daily_open, close AS price, begin_time AS last_update
    FROM ranked_daily WHERE rn = 1
),
-- Prev close = close последнего будня (пн-пт), как PREVPRICE на MOEX
-- На выходных ref = пятница, в понедельник ref = тоже пятница
prev_day_close AS (
    SELECT DISTINCT ON (secid) secid, close AS price
    FROM candles
    WHERE type = 'stock' AND interval = 24
      AND begin_time::date < CURRENT_DATE
      AND EXTRACT(DOW FROM begin_time) BETWEEN 1 AND 5
    ORDER BY secid, begin_time DESC
),
-- Real-time: последняя 5мин свеча сегодня
intraday_close AS (
    SELECT DISTINCT ON (secid) secid, close
    FROM candles
    WHERE type = 'stock' AND interval = 5
      AND begin_time::date = CURRENT_DATE
    ORDER BY secid, begin_time DESC
),
-- 7D: snapshot-to-snapshot. Только daily candles + split-adjustment.
price_1w AS (
    SELECT DISTINCT ON (c.secid) c.secid,
           CASE WHEN ks.split_date IS NOT NULL AND c.begin_time::date < ks.split_date
                THEN c.close / ks.ratio
                ELSE c.close END AS price
    FROM candles c
    LEFT JOIN known_splits ks ON ks.secid = c.secid
    WHERE c.type = 'stock' AND c.interval = 24
      AND c.begin_time <= NOW() - INTERVAL '7 days'
      AND c.begin_time >= NOW() - INTERVAL '10 days'
    ORDER BY c.secid, c.begin_time DESC
),
-- 30D: snapshot-to-snapshot. Только daily + split-adjustment.
price_1m AS (
    SELECT DISTINCT ON (c.secid) c.secid,
           CASE WHEN ks.split_date IS NOT NULL AND c.begin_time::date < ks.split_date
                THEN c.close / ks.ratio
                ELSE c.close END AS price
    FROM candles c
    LEFT JOIN known_splits ks ON ks.secid = c.secid
    WHERE c.type = 'stock' AND c.interval = 24
      AND c.begin_time <= NOW() - INTERVAL '30 days'
      AND c.begin_time >= NOW() - INTERVAL '35 days'
    ORDER BY c.secid, c.begin_time DESC
),
-- 1Y: snapshot-to-snapshot. Только daily + split-adjustment.
price_1y AS (
    SELECT DISTINCT ON (c.secid) c.secid,
           CASE WHEN ks.split_date IS NOT NULL AND c.begin_time::date < ks.split_date
                THEN c.close / ks.ratio
                ELSE c.close END AS price
    FROM candles c
    LEFT JOIN known_splits ks ON ks.secid = c.secid
    WHERE c.type = 'stock' AND c.interval = 24
      AND c.begin_time <= NOW() - INTERVAL '365 days'
      AND c.begin_time >= NOW() - INTERVAL '379 days'
    ORDER BY c.secid, c.begin_time DESC
),
stats_1d AS (
    SELECT secid, SUM(volume) AS volume, SUM(value) AS value
    FROM candles
    WHERE type = 'stock' AND interval = 24 AND begin_time >= CURRENT_DATE - INTERVAL '1 day'
    GROUP BY secid
),
stats_1w AS (
    SELECT secid, SUM(volume) AS volume, SUM(value) AS value
    FROM candles
    WHERE type = 'stock' AND interval = 24 AND begin_time >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY secid
),
stats_1m AS (
    SELECT secid, SUM(volume) AS volume, SUM(value) AS value
    FROM candles
    WHERE type = 'stock' AND interval = 24 AND begin_time >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY secid
),
latest_cap AS (
    SELECT DISTINCT ON (sec_id) sec_id, market_cap
    FROM stock_market_cap
    ORDER BY sec_id, period_date DESC
)
SELECT i.sec_id, i.name, i.sector,
    COALESCE(ic.close, ld.price) AS price,
    COALESCE(pdc.price, ld.daily_open) AS prev_close,
    -- 1D: текущая цена vs close последнего будня (= MOEX PREVPRICE)
    CASE
        WHEN pdc.price IS NOT NULL AND pdc.price > 0
        THEN ROUND((COALESCE(ic.close, ld.price) - pdc.price) / pdc.price * 100, 2)
        WHEN ld.daily_open > 0
        THEN ROUND((ld.price - ld.daily_open) / ld.daily_open * 100, 2)
        ELSE 0
    END AS change_1d,
    -- 7D: snapshot-to-snapshot
    CASE WHEN p1w.price > 0
        THEN ROUND((COALESCE(ic.close, ld.price) - p1w.price) / p1w.price * 100, 2)
        ELSE 0 END AS change_1w,
    -- 30D: snapshot-to-snapshot
    CASE WHEN p1m.price > 0
        THEN ROUND((COALESCE(ic.close, ld.price) - p1m.price) / p1m.price * 100, 2)
        ELSE 0 END AS change_1m,
    -- 1Y: snapshot-to-snapshot
    CASE WHEN p1y.price > 0
        THEN ROUND((COALESCE(ic.close, ld.price) - p1y.price) / p1y.price * 100, 2)
        ELSE 0 END AS change_1y,
    COALESCE(s1d.volume, 0) AS volume_1d,
    COALESCE(s1w.volume, 0) AS volume_1w,
    COALESCE(s1m.volume, 0) AS volume_1m,
    COALESCE(s1d.value, 0) AS value_1d,
    COALESCE(s1w.value, 0) AS value_1w,
    COALESCE(s1m.value, 0) AS value_1m,
    CASE WHEN i.sec_id IN ('SNGS', 'SNGSP')
        THEN COALESCE(mc.market_cap, 0) / 2
        ELSE COALESCE(mc.market_cap, 0) END AS market_cap
FROM instruments i
JOIN latest_daily ld ON ld.secid = i.sec_id
LEFT JOIN prev_day_close pdc ON pdc.secid = i.sec_id
LEFT JOIN intraday_close ic ON ic.secid = i.sec_id
LEFT JOIN price_1w p1w ON p1w.secid = i.sec_id
LEFT JOIN price_1m p1m ON p1m.secid = i.sec_id
LEFT JOIN price_1y p1y ON p1y.secid = i.sec_id
LEFT JOIN stats_1d s1d ON s1d.secid = i.sec_id
LEFT JOIN stats_1w s1w ON s1w.secid = i.sec_id
LEFT JOIN stats_1m s1m ON s1m.secid = i.sec_id
LEFT JOIN latest_cap mc ON mc.sec_id = i.sec_id
WHERE i.type = 'stock' AND i.sector IS NOT NULL
  AND i.sec_id NOT IN ('SBERP', 'TATNP');

CREATE UNIQUE INDEX ON mv_heatmap_stocks (sec_id);
