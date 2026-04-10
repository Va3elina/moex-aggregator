-- Materialized View: карта рынка акций
-- Обновляется каждые 5 минут через оркестратор (refresh_materialized_views)
--
-- change_1d: real-time, close предыдущего дня vs текущий close (учитывает утренние гэпы)
-- change_1w/1m/1y: snapshot-to-snapshot — close на дату T-7/30/365 дней назад (ближайший доступный)

DROP MATERIALIZED VIEW IF EXISTS mv_heatmap_stocks;

CREATE MATERIALIZED VIEW mv_heatmap_stocks AS
WITH ranked_daily AS (
    SELECT secid, open, close, begin_time,
           ROW_NUMBER() OVER (PARTITION BY secid ORDER BY begin_time DESC) AS rn
    FROM candles
    WHERE type = 'stock' AND interval = 24
),
latest_daily AS (
    SELECT secid, open AS daily_open, close AS price, begin_time AS last_update
    FROM ranked_daily WHERE rn = 1
),
-- Prev settlement = open текущей дневной свечи (Algopack open = settlement предыдущего аукциона)
prev_day_close AS (
    SELECT DISTINCT ON (secid) secid, open AS price
    FROM candles
    WHERE type = 'stock' AND interval = 24
      AND begin_time::date = CURRENT_DATE
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
-- 7D: close ближайшей свечи к дате T-7 дней (snapshot-to-snapshot)
price_1w AS (
    SELECT DISTINCT ON (secid) secid, close AS price
    FROM candles
    WHERE type = 'stock' AND interval = 24
      AND begin_time::date <= CURRENT_DATE - INTERVAL '7 days'
      AND begin_time::date >= CURRENT_DATE - INTERVAL '12 days'
    ORDER BY secid, begin_time DESC
),
-- 30D: close ближайшей свечи к дате T-30 дней
price_1m AS (
    SELECT DISTINCT ON (secid) secid, close AS price
    FROM candles
    WHERE type = 'stock' AND interval = 24
      AND begin_time::date <= CURRENT_DATE - INTERVAL '30 days'
      AND begin_time::date >= CURRENT_DATE - INTERVAL '37 days'
    ORDER BY secid, begin_time DESC
),
-- 1Y: close ближайшей свечи к дате T-365 дней
price_1y AS (
    SELECT DISTINCT ON (secid) secid, close AS price
    FROM candles
    WHERE type = 'stock' AND interval = 24
      AND begin_time::date <= CURRENT_DATE - INTERVAL '365 days'
      AND begin_time::date >= CURRENT_DATE - INTERVAL '372 days'
    ORDER BY secid, begin_time DESC
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
    -- 1D: close сейчас vs close вчера (с учётом утреннего гэпа)
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
