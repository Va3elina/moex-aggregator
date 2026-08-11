-- Materialized View: карта рынка акций
-- Обновляется каждые 5 минут через оркестратор (refresh_materialized_views)
--
-- change_1d: ход последнего торгового дня (тек. цена vs prev close = PREVPRICE
--   MOEX). На выходных/праздниках, когда сегодня сделок не было, «снимок» берётся
--   за последний торговый день: пятница vs четверг, а НЕ 0% (см. snap_date /
--   prev_day_close ниже). Раньше prev = последний будень < CURRENT_DATE совпадал
--   с latest_daily (та же пятница) → change_1d = 0 на всех выходных.
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
    -- secid, split_date, ratio (сколько новых акций на 1 старую).
    -- Применяется ТОЛЬКО к close < split_date (retroactive adjustment).
    -- ⚠️ ТОЛЬКО для секций, чьи дневные свечи ЕЩЁ СЫРЫЕ (не переимпортированы
    -- ISS-адъюстнутыми). Если свечи уже адъюстнуты — сюда НЕ добавлять, иначе
    -- ДВОЙНАЯ коррекция: T (1:10, 2026-04) был тут ПРИ адъюстнутых свечах →
    -- change_1y давал +811% вместо −9% (close делился на 10 дважды). Убран 2026-06.
    -- SFIN остаётся: его свечи СЫРЫЕ (разрыв 1828→947 на 2025-12-25 виден в БД).
    SELECT 'SFIN'::varchar AS secid, '2025-12-25'::date AS split_date, 1.93::numeric AS ratio
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
-- Последняя 5мин свеча сегодня — С ПОТОЛКОМ ЛИЦЕНЗИИ MOEX.
--
-- ⚠️ Цена раздаётся не свежее чем now−15 минут (условие контракта; решение
-- владельца 2026-08-11: правило распространяется на ЛЮБУЮ цену MOEX, а не
-- только на график ОИ). Отсюда price и все change_* в этой карте считаются от
-- цены 15-минутной давности. Раньше здесь была самая свежая 5-минутка, и карта
-- отдавала фактически realtime-фид: /api/heatmap/prices доступен даже без
-- авторизации.
--
-- ⚠️ ВРЕМЯ: begin_time — НАИВНОЕ московское, а now() у Postgres в UTC
-- (контейнер без TZ). Поэтому потолок считаем через AT TIME ZONE, иначе
-- сравнение уехало бы на три часа и отрезало всю сессию.
intraday_close AS (
    SELECT DISTINCT ON (secid) secid, close
    FROM candles
    WHERE type = 'stock' AND interval = 5
      AND begin_time::date = CURRENT_DATE
      AND begin_time <= (now() AT TIME ZONE 'Europe/Moscow') - interval '15 minutes'
    ORDER BY secid, begin_time DESC
),
-- Дата «снимка» карты per-секция: если сегодня уже были сделки (есть 5-мин
-- свеча) — снимок за сегодня; иначе (праздник/до открытия) — снимок за дату
-- последней дневной свечи = последний торговый день. В выходную сессию MOEX
-- (сб/вс) спот собирается → снимок = сегодня, карта показывает живой ход
-- выходных; на праздник сделок нет → снимок = последний торговый день.
snap_date AS (
    SELECT ld.secid,
           CASE WHEN ic.close IS NOT NULL THEN CURRENT_DATE
                ELSE ld.last_update::date END AS d
    FROM latest_daily ld
    LEFT JOIN intraday_close ic ON ic.secid = ld.secid
),
-- Prev close = close НЕПОСРЕДСТВЕННО ПРЕДЫДУЩЕЙ сессии этой бумаги, строго до
-- даты снимка (= PREVPRICE MOEX). Сессии идут непрерывной цепочкой, выходные НЕ
-- пропускаем: …чт→пт→сб→вс→пн→… Поэтому пн считается vs вс, вс vs сб, сб vs пт.
-- Для бумаги, которая по выходным НЕ торгуется, «предыдущая сессия» перед
-- понедельником сама окажется пятницей (нет сб/вс свечей) → эталон всегда =
-- последнее ФАКТИЧЕСКОЕ закрытие именно этой бумаги, без «прыжка через выходные».
-- «Строго до даты снимка» делает расчёт holiday-устойчивым (нет своей свечи в
-- день снимка — берём предыдущую). NB: instruments.py day_change_pct пока держит
-- DOW 1-5 (пн vs пт) — это отдельная поверхность (пикер), карту не трогает.
prev_day_close AS (
    SELECT DISTINCT ON (c.secid) c.secid, c.close AS price
    FROM candles c
    JOIN snap_date sd ON sd.secid = c.secid
    WHERE c.type = 'stock' AND c.interval = 24
      AND c.begin_time::date < sd.d
    ORDER BY c.secid, c.begin_time DESC
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
