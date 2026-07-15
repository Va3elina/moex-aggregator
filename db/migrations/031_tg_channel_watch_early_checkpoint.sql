-- Найдено 2026-07-15 (замер по 78 постам с обоими чекпоинтами): у ВСЕХ 6
-- постов, реально пересёкших порог хайпа, fwd_15 уже был выше ×3 к медиане
-- fwd_15-истории — ждать до +90мин не требовалось ни разу. Решение о хайпе
-- переезжает на чекпоинт +15мин (было +90мин).
--
-- fwd_3/fwd_3_at — НОВЫЙ более ранний чекпоинт (+3мин), ТОЛЬКО для измерения:
-- решение о хайпе он не принимает, копим данные несколько дней, чтобы честно
-- ответить, можно ли сократить decision-чекпоинт ещё ниже 15 минут.
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_3 INTEGER;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_3_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_3
    ON tg_channel_watch (channel, posted_at) WHERE fwd_3 IS NULL;
