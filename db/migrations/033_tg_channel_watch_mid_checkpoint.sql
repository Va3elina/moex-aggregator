-- Промежуточный измерительный чекпоинт +8мин (между +3 и +15) — Вадим спрашивал,
-- можно ли сократить decision-чекпоинт до 8-10 минут вместо 15. ТОЛЬКО измерение,
-- на решение о хайпе не влияет (см. signals/config.py:MTP_CHECKPOINT_MID_MIN).
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_8 INTEGER;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_8_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_8
    ON tg_channel_watch (channel, posted_at) WHERE fwd_8 IS NULL;
