-- Найдено 2026-07-16: decision-чекпоинт переезжает с +15мин сразу на +3мин
-- (Вадим осознанно выбрал скорость — на +3мин было видно только 46%
-- итогового fwd_15-сигнала у реально хайповых постов против 73% на +8мин,
-- т.е. решение формальнее рискованнее, но допустимо). Заодно добавляем
-- запасные измерительные точки 5/6/7/8 минут — если 3 минуты дадут слишком
-- много ложных срабатываний/пропусков, будет к чему откатить decision-
-- чекпоинт без даунтайма. ВСЕ новые колонки — только измерение, не влияют
-- на решение о хайпе (см. signals/config.py:MTP_CHECKPOINT_*_MIN).
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_5 INTEGER;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_5_at TIMESTAMPTZ;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_6 INTEGER;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_6_at TIMESTAMPTZ;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_7 INTEGER;
ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS fwd_7_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_5
    ON tg_channel_watch (channel, posted_at) WHERE fwd_5 IS NULL;
CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_6
    ON tg_channel_watch (channel, posted_at) WHERE fwd_6 IS NULL;
CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_7
    ON tg_channel_watch (channel, posted_at) WHERE fwd_7 IS NULL;

-- fwd_3/fwd_8 уже существуют (031/033) — fwd_3 был "ранний измерительный",
-- теперь становится decision-чекпоинтом (индекс под IS NULL уже есть,
-- idx_tg_channel_watch_pending_3 из 031); fwd_8 остаётся запасной
-- измерительной точкой. fwd_15/fwd_15_at остаются в схеме как исторические
-- данные (219+ наблюдений уже собраны), но новый код их больше не пишет.
