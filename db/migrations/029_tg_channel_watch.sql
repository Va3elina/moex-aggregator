-- Обобщение markettwits_watch под несколько TG-каналов (Этап 6 content-пайплайна):
-- добавляем @newssmartlab вторым источником репост-хайпа, тем же механизмом,
-- что и MarketTwits. message_id не глобально уникален между каналами (у каждого
-- канала своя нумерация сообщений) — PK теперь составной (channel, message_id).
ALTER TABLE markettwits_watch RENAME TO tg_channel_watch;

ALTER TABLE tg_channel_watch ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'markettwits';
ALTER TABLE tg_channel_watch ALTER COLUMN channel DROP DEFAULT;

ALTER TABLE tg_channel_watch DROP CONSTRAINT IF EXISTS markettwits_watch_pkey;
ALTER TABLE tg_channel_watch ADD PRIMARY KEY (channel, message_id);

DROP INDEX IF EXISTS idx_markettwits_watch_pending_15;
DROP INDEX IF EXISTS idx_markettwits_watch_pending_90;

CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_15
    ON tg_channel_watch (channel, posted_at) WHERE fwd_15 IS NULL;
CREATE INDEX IF NOT EXISTS idx_tg_channel_watch_pending_90
    ON tg_channel_watch (channel, posted_at) WHERE fwd_15 IS NOT NULL AND fwd_90 IS NULL;
