-- Ссылка на исходный пост (t.me/<channel>/<message_id> для markettwits/newssmartlab)
-- — нужна для уведомления коллеги (@hypeframebot) полным текстом + ссылкой на
-- оригинал. NULL для источников без прямой ссылки на пост (moex_calendar).
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS source_url TEXT;
