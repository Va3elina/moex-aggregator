-- Посты публичных Telegram-каналов для секции «Новости каналов» в колоколе сайта.
--
-- Источник: парсинг t.me/s/<channel> (публичное web-превью). РКН блокирует только
-- api.telegram.org (бот идёт через релей), а t.me/s достижим с прода НАПРЯМУЮ
-- (проверено: HTTP 200, IPv4/IPv6). Ридер — signals/channel_scan.py (host-cron),
-- парсит каналы из CHANNELS (сейчас только FrameTool; Thor_INV удалён 2026-07-03),
-- апсертит последние посты.
--
-- ОТДЕЛЬНАЯ таблица (не `anomalies`): её дедуп-ключ включает signal_date →
-- несколько постов за день схлопнулись бы в один. Здесь уник по (channel, post_id).
--
-- Идемпотентно. Применение:
--   cat db/migrations/016_channel_posts.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS channel_posts (
  id           BIGSERIAL PRIMARY KEY,
  channel      VARCHAR(64)  NOT NULL,        -- username канала (см. CHANNELS в channel_scan.py)
  channel_name VARCHAR(120),                 -- человекочитаемо («Фрейм» / «Thor Invest»)
  post_id      INTEGER      NOT NULL,        -- номер поста в канале (из data-post="ch/N")
  text         TEXT,                          -- текст поста (для заголовка-сниппета)
  photo_url    TEXT,                          -- превью-картинка поста (если есть)
  link         TEXT         NOT NULL,         -- https://t.me/<channel>/<post_id>
  posted_at    TIMESTAMPTZ,                   -- дата поста (из <time datetime>)
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Уник по каналу+посту → апсерт без дублей (пере-скан не плодит строк).
CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_posts ON channel_posts (channel, post_id);
-- Лента: свежие сначала.
CREATE INDEX IF NOT EXISTS idx_channel_posts_recent ON channel_posts (posted_at DESC NULLS LAST);
