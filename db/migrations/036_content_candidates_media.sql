-- Фото поста (MarketTwits/Smartlab часто публикуют с картинкой) — Вадим
-- 2026-07-16: "посты без фото приходят". Храним ТОЛЬКО имя файла (не полный
-- путь) — host-скрипт (tg_hype_scan.py) пишет в /opt/frame/data/content_media/,
-- api-контейнер читает то же самое через volume-маунт /data/content_media (ro).
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS media_filename TEXT;
