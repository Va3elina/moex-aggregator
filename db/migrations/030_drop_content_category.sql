-- Убираем категорийный матчинг Шага Б целиком (найдено 2026-07-15, Вадим):
-- деление по сектору/валюте/индексу оказалось слишком расплывчатым, чтобы
-- Шаг В писал корректный пост — оставляем только точное совпадение по тикеру
-- (см. 027_content_category.sql для истории, зачем он появлялся).
ALTER TABLE content_candidates DROP COLUMN IF EXISTS category;
ALTER TABLE content_candidates DROP COLUMN IF EXISTS match_type;

DROP TABLE IF EXISTS asset_category_map;
