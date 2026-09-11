-- Постоянный ID браузера в статистике посещений (2026-09-11).
--
-- До этого гость считался по вкладке (session_id в sessionStorage умирает при
-- закрытии), и «уникальные посетители» были уникальными вкладками, а возвраты
-- гостей не считались вовсе. visitor_id живёт в localStorage и cookie год,
-- как _ym_uid у Яндекс Метрики. Старые строки остаются с NULL — для них
-- статистика откатывается на session_id.
--
-- Идемпотентно, без блокировки таблицы на перезапись (колонка NULL без default).
-- Применение ДО деплоя кода, который пишет visitor_id:
--   cat db/migrations/096_analytics_visitor_id.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE analytics_events ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(36);

CREATE INDEX IF NOT EXISTS idx_ae_visitor_user
    ON analytics_events (visitor_id, user_id)
    WHERE visitor_id IS NOT NULL AND user_id IS NOT NULL;

COMMENT ON COLUMN analytics_events.visitor_id IS
    'Постоянный ID браузера (localStorage+cookie, 1 год). NULL у событий до 2026-09-11 и у старых бандлов.';
