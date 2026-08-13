-- 050_user_settings.sql — синхронизация настроек юзера между устройствами.
--
-- Один JSONB-блоб на юзера: снапшот отслеживаемых localStorage-ключей фронта
-- (frame:* — настройки индикаторов/песочницы/embed-панелей, избранное, тема,
-- see services/settingsSync.ts). Конфликты: last-write-wins целиком блобом —
-- для одного человека на нескольких устройствах этого достаточно.

CREATE TABLE IF NOT EXISTS user_settings (
    user_id     INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    data        JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
