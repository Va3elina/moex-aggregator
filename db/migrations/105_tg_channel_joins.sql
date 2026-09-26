-- Вступления/выходы подписчиков Telegram-каналов с привязкой к инвайт-ссылке.
-- Пишет tg_bot.py из апдейтов chat_member (бот должен быть админом канала
-- с правом «Добавление участников»). Ссылка чужого админа приходит
-- обрезанной (https://t.me/+AbCd...), поэтому ключ отчёта: invite_name, иначе invite_link.
CREATE TABLE IF NOT EXISTS tg_channel_joins (
    id           BIGSERIAL PRIMARY KEY,
    chat_id      BIGINT      NOT NULL,
    chat_title   TEXT,
    user_id      BIGINT      NOT NULL,
    event        TEXT        NOT NULL CHECK (event IN ('join', 'leave')),
    invite_link  TEXT,
    invite_name  TEXT,
    via_folder   BOOLEAN     NOT NULL DEFAULT FALSE,
    event_at     TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tg_channel_joins_chat_time_idx
    ON tg_channel_joins (chat_id, event_at);
CREATE INDEX IF NOT EXISTS tg_channel_joins_user_idx
    ON tg_channel_joins (chat_id, user_id, event_at);
