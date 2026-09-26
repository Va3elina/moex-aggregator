-- Снимки числа подписчиков канала (раз в час из tg_bot.py, последний за день
-- побеждает) и гейт «аномалия за день уже отправлена» для уведомлений админу.
-- Обе таблицы бот создаёт сам при старте (ensure_joins_table), файл для истории.
CREATE TABLE IF NOT EXISTS tg_channel_members (
    chat_id   BIGINT      NOT NULL,
    day       DATE        NOT NULL,
    members   INTEGER     NOT NULL,
    taken_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, day)
);

CREATE TABLE IF NOT EXISTS tg_join_alerts (
    chat_id  BIGINT      NOT NULL,
    day      DATE        NOT NULL,
    kind     TEXT        NOT NULL CHECK (kind IN ('join', 'leave')),
    count    INTEGER     NOT NULL,
    sent_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, day, kind)
);
