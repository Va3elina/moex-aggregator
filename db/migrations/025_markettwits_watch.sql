-- Этап 6 content-пайплайна: отслеживание репостов @markettwits между двумя
-- чекпоинтами (+15мин / +90мин от публикации) — формула калибрована вживую
-- 2026-07-13 (лонгитюдный замер 20 постов каждые 15 мин 2 часа): подавляющая
-- часть роста репостов укладывается в первые 15 минут, ~10% сообщений
-- получают отдельный поздний всплеск на 60-90-й минуте. Два чекпоинта, не
-- постоянный поллинг и не один замер.
--
-- Отдельная таблица (не content_candidates) — большинство постов канала НЕ
-- становятся кандидатами (это firehose, ~120 постов/день), учёт репостов
-- нужен ТОЛЬКО чтобы решить, кто пересёк порог хайпа.
CREATE TABLE IF NOT EXISTS markettwits_watch (
    message_id   BIGINT PRIMARY KEY,
    posted_at    TIMESTAMPTZ NOT NULL,
    msg_text     TEXT,
    fwd_15       INTEGER,
    fwd_15_at    TIMESTAMPTZ,
    fwd_90       INTEGER,
    fwd_90_at    TIMESTAMPTZ,
    promoted     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_markettwits_watch_pending_15
    ON markettwits_watch (posted_at) WHERE fwd_15 IS NULL;
CREATE INDEX IF NOT EXISTS idx_markettwits_watch_pending_90
    ON markettwits_watch (posted_at) WHERE fwd_15 IS NOT NULL AND fwd_90 IS NULL;
