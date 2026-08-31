-- Архив новостных Telegram-каналов (markettwits, newssmartlab) — сырьё для
-- второго мозга и для исследований.
--
-- ЗАЧЕМ. Конвейер постов прожевал 932 новостных кандидата и выбросил 850: каждый
-- был датирован, имел источник и уже прошёл ИИ-оценку значимости, но отбрасывался
-- за то, что не годился для поста ПРО ТИКЕР. Как «что происходит в мире» они
-- годятся целиком (research/content_pipeline_v2/MACRO_BRAIN.md).
-- Плюс живой ингест встал 11.08 и не может возобновиться: MTProto с сервера
-- заблокирован по всем дата-центрам (проверено 31.08). Историю иначе не достать —
-- Вадим выгружает её из Telegram Desktop.
--
-- ⚠️ ПОЧЕМУ НЕ pgvector. Его нет в доступных расширениях этого образа Postgres
-- (есть только pg_trgm и pg_stat_statements), а менять образ БД ради поиска —
-- несоразмерный риск. Вместо него:
--   • лексический поиск — родной полнотекстовый Postgres с РУССКИМ словарём
--     (конфигурация `russian` в образе есть), GIN-индекс, ноль новых зависимостей;
--   • семантический — эмбеддинги отдельным файлом-memmap рядом с приложением
--     (500 тыс. × 256 float16 ≈ 256 МБ), а не в БД: RAM на сервере всего 7 ГБ,
--     из них ~3 свободно, и держать полтерабайта… полгигабайта векторов в
--     Postgres при 17 ГБ уже занятой базы неразумно.
--
-- ⚠️ Масштаб: у markettwits на 31.08 message_id дошёл до 383013, у newssmartlab —
-- до 129768. То есть речь про сотни тысяч строк, а не про тысячи. Отсюда
-- отдельная таблица, а не расширение content_candidates: у той совсем другая
-- жизнь (статусы, ревью, дедуп по треду), и смешивать архив с очередью нельзя.

CREATE TABLE IF NOT EXISTS news_archive (
    channel      TEXT        NOT NULL,
    message_id   BIGINT      NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    text         TEXT        NOT NULL,
    views        INTEGER,              -- из веб-превью t.me/s/; в JSON-выгрузке Telegram
                                       -- Desktop просмотров НЕТ (технический потолок,
                                       -- проверено в июле) → у исторических строк NULL
    hashtags     TEXT[],
    entities     JSONB,                -- ссылки и упоминания, как пришли в выгрузке
    tickers      TEXT[],               -- распознанные тикеры (заполняется отдельным проходом)
    source       TEXT        NOT NULL DEFAULT 'tg_export',  -- tg_export | tg_web
    imported_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel, message_id)   -- нумерация у каждого канала своя, как в
                                        -- tg_channel_watch: составной ключ обязателен
);

-- Хронология: главный запрос второго мозга — «что происходило до даты X».
CREATE INDEX IF NOT EXISTS idx_news_archive_posted ON news_archive (posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_archive_channel_posted ON news_archive (channel, posted_at DESC);

-- Полнотекстовый поиск по-русски. Выражение в индексе, а не отдельная колонка
-- tsvector: колонка удвоила бы объём таблицы, а GENERATED STORED на сотнях тысяч
-- строк ещё и замедлит импорт. Запрос обязан использовать РОВНО то же выражение,
-- иначе индекс не подхватится:
--   WHERE to_tsvector('russian', text) @@ websearch_to_tsquery('russian', :q)
CREATE INDEX IF NOT EXISTS idx_news_archive_fts
    ON news_archive USING GIN (to_tsvector('russian', text));

-- Поиск по тикерам («все новости про GAZP») — массив, GIN.
CREATE INDEX IF NOT EXISTS idx_news_archive_tickers ON news_archive USING GIN (tickers);
