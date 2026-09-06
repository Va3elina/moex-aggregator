-- Документы компаний (план 2): файл с CDN FinanceMarker → версия по хэшу → текст и таблицы постранично.
--
-- ⚠️ ВЕРСИЯ = НОВЫЙ ХЭШ ПО ТОЙ ЖЕ ССЫЛКЕ. Документ у FM может смениться (link_update,
-- preliminary → итоговый); старая версия не перезаписывается, а закрывается superseded_at —
-- у извлечённых из неё фактов остаётся опора. Страницы хранятся отдельно: у каждой цифры
-- в посте должна быть страница-опора, а не «где-то в отчёте».
--
-- Файлы лежат на диске сервера (/opt/frame/docs/<SECID>/…), в базе — путь, хэш, размер.

CREATE TABLE IF NOT EXISTS document_versions (
    id            BIGSERIAL PRIMARY KEY,
    url           TEXT        NOT NULL,           -- ссылка из company_documents (CDN FM)
    issuer_id     INTEGER,
    secid         VARCHAR(24),
    doc_type      VARCHAR(24) NOT NULL,           -- financial_report | presentation
    standard      VARCHAR(12),                    -- МСФО | РСБУ | NULL (из имени файла)
    period_code   VARCHAR(8),                     -- q | 6m | 9m | y (из имени файла)
    year          SMALLINT,
    month         SMALLINT,
    file_name     TEXT,                           -- имя у FM (sber_2026_6_6m_msfo.pdf)
    file_path     TEXT        NOT NULL,           -- где лежит на диске
    sha256        CHAR(64)    NOT NULL,
    bytes         INTEGER     NOT NULL,
    pages         SMALLINT,
    text_chars    INTEGER,                        -- сумма знаков текста: 0 = скан, ИИ по нему не читает
    tables        SMALLINT,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_at TIMESTAMPTZ,                    -- закрыта более новой версией той же ссылки
    note          TEXT,
    UNIQUE (url, sha256)
);
CREATE INDEX IF NOT EXISTS idx_docver_secid ON document_versions (secid, year DESC, month DESC);
CREATE INDEX IF NOT EXISTS idx_docver_live ON document_versions (url) WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS document_pages (
    version_id  BIGINT   NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
    page        SMALLINT NOT NULL,
    text        TEXT,
    tables      JSONB,                            -- [[[ячейки]]] как отдал pdfplumber; NULL = таблиц нет
    PRIMARY KEY (version_id, page)
);
CREATE INDEX IF NOT EXISTS idx_docpages_fts ON document_pages USING GIN (to_tsvector('russian', COALESCE(text, '')));
