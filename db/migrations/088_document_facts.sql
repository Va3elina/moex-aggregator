-- Чтение документов ИИ (план 2, этап 3): один прогон читателя = document_reads,
-- каждое извлечённое значение = document_facts с обязательной страницей и цитатой-опорой.
-- Факты не перезаписываются: новое чтение — новые строки, сравнение — по read_id.
CREATE TABLE IF NOT EXISTS document_reads (
    id           BIGSERIAL PRIMARY KEY,
    version_id   BIGINT NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
    schema_name  VARCHAR(40) NOT NULL,          -- ifrs_interim | ifrs_annual | rsbu | presentation | operational
    model        VARCHAR(80),
    summary      JSONB,                          -- краткое содержание по схеме (прогноз, сегменты, разовые, риски)
    facts_count  INTEGER NOT NULL DEFAULT 0,
    mismatches   INTEGER NOT NULL DEFAULT 0,     -- сколько цифр разошлись с карточкой FM > 2%
    candidate_id INTEGER,                        -- если чтение шло под кандидата
    pages_read   INTEGER,                        -- сколько страниц агент реально запросил
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_docreads_version ON document_reads (version_id, created_at DESC);

CREATE TABLE IF NOT EXISTS document_facts (
    id           BIGSERIAL PRIMARY KEY,
    read_id      BIGINT NOT NULL REFERENCES document_reads(id) ON DELETE CASCADE,
    version_id   BIGINT NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
    field        VARCHAR(80) NOT NULL,           -- код поля схемы (net_profit, guidance, segment:…)
    value_num    DOUBLE PRECISION,
    value_text   TEXT,
    unit         VARCHAR(24),
    page         SMALLINT,
    quote        TEXT,                           -- дословная опора со страницы
    fm_value     DOUBLE PRECISION,               -- что говорит карточка FM по тому же полю (если есть)
    mismatch     BOOLEAN,                        -- расхождение с FM > 2%
    confidence   VARCHAR(8),                     -- высокая | средняя | низкая
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_docfacts_version ON document_facts (version_id, field);
