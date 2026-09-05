-- Второй мозг как карта нодов: узлы и типизированные рёбра, материализованные из
-- существующих таблиц. Ничего здесь не является источником истины — это индекс
-- связей поверх news_archive, content_candidates, company_documents, world_facts,
-- fund_holdings_history, index_composition, anomalies, ownership_signals,
-- company_shareholders. Пересобирается пайплайном brain_sync (инкремент по
-- водяным знакам, полная пересборка по --full).
--
-- ⚠️ ВХОД ВСЕГДА ЧЕРЕЗ УЗЕЛ. Индексы заточены под «соседи узла по типу за период»
-- (dst, kind, ts) и (src, kind, ts) — один range-scan на кольцо. Обхода всей
-- таблицы рёбер не бывает.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS brain_nodes (
    id         TEXT PRIMARY KEY,            -- вид:ключ, стабильный: company:SBER, news:markettwits/130070
    kind       TEXT NOT NULL,               -- company | news | candidate | post | doc | fund | index | fact | anomaly | signal | holder
    key        TEXT NOT NULL,
    title      TEXT NOT NULL,
    summary    TEXT,
    ts         TIMESTAMPTZ,                 -- когда случилось / на какую дату актуально
    payload    JSONB,                       -- только то, что нужно показать на странице узла
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_nodes_kind_ts ON brain_nodes (kind, ts DESC);
CREATE INDEX IF NOT EXISTS idx_brain_nodes_title_trgm ON brain_nodes USING gin (title gin_trgm_ops);

CREATE TABLE IF NOT EXISTS brain_edges (
    src    TEXT NOT NULL,
    dst    TEXT NOT NULL,
    kind   TEXT NOT NULL,                   -- упоминает | о | из_новости | отчитался | держит | включает | владеет | факт_о | аномалия_по | сигнал_о | владеет_долей
    ts     TIMESTAMPTZ,
    weight REAL,
    source TEXT,
    PRIMARY KEY (src, dst, kind)
);
CREATE INDEX IF NOT EXISTS idx_brain_edges_dst ON brain_edges (dst, kind, ts DESC);
CREATE INDEX IF NOT EXISTS idx_brain_edges_src ON brain_edges (src, kind, ts DESC);

-- Водяные знаки инкремента: до какого времени источник уже разобран.
CREATE TABLE IF NOT EXISTS brain_sync_state (
    source     TEXT PRIMARY KEY,
    watermark  TIMESTAMPTZ,
    rows_last  INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Тикер (бумага, код фьючерса) → узел компании. Пересобирается каждым прогоном.
CREATE TABLE IF NOT EXISTS brain_ticker_map (
    ticker     TEXT PRIMARY KEY,
    company_id TEXT NOT NULL
);
