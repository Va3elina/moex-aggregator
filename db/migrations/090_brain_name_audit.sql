-- Аудит разметки второго мозга по имени (Routine frame-brain-audit, 10.09.2026).
-- ⚠️ Зеркало Brain/brain_sync.py:таблицы_аудита — синк создаёт таблицы сам, файл для истории.
--
-- brain_edge_reviews — вердикт агента (или человека) по связи «новость упоминает компанию»,
-- поставленной правилом имён. 'неверно' синк больше не строит — ни инкрементом, ни полной
-- пересборкой (решение живёт отдельно, как решения по держателям в brain_holder_map).
CREATE TABLE IF NOT EXISTS brain_edge_reviews (
    src         TEXT NOT NULL,
    dst         TEXT NOT NULL,
    kind        TEXT NOT NULL DEFAULT 'упоминает',
    verdict     TEXT NOT NULL CHECK (verdict IN ('верно', 'неверно', 'неясно')),
    reason      TEXT,
    reviewer    TEXT NOT NULL DEFAULT 'routine',
    batch       TEXT,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (src, dst, kind)
);
CREATE INDEX IF NOT EXISTS idx_brain_edge_reviews_when ON brain_edge_reviews (reviewed_at DESC);

-- brain_rule_proposals — исключение, которое агент предлагает дописать к правилам компании
-- (регэксп вырезает ложный оборот до поиска имени). Принимает человек в панели.
CREATE TABLE IF NOT EXISTS brain_rule_proposals (
    id            BIGSERIAL PRIMARY KEY,
    company_id    TEXT NOT NULL,
    exclude_regex TEXT NOT NULL,
    examples      JSONB,
    reason        TEXT,
    status        TEXT NOT NULL DEFAULT 'на_проверке',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at    TIMESTAMPTZ
);
