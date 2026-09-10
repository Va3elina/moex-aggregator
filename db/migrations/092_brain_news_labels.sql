-- Ярлыки новостей второго мозга (10.09.2026). ⚠️ Зеркало Brain/brain_sync.py:таблицы_аудита.
--
-- brain_news_labels — тип события и роли компаний у новости (узел news:…). Правила синка
-- (ярлыки_новостей) ставят тип по хэштегам и ключевым словам и роль по началу текста;
-- что правила не разметили, дорабатывает ночной агент (режим labels аудита).
-- Отдельная таблица, а не payload узла: новости() перезаписывает payload при повторном
-- импорте, и разметка агента пропала бы.
CREATE TABLE IF NOT EXISTS brain_news_labels (
    node_id        TEXT PRIMARY KEY,
    тип            TEXT,
    тип_источник   TEXT,          -- правило | агент | admin
    роли           JSONB,         -- {"company:SBER": "главная" | "упоминание"}
    роли_источник  TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Роль компании на самой связи «новость упоминает компанию» — копия из brain_news_labels,
-- чтобы обход карты отличал новость про компанию от перечня.
ALTER TABLE brain_edges ADD COLUMN IF NOT EXISTS role TEXT;
