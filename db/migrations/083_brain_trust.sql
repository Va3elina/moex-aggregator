-- Слой доверия карты нодов + правила, которые пополняют её сами.
--
-- ⚠️ УРОВЕНЬ — У КАЖДОГО РЕБРА, И ОН ПРИХОДИТ В ОТВЕТАХ РУЧЕК.
--   A — факт из первоисточника с датой (раскрытие УК, состав индекса МосБиржи, наша
--       собственная связь кандидат→новость, подтверждённое Вадимом владение);
--   B — факт от посредника (FinanceMarker, smart-lab, хэштег автора канала);
--   C — наш вывод по правилу (разметка новости по имени компании, детектор аномалий);
--   D — статистическая подсказка (вместе в новостях, векторная близость).
-- Правило для агентов: A и B можно утверждать со ссылкой и датой, C — «по нашей
-- разметке», D — не утверждать, только искать.

ALTER TABLE brain_edges
    ADD COLUMN IF NOT EXISTS level TEXT,           -- A | B | C | D
    ADD COLUMN IF NOT EXISTS method TEXT,          -- откуда взялось: хэштег, имя, акционеры_fm, раскрытие_ук…
    ADD COLUMN IF NOT EXISTS snapshot_date DATE;   -- на какую дату верен факт (не когда записали)

-- Нормализация имени организации: регистр, кавычки, правовые формы, пунктуация.
-- Одна функция для держателей, псевдонимов и правил — иначе три разные «почти
-- одинаковые» нормализации разойдутся через месяц.
CREATE OR REPLACE FUNCTION brain_norm(s TEXT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
    SELECT trim(regexp_replace(regexp_replace(regexp_replace(lower(coalesce(s, '')),
        '[«»"''`“”„]', '', 'g'),
        '\m(публичное акционерное общество|акционерное общество|общество с ограниченной ответственностью|закрытое акционерное общество|открытое акционерное общество|public joint[- ]stock company|joint[- ]stock company|limited liability company|пао|оао|ооо|зао|ао|нко|нкоао|мкпао|мкао|plc|llc|ltd|limited|inc|corp|s\.a\.|b\.v\.|n\.v\.|gmbh|ag|cy|llp|dmcc)\M\.?', ' ', 'g'),
        '[^[:alnum:]]+', ' ', 'g'))
$$;

-- Правила разметки новостей по имени компании. Сеются из справочника, но правки
-- руками (ambiguous, enabled, note) переживают пересев: ON CONFLICT DO NOTHING.
CREATE TABLE IF NOT EXISTS brain_name_rules (
    id         SERIAL PRIMARY KEY,
    pattern    TEXT NOT NULL,                  -- как ищем (полнотекстом, с морфологией)
    company_id TEXT NOT NULL,                  -- company:SBER
    ambiguous  BOOLEAN NOT NULL DEFAULT FALSE, -- обычное слово (Магнит, Полюс, Система): не размечаем автоматически
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    source     TEXT,                           -- name_short | display_name | fund_asset_name | ручное
    note       TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pattern, company_id)
);

-- Сопоставление держателей (акционеров строкой) с компаниями справочника.
-- Точное совпадение — «авто», похожее — «на_проверке» (очередь на экране),
-- решение человека — «подтверждено» / «отклонено» и переживает пересборку.
CREATE TABLE IF NOT EXISTS brain_holder_map (
    holder_norm TEXT PRIMARY KEY,
    holder      TEXT NOT NULL,
    company_id  TEXT,
    method      TEXT,                          -- точное | похожее | ручное
    confidence  REAL,
    status      TEXT NOT NULL DEFAULT 'авто',  -- авто | на_проверке | подтверждено | отклонено
    candidates  JSONB,                         -- варианты для проверки: [{company_id, sim}]
    reviewed_at TIMESTAMPTZ,
    note        TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_holder_map_status ON brain_holder_map (status);
