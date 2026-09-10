-- Второе мнение для аудита разметки (10.09.2026). ⚠️ Зеркало Brain/brain_sync.py:таблицы_аудита.
-- «неверно» одного агента связь больше не удаляет: удаляет только согласие двух независимых
-- проверок или решение человека. Расхождение — очередь «спорные» в панели.
ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS second_verdict TEXT
    CHECK (second_verdict IN ('верно', 'неверно', 'неясно'));
ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS second_reason TEXT;
ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS second_at TIMESTAMPTZ;
ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS human_decision TEXT
    CHECK (human_decision IN ('убрать', 'оставить'));
ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS human_at TIMESTAMPTZ;
