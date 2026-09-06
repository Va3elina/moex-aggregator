-- Старый ключ (source, code, event_date, category, title) ломается на реальной ленте:
-- у Яндекса 01.09.2026 два разных сообщения «Прекращение у лица права распоряжаться…»
-- в один день (разные лица, разные fm_id). С миграции 085 ключ дедупликации — fm_id.
ALTER TABLE disclosure_events DROP CONSTRAINT IF EXISTS disclosure_events_source_code_event_date_category_title_key;
CREATE INDEX IF NOT EXISTS idx_disclosure_code_date ON disclosure_events (code, event_date DESC);
