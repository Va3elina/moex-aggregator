-- Лента раскрытия FinanceMarker как источник кандидатов в посты.
--
-- ⚠️ ТЕЛО СООБЩЕНИЯ ЕСТЬ ТОЛЬКО НА САЙТЕ. API /fm/v2/disclosure отдаёт десять полей
-- (категория, тикер, дата, заголовок, ссылка…) и ни одного текстового; описание
-- события («2. Содержание сообщения…») лежит в серверном состоянии Nuxt страницы
-- financemarker.ru/disclosure/ (window.__NUXT__, 30 записей, ?offset=N). Страница
-- публичная, токена и суточной квоты не тратит. Парсер — signals/fm_disclosure_scan.py.
--
-- fm_id — их внутренний id события: единственный надёжный ключ дедупликации
-- (заголовок+дата у «Проведение заседания совета директоров» повторяются).

ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS fm_id            BIGINT;
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS name             TEXT;
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS description      TEXT;
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS dir_link         TEXT;
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS dividend_status  VARCHAR(24);
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS transaction_type VARCHAR(64);
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS candidate_id     INTEGER REFERENCES content_candidates(id) ON DELETE SET NULL;
ALTER TABLE disclosure_events ADD COLUMN IF NOT EXISTS updated_at       TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS uq_disclosure_fm_id ON disclosure_events (fm_id) WHERE fm_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_disclosure_candidate ON disclosure_events (candidate_id) WHERE candidate_id IS NOT NULL;
