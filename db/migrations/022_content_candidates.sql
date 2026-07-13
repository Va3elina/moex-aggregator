-- content_candidates — кандидаты рыночных новостей на пути через content-пайплайн
-- (RSS/Telegram-агрегаторы → кросс-источниковая конвергенция → оценка ИИ → сверка
-- с данными (OI/потоки фондов) → черновик поста → ревью в Telegram-боте → публикация
-- в канал). Эта миграция — ТОЛЬКО хранилище состояний под админ-Kanban
-- (/admin/content-news, api/routers/content_news.py). Сам пайплайн сбора/оценки
-- (RSS-парсер, MTProto-листенер, вызовы Haiku) и публикация с кнопками ревью в
-- Telegram-боте — отдельные задачи.
--
-- STATUS STATE MACHINE:
--   candidate   → discarded    (Шаг А: не прошёл оценку релевантности/значимости;
--                                причина — в reasoning)
--   candidate   → pending      (Шаг А: релевантно, ждём подтверждения в данных;
--                                pending_expires_at = created_at + 5 дней)
--   pending     → draft_ready  (нашлась связанная аномалия → ИИ написал черновик)
--   pending     → no_data      (истёк pending_expires_at, совпадение не нашлось)
--   draft_ready → in_review    (черновик отправлен в Telegram-бот на решение человека)
--   in_review   → published | rejected  (решение человека; reviewer_action/reviewer_id)
--
-- thread_key группирует повторные сигналы по одному и тому же тикеру/событию
-- (напр. несколько кандидатов про SBER за неделю) — рендерятся связанными в UI.
-- parent_candidate_id — прямая ссылка на предыдущего кандидата того же треда.
--
-- Идемпотентно. Применение:
--   cat db/migrations/022_content_candidates.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS content_candidates (
  id                    SERIAL PRIMARY KEY,
  status                VARCHAR(20) NOT NULL DEFAULT 'candidate'
    CHECK (status IN (
      'candidate', 'discarded', 'pending', 'draft_ready',
      'no_data', 'in_review', 'published', 'rejected'
    )),
  source                VARCHAR(64),          -- 'rss:kommersant'|'markettwits'|'moex_calendar'
  headline              TEXT NOT NULL,
  raw_text              TEXT,
  tickers               TEXT[],
  futures_ticker        VARCHAR(24),          -- маппинг на asset_id в open_interest/anomalies
  event_type            VARCHAR(40),
  importance_1_5        SMALLINT CHECK (importance_1_5 BETWEEN 1 AND 5),
  reasoning             TEXT,                 -- обоснование Шага А (в т.ч. причина отбраковки)
  matched_anomaly_id    BIGINT REFERENCES anomalies(id) ON DELETE SET NULL,
  draft_text            TEXT,
  synth_declined_reason TEXT,
  thread_key            VARCHAR(64),
  parent_candidate_id   INTEGER REFERENCES content_candidates(id) ON DELETE SET NULL,
  forwards_count        INTEGER,              -- репосты (source = markettwits)
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  pending_expires_at    TIMESTAMPTZ,
  published_at          TIMESTAMPTZ,
  reviewer_action       VARCHAR(20) CHECK (reviewer_action IN ('approved', 'rejected')),
  reviewer_id           INTEGER REFERENCES users(id) ON DELETE SET NULL
);

-- Kanban-доска читает по статусу, свежие сначала.
CREATE INDEX IF NOT EXISTS idx_content_candidates_status ON content_candidates (status, created_at DESC);
-- Группировка треда (детальная модалка + визуальная связка карточек).
CREATE INDEX IF NOT EXISTS idx_content_candidates_thread ON content_candidates (thread_key) WHERE thread_key IS NOT NULL;
-- Общая лента / дефолтная сортировка.
CREATE INDEX IF NOT EXISTS idx_content_candidates_created ON content_candidates (created_at DESC);
