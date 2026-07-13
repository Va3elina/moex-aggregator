-- ticker_futures_map — справочник акция↔фьючерс для Шага А2 content-пайплайна
-- (см. db/migrations/022_content_candidates.sql). Явного маппинга тикер-акции
-- на тикер-фьючерса в БД до этой миграции не было (проверено — только неявная
-- связь через instruments.sectype, которая не даёт обратного индекса
-- акция→фьючерс). futures_sectype — код в open_interest.sectype/anomalies.asset_id
-- (напр. GAZPF, PX «Полюс мини» — не всегда простой суффикс тикера акции).
--
-- Плюс доп.колонки в content_candidates, не вошедшие в 022:
--   sources          — JSONB со списком источников кросс-издательской конвергенции
--                       (publisher/url/published_at) — конвергенция по определению
--                       про НЕСКОЛЬКО источников на одну новость.
--   views_count      — просмотры (MarketTwits, слабый сигнал, отдельно от forwards_count).
--   last_checked_at  — когда кандидат последний раз сверялся с anomalies (Шаг Б,
--                       нужно для asset-cadence-aware очереди ожидания: дневные-only
--                       активы не проверяются повторно чаще раза в сутки).
--
-- Идемпотентно. Применение:
--   cat db/migrations/023_ticker_futures_map.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS ticker_futures_map (
  stock_ticker    VARCHAR(20) PRIMARY KEY,      -- 'GAZP', 'PLZL'
  futures_sectype VARCHAR(24) NOT NULL,         -- 'GAZPF', 'PX' — sectype в open_interest/anomalies
  display_name    VARCHAR(120),
  source          VARCHAR(32) NOT NULL DEFAULT 'manual',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticker_futures_map_sectype ON ticker_futures_map (futures_sectype);

-- Фикс на случай, если таблица уже создана предыдущим (более узким) прогоном этого файла.
ALTER TABLE ticker_futures_map ALTER COLUMN source TYPE VARCHAR(32);

-- Сидируем известными случаями, провалидированными вживую 2026-07-13
-- (реальные аномалии в anomalies подтверждены под этими sectype).
INSERT INTO ticker_futures_map (stock_ticker, futures_sectype, display_name, source) VALUES
  ('GAZP', 'GAZPF', 'Газпром', 'validated_2026-07-13'),
  ('PLZL', 'PX',    'Полюс',   'validated_2026-07-13'),
  ('SBER', 'SBERF', 'Сбербанк','validated_2026-07-13')
ON CONFLICT (stock_ticker) DO NOTHING;

ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS sources JSONB;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS views_count INTEGER;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
