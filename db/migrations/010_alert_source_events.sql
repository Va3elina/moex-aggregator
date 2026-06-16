-- Батч 2 (фундамент системы алертов): источник сигнала на алерте + лог
-- жизненного цикла алертов для админ-статистики.
--
-- (a) alerts.source — разделение сигналов по источнику в кабинете:
--     'oi' (текущие OI/цена-алерты) | 'funds' (будущий фонд-детектор).
--     Все существующие алерты = 'oi' (indicator ∈ {price, oi_*}). Колонка с
--     NOT NULL DEFAULT — добавление безопасно и обратносовместимо: старый код,
--     работающий до деплоя, просто не видит её.
-- (b) alert_events — трекинг «поставил/убрал/пауза/возобновил». Намеренно БЕЗ
--     FK на alerts: событие 'deleted' должно пережить удаление алерта.
--     Денормализованы asset/indicator/source, чтобы строить статистику без
--     джойна на (возможно удалённый) алерт.
--
-- Идемпотентно (IF NOT EXISTS везде) и обратносовместимо.
-- Применение: cat db/migrations/010_alert_source_events.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

-- (a) Источник сигнала на алерте.
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS source VARCHAR(16) NOT NULL DEFAULT 'oi';
-- Бэкфилл явно (на случай если колонка уже существовала без дефолта): все
-- текущие алерты — OI/цена → 'oi'.
UPDATE alerts SET source = 'oi' WHERE source IS NULL OR source = '';

-- (b) Лог жизненного цикла алертов (для админ-статистики).
CREATE TABLE IF NOT EXISTS alert_events (
  id         SERIAL PRIMARY KEY,
  alert_id   INTEGER,                       -- БЕЗ FK: алерт мог быть удалён
  user_id    INTEGER NOT NULL,
  event      VARCHAR(16) NOT NULL,          -- 'created'|'deleted'|'paused'|'resumed'
  asset      VARCHAR(20),
  indicator  VARCHAR(24),
  source     VARCHAR(16),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alert_events_user    ON alert_events(user_id);
CREATE INDEX IF NOT EXISTS idx_alert_events_created ON alert_events(created_at);
