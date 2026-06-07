-- Phase 2 Telegram alert-bot: пользовательские алерты + история срабатываний.
-- Юзер задаёт на сайте (актив/метрика/условие/порог) → host-side eval-loop (Phase 3)
-- проверяет → бот шлёт пуш. Квота по тарифу (features.py telegram_alerts_quota):
-- free=0, basic=20, pro=∞. MVP-метрики: 'price' (cross) + 'oi_zscore'. Режим 'once'.
-- Применение: cat db/migrations/008_alerts.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
CREATE TABLE IF NOT EXISTS alerts (
  id             SERIAL PRIMARY KEY,
  user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  indicator      VARCHAR(24) NOT NULL,        -- 'price' | 'oi_zscore'
  asset          VARCHAR(20) NOT NULL,        -- sectype: 'SR','Si','AFLT'...
  asset_name     VARCHAR(120),                -- для текста уведомления
  metric         VARCHAR(24) NOT NULL,        -- price: 'close'; oi: 'zscore'
  clgroup        VARCHAR(3),                  -- OI: 'FIZ'|'YUR'; price: NULL
  op             VARCHAR(12) NOT NULL,        -- 'gt'|'lt'|'cross_up'|'cross_down'
  threshold      NUMERIC(20,6) NOT NULL,
  mode           VARCHAR(8)  NOT NULL DEFAULT 'once',   -- 'once'|'repeat'
  cooldown_hours INTEGER     NOT NULL DEFAULT 24,
  status         VARCHAR(10) NOT NULL DEFAULT 'active', -- 'active'|'paused'|'fired'
  last_value     NUMERIC(20,6),               -- prev-снимок для детекта cross
  last_fired_at  TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_alerts_user   ON alerts(user_id);

CREATE TABLE IF NOT EXISTS alert_fires (
  id           SERIAL PRIMARY KEY,
  alert_id     INTEGER NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
  fired_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  value        NUMERIC(20,6),
  message_text TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_fires_alert ON alert_fires(alert_id, fired_at DESC);
