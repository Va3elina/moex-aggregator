-- ─────────────────────────────────────────────────────
-- signal_log — журнал автоматических сигналов.
-- Используется для cooldown-проверок (don't double-fire)
-- и для последующего анализа качества сигналов.
-- ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signal_log (
    id              SERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    asset           VARCHAR(20)  NOT NULL,        -- sectype (SR, GZ, ...)
    indicator       VARCHAR(40)  NOT NULL,        -- 'oi' | 'funds_money' | ...
    signal_type     VARCHAR(60)  NOT NULL,        -- 'phys_net_long_anomaly' | ...
    direction       VARCHAR(10),                  -- 'up' | 'down'
    z_score         NUMERIC,                      -- значение z-score (для аналитики)
    raw_value       NUMERIC,                      -- сам Δ (daily diff)
    channel_id      BIGINT,                       -- куда отправлено
    message_id      BIGINT,                       -- TG message_id (если sent)
    message_text    TEXT                          -- что отправили
);

CREATE INDEX IF NOT EXISTS idx_signal_log_cooldown
    ON signal_log (asset, indicator, signal_type, ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_log_ts
    ON signal_log (ts DESC);

COMMENT ON TABLE signal_log IS 'Журнал автоматических Telegram-сигналов от signal-engine';
COMMENT ON COLUMN signal_log.z_score IS 'Rolling z-score (60d window) на момент отправки';
COMMENT ON COLUMN signal_log.raw_value IS 'Daily Δ чистой позиции (для OI signals)';
