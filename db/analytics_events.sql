-- Analytics events table — log пользовательских событий для admin-stats.
--
-- Принципы хранения:
-- - PII не хранится: email/имя/IP — нет. Только country code из X-Forwarded-Country
--   (если nginx/cloudflare прокидывает) и обобщённое device-type.
-- - Retention 180 дней (cron в orchestrator удаляет старые).
-- - JSONB payload — для произвольных полей (secid, mode, period, indicator).
-- - Индексы заточены под admin-stats queries: GROUP BY date, event_type, event_path.

CREATE TABLE IF NOT EXISTS analytics_events (
    event_id    BIGSERIAL PRIMARY KEY,
    user_id     INT REFERENCES users(id) ON DELETE SET NULL,  -- NULL для гостей
    session_id  VARCHAR(36) NOT NULL,                          -- UUID на client (sessionStorage)
    event_type  VARCHAR(50) NOT NULL,                          -- pageview / indicator_view / chart_export / ...
    event_path  VARCHAR(255),                                  -- e.g. /seasonality, /heatmap (NULL если не applicable)
    payload     JSONB,                                          -- {secid, mode, period, ...}
    client_ts   TIMESTAMP NOT NULL,                            -- когда event случился у user (его часы)
    server_ts   TIMESTAMP NOT NULL DEFAULT NOW(),              -- когда долетело до сервера
    ip_country  VARCHAR(2),                                     -- 'RU' / 'BY' / 'KZ' / ... (без точного IP)
    device      VARCHAR(20)                                     -- 'mobile' / 'tablet' / 'desktop' / 'bot'
);

-- Индексы:
-- 1. По server_ts DESC — для recent events / "что сейчас происходит"
CREATE INDEX IF NOT EXISTS idx_ae_server_ts ON analytics_events (server_ts DESC);
-- 2. По (event_type, server_ts) — для top-events / time-series
CREATE INDEX IF NOT EXISTS idx_ae_type_ts ON analytics_events (event_type, server_ts DESC);
-- 3. По (user_id, server_ts) — user-specific stats (e.g. "что user X смотрит")
CREATE INDEX IF NOT EXISTS idx_ae_user_ts ON analytics_events (user_id, server_ts DESC) WHERE user_id IS NOT NULL;
-- 4. По event_path — для top-pages aggregation
CREATE INDEX IF NOT EXISTS idx_ae_path ON analytics_events (event_path) WHERE event_path IS NOT NULL;
-- 5. По session_id — для session reconstruction (avg session duration)
CREATE INDEX IF NOT EXISTS idx_ae_session ON analytics_events (session_id, server_ts);
-- 6. GIN на payload — для queries типа "WHERE payload @> '{secid: SBER}'"
CREATE INDEX IF NOT EXISTS idx_ae_payload_gin ON analytics_events USING GIN (payload);

COMMENT ON TABLE analytics_events IS
    'Лог пользовательских событий для admin-аналитики. Retention 180 дней.';
COMMENT ON COLUMN analytics_events.user_id IS
    'NULL для незалогиненных гостей. ON DELETE SET NULL — события сохраняются если user удаляется.';
COMMENT ON COLUMN analytics_events.session_id IS
    'UUID generated на client при первом event. sessionStorage → умирает на закрытие вкладки.';
COMMENT ON COLUMN analytics_events.payload IS
    'JSONB с произвольными полями: {secid, mode, period, indicator, from, ...}.';
