-- Стенд: раскладки интерфейса (рабочие пространства), 2026-09-17. Хранит и настройки «как я оставил» (имя default),
-- и раскладки, которые собирает агент, чтобы дать ссылку /admin/backtest?ws=<имя>. Идемпотентно, применить ДО деплоя:
--   cat db/migrations/099_bt_workspaces.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
CREATE TABLE IF NOT EXISTS bt_workspaces (
    name        TEXT PRIMARY KEY,
    data        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bt_workspaces IS 'Стенд: раскладки интерфейса бэктест-терминала (admin-only, /api/admin/bt/workspaces)';
