-- Стенд: свой бэктест-терминал (/admin/backtest, только админы), 2026-09-17.
-- Очередь прогонов и их результаты. Считает отдельный контейнер bt-worker (пакет backtest/), API только читает.
-- Идемпотентно. Применение ДО деплоя кода:
--   cat db/migrations/098_bt_stand.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS bt_runs (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT,
    created_by   INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'queued',      -- queued | running | done | error
    spec         JSONB NOT NULL,                      -- как попросили
    spec_full    JSONB,                               -- как посчитали (правило развёрнуто, умолчания подставлены)
    result       JSONB,                               -- метрики
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_bt_runs_status ON bt_runs (status, id);

CREATE TABLE IF NOT EXISTS bt_signals (                -- журнал решений: день бумаги в ряду
    run_id BIGINT NOT NULL REFERENCES bt_runs(id) ON DELETE CASCADE,
    st TEXT NOT NULL, d DATE NOT NULL, secid TEXT,
    pa DOUBLE PRECISION, pb DOUBLE PRECISION, move DOUBLE PRECISION,
    thr_up DOUBLE PRECISION, thr_dn DOUBLE PRECISION, straight DOUBLE PRECISION,
    side SMALLINT, tradable BOOLEAN, skip TEXT,
    PRIMARY KEY (run_id, st, d)
);

CREATE TABLE IF NOT EXISTS bt_trades (                 -- сделки: на 1 контракт (доли) и на счёте (рубли)
    run_id BIGINT NOT NULL REFERENCES bt_runs(id) ON DELETE CASCADE,
    st TEXT NOT NULL, d DATE NOT NULL, secid TEXT, side SMALLINT,
    move DOUBLE PRECISION, thr DOUBLE PRECISION,
    px_in DOUBLE PRECISION, d_out DATE, px_out DOUBLE PRECISION,
    gross DOUBLE PRECISION, comm DOUBLE PRECISION, spread DOUBLE PRECISION, net DOUBLE PRECISION,
    qty INTEGER, notional DOUBLE PRECISION, go DOUBLE PRECISION, equity_in DOUBLE PRECISION,
    comm_rub DOUBLE PRECISION, spread_rub DOUBLE PRECISION, pnl_rub DOUBLE PRECISION,
    account_skip TEXT,                                 -- сигнал был, на счёте не исполнен: причина
    PRIMARY KEY (run_id, st, d)
);

CREATE TABLE IF NOT EXISTS bt_equity (
    run_id BIGINT NOT NULL REFERENCES bt_runs(id) ON DELETE CASCADE,
    d DATE NOT NULL, equity DOUBLE PRECISION, positions SMALLINT,
    notional DOUBLE PRECISION, go_used DOUBLE PRECISION,
    PRIMARY KEY (run_id, d)
);

COMMENT ON TABLE bt_runs IS 'Стенд: очередь и результаты прогонов бэктеста (admin-only, /api/admin/bt)';
