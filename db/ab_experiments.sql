-- A/B testing infrastructure — конфиги экспериментов + per-subject assignments.
--
-- Принципы:
-- - name unique (используется в URL для assignment endpoint)
-- - Deterministic split: hash(experiment_name + subject_id) mod 100 → bucket
-- - subject_id = user_id если auth, иначе session_id (в realtime) или 'anonymous'
-- - assignments idempotent (ON CONFLICT DO NOTHING)
-- - При удалении experiment'а — assignments удаляются cascade'ом

CREATE TABLE IF NOT EXISTS ab_experiments (
    name           VARCHAR(64) PRIMARY KEY,           -- e.g. "homepage_screener_v1"
    description    TEXT,
    variant_a      VARCHAR(64) NOT NULL,              -- e.g. "old", "control"
    variant_b      VARCHAR(64) NOT NULL,              -- e.g. "new", "with_screener"
    traffic_split  INT NOT NULL DEFAULT 50 CHECK (traffic_split BETWEEN 0 AND 100),  -- % в variant_a
    active         BOOLEAN NOT NULL DEFAULT TRUE,     -- false = endpoint возвращает default fallback (a)
    created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ab_assignments (
    experiment_name  VARCHAR(64) NOT NULL REFERENCES ab_experiments(name) ON DELETE CASCADE,
    subject_id       VARCHAR(64) NOT NULL,            -- user_id или session_id
    variant          VARCHAR(1)  NOT NULL CHECK (variant IN ('a', 'b')),
    assigned_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (experiment_name, subject_id)
);

CREATE INDEX IF NOT EXISTS idx_ab_assignments_subject
    ON ab_assignments (subject_id);
CREATE INDEX IF NOT EXISTS idx_ab_assignments_variant
    ON ab_assignments (experiment_name, variant);

COMMENT ON TABLE ab_experiments IS
    'Конфиги A/B экспериментов. name — уникальный slug для URL и кода.';
COMMENT ON COLUMN ab_experiments.traffic_split IS
    '% трафика в variant_a (например 50 → 50/50, 30 → 30% A / 70% B).';
COMMENT ON TABLE ab_assignments IS
    'Persistent assignment subject → variant. ON CONFLICT DO NOTHING (idempotent).';
