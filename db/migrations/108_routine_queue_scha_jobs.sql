-- 108: общая очередь рутин (Claude Code Routines) + задания по справкам о СЧА.
--
-- routine_queue — ЕДИНСТВЕННЫЙ путь запуска рутин для всех заводов. Раньше каждый скрипт стрелял
-- /fire сам (signals/content_ai.py, monitor, mandate-scan); одновременные запуски падали ещё на
-- подъёме облачного контейнера (14.07.2026), а кончившаяся квота подписки роняла всё разом (16.07).
-- Теперь завод кладёт задание, а запускает только signals/routine_dispatch.py: не больше N
-- сессий одновременно, пауза между запусками, общая пауза очереди при ошибке квоты.
--
-- scha_jobs — одна найденная справка о СЧА (форма 0420502) с сайта УК: файл и текст в архиве,
-- чтение рутиной frame-scha-reader (она же проверяет себя по брифу), заливка, сообщение.
-- Конвейер — signals/scha_pipeline.py, план — research/scha_pipeline/PLAN.md.

CREATE TABLE IF NOT EXISTS routine_queue (
    id           BIGSERIAL PRIMARY KEY,
    routine      VARCHAR(40)  NOT NULL,          -- ключ рутины: SCHA_CHECK → TRIGGER_ID_SCHA_CHECK / CLAUDE_ROUTINE_FIRE_TOKEN_SCHA_CHECK
    payload      TEXT         NOT NULL,          -- текст, который уходит в /fire как {"text": ...}
    source       VARCHAR(40)  NOT NULL,          -- кто поставил: scha, content_ai, ...
    priority     SMALLINT     NOT NULL DEFAULT 100,   -- меньше = раньше (посты 10, справки 100)
    dedup_key    VARCHAR(120),                   -- одно активное задание на ключ
    status       VARCHAR(12)  NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'firing', 'running', 'done', 'failed', 'cancelled')),
    attempts     SMALLINT     NOT NULL DEFAULT 0,
    max_attempts SMALLINT     NOT NULL DEFAULT 3,
    timeout_sec  INTEGER      NOT NULL DEFAULT 1800,  -- сколько ждать колбэк после запуска
    not_before   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    fired_at     TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    session_id   VARCHAR(80),
    last_error   TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_routine_queue_pick ON routine_queue (status, priority, id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_routine_queue_active_dedup
    ON routine_queue (dedup_key) WHERE dedup_key IS NOT NULL AND status IN ('queued', 'firing', 'running');

-- Одна строка: общая пауза очереди (квота) и время последнего запуска (пауза между запусками).
CREATE TABLE IF NOT EXISTS routine_queue_state (
    id           SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    paused_until TIMESTAMPTZ,
    pause_reason TEXT,
    last_fire_at TIMESTAMPTZ
);
INSERT INTO routine_queue_state (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS scha_jobs (
    id            BIGSERIAL PRIMARY KEY,
    ticker        VARCHAR(20)  NOT NULL,         -- funds.ticker
    period        CHAR(7)      NOT NULL,         -- YYYY-MM
    url           TEXT         NOT NULL,
    sha256        CHAR(64)     NOT NULL UNIQUE,  -- один файл — одно задание, сколько бы раз его ни нашли
    file_path     TEXT         NOT NULL,         -- хост: /opt/frame/data/manual_scha/auto/ТИКЕР/...
    text_path     TEXT,                          -- вынутый текст рядом с файлом (.txt)
    status        VARCHAR(12)  NOT NULL DEFAULT 'new'
                  CHECK (status IN ('new', 'text_ready', 'agent_wait', 'agent_done',
                                    'imported', 'report_only', 'doubt', 'failed')),
    agent         JSONB,                         -- ответ агента: бумаги, итоги, самопроверка, сомнения
    agent_ok      BOOLEAN,                       -- агент написал «сошлось»
    queue_id      BIGINT REFERENCES routine_queue(id),
    attempts      SMALLINT     NOT NULL DEFAULT 0,
    next_try_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error    TEXT,
    import_result TEXT,
    notified_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scha_jobs_work ON scha_jobs (status, next_try_at);
CREATE INDEX IF NOT EXISTS idx_scha_jobs_fund ON scha_jobs (ticker, period);

-- Журнал: каждый переход задания с временем — чтобы восстановить, что и когда случилось.
CREATE TABLE IF NOT EXISTS scha_job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     BIGINT       REFERENCES scha_jobs(id) ON DELETE CASCADE,
    event      VARCHAR(30)  NOT NULL,
    detail     TEXT,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_scha_job_events_job ON scha_job_events (job_id, id);

-- Антиспам уведомлений о сбоях: не чаще раза в сутки на один и тот же ключ (сайт, рутина, квота).
CREATE TABLE IF NOT EXISTS scha_notify_log (
    key     VARCHAR(120) PRIMARY KEY,
    sent_at TIMESTAMPTZ  NOT NULL
);
