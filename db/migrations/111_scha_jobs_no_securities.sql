-- Справка о СЧА, в которой у фонда НЕТ ценных бумаг (только деньги, РЕПО, дебиторка): фонд только
-- запустился (WFLO 09.2026) или держит одно обратное РЕПО (SAFE 2021–2024). Агент читает это верно,
-- но такое прочтение нечего заливать — это не сомнение и не ошибка (16–17.10.2026: 19 «сомнений»).
--   cat db/migrations/111_scha_jobs_no_securities.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE scha_jobs DROP CONSTRAINT IF EXISTS scha_jobs_status_check;
ALTER TABLE scha_jobs ADD CONSTRAINT scha_jobs_status_check
    CHECK (status IN ('new', 'text_ready', 'agent_wait', 'agent_done',
                      'imported', 'report_only', 'doubt', 'failed', 'no_securities'));
