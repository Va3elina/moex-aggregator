-- 080: журнал прогонов — история, которой у пульса не было.
--
-- pipeline_runs хранит ОДНУ строку на процесс: последний запуск и всё. Из этого
-- нельзя построить ни «пульс за сутки», ни ленту «что капает сейчас», ни ковёр
-- шагов оркестратора — и, главное, нельзя отличить быстрый-нормальный прогон от
-- быстрого-сломанного: для этого нужна типичная длительность, а её без истории
-- нет (эвристика «меньше секунды» на живых данных врала и была снята).
--
-- Строка на каждый завершённый прогон. ~90 в час → ~60 тыс. за месяц; старше
-- 30 дней чистится в analytics cleanup оркестратора.
--
-- rows_written — сколько записей прогон сообщил в своём итоге (сумма известных
-- ключей вроде «вставлено», «записей», «строк»). NULL — когда итог таких чисел
-- не содержит; выдумывать ноль нельзя, он читается как «ничего не записал».

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      TEXT        NOT NULL,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ NOT NULL,
    status        TEXT        NOT NULL,
    duration_sec  DOUBLE PRECISION,
    rows_written  INTEGER,
    note          TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_pipeline_time
    ON pipeline_run_log (pipeline, finished_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_time
    ON pipeline_run_log (finished_at DESC);
COMMENT ON TABLE pipeline_run_log IS 'история прогонов; одна строка на каждый завершённый запуск скрипта';
