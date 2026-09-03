-- След агента: что он искал, что нашлось и что дошло до текста.
--
-- ЗАЧЕМ. Воронка постов говорит, что из 1 117 кандидатов опубликовано 5, но не
-- говорит ПОЧЕМУ именно эти. Сейчас работа агента непрозрачна целиком: видно вход
-- (новость) и выход (черновик), между ними — ничего. Разбор кандидата 1638 занял
-- полдня чтения логов, и это при том, что логи были.
--
-- ⚠️ ПОЧЕМУ ТАБЛИЦА, А НЕ РАЗБОР ЛОГОВ. Логи пишутся для человека и меняются вместе
-- с кодом: любая правка формулировки ломает парсер. След — это данные, у которых
-- есть схема. Плюс объём: строка на обращение весит около 400 байт, девять обращений
-- на кандидата — 4 КБ. Разбирать ради того же гигабайты текста бессмысленно.
--
-- ⚠️ САМОЕ ЦЕННОЕ ПОЛЕ — outcome, а не result_count. Знать, что нашлось 6 рёбер,
-- полезно; знать, что из них агент взял 2, а 4 отбросил и по какой причине — это и
-- есть его «чуйка», выраженная данными. Отказ здесь такой же результат, как находка,
-- поэтому пустой ответ тоже пишется строкой, а не пропускается.
--
-- Применение на проде:
--   docker cp db/migrations/070_agent_trace.sql frame-db-1:/tmp/
--   docker exec frame-db-1 psql -U postgres -d moex_db -f /tmp/070_agent_trace.sql

CREATE TABLE IF NOT EXISTS agent_trace (
    id             BIGSERIAL PRIMARY KEY,
    -- Кандидат, ради которого агент ходил в данные. Без внешнего ключа намеренно:
    -- след не должен мешать чистке content_candidates и не должен падать, если
    -- кандидат исчез. Осиротевшие строки чистятся по created_at.
    candidate_id   INTEGER,
    step           VARCHAR(24) NOT NULL,     -- 'бриф' | 'судья' | 'писатель'
    seq            SMALLINT    NOT NULL,     -- порядок обращения внутри шага
    source         VARCHAR(40) NOT NULL,     -- world_facts | news_archive | candles | ...
    -- Вопрос человеческим языком: «кто связан с AFKS», «что было вокруг OZON».
    -- Не SQL: след читают глазами в дашборде, а не воспроизводят машиной.
    question       TEXT        NOT NULL,
    params         JSONB,                    -- {"tickers": ["AFKS"], "as_of": "2026-09-01"}
    result_count   INTEGER     NOT NULL DEFAULT 0,
    result_note    TEXT,                     -- '6 рёбер', 'пусто'
    -- взято     — попало в бриф и дошло до модели;
    -- не_взято  — нашлось, но агент отбросил (причина в outcome_reason);
    -- пусто     — источник ничего не вернул.
    outcome        VARCHAR(16) NOT NULL,
    outcome_reason TEXT,
    duration_ms    INTEGER,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Главный запрос дашборда: «покажи весь путь кандидата по порядку».
CREATE INDEX IF NOT EXISTS idx_agent_trace_candidate ON agent_trace (candidate_id, step, seq);
-- Агрегаты «куда агент ходит чаще всего» и «что чаще всего пустое».
CREATE INDEX IF NOT EXISTS idx_agent_trace_source ON agent_trace (source, outcome);
CREATE INDEX IF NOT EXISTS idx_agent_trace_created ON agent_trace (created_at DESC);

COMMENT ON COLUMN agent_trace.outcome IS
  'взято | не_взято | пусто. Отказ — такой же результат, как находка: именно по '
  'не_взято видно, что агент посмотрел связь и сознательно прошёл мимо.';
COMMENT ON COLUMN agent_trace.question IS
  'Человеческим языком, не SQL: след читают глазами в дашборде.';
