-- Журнал решений ревьюера со СНИМКОМ черновика. Заказ Вадима 01.09.2026.
--
-- ⚠️ Что сломано без этой таблицы. Обратная связь жила в колонках самого кандидата:
-- draft_text_ai (оригинал ИИ), reviewer_reason_code, reviewer_reason. Колонка ОДНА,
-- и повторный прогон Шага В её перезаписывает — вместе с тем текстом, к которому
-- относилась причина отказа. То есть датасет «чуйки» самоуничтожался при каждой
-- правке промпта: чем активнее мы улучшаем пайплайн, тем быстрее теряем разметку,
-- на которой только и можно проверить, стало ли лучше. Живой случай: кандидата 1638
-- нельзя было перегенерировать, не потеряв разбор Вадима.
--
-- ⚠️ Поэтому таблица APPEND-ONLY. Никаких UPDATE: каждое решение — новая строка со
-- своим снимком. Ровно перезапись по месту и была корнем проблемы, и «обновлять
-- последнюю строку» воспроизвело бы её в новом месте.
--
-- ⚠️ Каждая строка САМОДОСТАТОЧНА. Причина приходит отдельным сообщением ПОСЛЕ
-- решения (бот спрашивает её вторым шагом), поэтому одно решение человека может
-- дать две строки. Вместо ссылок между ними каждая строка несёт свой снимок
-- черновика: так строку можно читать как пример, не собирая её из кусков.
--
-- ⚠️ Вердикт судьи фиксируется НА МОМЕНТ РЕШЕНИЯ. Это и есть главная ценность:
-- расхождение «судья сказал годится, человек забраковал» — единственная земля, по
-- которой калибруется строгость. Хранить его в кандидате бесполезно: следующий
-- прогон судьи затрёт его так же, как Шаг В затирает черновик.
CREATE TABLE IF NOT EXISTS content_feedback (
    id               BIGSERIAL PRIMARY KEY,
    candidate_id     BIGINT NOT NULL REFERENCES content_candidates(id) ON DELETE CASCADE,
    event            TEXT   NOT NULL,   -- approved | rejected | edited | comment
    draft_ai         TEXT,              -- снимок черновика ИИ, к которому относится решение
    draft_human      TEXT,              -- чем заменил человек (только для edited)
    reason_code      TEXT,
    reason_text      TEXT,
    brief_version    INT,               -- под какой версией брифа написан черновик
    judge_verdict    TEXT,              -- что говорил судья В ЭТОТ МОМЕНТ
    judge_failed     TEXT[],
    judge_defects    TEXT[],
    judge_paragraphs JSONB,
    reviewer_id      BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_content_feedback_candidate
    ON content_feedback (candidate_id, created_at DESC);
-- Выборка примеров для судьи идёт по «есть содержательная причина» + свежести.
CREATE INDEX IF NOT EXISTS idx_content_feedback_recent
    ON content_feedback (created_at DESC) WHERE reason_text IS NOT NULL;

-- ── Перенос того, что уже накоплено ──────────────────────────────────────────
-- ⚠️ judge_* переносим ТОЛЬКО там, где точно известно, что судья говорил на момент
-- решения человека. Для 1638 это известно из хода сессии: «годится», ноль провалов,
-- ноль дефектов — и именно это расхождение с отказом человека делает запись
-- ценной. Для остальных ставим NULL: подставить сегодняшний вердикт значило бы
-- записать то, чего ревьюер не видел.
INSERT INTO content_feedback (candidate_id, event, draft_ai, reason_code, reason_text,
                               brief_version, judge_verdict, judge_failed, judge_defects,
                               created_at)
SELECT c.id,
       'comment',
       c.draft_text_ai,
       c.reviewer_reason_code,
       c.reviewer_reason,
       c.brief_version,
       CASE WHEN c.id = 1638 THEN 'годится' END,
       CASE WHEN c.id = 1638 THEN ARRAY[]::text[] END,
       CASE WHEN c.id = 1638 THEN ARRAY[]::text[] END,
       coalesce(c.updated_at, now())
FROM content_candidates c
WHERE (c.reviewer_reason IS NOT NULL OR c.reviewer_reason_code IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM content_feedback f WHERE f.candidate_id = c.id);
