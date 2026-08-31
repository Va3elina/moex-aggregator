-- Шаг Г: судья между генератором и человеком.
--
-- ЗАЧЕМ. Живой прогон 31.08 показал, что короткий промпт сам по себе фактуру не
-- гарантирует: в черновике 1104 новость говорит «против прибыли ₽147,55 млрд ГОДОМ
-- ранее», а черновик написал «КВАРТАЛОМ ранее». Ворота №1 промпта («каждое число
-- трассируется в бриф, пройди по тексту и найди источник») подмену НЕ поймали —
-- число-то на месте, подменён период сравнения. Инструкция «проверь себя» работает
-- хуже отдельного проверяющего прохода. Второй дефект того же прогона: в 1357
-- последний абзац повторяет первый теми же цифрами.
--
-- ⚠️ СУДЬЯ НЕ БЛОКИРУЕТ, А РАЗМЕЧАЕТ. Автоотбраковка с отправкой на переписывание
-- жгла бы облачные сессии по кругу и могла зациклиться; решение остаётся за
-- человеком. Задача Шага Г — чтобы дефект не прошёл НЕЗАМЕЧЕННЫМ, а не чтобы
-- заменить ревьюера.
--
-- ⚠️ ВЕРДИКТ СЧИТАЕТ КОД, а не модель. Модель заполняет только бинарные пункты
-- рубрики; вердикт выводится правилом в api/routers/content_news.py. Так он
-- воспроизводим, видно из чего собран, а расхождение кодового и модельного
-- вердикта само по себе сигналит о плохо сформулированном пункте.
-- Рубрика и обоснование групп — research/content_pipeline_v2/RUBRIC.md.

ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_items JSONB;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_verdict VARCHAR(16);
-- Проваленные ВОРОТА (группы A и B) — они определяют вердикт.
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_failed TEXT[];
-- Проваленные пункты ЧЕК-ЛИСТА ПРОИЗВОДСТВА (группа C) — на вердикт не влияют.
-- Отдельная колонка, а не общий список: пункты группы C срабатывают и на реальных
-- постах канала (длинное тире у 11%, плотность >2 у 18%), браковать по ним значило
-- бы забраковать сам канал. Но именно они показывают, что чинить в генераторе.
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_defects TEXT[];
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_note TEXT;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_checked_at TIMESTAMPTZ;
-- Тот же бэкстоп, что у Шагов А/В/Н: облачная сессия иногда падает на провижининге
-- ДО старта агента, и без ограничения попыток бесконечный ретрай молча жжёт деньги.
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_dispatch_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS judge_gave_up_at TIMESTAMPTZ;

COMMENT ON COLUMN content_candidates.judge_verdict IS
  'годится | спорно | брак. Считается КОДОМ из judge_items: любой провал ворот '
  'группы A = брак; 2+ провала группы B = брак, один = спорно; группа C на вердикт '
  'не влияет. Не блокирует ревью — размечает.';

-- Выборка для диспетчера Шага Г: черновик есть, судья ещё не смотрел.
CREATE INDEX IF NOT EXISTS idx_content_candidates_judge_pending
    ON content_candidates (id)
    WHERE draft_text IS NOT NULL AND judge_verdict IS NULL AND judge_gave_up_at IS NULL;
