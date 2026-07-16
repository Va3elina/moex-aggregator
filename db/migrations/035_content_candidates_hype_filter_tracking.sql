-- Отслеживание Шага Н (независимый фильтр "шутка/мусор vs новость" для
-- уведомления коллеги) — нужно для бэкстопа с ограниченным числом повторов
-- (найдено 2026-07-16: Шаг Н раньше стрелял ОДИН раз и никогда не повторялся
-- при сбое — молчаливая потеря, как было с egress-блоком на env_...MNVpX).
-- hype_filter_result: NULL = ещё не ответила/не пробовали, TRUE/FALSE = ответ.
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS hype_filter_result BOOLEAN;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS hype_filter_dispatch_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS hype_filter_checked_at TIMESTAMPTZ;
