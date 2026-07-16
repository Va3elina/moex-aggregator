-- Найдено 2026-07-16: last_checked_at была ОДНОЙ колонкой на ТРИ независимых
-- смысла (tg_hype_scan.py — "продиспатчил Шаг А", content_match.py — "уже
-- проверял pending↔anomalies сегодня", content_review_bot.py — "ещё не
-- отправлял ревьюеру"). Взаимное затирание: content_match сам ставил
-- last_checked_at=now() В ТОЙ ЖЕ команде, что переводила статус в
-- draft_ready, а content_review_bot ждал именно last_checked_at IS NULL
-- как признак "не отправлено" — условие никогда не выполнялось, бот
-- фактически не работал ни разу (0 published/rejected через него).
-- Отдельно: дневные-only тикеры (VK и т.п.) ложно считались "уже
-- проверенными сегодня" из-за марки tg_hype_scan сразу при создании
-- кандидата (ещё до первой реальной попытки сверки).
-- Разводим: last_checked_at остаётся за tg_hype_scan/content_ai.py
-- (диспатч-троттлинг Шага А/В на статусе 'candidate'), content_match.py
-- получает свою step_b_checked_at, content_review_bot.py — свою
-- reviewer_notified_at.
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS step_b_checked_at TIMESTAMPTZ;
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS reviewer_notified_at TIMESTAMPTZ;
