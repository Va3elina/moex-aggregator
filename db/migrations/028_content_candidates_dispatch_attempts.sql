-- Найдено 2026-07-14: Routine-сессии иногда падают на этапе провижининга
-- облачного контейнера (до старта Claude Code, до нашего промта/коллбэка) —
-- живой пример: 3 кандидата (491/492/493) зависли в status='candidate'
-- навсегда, т.к. content_ai.py-бэкстоп существовал, но не был на кроне, и
-- ретраил бы их бесконечно, если бы был — если Routine сломана системно
-- (не разово), бесконечный ретрай жжёт деньги без всякого смысла.
--
-- dispatch_attempts считает ТОЛЬКО повторные выстрелы бэкстопа (content_ai.py),
-- не оригинальный выстрел из rss_scan.py/marketwits_scan.py/content_match.py/
-- anomaly_context_scan.py — те не трогаем, у них уже есть свой event-driven
-- fire с cooldown-троттлингом (last_checked_at), это НЕ повтор, это первая
-- попытка.
ALTER TABLE content_candidates
    ADD COLUMN IF NOT EXISTS dispatch_attempts INTEGER NOT NULL DEFAULT 0;
