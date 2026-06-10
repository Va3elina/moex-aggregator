-- Перформанс-индексы под тяжёлые агрегации (аудит 2026-06-10).
--
-- /api/stats главный запрос: WHERE clgroup=? AND interval=? AND tradedate>=?
-- GROUP BY tradedate. Без композитного индекса планировщик делал BitmapAnd двух
-- больших однополевых индексов (idx_open_interest_interval ~370k ×
-- idx_oi_clgroup_tradedate ~1.1M) → на холодных страницах 3.85с. Композит даёт
-- прямой range-scan: 3.85с → ~6мс. Тот же индекс ускоряет COUNT(DISTINCT sectype)
-- в том же эндпоинте.
--
-- Применение (CONCURRENTLY нельзя в транзакции — выполнять построчно в autocommit):
--   cat db/migrations/009_perf_indexes.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
-- На проде уже применён 2026-06-10.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oi_clgroup_interval_tradedate
  ON open_interest (clgroup, "interval", tradedate);
