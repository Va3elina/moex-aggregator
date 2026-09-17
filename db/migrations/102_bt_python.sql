-- Стенд, этап 5 часть 2: стратегии кодом на Python — в день по бумаге может быть несколько сделок. 2026-09-17.
-- Ключ сделки: (run_id, n) вместо (run_id, st, d). Идемпотентно, применить ДО деплоя:
--   cat db/migrations/102_bt_python.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS n INTEGER;
UPDATE bt_trades t SET n = x.rn FROM (SELECT run_id, st, d, row_number() OVER (PARTITION BY run_id ORDER BY d, st) - 1 AS rn FROM bt_trades WHERE n IS NULL) x
 WHERE t.run_id = x.run_id AND t.st = x.st AND t.d = x.d AND t.n IS NULL;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bt_trades_pkey' AND pg_get_constraintdef(oid) LIKE '%st, d%') THEN
    ALTER TABLE bt_trades DROP CONSTRAINT bt_trades_pkey;
    ALTER TABLE bt_trades ALTER COLUMN n SET NOT NULL;
    ALTER TABLE bt_trades ADD PRIMARY KEY (run_id, n);
    CREATE INDEX IF NOT EXISTS idx_bt_trades_run_st ON bt_trades (run_id, st, d);
  END IF;
END $$;
