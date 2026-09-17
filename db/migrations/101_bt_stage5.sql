-- Стенд, этап 5: досрочные выходы (стоп/тейк/трейлинг) и перебор параметров. 2026-09-17.
-- Идемпотентно, применить ДО деплоя:  cat db/migrations/101_bt_stage5.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS m_in SMALLINT;          -- минута дня свечи входа (МСК)
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS m_out SMALLINT;         -- минута дня свечи фактического выхода
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS exit_reason TEXT;       -- время | стоп | тейк | трейлинг
ALTER TABLE bt_runs   ADD COLUMN IF NOT EXISTS progress SMALLINT;      -- % готовности (перебор идёт минуты)
