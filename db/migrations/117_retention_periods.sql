-- Retention-скидка на несколько продлений подряд (путь отмены подписки, 2026-11-22).
-- subscriptions.discount_periods_left — сколько продлений ещё идут со скидкой
--   discount_pct. При каждом продлении новая запись наследует скидку с периодом
--   на 1 меньше. 0 при discount_pct>0 (старые записи) = одно продление.
-- Применение: cat db/migrations/117_retention_periods.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS discount_periods_left SMALLINT NOT NULL DEFAULT 0;
