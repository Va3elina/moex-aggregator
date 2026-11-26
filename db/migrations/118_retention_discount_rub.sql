-- Годовая retention-скидка «месяц в подарок» в рублях (2026-11-22).
-- subscriptions.discount_rub > 0 — из ближайшего продления вычитается эта сумма
--   (цена месяца того же тарифа). discount_pct у таких записей приблизительный,
--   нужен триггеру billing_events (kind='retention') и уведомлению админу.
-- Применение: cat db/migrations/118_retention_discount_rub.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS discount_rub NUMERIC(10,2) NOT NULL DEFAULT 0;
