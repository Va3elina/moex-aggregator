-- Стенд, этап 4 (реализм): плечо и стресс ГО → на счёте появляются маржин-коллы и сделки, урезанные по ГО. 2026-09-17.
-- Идемпотентно, применить ДО деплоя:  cat db/migrations/100_bt_realism.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE bt_equity ADD COLUMN IF NOT EXISTS margin_call SMALLINT;
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS go_cut BOOLEAN;
