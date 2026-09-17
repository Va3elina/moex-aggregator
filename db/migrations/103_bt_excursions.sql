-- Стенд: благоприятное / неблагоприятное отклонение сделки (MFE / MAE) для графика «Динамика». 2026-09-17.
-- Идемпотентно, применить ДО деплоя:  cat db/migrations/103_bt_excursions.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS mfe DOUBLE PRECISION;
ALTER TABLE bt_trades ADD COLUMN IF NOT EXISTS mae DOUBLE PRECISION;
