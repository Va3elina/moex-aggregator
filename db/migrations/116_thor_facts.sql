-- Копилка фактов для постов @Thor_INV (02.11.2026): сделки и истории автора, команда /факт в @frameadminbot.
-- Завод личных постов берёт живые детали только отсюда. Читает signals/thor_radar.py.
-- Таблицу создаёт и сам скрипт (CREATE IF NOT EXISTS), эта миграция для истории.
--   cat db/migrations/116_thor_facts.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS thor_facts (
    id          SERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    chat_id     BIGINT NOT NULL,
    body        TEXT NOT NULL
);
