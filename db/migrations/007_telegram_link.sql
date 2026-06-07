-- Phase 1 Telegram alert-bot: привязка аккаунта Фрейм к Telegram-чату.
-- Бот НЕ может писать юзеру первым (Telegram privacy) → deep-link /start <token>:
-- сайт генерит одноразовый токен (таблица ниже), юзер тапает t.me/<bot>?start=<token>,
-- host-side бот-поллер ловит /start, связывает chat_id ↔ user.
-- Применение: cat db/migrations/007_telegram_link.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE users ADD COLUMN IF NOT EXISTS telegram_chat_id   BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS telegram_username  VARCHAR(64);
ALTER TABLE users ADD COLUMN IF NOT EXISTS telegram_linked_at TIMESTAMPTZ;
-- Один Telegram-чат = один аккаунт Фрейм.
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_tg_chat ON users(telegram_chat_id)
  WHERE telegram_chat_id IS NOT NULL;

-- Одноразовые токены привязки. В БД (НЕ Redis): host-side бот читает их, а
-- host↔DB проверен (signal-engine), host↔Redis не гарантирован.
CREATE TABLE IF NOT EXISTS telegram_link_tokens (
  token      VARCHAR(64) PRIMARY KEY,
  user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tg_link_tokens_expires ON telegram_link_tokens(expires_at);
