-- 004_extension_tokens.sql
-- Токены для браузерного расширения (плавающее окно индикаторов в терминале
-- Т-Инвестиций). PRO-подписчик генерит токен в личном кабинете и вставляет его
-- в расширение; embed-роуты обменивают его на JWT (/api/extension/exchange),
-- где PRO-тариф проверяется ВЖИВУЮ. Зеркалит схему api_keys.
--
-- Применение на сервере:
--   docker exec -i frame-db-1 psql -U postgres -d moex_db < db/migrations/004_extension_tokens.sql

CREATE TABLE IF NOT EXISTS extension_tokens (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- sha256(plain_token) hex (64). Plain юзеру показываем ОДИН раз.
    token_hash    VARCHAR(64) NOT NULL UNIQUE,
    -- "ext_abcd…" — первые 12 char для display в кабинете (не secret).
    token_prefix  VARCHAR(20) NOT NULL,
    name          VARCHAR(100),
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at  TIMESTAMP,
    -- Soft delete: строку оставляем для audit, token_hash перестаёт работать.
    is_revoked    BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at    TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ext_tokens_user ON extension_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_ext_tokens_hash ON extension_tokens (token_hash);
