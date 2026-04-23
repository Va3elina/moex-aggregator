-- Миграция: subscription_invites + redemptions
--
-- Дата: 2026-04-23
-- Цель: раздача подписок по ссылке (redeem-token паттерн).
--       Админ генерирует токены, шлёт ссылки в телегу/чат;
--       пользователь кликает → залогинен → активируется подписка.

CREATE TABLE IF NOT EXISTS subscription_invites (
    token            VARCHAR(64) PRIMARY KEY,
    tier             VARCHAR(16) NOT NULL,                        -- basic/pro/premium
    duration_days    INTEGER     NOT NULL,                        -- на сколько активируется
    max_uses         INTEGER     NOT NULL DEFAULT 1,
    uses_count       INTEGER     NOT NULL DEFAULT 0,
    expires_at       TIMESTAMPTZ NOT NULL,                        -- токен истекает (даже неиспользованный)
    created_by_user_id INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    revoked_at       TIMESTAMPTZ,                                 -- если админ отозвал
    note             VARCHAR(255),                                -- "для Пети" / "конкурс янв 2026"
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_invites_by_admin
    ON subscription_invites (created_by_user_id, created_at DESC);

-- Для быстрого listing активных: WHERE uses_count < max_uses AND expires_at > now AND revoked_at IS NULL
CREATE INDEX IF NOT EXISTS idx_invites_active
    ON subscription_invites (expires_at)
    WHERE revoked_at IS NULL;


CREATE TABLE IF NOT EXISTS subscription_invite_redemptions (
    id                SERIAL PRIMARY KEY,
    token             VARCHAR(64) NOT NULL REFERENCES subscription_invites(token) ON DELETE CASCADE,
    user_id           INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subscription_id   INTEGER     REFERENCES subscriptions(id) ON DELETE SET NULL,
    redeemed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Уникальная пара (token, user_id) — один пользователь не может применить токен дважды
    UNIQUE (token, user_id)
);

CREATE INDEX IF NOT EXISTS idx_redemptions_user
    ON subscription_invite_redemptions (user_id, redeemed_at DESC);
