-- Миграция: создание таблицы subscriptions
--
-- Дата: 2026-04-22
-- Цель: хранение истории платежей и Pro-подписок пользователей.
--
-- Поле users.role ('user'/'pro'/'admin') уже существует и используется
-- для быстрой проверки доступа. Эта таблица — источник истины для
-- полной истории, дат истечения и реквизитов ЮKassa.
--
-- Применение на проде:
--   docker exec -i frame-db-1 psql -U frame -d frame < 001_subscriptions.sql

CREATE TABLE IF NOT EXISTS subscriptions (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    tier            VARCHAR(16)  NOT NULL,  -- 'basic' / 'pro' / 'premium'
    period          VARCHAR(16)  NOT NULL,  -- 'monthly' / 'yearly'
    plan_id         VARCHAR(32)  NOT NULL,  -- SKU ('pro_monthly', 'premium_yearly', ...)
    amount          NUMERIC(10,2) NOT NULL,
    currency        VARCHAR(3)   NOT NULL DEFAULT 'RUB',

    status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
                    -- pending / active / cancelled / expired / failed / refunded

    started_at      TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,

    yk_payment_id   VARCHAR(64)  UNIQUE,
    yk_method       VARCHAR(32),

    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Индекс для быстрой выборки активных подписок пользователя
CREATE INDEX IF NOT EXISTS idx_subs_user_id ON subscriptions (user_id);

-- Индекс для cron'а "кто скоро истекает": WHERE status='active' ORDER BY expires_at
CREATE INDEX IF NOT EXISTS idx_subs_active_expires
    ON subscriptions (status, expires_at)
    WHERE status = 'active';

-- Индекс для webhook handler'а: lookup по yk_payment_id
-- (UNIQUE constraint выше сам создаёт B-tree индекс, явный не нужен)

-- Trigger для авто-обновления updated_at
CREATE OR REPLACE FUNCTION update_subs_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_subs_updated_at ON subscriptions;
CREATE TRIGGER trg_subs_updated_at
    BEFORE UPDATE ON subscriptions
    FOR EACH ROW
    EXECUTE FUNCTION update_subs_updated_at();
