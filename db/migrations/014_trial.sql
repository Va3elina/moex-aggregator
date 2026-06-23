-- Миграция: бесплатный пробный период (free trial)
--
-- Дата: 2026-06-23
-- Цель: триал 14 дней Basic / 7 дней Pro с привязкой карты и автосписанием
--       выбранного плана по окончании. Анти-абуз: один триал на идентичность
--       (oauth_id / нормализованный email / отпечаток карты), хранятся ХЕШИ.
--
-- Архитектура (после адверсариального ревью):
--   - Триал = ОТДЕЛЬНАЯ строка subscriptions: status='active', is_trial=true,
--     amount=0, period='trial', plan_id=РЕАЛЬНОГО платного плана (целевой),
--     expires_at=now+N, payment_method_id=привязанная карта, yk_payment_id=NULL.
--     NULL yk_payment_id делает триал-строку иммунной к webhook'у привязочного
--     платежа (cancel_by_webhook ищет по yk_payment_id) — нет ложной отмены.
--   - Привязка карты — отдельная 1₽-строка (yk_payment_id=PaymentId), которую
--     переводим в reversed/refunded; она НЕ даёт доступа.
--
-- 152-ФЗ: идентификаторы анти-абуза хранятся ТОЛЬКО как SHA-256 с серверной
--   солью (env TRIAL_HASH_SALT), не сырьём.
--
-- Применять ВРУЧНУЮ на проде ПОСЛЕ push (CI не накатывает миграции):
--   cat db/migrations/014_trial.sql | ssh root@103.88.243.232 \
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'

-- === Поля на subscriptions ===
-- is_trial: эта строка — пробный период (amount=0; реальная цена конверсии —
--   из plan_id через plans.py: renew_expiring_subs списывает get_plan(plan_id).amount).
-- trial_reminder_sent: T-1 уведомление «триал заканчивается, спишем X₽» отправлено.
-- trial_consent_at / trial_consent_version: зафиксированный акцепт автосписания
--   (ГК ст.438 + ЗоЗПП ст.10 — доказательство правомерности списания при споре).
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS is_trial              BOOLEAN     NOT NULL DEFAULT false;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS trial_reminder_sent   BOOLEAN     NOT NULL DEFAULT false;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS trial_consent_at      TIMESTAMPTZ;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS trial_consent_version VARCHAR(16);

-- Для T-1 уведомлений и конверсии: WHERE is_trial AND status='active'
CREATE INDEX IF NOT EXISTS idx_subs_trial_active
    ON subscriptions (is_trial, status, expires_at)
    WHERE is_trial = true;


-- === Поля на users ===
-- trial_used: быстрый флаг «этот аккаунт уже брал триал» (зеркало retention_used).
-- registration_ip: пишем при регистрации (сейчас не пишется) — мягкий анти-абуз
--   сигнал (НИКОГДА не hard-block: RU mobile NAT/VPN → ложные срабатывания).
ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used      BOOLEAN     NOT NULL DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS registration_ip VARCHAR(64);


-- === Анти-абуз: один триал на идентичность ===
-- Запись создаётся в момент УСПЕШНОЙ привязки карты (не на старте — иначе
-- брошенная привязка сожгла бы право на триал). UNIQUE(user_id) — один триал
-- на аккаунт. Кросс-аккаунтные ключи дедупа против «100 аккаунтов» — ХЕШИ
-- (152-ФЗ: sha256(значение + TRIAL_HASH_SALT), не сырьё):
--   oauth_hash       — sha256('vk:322700506' + salt)
--   email_hash       — sha256(нормализованный email + salt) (gmail: срезаны
--                      точки и +алиасы ДО хеширования)
--   card_fingerprint — sha256(маскированный PAN + salt) — first6+last4 стабильны
--                      между CustomerKey'ями (rebill_id — нет, он per-привязка).
--                      Заполняется ТОЛЬКО если T-Bank подтвердит стабильный
--                      отпечаток; иначе остаётся NULL (см. session decision).
CREATE TABLE IF NOT EXISTS trial_redemptions (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subscription_id  INTEGER     REFERENCES subscriptions(id) ON DELETE SET NULL,
    tier             VARCHAR(16) NOT NULL,                 -- basic / pro
    oauth_hash       VARCHAR(64),                          -- sha256(provider:oauth_id + salt)
    email_hash       VARCHAR(64),                          -- sha256(email_norm + salt)
    card_fingerprint VARCHAR(64),                          -- sha256(masked PAN + salt)
    ip               VARCHAR(64),                          -- мягкий сигнал (не блок)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- один триал на аккаунт
    UNIQUE (user_id)
);

-- Быстрая проверка eligibility по кросс-аккаунтным ключам (хеши)
CREATE INDEX IF NOT EXISTS idx_trial_redemptions_oauth ON trial_redemptions (oauth_hash);
CREATE INDEX IF NOT EXISTS idx_trial_redemptions_email ON trial_redemptions (email_hash);
CREATE INDEX IF NOT EXISTS idx_trial_redemptions_card  ON trial_redemptions (card_fingerprint);
