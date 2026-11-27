-- 123: акция (цена со скидкой) и пересчёт остатка при повышении тарифа.
--
-- Дата: 2026-11-27
--   promo_code — метка акции, по цене которой куплена подписка
--                (env BILLING_SALE_CODE, api/billing/sale.py). По ней же
--                проверяется лимит «одна покупка по акции на аккаунт».
--   bonus_days — сколько дней добавлено новой подписке за неиспользованный
--                остаток младшего тарифа (2 дня Basic = 1 день Pro).
--
-- ПОРЯДОК ВЫКЛАДКИ: миграцию применить ДО включения акции. Колонки NULL-able
-- и вне ORM-маппинга: код без миграции биллинг не роняет, но акцию не продаёт
-- (service._promo_used при ошибке запроса считает лимит исчерпанным), а
-- bonus_days не записывается (сам срок при этом продлевается).
--
--   Get-Content db/migrations/123_sale_promo.sql | ssh frame `
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'

ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS promo_code VARCHAR(32);
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS bonus_days INTEGER;
-- gift_days — подарочные дни акции сверх оплаченного срока.
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS gift_days INTEGER;
-- На закрытой младшей подписке: в какую подписку переведён её остаток и до
-- какого дня она действовала бы. По ним возврат денег за старшую подписку
-- восстанавливает младшую (service._restore_converted).
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS converted_into INTEGER;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS converted_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS subscriptions_promo_idx
    ON subscriptions (user_id, promo_code) WHERE promo_code IS NOT NULL;
