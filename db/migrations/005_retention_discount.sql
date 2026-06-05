-- Retention-скидка при отмене автопродления.
-- Бизнес-правила (решение 2026-06-05): 40% на 1 СЛЕДУЮЩЕЕ продление, 1 раз на аккаунт.
--   subscriptions.discount_pct — % скидки, применяемой к ближайшему рекуррентному
--     списанию (charge_recurrent). Новая renewal-подписка создаётся с discount_pct=0
--     → скидка одноразовая, дальше полная цена.
--   users.retention_used — флаг анти-абуза: скидку можно получить только один раз.
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS discount_pct SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE users         ADD COLUMN IF NOT EXISTS retention_used BOOLEAN NOT NULL DEFAULT false;
