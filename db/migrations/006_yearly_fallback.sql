-- Card 1: fallback годовой→месячный при недостатке средств (NSF).
-- Бизнес-правило (2026-06-07): при ПЕРВОМ отказе годового рекуррентного
-- списания по причине «недостаточно средств» (T-Bank NSF ErrorCode) — вместо
-- годового списывается МЕСЯЧНЫЙ тариф того же уровня (Basic/Pro), и дальнейшее
-- автопродление идёт помесячно. N=1 попытка, в существующем 24ч-окне renew.
--
--   subscriptions.fallback_done — годовой окончательно провалился по NSF и
--     заменён месячным. renew_expiring_subs при fallback_done списывает
--     МЕСЯЧНЫЙ план того же tier (не годовой). Терминальный маркер.
--   subscriptions.renewal_last_error — последний T-Bank ErrorCode провала
--     продления (observability/диагностика).
--
-- Применение вручную (как 005):
--   cat db/migrations/006_yearly_fallback.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS fallback_done      BOOLEAN     NOT NULL DEFAULT false;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS renewal_last_error VARCHAR(32);
