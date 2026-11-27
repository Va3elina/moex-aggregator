-- 122: алерт «оплата без карты» — событие paid_no_method в billing_events.
--
-- Дата: 2026-11-27
-- Подписка оплачена на форме (не триал, не инвайт, amount > 0), а через 15
-- минут после активации у неё так и нет payment_method_id: клиент заплатил, но
-- автопродления не будет. С мая по сентябрь 2026 таких оплат было 7, ни одну
-- не заметили. Событие заводит оркестратор
-- (api/billing/admin_notify.flag_paid_without_method, раз в 15 минут), в чат
-- оно уходит обычной доставкой billing_events.
--
-- Схема не меняется: kind — свободный TEXT. Здесь только страховка «одно
-- событие на подписку» на уровне БД (код и сам проверяет перед вставкой) и
-- обновлённый список видов. Порядок с кодом любой:
--   Get-Content db/migrations/122_paid_no_method_alert.sql | ssh frame `
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'

CREATE UNIQUE INDEX IF NOT EXISTS billing_events_paid_no_method_uq
    ON billing_events (subscription_id) WHERE kind = 'paid_no_method';

COMMENT ON COLUMN billing_events.kind IS 'payment | trial_start | grant | payment_failed | autorenew_off | autorenew_on | retention | autopay_bound | refund | reversed | expired | method_unlinked | double_charge | paid_no_method | digest_sent';
