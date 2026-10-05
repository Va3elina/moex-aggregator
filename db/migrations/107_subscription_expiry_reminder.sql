-- 107: флаг письма «подписка заканчивается» для подписок без автопродления.
--
-- Подписка, оплаченная без привязки (СБП на форме банка, SberPay), или та, где
-- юзер выключил автопродление, сама не продлится. За 3 дня до конца шлём письмо
-- со ссылкой на продление (service.send_expiry_reminders, крон оркестратора).
-- Флаг не даёт отправить письмо дважды. Обновление флага не порождает событий
-- в billing_events: триггер 106 смотрит только на status, cancelled_at,
-- payment_method_id и discount_pct.
--
-- Идемпотентно. Применять ДО деплоя кода (модель читает колонку):
--   cat db/migrations/107_subscription_expiry_reminder.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS expiry_reminder_sent BOOLEAN NOT NULL DEFAULT false;
