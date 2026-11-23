-- Миграция: журнал уведомлений о предстоящем автосписании (за 3 дня)
--
-- Дата: 2026-11-23
-- Цель: письмо «через 3 дня спишем X ₽» перед каждым продлением платной
--       подписки (Единые правила защиты прав потребителей СГ, ст. 43) и
--       доказательство отправки при споре/чарджбэке.
--
-- Код (api/billing/renewal_notice.py) создаёт ту же таблицу сам через
-- CREATE TABLE IF NOT EXISTS, так что порядок «деплой → миграция» безопасен.
-- Файл — для истории схемы. Применить вручную при желании:
--   cat db/migrations/119_renewal_reminders.sql | ssh root@103.88.243.232 \
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'
--
-- Одна строка на подписку (subscription_id = PK → анти-дубль).
-- status: sent | late (ушло позже чем за 3 дня до списания) | no_email.

CREATE TABLE IF NOT EXISTS renewal_reminders (
    subscription_id INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    charge_at       TIMESTAMPTZ NOT NULL,
    amount          NUMERIC(12, 2),
    email           VARCHAR(255),
    status          VARCHAR(16) NOT NULL
);
