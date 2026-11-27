-- 121: журнал входящих вебхуков банка (T-Bank, POST /api/billing/webhook).
--
-- Дата: 2026-11-27
-- Логи API теряются при каждом деплое, сырые уведомления банка не хранились
-- нигде: по подписке без сохранённой карты (sub 342, 20.11.2026) было не
-- восстановить, что и когда прислал банк. Теперь на каждый вебхук одна строка.
--
-- RebillId и AccountToken — ключи к списанию денег — НЕ хранятся, только
-- признак, что они были в теле. Подпись (Token), маска карты, почта и
-- остальное тело тоже не сохраняются.
--
-- Код (api/billing/webhook_log.py) создаёт ту же таблицу сам при первом
-- вебхуке, так что порядок «деплой → миграция» безопасен. Файл — для истории
-- схемы и индексов; применить можно в любой момент:
--   Get-Content db/migrations/121_bank_webhook_log.sql | ssh frame `
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'
--
-- result — чем кончилась обработка:
--   bad_body        тело не JSON
--   rejected        не прошла подпись / чужой терминал / нет PaymentId
--   skipped_status  промежуточный статус (AUTHORIZED и т.п.), не обрабатываем
--   no_subscription платёж не найден среди подписок
--   activated       подписка включена этим вебхуком
--   binding_saved   подписка уже была включена (/sync), вебхук досохранил карту
--   duplicate       подписка уже включена, вебхук ничего не изменил
--   not_activated   оплата не подтвердилась (GetState / сумма / защита от дубля)
--   trial_bound     привязочный платёж триала
--   cancelled / refunded  отмена или возврат применены
--   noop            отмена/возврат ничего не изменили
--   error           обработка упала (detail — класс исключения)

CREATE TABLE IF NOT EXISTS bank_webhook_log (
    id                BIGSERIAL PRIMARY KEY,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    payment_id        VARCHAR(64),
    status            VARCHAR(32),
    error_code        VARCHAR(16),
    amount_kopecks    BIGINT,
    has_rebill_id     BOOLEAN     NOT NULL DEFAULT FALSE,
    has_account_token BOOLEAN     NOT NULL DEFAULT FALSE,
    subscription_id   INTEGER,
    result            VARCHAR(32) NOT NULL,
    detail            VARCHAR(200)
);

CREATE INDEX IF NOT EXISTS bank_webhook_log_payment_idx
    ON bank_webhook_log (payment_id, received_at);
CREATE INDEX IF NOT EXISTS bank_webhook_log_time_idx
    ON bank_webhook_log (received_at);

COMMENT ON TABLE bank_webhook_log IS 'Журнал входящих вебхуков T-Bank: что прислал банк и чем кончилась обработка. Ключи списания (RebillId/AccountToken) не хранятся';
COMMENT ON COLUMN bank_webhook_log.has_rebill_id IS 'В теле был RebillId (привязка карты). Само значение не хранится';
COMMENT ON COLUMN bank_webhook_log.has_account_token IS 'В теле был AccountToken (привязка счёта СБП). Само значение не хранится';
