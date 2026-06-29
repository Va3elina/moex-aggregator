-- Migration 017: рекуррентный СБП (QR) — расширение user_payment_methods
--
-- Применяется один раз на проде:
--   cat db/migrations/017_sbp_payment_methods.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
--
-- Что делает:
--   1. Добавляет дискриминатор method_type ('card' | 'sbp') — у существующих
--      строк по умолчанию 'card'.
--   2. Добавляет account_token — СБП-аналог карточного rebill_id (приходит в
--      нотификации T-Bank при привязке счёта по QR, Вариант №2 / GetQr-flow).
--   3. Делает rebill_id NULLABLE — у СБП-привязок карточного токена нет.
--   4. Пересоздаёт uq_payment_methods_active с условием rebill_id IS NOT NULL
--      (чтобы NULL-rebill СБП-строки не считались дублями) + добавляет
--      аналогичный partial-unique по account_token.
--
-- Используется backend'ом:
--   - api/billing/tbank.py: create_sbp_checkout (Init Recurrent=Y, DATA={"QR":"true"}
--     + GetQr) → webhook возвращает AccountToken → service.py создаёт строку
--     method_type='sbp', account_token=...
--   - api/billing/service.py::charge_recurrent — ChargeQr по account_token.

BEGIN;

-- ─── Новые колонки ───────────────────────────────────────────────────
ALTER TABLE user_payment_methods
    ADD COLUMN IF NOT EXISTS method_type   VARCHAR(8)  NOT NULL DEFAULT 'card',
    ADD COLUMN IF NOT EXISTS account_token VARCHAR(64);

-- У СБП-строк карточного rebill_id нет → снимаем NOT NULL.
ALTER TABLE user_payment_methods
    ALTER COLUMN rebill_id DROP NOT NULL;

-- ─── Уникальность активных привязок ──────────────────────────────────
-- Старый индекс уникален по (user_id, rebill_id) WHERE deleted_at IS NULL.
-- rebill_id теперь nullable → добавляем guard rebill_id IS NOT NULL, иначе
-- СБП-строки (rebill_id NULL) попадали бы под карточный constraint.
DROP INDEX IF EXISTS uq_payment_methods_active;
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_methods_active
ON user_payment_methods (user_id, rebill_id)
WHERE deleted_at IS NULL AND rebill_id IS NOT NULL;

-- Один account_token у юзера активен (НЕ deleted) только один раз.
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_methods_sbp_active
ON user_payment_methods (user_id, account_token)
WHERE deleted_at IS NULL AND account_token IS NOT NULL;

COMMIT;
