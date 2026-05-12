-- Migration 003: user_payment_methods — сохранённые карты для T-Bank рекуррентов
--
-- Применяется один раз на проде:
--   cat db/migrations/003_user_payment_methods.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
--
-- Что делает:
--   1. Создаёт таблицу user_payment_methods для хранения rebill_id'ов T-Bank
--      (токенов привязанных карт) с metadata для UI (last4, brand, default-флаг)
--   2. Добавляет в subscriptions колонку payment_method_id (FK на новую таблицу)
--      для отслеживания "какой картой оплачена эта подписка"
--   3. Триггер updated_at + partial indexes для быстрого поиска активных карт
--
-- Используется backend'ом:
--   - api/billing/tbank.py: Init с Recurrent="Y" + CustomerKey → webhook возвращает
--     RebillId → service.py создаёт UserPaymentMethod
--   - api/billing/service.py::charge_recurrent — для auto-renewal списаний
--
-- На проде уже применено 2026-05-12 (см. .claude/HANDOFF.md).

BEGIN;

-- ─── Таблица сохранённых карт ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_payment_methods (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- 'tbank' — единственный поддерживающий рекурренты у нас сейчас.
    provider      VARCHAR(16) NOT NULL DEFAULT 'tbank',
    -- T-Bank RebillId — постоянный токен карты.
    rebill_id     VARCHAR(64) NOT NULL,
    -- CustomerKey которым карта была зарегистрирована (для DeleteCard API).
    customer_key  VARCHAR(64),
    -- Display info для UI «Сохранённые карты».
    card_last4    VARCHAR(4),
    card_brand    VARCHAR(16),         -- VISA / MASTERCARD / MIR / MAESTRO / ...
    -- Если у юзера несколько карт — какая по умолчанию для auto-renewal.
    is_default    BOOLEAN NOT NULL DEFAULT FALSE,
    -- Последнее успешное списание — для UI и для cleanup'а зомби-карт.
    last_used_at  TIMESTAMP WITH TIME ZONE,
    -- Soft delete: юзер удалил карту через UI. Не дропаем т.к. subscriptions
    -- ссылаются на эту запись (история — какой картой оплачено).
    deleted_at    TIMESTAMP WITH TIME ZONE,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Одна карта (rebill_id) у user'а может быть активна (НЕ deleted) только один раз.
-- При удалении и повторной привязке создаётся новая строка.
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_methods_active
ON user_payment_methods (user_id, rebill_id)
WHERE deleted_at IS NULL;

-- Для быстрого поиска "карты user'а" (UI «Сохранённые карты»).
CREATE INDEX IF NOT EXISTS idx_payment_methods_user
ON user_payment_methods (user_id)
WHERE deleted_at IS NULL;

-- ─── updated_at trigger ───────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_payment_methods_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_payment_methods_updated_at ON user_payment_methods;
CREATE TRIGGER trg_payment_methods_updated_at
BEFORE UPDATE ON user_payment_methods
FOR EACH ROW EXECUTE FUNCTION update_payment_methods_updated_at();

-- ─── FK в subscriptions ────────────────────────────────────────────────
-- Какая карта была использована для оплаты этой подписки.
-- NULL = разовая оплата без сохранения, NOT NULL = можно auto-renew.
ALTER TABLE subscriptions
ADD COLUMN IF NOT EXISTS payment_method_id INTEGER
REFERENCES user_payment_methods(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_subs_payment_method
ON subscriptions (payment_method_id)
WHERE payment_method_id IS NOT NULL;

COMMIT;
