-- Migration 018: рекуррентный СБП — поля привязки (BankMemberId + RequestKey)
--
-- Применяется один раз на проде:
--   cat db/migrations/018_sbp_binding_fields.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
--
-- Зачем (вытащено живым тестом 2026-06-29):
--   1. ChargeQr ОБЯЗАТЕЛЬНО требует BankMemberId (id банка плательщика, приходит
--      в GetAddAccountQrState рядом с AccountToken). Без него → ErrorCode 5031
--      «привязка не найдена». Поэтому храним его рядом с account_token.
--   2. Надёжный способ получить AccountToken после оплаты — поллить
--      GetAddAccountQrState(RequestKey). RequestKey приходит в ответе GetQr при
--      создании СБП-чекаута → сохраняем на подписке, чтобы reconciler в
--      orchestrator дотянул токен после оплаты (webhook о привязке ненадёжен).

BEGIN;

-- BankMemberId привязки (нужен для ChargeQr). NULL для карт.
ALTER TABLE user_payment_methods
    ADD COLUMN IF NOT EXISTS bank_member_id VARCHAR(32);

-- RequestKey СБП-привязки — для дотягивания AccountToken после оплаты.
-- Живёт на подписке до момента, пока reconciler не сохранит привязку (или не
-- сдастся по таймауту). NULL для карточных/обычных подписок.
ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS sbp_request_key VARCHAR(64);

-- Быстрый поиск «СБП-подписок, ждущих привязки» для reconciler'а.
CREATE INDEX IF NOT EXISTS idx_subs_sbp_request_key
ON subscriptions (sbp_request_key)
WHERE sbp_request_key IS NOT NULL;

COMMIT;
