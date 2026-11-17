-- Бот оплаты @FrameDataPayBot (docs/tg-pay-bot.md): воронка канал → бот → сайт → оплата.
-- Бот хранит контакт (chat_id по /start), платёж идёт на сайте. Вход через
-- Telegram НЕ делаем (406-ФЗ): чат привязывается к аккаунту claim-токеном
-- после входа на сайт через VK/Яндекс/почту.
-- Применение: cat db/migrations/116_tg_pay_bot.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

-- Лиды: один Telegram-пользователь = одна строка. chat_id приватного чата =
-- Telegram user id, но писать можно только тем, кто нажал /start в ЭТОМ боте.
-- users.telegram_chat_id (алерт-бот) не трогаем: у него своя уникальность и отвязка.
CREATE TABLE IF NOT EXISTS tg_pay_leads (
  chat_id            BIGINT PRIMARY KEY,
  username           VARCHAR(64),
  first_name         VARCHAR(128),
  start_param        VARCHAR(64),          -- кампания первого входа (p1812, pin, ...)
  first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  user_id            INTEGER REFERENCES users(id) ON DELETE SET NULL,
  linked_at          TIMESTAMPTZ,
  consent_marketing  BOOLEAN NOT NULL DEFAULT false,  -- 38-ФЗ ст. 18: реклама только с согласия
  consent_at         TIMESTAMPTZ,
  opted_out_at       TIMESTAMPTZ,          -- /stop: никаких дожимов
  blocked_at         TIMESTAMPTZ           -- Telegram ответил 403: бот заблокирован
);
CREATE INDEX IF NOT EXISTS idx_tg_pay_leads_user ON tg_pay_leads(user_id);

-- Одноразовые claim-токены бот → сайт. 24 ч: человек может войти не сразу.
CREATE TABLE IF NOT EXISTS tg_pay_claims (
  token      VARCHAR(64) PRIMARY KEY,
  chat_id    BIGINT NOT NULL REFERENCES tg_pay_leads(chat_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  used_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_tg_pay_claims_expires ON tg_pay_claims(expires_at);

-- Журнал воронки: аналитика + защита от повторной отправки (paid, дожимы).
CREATE TABLE IF NOT EXISTS tg_pay_events (
  id       BIGSERIAL PRIMARY KEY,
  chat_id  BIGINT NOT NULL,
  user_id  INTEGER,
  kind     VARCHAR(32) NOT NULL,  -- start, claim, paid, consent, opt_out, blocked, nudge_<код>
  meta     JSONB,
  at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tg_pay_events_chat_kind ON tg_pay_events(chat_id, kind);
CREATE INDEX IF NOT EXISTS idx_tg_pay_events_at ON tg_pay_events(at);
