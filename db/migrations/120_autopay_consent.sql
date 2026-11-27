-- 120: согласие на автосписание при обычной оплате.
--
-- Дата: 2026-11-27
-- До этого согласие (галочка «Согласен на автоматическое списание N ₽ раз в …»
-- в окне оплаты на /pricing) проверял только фронт, в базе следа не оставалось.
-- Для триала оно пишется с миграции 014 (trial_consent_at / _version); здесь то
-- же для платной подписки: карта с Recurrent="Y" и СБП с привязкой счёта.
--
--   autopay_consent_at      — когда сервер принял consent=true (POST /checkout,
--                             POST /sbp/bind)
--   autopay_consent_version — версия текста согласия (plans.AUTOPAY_CONSENT_VERSION,
--                             сейчас 'autopay-v1'). 'legacy' при пустом
--                             autopay_consent_at — платёж создала старая сборка
--                             фронта без поля consent: согласия сервер НЕ видел
--   autopay_consent_ip      — IP, с которого пришёл запрос (v4 или v6)
--
-- Пишется на первую строку подписки (у СБП-привязки переносится с заявки на
-- подписку, созданную списанием). Продления, триалы, инвайты — NULL.
--
-- ПОРЯДОК ВЫКЛАДКИ: миграцию применить ДО выкатки кода. Колонки NULL-able,
-- старому коду не мешают. Если код окажется на проде раньше — биллинг не
-- упадёт (колонки в ORM отложенные, запись идёт отдельным UPDATE в savepoint),
-- но согласия до применения миграции не сохранятся.
--
--   Get-Content db/migrations/120_autopay_consent.sql | ssh frame `
--     'docker exec -i frame-db-1 psql -U postgres -d moex_db'

ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS autopay_consent_at      TIMESTAMPTZ;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS autopay_consent_version VARCHAR(16);
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS autopay_consent_ip      VARCHAR(45);

COMMENT ON COLUMN subscriptions.autopay_consent_at IS 'Момент согласия на автосписание при обычной оплате (сервер принял consent=true)';
COMMENT ON COLUMN subscriptions.autopay_consent_version IS 'Версия текста согласия (autopay-v1); legacy при пустом autopay_consent_at = старый фронт без поля consent';
COMMENT ON COLUMN subscriptions.autopay_consent_ip IS 'IP запроса, в котором пришло согласие на автосписание';
