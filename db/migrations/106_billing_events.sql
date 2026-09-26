-- 106: журнал событий биллинга + уведомления в @frameadminbot.
--
-- Каждый переход подписки (оплата, продление, отказ от автопродления, возврат,
-- истечение, провал списания) и отвязка карты пишутся ТРИГГЕРОМ в billing_events.
-- Триггер, а не вызовы из Python: переходы размазаны по service.py, trial.py,
-- invites.py, роутеру и оркестратору, а в БД их не пропустить (в том числе
-- ручные UPDATE из psql).
--
-- Доставка — outbox: api/billing/admin_notify.py забирает строки с
-- notified_at IS NULL (FOR UPDATE SKIP LOCKED — безопасно из трёх воркеров
-- API и оркестратора сразу), шлёт в Телеграм и ставит notified_at. Сразу
-- после коммита будит pg_notify('billing_event'), страховочный проход — в
-- оркестраторе каждые 15 минут. skipped = событие системное (продление
-- поверх истечения, апгрейд гасит старую подписку) и в чат не ушло.

CREATE TABLE IF NOT EXISTS billing_events (
    id                BIGSERIAL PRIMARY KEY,
    kind              TEXT        NOT NULL,
    user_id           INTEGER,
    subscription_id   INTEGER,
    payment_method_id INTEGER,
    old_status        TEXT,
    new_status        TEXT,
    amount            NUMERIC(10, 2),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    notified_at       TIMESTAMPTZ,
    skipped           BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS billing_events_pending_idx
    ON billing_events (id) WHERE notified_at IS NULL;
CREATE INDEX IF NOT EXISTS billing_events_user_idx
    ON billing_events (user_id, created_at);
CREATE INDEX IF NOT EXISTS billing_events_kind_time_idx
    ON billing_events (kind, created_at);

COMMENT ON TABLE billing_events IS 'Журнал переходов подписок и способов оплаты; outbox уведомлений в админ-бот';
COMMENT ON COLUMN billing_events.kind IS 'payment | trial_start | grant | payment_failed | autorenew_off | autorenew_on | retention | autopay_bound | refund | reversed | expired | method_unlinked | digest_sent';


CREATE OR REPLACE FUNCTION billing_log_subscription() RETURNS trigger AS $$
DECLARE
    kinds TEXT[] := '{}';
    k     TEXT;
    new_id BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Инвайт создаёт подписку сразу active; чекауты и продления — pending.
        IF NEW.status = 'active' THEN
            kinds := kinds || CASE
                WHEN NEW.is_trial THEN 'trial_start'
                WHEN NEW.yk_payment_id IS NULL THEN 'grant'
                ELSE 'payment' END;
        END IF;
    ELSE
        IF NEW.status = 'active' AND OLD.status IS DISTINCT FROM 'active' THEN
            kinds := kinds || CASE WHEN NEW.is_trial THEN 'trial_start' ELSE 'payment' END;
        END IF;
        IF NEW.status = 'failed' AND OLD.status IN ('pending', 'binding') THEN
            kinds := kinds || 'payment_failed'::TEXT;
        END IF;
        IF NEW.status = 'refunded' AND OLD.status IS DISTINCT FROM 'refunded' THEN
            kinds := kinds || 'refund'::TEXT;
        END IF;
        IF NEW.status = 'cancelled' AND OLD.status = 'active' THEN
            kinds := kinds || 'reversed'::TEXT;
        END IF;
        IF NEW.status = 'expired' AND OLD.status = 'active' THEN
            kinds := kinds || 'expired'::TEXT;
        END IF;
        IF NEW.status = 'active' AND OLD.status = 'active' THEN
            IF OLD.cancelled_at IS NULL AND NEW.cancelled_at IS NOT NULL THEN
                kinds := kinds || 'autorenew_off'::TEXT;
            ELSIF OLD.cancelled_at IS NOT NULL AND NEW.cancelled_at IS NULL THEN
                kinds := kinds || 'autorenew_on'::TEXT;
            END IF;
            IF OLD.payment_method_id IS NULL AND NEW.payment_method_id IS NOT NULL THEN
                kinds := kinds || 'autopay_bound'::TEXT;
            END IF;
            IF COALESCE(OLD.discount_pct, 0) = 0 AND COALESCE(NEW.discount_pct, 0) > 0 THEN
                kinds := kinds || 'retention'::TEXT;
            END IF;
        END IF;
    END IF;

    FOREACH k IN ARRAY kinds LOOP
        INSERT INTO billing_events
            (kind, user_id, subscription_id, payment_method_id, old_status, new_status, amount)
        VALUES
            (k, NEW.user_id, NEW.id, NEW.payment_method_id,
             CASE WHEN TG_OP = 'UPDATE' THEN OLD.status END, NEW.status, NEW.amount)
        RETURNING id INTO new_id;
        PERFORM pg_notify('billing_event', new_id::TEXT);
    END LOOP;
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    -- Журнал не должен ронять оплату: активация подписки важнее уведомления.
    RAISE WARNING 'billing_log_subscription: %', SQLERRM;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_subs_billing_events ON subscriptions;
CREATE TRIGGER trg_subs_billing_events
    AFTER INSERT OR UPDATE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION billing_log_subscription();


CREATE OR REPLACE FUNCTION billing_log_payment_method() RETURNS trigger AS $$
DECLARE
    new_id BIGINT;
BEGIN
    IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        INSERT INTO billing_events (kind, user_id, payment_method_id)
        VALUES ('method_unlinked', NEW.user_id, NEW.id)
        RETURNING id INTO new_id;
        PERFORM pg_notify('billing_event', new_id::TEXT);
    END IF;
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'billing_log_payment_method: %', SQLERRM;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pm_billing_events ON user_payment_methods;
CREATE TRIGGER trg_pm_billing_events
    AFTER UPDATE ON user_payment_methods
    FOR EACH ROW EXECUTE FUNCTION billing_log_payment_method();
