"""Автопродление и формы оплаты, открытые клиентом (api/billing/service.py).

Что проверяем:
  - брошенная форма оплаты больше не выключает автопродление;
  - перед списанием крон сверяет незакрытые формы с банком;
  - оплата поверх того же тарифа выключает автопродление старой подписки,
    сроки складываются;
  - две оплаты за один тариф находятся как двойное списание.
"""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from api.billing import service
from api.billing import tbank
from api.billing.provider import WebhookEvent

NOW = datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture()
def db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from api.models.payment_method import UserPaymentMethod
    from api.models.subscription import Subscription
    from api.models.user import User

    engine = create_engine("sqlite:///:memory:")
    for model in (User, UserPaymentMethod, Subscription):
        model.__table__.create(engine)
    s = sessionmaker(bind=engine)()
    s.add(User(id=1, email="u@example.com", hashed_password="x", role="basic", is_verified=True))
    s.flush()
    yield s
    s.close()


def _sub(db, status, *, created_h, tier="basic", period="monthly", pm=None, pid=None,
         expires_h=None, started_h=None, cancelled=False, user=1):
    from api.models.subscription import Subscription
    r = Subscription(
        user_id=user, tier=tier, period=period, plan_id=f"{tier}_{period}",
        amount=2900, status=status, payment_method_id=pm, yk_payment_id=pid,
        created_at=NOW + timedelta(hours=created_h),
        started_at=NOW + timedelta(hours=started_h) if started_h is not None else None,
        expires_at=NOW + timedelta(hours=expires_h) if expires_h is not None else None,
        cancelled_at=NOW if cancelled else None,
    )
    db.add(r)
    db.flush()
    return r


class FakeBank:
    name = "tbank"

    def __init__(self, states):
        self.states = states          # payment_id → Status | None (банк не ответил)
        self.asked = []

    def verify_payment(self, payment_id):
        self.asked.append(payment_id)
        st = self.states.get(payment_id)
        return None if st is None else {"Status": st, "Amount": 290000}


@pytest.fixture()
def bank(monkeypatch):
    b = FakeBank({})
    monkeypatch.setattr(service, "get_provider_for", lambda user: b)
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: b)
    monkeypatch.setattr(service, "get_payment_provider", lambda: b)
    return b


@pytest.fixture()
def alerts(monkeypatch):
    got = []
    monkeypatch.setattr(service, "_log_double_charge", lambda db, sub: got.append(sub.id))
    return got


# ── Guard 1: что считается «уже продлили» ────────────────────────────────────

def _follow_ups(db, base):
    from api.models.subscription import Subscription
    return [r.id for r in db.query(Subscription).filter(
        Subscription.user_id == base.user_id, Subscription.tier == base.tier,
        Subscription.id != base.id, service.follow_up_condition(),
        Subscription.created_at > base.created_at,
    ).all()]


def test_abandoned_form_is_not_a_follow_up(db):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    _sub(db, "pending", created_h=-48, pid="form-1")          # открыл форму и ушёл
    assert _follow_ups(db, base) == []


def test_inflight_recurrent_charge_and_paid_sub_are_follow_ups(db):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    sbp = _sub(db, "pending", created_h=-1, pm=10, pid="charge-1")   # СБП-списание в полёте
    paid = _sub(db, "active", created_h=-2, pid="pay-1", expires_h=800)
    assert sorted(_follow_ups(db, base)) == sorted([sbp.id, paid.id])


@pytest.mark.parametrize("status,expected", [
    ("CONFIRMED", "paid"), ("REJECTED", "dead"), ("DEADLINE_EXPIRED", "dead"),
    ("CANCELED", "dead"), ("REFUNDED", "refunded"), ("NEW", "open"),
    ("FORM_SHOWED", "open"), ("AUTHORIZING", "open"), ("", "open"), (None, "open"),
])
def test_checkout_state(status, expected):
    assert service.checkout_state(status) == expected


# ── Сверка форм с банком перед списанием ─────────────────────────────────────

def test_no_forms_no_bank_calls(db, bank):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    assert service._reconcile_open_checkouts(db, base, NOW) is None
    assert bank.asked == []


def test_old_abandoned_form_is_ignored_without_asking_bank(db, bank):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    _sub(db, "pending", created_h=-(service.CHECKOUT_WAIT_HOURS + 1), pid="form-old")
    assert service._reconcile_open_checkouts(db, base, NOW) is None
    assert bank.asked == []


def test_open_form_postpones_renewal(db, bank):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    form = _sub(db, "pending", created_h=-0.2, pid="form-1")
    bank.states["form-1"] = "FORM_SHOWED"
    assert "ещё открыта" in service._reconcile_open_checkouts(db, base, NOW)
    assert form.status == "pending"


def test_bank_silent_postpones_renewal(db, bank):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    _sub(db, "pending", created_h=-0.2, pid="form-1")          # states пуст → None
    assert "банк не ответил" in service._reconcile_open_checkouts(db, base, NOW)


def test_expired_form_is_closed_and_renewal_goes_on(db, bank):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    form = _sub(db, "pending", created_h=-3, pid="form-1")
    bank.states["form-1"] = "DEADLINE_EXPIRED"
    assert service._reconcile_open_checkouts(db, base, NOW) is None
    assert form.status == "failed"
    assert base.cancelled_at is None


def test_fresh_paid_form_waits_for_webhook(db, bank):
    # Привязку карты приносит только webhook — даём ему время.
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10)
    form = _sub(db, "pending", created_h=-0.5, pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    assert "ждём webhook" in service._reconcile_open_checkouts(db, base, NOW)
    assert form.status == "pending"


def test_paid_form_with_lost_webhook_is_activated(db, bank, alerts):
    base = _sub(db, "active", created_h=-700, pm=10, expires_h=10, started_h=-700, pid="old-pay")
    form = _sub(db, "pending", created_h=-(service.CHECKOUT_WEBHOOK_GRACE_HOURS + 1), pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    assert service._reconcile_open_checkouts(db, base, NOW) is None
    assert form.status == "active"
    assert base.cancelled_at is not None      # автопродление переехало на новую
    assert alerts == []


# ── Покупка поверх того же тарифа ────────────────────────────────────────────

def test_repurchase_same_tier_stacks_and_moves_autorenew(db, bank, alerts):
    old = _sub(db, "active", created_h=-700, pm=10, expires_h=240, started_h=-700, pid="old-pay")
    new = _sub(db, "pending", created_h=-0.1, period="yearly", pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert new.status == "active"
    assert old.cancelled_at is not None and old.status == "active"   # дослуживает
    # Новый период начинается с конца старого, а не с момента оплаты.
    old_exp = old.expires_at.replace(tzinfo=timezone.utc) if old.expires_at.tzinfo is None else old.expires_at
    new_exp = new.expires_at.replace(tzinfo=timezone.utc) if new.expires_at.tzinfo is None else new.expires_at
    assert new_exp == old_exp + timedelta(days=365)
    assert alerts == []


def test_cron_renewal_does_not_cancel_renewed_row(db, bank, alerts):
    old = _sub(db, "active", created_h=-700, pm=10, expires_h=10, started_h=-700, pid="old-pay")
    renewal = _sub(db, "pending", created_h=0, pm=10, pid="charge-1")   # как charge_recurrent
    bank.states["charge-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="charge-1", event_type="payment.succeeded", amount=2900.0))
    assert renewal.status == "active"
    assert old.cancelled_at is None
    assert alerts == []


# ── Двойное списание ─────────────────────────────────────────────────────────

def test_double_charge_when_cron_charged_while_form_was_open(db, bank, alerts):
    _sub(db, "expired", created_h=-800, pm=10, expires_h=-1, started_h=-800, pid="old-pay")
    form = _sub(db, "pending", created_h=-0.5, period="yearly", pid="form-1")
    # крон списал продление, пока форма была открыта
    renewal = _sub(db, "active", created_h=-0.2, pm=10, pid="charge-1", started_h=-0.2, expires_h=720)
    bank.states["form-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert alerts == [form.id]
    assert service.find_double_charge(db, form).id == renewal.id
    # Автопродление остаётся одно — у купленной клиентом.
    assert renewal.cancelled_at is not None and form.cancelled_at is None


def test_no_double_charge_for_earlier_payment(db, bank, alerts):
    # Старая подписка оплачена ДО открытия формы — это обычная покупка поверх.
    _sub(db, "active", created_h=-100, pm=10, pid="old-pay", started_h=-100, expires_h=620)
    form = _sub(db, "pending", created_h=-0.5, pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert form.status == "active"
    assert alerts == []


def test_other_tier_is_not_double_charge(db, bank, alerts):
    _sub(db, "active", created_h=-0.2, pm=10, pid="charge-1", started_h=-0.2, expires_h=720)
    form = _sub(db, "pending", created_h=-0.5, tier="pro", pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert alerts == []


# ── Крон целиком ─────────────────────────────────────────────────────────────

@pytest.fixture()
def charges(db, monkeypatch):
    from api.models.payment_method import UserPaymentMethod
    db.add(UserPaymentMethod(id=10, user_id=1, provider="tbank", method_type="card", rebill_id="r1"))
    db.flush()
    got = []

    def fake_charge(db_, user, plan_id, pm, **kw):
        got.append(plan_id)
        return {"ok": True, "subscription_id": 999}

    monkeypatch.setattr(service, "charge_recurrent", fake_charge)
    return got


def test_cron_renews_despite_abandoned_form(db, bank, charges):
    _sub(db, "active", created_h=-700, pm=10, expires_h=10, pid="old-pay")
    _sub(db, "pending", created_h=-48, pid="form-1")            # брошена двое суток назад
    summary = service.renew_expiring_subs(db)
    assert charges == ["basic_monthly"]
    assert summary["renewed"] == 1
    assert bank.asked == []                                      # старую форму не трогаем


def test_cron_waits_while_form_is_open(db, bank, charges):
    _sub(db, "active", created_h=-700, pm=10, expires_h=10, pid="old-pay")
    _sub(db, "pending", created_h=-0.2, pid="form-1")
    bank.states["form-1"] = "NEW"
    summary = service.renew_expiring_subs(db)
    assert charges == [] and summary["skipped"] == 1


def test_cron_skips_after_customer_bought_on_form(db, bank, charges, alerts):
    old = _sub(db, "active", created_h=-700, pm=10, expires_h=10, started_h=-700, pid="old-pay")
    _sub(db, "pending", created_h=-3, period="yearly", pid="form-1")
    bank.states["form-1"] = "CONFIRMED"                          # оплачена, webhook потерялся
    summary = service.renew_expiring_subs(db)
    assert charges == [] and summary["skipped"] == 1
    assert old.cancelled_at is not None


# ── Срок жизни формы ─────────────────────────────────────────────────────────

def test_redirect_due_date_is_one_hour_by_default(monkeypatch):
    monkeypatch.delenv("TBANK_FORM_TTL_MINUTES", raising=False)
    assert tbank.checkout_form_ttl_minutes() == 60
    due = tbank._redirect_due_date()
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+03:00", due)
    left = datetime.fromisoformat(due) - datetime.now(timezone.utc)
    assert timedelta(minutes=59) < left <= timedelta(minutes=60)


def test_redirect_due_date_can_be_disabled(monkeypatch):
    monkeypatch.setenv("TBANK_FORM_TTL_MINUTES", "0")
    assert tbank._redirect_due_date() is None
    monkeypatch.setenv("TBANK_FORM_TTL_MINUTES", "abc")
    assert tbank.checkout_form_ttl_minutes() == 60


# ── Сообщение в FRAME · Касса ────────────────────────────────────────────────

def test_double_charge_message_for_admins(db, bank, alerts):
    from api.billing import admin_notify
    from api.models.user import User

    form = _sub(db, "pending", created_h=-0.5, period="yearly", pid="form-1")
    renewal = _sub(db, "active", created_h=-0.2, pm=None, pid="charge-1", started_h=-0.2, expires_h=720)
    bank.states["form-1"] = "CONFIRMED"
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    user = db.query(User).filter(User.id == 1).first()
    msg = admin_notify._render_double_charge(db, form, user)
    assert "Двойное списание" in msg
    assert f"sub #{form.id}" in msg and f"sub #{renewal.id}" in msg
    assert "form-1" in msg and "charge-1" in msg
    assert "НЕ сделан" in msg

    # Одну из оплат вернули — повторно не шумим.
    renewal.status = "refunded"
    db.flush()
    assert admin_notify._render_double_charge(db, form, user) is None


# ── Webhook опоздал: подписку уже включил /sync ──────────────────────────────

def _late_webhook(pid="form-1"):
    return WebhookEvent(
        payment_id=pid, event_type="payment.succeeded", amount=2900.0, provider_name="tbank",
        payment_method="bank_card", rebill_id="rb-1", customer_key="1", card_last4="0333",
    )


def test_late_webhook_saves_card_for_already_active_sub(db, bank, alerts):
    from api.models.payment_method import UserPaymentMethod

    form = _sub(db, "pending", created_h=-0.1, pid="form-1")
    bank.states["form-1"] = "CONFIRMED"
    # /sync: GetState подтвердил оплату, привязки в ответе нет
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert form.status == "active" and form.payment_method_id is None
    expires = form.expires_at

    service.activate_from_webhook(db, _late_webhook())
    pm = db.query(UserPaymentMethod).one()
    assert form.payment_method_id == pm.id and pm.rebill_id == "rb-1"
    assert form.expires_at == expires and form.status == "active"   # срок не трогаем

    # повтор того же webhook'а ничего не плодит
    service.activate_from_webhook(db, _late_webhook())
    assert db.query(UserPaymentMethod).count() == 1


def test_late_webhook_does_not_touch_sub_with_card_or_old_sub(db, bank):
    from api.models.payment_method import UserPaymentMethod

    with_card = _sub(db, "active", created_h=-1, started_h=-1, expires_h=700, pm=10, pid="form-1")
    service.activate_from_webhook(db, _late_webhook("form-1"))
    assert with_card.payment_method_id == 10

    old = _sub(db, "active", created_h=-100, started_h=-(service.LATE_BINDING_HOURS + 1),
               expires_h=600, pid="form-2")
    service.activate_from_webhook(db, _late_webhook("form-2"))
    assert old.payment_method_id is None
    assert db.query(UserPaymentMethod).count() == 0


def test_late_webhook_without_binding_is_noop(db, bank):
    sub = _sub(db, "active", created_h=-1, started_h=-1, expires_h=700, pid="form-1")
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="form-1", event_type="payment.succeeded", amount=2900.0, provider_name="tbank"))
    assert sub.payment_method_id is None
