"""Акция, потолок оплаченного срока и пересчёт остатка при повышении тарифа
(api/billing/sale.py + service.checkout_offer / assert_can_purchase /
activate_from_webhook)."""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api.billing import sale, service
from api.billing.plans import get_plan
from api.billing.provider import WebhookEvent

NOW = datetime.now(timezone.utc).replace(microsecond=0)
BASIC_Y, BASIC_M = get_plan("basic_yearly"), get_plan("basic_monthly")
PRO_Y, PRO_M = get_plan("pro_yearly"), get_plan("pro_monthly")


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
    s.add(User(id=1, email="u@example.com", hashed_password="x", role="free", is_verified=True))
    s.flush()
    yield s
    s.close()


@pytest.fixture()
def user(db):
    from api.models.user import User
    return db.query(User).filter(User.id == 1).one()


@pytest.fixture()
def sale_on(monkeypatch):
    monkeypatch.setenv("BILLING_SALE_CODE", "bf2026")
    monkeypatch.setenv("BILLING_SALE_PCT_BASIC", "30")
    monkeypatch.setenv("BILLING_SALE_PCT_PRO", "45")
    monkeypatch.setenv("BILLING_SALE_GIFT_DAYS", "30")
    monkeypatch.delenv("BILLING_SALE_PCT", raising=False)
    monkeypatch.setenv("BILLING_SALE_START", (NOW - timedelta(days=1)).isoformat())
    monkeypatch.setenv("BILLING_SALE_END", (NOW + timedelta(days=3)).isoformat())
    monkeypatch.setenv("BILLING_SALE_TITLE", "Чёрная пятница")
    monkeypatch.delenv("BILLING_SALE_MONTHLY", raising=False)


@pytest.fixture()
def sale_off(monkeypatch):
    for k in ("BILLING_SALE_CODE", "BILLING_SALE_PCT", "BILLING_SALE_PCT_BASIC", "BILLING_SALE_PCT_PRO",
              "BILLING_SALE_GIFT_DAYS", "BILLING_SALE_START", "BILLING_SALE_END"):
        monkeypatch.delenv(k, raising=False)


class FakeBank:
    name = "tbank"

    def verify_payment(self, payment_id):
        return {"Status": "CONFIRMED"}


@pytest.fixture(autouse=True)
def bank(monkeypatch):
    b = FakeBank()
    monkeypatch.setattr(service, "get_provider_for", lambda user: b)
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: b)
    monkeypatch.setattr(service, "get_payment_provider", lambda: b)
    monkeypatch.setattr(service, "_log_double_charge", lambda db, sub: None)
    return b


def _sub(db, status, plan, *, created_d=0.0, expires_d=None, started_d=None, pm=None,
         pid="auto", amount=None, trial=False, promo=None, cancelled=False):
    from api.models.subscription import Subscription
    r = Subscription(
        user_id=1, tier=plan.tier, period=plan.period, plan_id=plan.plan_id,
        amount=plan.amount if amount is None else amount, status=status,
        payment_method_id=pm, is_trial=trial,
        created_at=NOW + timedelta(days=created_d),
        started_at=NOW + timedelta(days=started_d) if started_d is not None else None,
        expires_at=NOW + timedelta(days=expires_d) if expires_d is not None else None,
        cancelled_at=NOW if cancelled else None,
    )
    db.add(r)
    db.flush()
    if pid == "auto":
        pid = f"pay-{r.id}"
    r.yk_payment_id = pid
    db.flush()
    if promo:
        db.execute(text("UPDATE subscriptions SET promo_code = :p WHERE id = :i"), {"p": promo, "i": r.id})
    return r


def _pay(db, sub):
    return service.activate_from_webhook(db, WebhookEvent(
        payment_id=sub.yk_payment_id, event_type="payment.succeeded",
        amount=float(sub.amount), provider_name="tbank"))


def _aware(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _days(a, b):
    return round((_aware(a) - _aware(b)).total_seconds() / 86400)


# ── настройка акции ──────────────────────────────────────────────────────────

def test_sale_is_off_without_env(sale_off):
    assert sale.active_sale() is None


def test_sale_window_and_price(sale_on):
    s = sale.active_sale(NOW)
    assert s.code == "bf2026" and not s.monthly_available
    assert s.pct("basic") == 30 and s.pct("pro") == 45 and s.max_pct == 45
    assert s.gift_days("pro") == 30
    assert sale.active_sale(NOW + timedelta(days=4)) is None
    assert sale.active_sale(NOW - timedelta(days=2)) is None
    # как на витрине: скидка от цены 12 месяцев по месячному тарифу
    assert sale.sale_price(BASIC_Y, s) == 24360 and sale.sale_price(PRO_Y, s) == 38940
    assert sale.is_sale_plan(BASIC_Y, s) and not sale.is_sale_plan(BASIC_M, s)


@pytest.mark.parametrize("pct", ["0", "95", "abc", ""])
def test_broken_sale_config_means_no_sale(sale_on, monkeypatch, pct):
    monkeypatch.setenv("BILLING_SALE_PCT_BASIC", pct)
    monkeypatch.setenv("BILLING_SALE_PCT_PRO", pct)
    assert sale.active_sale(NOW) is None


def test_naive_dates_are_moscow_time(monkeypatch):
    monkeypatch.setenv("BILLING_SALE_CODE", "x")
    monkeypatch.setenv("BILLING_SALE_PCT", "25")
    monkeypatch.setenv("BILLING_SALE_START", "2026-11-28T00:00:00")
    monkeypatch.setenv("BILLING_SALE_END", "2026-12-01T00:00:00")
    s = sale.configured_sale()
    assert s.start == datetime(2026, 11, 27, 21, 0, tzinfo=timezone.utc)


# ── пересчёт дней ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("left_days,expected", [(300, 150), (1, 1), (3, 2), (0, 0), (-5, 0)])
def test_two_basic_days_make_one_pro_day(left_days, expected):
    assert sale.bonus_days("basic", "pro", timedelta(days=left_days)) == expected


def test_no_conversion_downwards_or_same_tier():
    assert sale.bonus_days("pro", "basic", timedelta(days=100)) == 0
    assert sale.bonus_days("basic", "basic", timedelta(days=100)) == 0


# ── цена и лимит акции ───────────────────────────────────────────────────────

def test_offer_full_price_without_sale(db, user, sale_off):
    o = service.checkout_offer(db, user, BASIC_Y, NOW)
    assert o["amount"] == BASIC_Y.amount and o["promo_code"] is None
    assert _days(o["expires_at"], NOW) == 365


def test_offer_sale_price_on_yearly_only(db, user, sale_on):
    o = service.checkout_offer(db, user, PRO_Y, NOW)
    assert o["amount"] == 38940 and o["promo_code"] == "bf2026" and o["gift_days"] == 30
    assert _days(o["expires_at"], NOW) == 365 + 30
    m = service.checkout_offer(db, user, PRO_M, NOW)
    assert m["amount"] == PRO_M.amount and m["promo_code"] is None


def test_sale_only_once_per_account(db, user, sale_on):
    _sub(db, "active", BASIC_Y, created_d=-1, started_d=-1, expires_d=364, promo="bf2026")
    o = service.checkout_offer(db, user, PRO_Y, NOW)        # другой тариф — лимит общий
    assert o["promo_code"] is None and o["amount"] == PRO_Y.amount


def test_refunded_or_unpaid_sale_purchase_does_not_use_limit(db, user, sale_on):
    _sub(db, "refunded", BASIC_Y, created_d=-1, started_d=-1, expires_d=364, promo="bf2026")
    _sub(db, "pending", BASIC_Y, created_d=-0.01, promo="bf2026")     # форма не оплачена
    assert service.checkout_offer(db, user, BASIC_Y, NOW)["promo_code"] == "bf2026"


def test_previous_sale_does_not_block_new_one(db, user, sale_on):
    _sub(db, "expired", BASIC_Y, created_d=-400, started_d=-400, expires_d=-35, promo="bf2025")
    assert service.checkout_offer(db, user, BASIC_Y, NOW)["promo_code"] == "bf2026"


# ── гейт покупки ─────────────────────────────────────────────────────────────

def test_yearly_subscriber_can_buy_same_plan_on_sale(db, user, sale_on):
    _sub(db, "active", BASIC_Y, created_d=-100, started_d=-100, expires_d=265, pm=10)
    service.assert_can_purchase(db, user, BASIC_Y)                     # не бросает
    o = service.checkout_offer(db, user, BASIC_Y, NOW)
    assert _days(o["expires_at"], NOW) == 265 + 365 + 30               # год + подарочный месяц


def test_same_plan_still_blocked_without_sale(db, user, sale_off):
    _sub(db, "active", BASIC_Y, created_d=-100, started_d=-100, expires_d=265, pm=10)
    with pytest.raises(ValueError, match="уже активен"):
        service.assert_can_purchase(db, user, BASIC_Y)


def test_same_plan_blocked_after_sale_limit_used(db, user, sale_on):
    _sub(db, "active", BASIC_Y, created_d=-1, started_d=-1, expires_d=364, pm=10, promo="bf2026")
    with pytest.raises(ValueError, match="уже активен"):
        service.assert_can_purchase(db, user, BASIC_Y)


def test_monthly_not_sold_during_sale(db, user, sale_on, monkeypatch):
    with pytest.raises(ValueError, match="только годовую"):
        service.assert_can_purchase(db, user, BASIC_M)
    monkeypatch.setenv("BILLING_SALE_MONTHLY", "1")
    service.assert_can_purchase(db, user, BASIC_M)


def test_three_year_cap(db, user, sale_off):
    _sub(db, "active", BASIC_M, created_d=-10, started_d=-10, expires_d=2 * 365 + 20, cancelled=True)
    with pytest.raises(ValueError, match="3 года"):
        service.assert_can_purchase(db, user, BASIC_Y)                 # вышло бы 3 года + 20 дней
    service.assert_can_purchase(db, user, BASIC_M)                     # +30 дней — влезает


def test_three_year_cap_applies_to_sale_too(db, user, sale_on):
    _sub(db, "active", BASIC_Y, created_d=-10, started_d=-10, expires_d=2 * 365 + 20, cancelled=True)
    with pytest.raises(ValueError, match="3 года"):
        service.assert_can_purchase(db, user, BASIC_Y)


def test_cap_counts_bonus_days(db, user, sale_off):
    _sub(db, "active", BASIC_Y, created_d=-10, started_d=-10, expires_d=2 * 365 + 300, cancelled=True)
    # Pro год = 365 + (1030/2 = 515) бонусных = 880 дней — в потолок влезает
    service.assert_can_purchase(db, user, PRO_Y)
    o = service.checkout_offer(db, user, PRO_Y, NOW)
    assert o["bonus_days"] == 515 and o["bonus_from"] == "basic"


# ── оплата: сумма, метка, срок ───────────────────────────────────────────────

def test_checkout_charges_sale_price_and_marks_promo(db, user, sale_on, monkeypatch):
    created = {}

    class Prov(FakeBank):
        def create_checkout(self, **kw):
            created.update(kw)
            return SimpleNamespace(payment_id="pay-new", confirmation_url="https://pay")

    p = Prov()
    monkeypatch.setattr(service, "get_provider_for", lambda u: p)
    res = service.create_checkout_for_user(
        db, user, "basic_yearly", "https://x/billing/success", recurrent=True, consent=True)
    assert res["amount"] == created["amount"] == 24360
    promo = db.execute(text("SELECT promo_code FROM subscriptions WHERE id = :i"),
                       {"i": res["subscription_id"]}).scalar()
    assert promo == "bf2026"


def test_sale_year_stacks_on_existing_year_and_moves_autorenew(db, user, sale_on):
    old = _sub(db, "active", BASIC_Y, created_d=-100, started_d=-100, expires_d=265, pm=10)
    new = _sub(db, "pending", BASIC_Y, created_d=-0.001, amount=24360, promo="bf2026")
    offer = service.checkout_offer(db, user, BASIC_Y)
    _pay(db, new)
    assert new.status == "active" and _days(new.expires_at, NOW) == 265 + 365 + 30
    assert _days(new.expires_at, offer["expires_at"]) == 0             # обещали то же, что дали
    gift = db.execute(text("SELECT gift_days FROM subscriptions WHERE id = :i"), {"i": new.id}).scalar()
    assert gift == 30
    assert old.cancelled_at is not None and old.status == "active"
    # лимит акции теперь исчерпан
    assert service.checkout_offer(db, user, PRO_Y)["promo_code"] is None


# ── повышение тарифа ─────────────────────────────────────────────────────────

def test_upgrade_converts_basic_remainder_into_pro_days(db, user, sale_off):
    basic = _sub(db, "active", BASIC_Y, created_d=-65, started_d=-65, expires_d=300, pm=10)
    pro = _sub(db, "pending", PRO_Y, created_d=-0.001)
    offer = service.checkout_offer(db, user, PRO_Y)
    _pay(db, pro)
    assert _days(pro.expires_at, NOW) == 365 + 150            # 300 дней Basic → 150 дней Pro
    assert _days(pro.expires_at, offer["expires_at"]) == 0    # обещали то же, что дали
    assert basic.status == "expired" and basic.cancelled_at is not None
    assert _days(basic.expires_at, NOW) == 0
    bonus = db.execute(text("SELECT bonus_days FROM subscriptions WHERE id = :i"), {"i": pro.id}).scalar()
    assert bonus == 150
    assert user.role == "pro"


def test_upgrade_counts_stacked_basic_chain_once(db, user, sale_off):
    _sub(db, "active", BASIC_M, created_d=-20, started_d=-20, expires_d=10, cancelled=True)
    _sub(db, "active", BASIC_Y, created_d=-5, started_d=-5, expires_d=375, pm=10)   # прибавлен к месяцу
    pro = _sub(db, "pending", PRO_M, created_d=-0.001)
    _pay(db, pro)
    assert _days(pro.expires_at, NOW) == 30 + 188             # ceil(375 / 2)


def test_trial_and_invite_are_not_converted(db, user, sale_off):
    trial = _sub(db, "active", BASIC_M, created_d=-3, started_d=-3, expires_d=11, trial=True, amount=0, pid=None)
    invite = _sub(db, "active", BASIC_M, created_d=-3, started_d=-3, expires_d=60, amount=0, pid=None)
    pro = _sub(db, "pending", PRO_M, created_d=-0.001)
    _pay(db, pro)
    assert _days(pro.expires_at, NOW) == 30
    assert trial.status == "active" and invite.status == "active"


def test_renewal_of_pro_gets_no_bonus(db, user, sale_off):
    pro = _sub(db, "active", PRO_M, created_d=-29, started_d=-29, expires_d=1, pm=10)
    renewal = _sub(db, "pending", PRO_M, created_d=0, pm=10)
    _pay(db, renewal)
    assert _days(renewal.expires_at, pro.expires_at) == 30


# ── акция: край ──────────────────────────────────────────────────────────────

def test_small_discount_never_costs_more_than_regular_year(sale_on, monkeypatch):
    monkeypatch.setenv("BILLING_SALE_PCT_BASIC", "10")     # 34 800 − 10% = 31 320 > 27 840
    s = sale.active_sale(NOW)
    assert sale.sale_price(BASIC_Y, s) == BASIC_Y.amount


def test_form_paid_after_sale_end_keeps_gift(db, user, sale_on, monkeypatch):
    new = _sub(db, "pending", PRO_Y, created_d=-0.001, amount=38940, promo="bf2026")
    monkeypatch.setenv("BILLING_SALE_END", (NOW - timedelta(minutes=5)).isoformat())
    assert sale.active_sale() is None
    _pay(db, new)
    assert _days(new.expires_at, NOW) == 365 + 30


def test_full_price_purchase_gets_no_gift(db, user, sale_on):
    new = _sub(db, "pending", PRO_Y, created_d=-0.001)      # без метки акции
    _pay(db, new)
    assert _days(new.expires_at, NOW) == 365


def test_second_sale_purchase_alerts_admins(db, user, sale_on, monkeypatch):
    events = []
    monkeypatch.setattr(service, "_log_billing_event", lambda db, kind, sub: events.append((kind, sub.id)))
    first = _sub(db, "pending", BASIC_Y, created_d=-0.002, amount=24360, promo="bf2026")
    second = _sub(db, "pending", PRO_Y, created_d=-0.001, amount=38940, promo="bf2026")
    _pay(db, first)
    assert events == []
    _pay(db, second)
    assert events == [("promo_twice", second.id)]


# ── Basic при действующем Pro ────────────────────────────────────────────────

def test_basic_bought_under_cancelled_pro_starts_after_pro(db, user, sale_off):
    _sub(db, "active", PRO_M, created_d=-10, started_d=-10, expires_d=20, pm=10, cancelled=True)
    offer = service.checkout_offer(db, user, BASIC_Y)
    assert _days(offer["starts_at"], NOW) == 20
    basic = _sub(db, "pending", BASIC_Y, created_d=-0.001)
    _pay(db, basic)
    assert _days(basic.expires_at, NOW) == 20 + 365


def test_pro_does_not_wait_for_basic(db, user, sale_off):
    _sub(db, "active", BASIC_M, created_d=-10, started_d=-10, expires_d=20, cancelled=True)
    assert _days(service.checkout_offer(db, user, PRO_M)["starts_at"], NOW) == 0


# ── возврат после повышения тарифа ───────────────────────────────────────────

def test_refund_of_upgrade_restores_basic(db, user, sale_off):
    basic = _sub(db, "active", BASIC_Y, created_d=-65, started_d=-65, expires_d=300, pm=10)
    pro = _sub(db, "pending", PRO_Y, created_d=-0.001)
    _pay(db, pro)
    assert basic.status == "expired" and user.role == "pro"

    service.cancel_by_webhook(db, WebhookEvent(payment_id=pro.yk_payment_id, event_type="refund.succeeded"))
    assert pro.status == "refunded"
    assert basic.status == "active" and _days(basic.expires_at, NOW) == 300
    assert basic.cancelled_at is not None          # автопродление не возвращаем
    assert user.role == "basic"


def test_refund_does_not_revive_basic_that_would_have_ended(db, user, sale_off):
    basic = _sub(db, "active", BASIC_M, created_d=-29, started_d=-29, expires_d=1)
    pro = _sub(db, "pending", PRO_M, created_d=-0.001)
    _pay(db, pro)
    later = datetime.now(timezone.utc) + timedelta(days=5)
    assert service._restore_converted(db, pro, later) == 0
    assert basic.status == "expired"


# ── переход с Pro на Basic одним действием ───────────────────────────────────

def test_pro_subscriber_blocked_from_basic_without_confirmation(db, user, sale_on):
    _sub(db, "active", PRO_M, created_d=-10, started_d=-10, expires_d=20, pm=10)
    with pytest.raises(ValueError, match="более высокий тариф"):
        service.assert_can_purchase(db, user, BASIC_Y)
    service.assert_can_purchase(db, user, BASIC_Y, switch_down=True)   # подтвердил в окне


def test_switch_down_keeps_pro_until_end_then_basic(db, user, sale_on):
    pro = _sub(db, "active", PRO_M, created_d=-10, started_d=-10, expires_d=20, pm=10)
    offer = service.checkout_offer(db, user, BASIC_Y)
    assert offer["amount"] == 24360 and _days(offer["starts_at"], NOW) == 20
    basic = _sub(db, "pending", BASIC_Y, created_d=-0.001, amount=24360, promo="bf2026")
    _pay(db, basic)
    # Pro дослуживает, его автопродление выключено в момент оплаты
    assert pro.status == "active" and pro.cancelled_at is not None
    assert _days(pro.expires_at, NOW) == 20
    # Basic: после Pro, год + подарочный месяц
    assert _days(basic.expires_at, NOW) == 20 + 365 + 30
    assert basic.cancelled_at is None
    assert user.role == "pro"                      # пока действует Pro


def test_abandoned_switch_form_does_not_touch_pro(db, user, sale_on):
    pro = _sub(db, "active", PRO_M, created_d=-10, started_d=-10, expires_d=20, pm=10)
    _sub(db, "pending", BASIC_Y, created_d=-0.001, amount=24360, promo="bf2026")   # форму открыл и ушёл
    assert pro.cancelled_at is None


# ── тест-аккаунт админа ──────────────────────────────────────────────────────

def test_admin_cannot_buy_unless_listed_as_tester(db, user, sale_on, monkeypatch):
    user.role = "admin"
    db.flush()
    monkeypatch.delenv("BILLING_TEST_USER_IDS", raising=False)
    with pytest.raises(ValueError, match="админа"):
        service.assert_can_purchase(db, user, PRO_Y)
    monkeypatch.setenv("BILLING_TEST_USER_IDS", "1")
    service.assert_can_purchase(db, user, PRO_Y)          # тест-аккаунт проходит


def test_tester_pays_test_price_and_keeps_admin_role(db, user, sale_on, monkeypatch):
    user.role = "admin"
    db.flush()
    monkeypatch.setenv("BILLING_TEST_USER_IDS", "1")
    monkeypatch.setenv("BILLING_TEST_PRICE_RUB", "20")
    created = {}

    class Prov(FakeBank):
        def create_checkout(self, **kw):
            created.update(kw)
            return SimpleNamespace(payment_id="pay-test", confirmation_url="https://pay")

    p = Prov()
    monkeypatch.setattr(service, "get_provider_for", lambda u: p)
    res = service.create_checkout_for_user(
        db, user, "pro_yearly", "https://x/billing/success", recurrent=True, consent=True)
    assert res["amount"] == created["amount"] == 20.0     # в банк уходит тест-цена
    from api.models.subscription import Subscription
    sub = db.query(Subscription).filter(Subscription.id == res["subscription_id"]).one()
    service.activate_from_webhook(db, WebhookEvent(
        payment_id="pay-test", event_type="payment.succeeded", amount=20.0, provider_name="tbank"))
    assert sub.status == "active" and _days(sub.expires_at, NOW) == 365 + 30   # год + подарок
    assert user.role == "admin"                           # роль не тронута
    # другому аккаунту тест-цена не достаётся
    monkeypatch.setenv("BILLING_TEST_USER_IDS", "999")
    assert service.test_price_for(user, 38940) == 38940
