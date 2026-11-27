"""Согласие на автосписание при обычной оплате (api/billing/service.py, роутер).

Что проверяем:
  - платёж с автопродлением без согласия не создаётся (ни строки, ни запроса в банк);
  - согласие пишется на подписку: момент, версия текста, IP;
  - переходный период: фронт без поля consent проходит с меткой 'legacy',
    а со строгим режимом (BILLING_CONSENT_STRICT) — отказ;
  - СБП-привязка: согласие с заявки переезжает на подписку, созданную списанием;
  - код, выкаченный раньше миграции 120, оплату не роняет.
"""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api.billing import service
from api.billing.plans import AUTOPAY_CONSENT_LEGACY, AUTOPAY_CONSENT_VERSION


@pytest.fixture()
def engine():
    from sqlalchemy import create_engine
    from api.models.payment_method import UserPaymentMethod
    from api.models.subscription import Subscription
    from api.models.user import User

    eng = create_engine("sqlite:///:memory:")
    for model in (User, UserPaymentMethod, Subscription):
        model.__table__.create(eng)
    return eng


@pytest.fixture()
def db(engine):
    from sqlalchemy.orm import sessionmaker
    from api.models.user import User

    s = sessionmaker(bind=engine)()
    s.add(User(id=1, email="u@example.com", hashed_password="x", role="user", is_verified=True))
    s.commit()
    yield s
    s.close()


@pytest.fixture()
def user(db):
    from api.models.user import User
    return db.query(User).filter(User.id == 1).one()


class FakeBank:
    name = "tbank"

    def __init__(self):
        self.calls = []

    def create_checkout(self, **kw):
        self.calls.append(("card", kw))
        return SimpleNamespace(payment_id=f"pay-{len(self.calls)}", confirmation_url="https://bank/form")

    def create_sbp_checkout(self, **kw):
        self.calls.append(("sbp", kw))
        return SimpleNamespace(payment_id=f"pay-{len(self.calls)}", confirmation_url="",
                               qr_image="img", qr_payload="link", request_key="rk-1")

    def add_account_qr(self, **kw):
        self.calls.append(("bind", kw))
        return {"request_key": "rk-bind", "data": "https://sub.nspk.ru/x"}

    def get_account_qr_state(self, request_key):
        return {"status": "ACTIVE", "account_token": "tok-secret", "bank_member_id": "100"}


@pytest.fixture()
def bank(monkeypatch):
    b = FakeBank()
    monkeypatch.setattr(service, "get_provider_for", lambda user: b)
    monkeypatch.setattr(service, "get_payment_provider", lambda: b)
    monkeypatch.delenv("BILLING_CONSENT_STRICT", raising=False)
    return b


def _consent(db, sub_id):
    return db.execute(text(
        "SELECT autopay_consent_at, autopay_consent_version, autopay_consent_ip "
        "FROM subscriptions WHERE id = :id"
    ), {"id": sub_id}).one()


def _count_subs(db):
    return db.execute(text("SELECT count(*) FROM subscriptions")).scalar()


def _checkout(db, user, **kw):
    kw.setdefault("recurrent", True)
    return service.create_checkout_for_user(
        db=db, user=user, plan_id="basic_monthly", return_url="https://x/billing/success", **kw)


# ── Карта ────────────────────────────────────────────────────────────────────

def test_consent_is_saved_with_time_version_and_ip(db, user, bank):
    res = _checkout(db, user, consent=True, consent_ip="203.0.113.7")
    at, ver, ip = _consent(db, res["subscription_id"])
    assert at is not None
    assert ver == AUTOPAY_CONSENT_VERSION
    assert ip == "203.0.113.7"
    assert bank.calls[0][1]["recurrent"] is True


def test_explicit_refusal_blocks_recurrent_payment(db, user, bank):
    with pytest.raises(ValueError, match="согласие"):
        _checkout(db, user, consent=False)
    assert bank.calls == []            # в банк не ходили
    assert _count_subs(db) == 0        # и строку не завели


def test_explicit_refusal_blocks_even_in_transition_period(db, user, bank, monkeypatch):
    monkeypatch.setenv("BILLING_CONSENT_STRICT", "0")
    with pytest.raises(ValueError):
        _checkout(db, user, consent=False)


def test_old_frontend_without_field_passes_with_legacy_mark(db, user, bank):
    res = _checkout(db, user)          # consent не прислали вовсе
    at, ver, ip = _consent(db, res["subscription_id"])
    assert at is None                  # согласия сервер не видел — не выдумываем
    assert ver == AUTOPAY_CONSENT_LEGACY
    assert ip is None
    assert len(bank.calls) == 1


def test_strict_mode_rejects_missing_field(db, user, bank, monkeypatch):
    monkeypatch.setenv("BILLING_CONSENT_STRICT", "1")
    with pytest.raises(ValueError, match="согласие"):
        _checkout(db, user)
    assert bank.calls == [] and _count_subs(db) == 0
    assert _checkout(db, user, consent=True)["subscription_id"]


def test_one_time_payment_needs_no_consent(db, user, bank, monkeypatch):
    monkeypatch.setenv("BILLING_CONSENT_STRICT", "1")
    res = _checkout(db, user, recurrent=False)
    assert _consent(db, res["subscription_id"]) == (None, None, None)


def test_sbp_rail_is_always_recurrent_and_needs_consent(db, user, bank, monkeypatch):
    monkeypatch.setenv("BILLING_CONSENT_STRICT", "1")
    with pytest.raises(ValueError):
        _checkout(db, user, recurrent=False, rail="sbp")
    res = _checkout(db, user, recurrent=False, rail="sbp", consent=True, consent_ip="2001:db8::1")
    at, ver, ip = _consent(db, res["subscription_id"])
    assert at is not None and ver == AUTOPAY_CONSENT_VERSION and ip == "2001:db8::1"


def test_demo_terminal_does_not_ask_for_consent(db, user, bank, monkeypatch):
    # На демо-терминале автопродление выключается принудительно.
    bank.name = "tbank_demo"
    monkeypatch.setenv("BILLING_CONSENT_STRICT", "1")
    res = _checkout(db, user)
    assert bank.calls[0][1]["recurrent"] is False
    assert _consent(db, res["subscription_id"]) == (None, None, None)


def test_code_deployed_before_migration_does_not_break_checkout(db, user, bank):
    # Колонок согласия ещё нет: оплата и чтение подписок должны работать.
    from api.models.subscription import Subscription
    for col in ("autopay_consent_at", "autopay_consent_version", "autopay_consent_ip"):
        db.execute(text(f"ALTER TABLE subscriptions DROP COLUMN {col}"))
    db.commit()

    res = _checkout(db, user, consent=True, consent_ip="203.0.113.7")
    sub = db.query(Subscription).filter(Subscription.id == res["subscription_id"]).one()
    assert sub.status == "pending" and sub.yk_payment_id == res["payment_id"]
    assert service.current_subscription(db, user) is None      # обычные SELECT живы


# ── СБП-привязка счёта ───────────────────────────────────────────────────────

def test_sbp_binding_requires_and_saves_consent(db, user, bank, monkeypatch):
    with pytest.raises(ValueError, match="согласие"):
        service.start_sbp_binding(db, user, "basic_monthly", consent=False)
    assert bank.calls == [] and _count_subs(db) == 0

    res = service.start_sbp_binding(db, user, "basic_monthly", consent=True, consent_ip="203.0.113.9")
    at, ver, ip = _consent(db, res["subscription_id"])
    assert at is not None and ver == AUTOPAY_CONSENT_VERSION and ip == "203.0.113.9"

    monkeypatch.setenv("BILLING_CONSENT_STRICT", "1")
    with pytest.raises(ValueError):
        service.start_sbp_binding(db, user, "basic_monthly")


def test_sbp_consent_moves_to_the_charged_subscription(db, user, bank, monkeypatch):
    from api.models.subscription import Subscription

    bind = service.start_sbp_binding(db, user, "basic_monthly", consent=True, consent_ip="203.0.113.9")

    def fake_charge(db_, user_, plan_id, pm, **kw):
        paid = Subscription(user_id=user_.id, tier="basic", period="monthly", plan_id=plan_id,
                            amount=2900, status="active", payment_method_id=pm.id, yk_payment_id="chg-1")
        db_.add(paid)
        db_.commit()
        return {"ok": True, "subscription_id": paid.id, "tier": "basic", "expires_at": None}

    monkeypatch.setattr(service, "charge_recurrent", fake_charge)
    out = service.poll_sbp_binding(db, user, bind["subscription_id"])
    assert out["status"] == "charged"
    assert tuple(_consent(db, out["subscription_id"])) == tuple(_consent(db, bind["subscription_id"]))
    assert _consent(db, out["subscription_id"])[1] == AUTOPAY_CONSENT_VERSION


# ── Роутер ───────────────────────────────────────────────────────────────────

def _request(ip="198.51.100.4"):
    return SimpleNamespace(headers={"origin": "https://framedata.ru"}, client=SimpleNamespace(host=ip))


def test_checkout_endpoint_rejects_without_consent_and_passes_ip(db, user, bank):
    pytest.importorskip("fastapi")   # локально без зависимостей API — пропуск
    from fastapi import HTTPException
    from api.routers import billing as router

    body = router.CheckoutRequest(plan_id="basic_monthly", recurrent=True, consent=False)
    with pytest.raises(HTTPException) as err:
        asyncio.run(router.checkout(body, _request(), user=user, db=db))
    assert err.value.status_code == 400 and "согласие" in err.value.detail

    body = router.CheckoutRequest(plan_id="basic_monthly", recurrent=True, consent=True)
    resp = asyncio.run(router.checkout(body, _request(), user=user, db=db))
    at, ver, ip = _consent(db, resp.subscription_id)
    assert at is not None and ip == "198.51.100.4"


def test_sbp_bind_endpoint_rejects_without_consent(db, user, bank):
    pytest.importorskip("fastapi")   # локально без зависимостей API — пропуск
    from fastapi import HTTPException
    from api.routers import billing as router

    body = router.SbpBindRequest(plan_id="basic_monthly", consent=False)
    with pytest.raises(HTTPException) as err:
        asyncio.run(router.sbp_bind_start(body, _request(), user=user, db=db))
    assert err.value.status_code == 400

    body = router.SbpBindRequest(plan_id="basic_monthly", consent=True)
    out = asyncio.run(router.sbp_bind_start(body, _request("198.51.100.5"), user=user, db=db))
    assert _consent(db, out["subscription_id"])[2] == "198.51.100.5"
