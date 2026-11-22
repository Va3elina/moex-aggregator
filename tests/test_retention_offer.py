"""Retention-скидка из пути отмены: 25% на 2 месячных продления, годовым — та же
сумма в рублях одним вычетом (api/billing/plans.retention_terms + charge_recurrent)."""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

from decimal import Decimal
from types import SimpleNamespace

import pytest

from api.billing import service
from api.billing.plans import PLANS, RETENTION_MONTHS, RETENTION_PCT, retention_terms


def test_monthly_terms_25pct_two_renewals():
    p = PLANS["basic_monthly"]
    t = retention_terms("basic_monthly")
    assert t["period"] == "monthly"
    assert t["pct"] == RETENTION_PCT == 25
    assert t["periods"] == RETENTION_MONTHS == 2
    assert t["discount_rub"] == round(p.amount * 0.25 * 2)


@pytest.mark.parametrize("tier", ["basic", "pro"])
def test_yearly_terms_one_month_free(tier):
    yearly = retention_terms(f"{tier}_yearly")
    assert yearly["period"] == "yearly"
    assert yearly["periods"] == 1
    # «Месяц в подарок»: ровно цена месяца того же тарифа, ~10% года.
    assert yearly["discount_rub"] == round(PLANS[f"{tier}_monthly"].amount)
    assert yearly["pct"] == 10


def test_no_terms_for_invite_or_unknown_plans():
    assert retention_terms("invite_basic") is None
    assert retention_terms(None) is None


class _FakeDB:
    def __init__(self):
        self.added = []

    def add(self, obj):
        obj.id = 1000 + len(self.added)
        self.added.append(obj)

    def flush(self):
        pass

    def commit(self):
        pass


class _FailingProvider:
    name = "fake"

    def charge(self, **kw):
        self.kw = kw
        raise RuntimeError("stop after Sub(pending) created")


def _charge(monkeypatch, discount_pct, carry_periods, plan_id="basic_monthly", discount_rub=0):
    prov = _FailingProvider()
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: prov)
    monkeypatch.setattr(service, "test_price_for", lambda user, amount: amount)
    db = _FakeDB()
    user = SimpleNamespace(id=7, email="t@example.com")
    pm = SimpleNamespace(
        id=3, user_id=7, deleted_at=None, provider="tbank", method_type="card",
        rebill_id="R1", customer_key="ck",
    )
    service.charge_recurrent(db, user, plan_id, pm, discount_pct=discount_pct,
                             carry_periods=carry_periods, discount_rub=discount_rub)
    return db.added[0], prov.kw


def test_first_of_two_renewals_carries_discount(monkeypatch):
    sub, kw = _charge(monkeypatch, discount_pct=25, carry_periods=1)
    full = Decimal(str(PLANS["basic_monthly"].amount))
    assert kw["amount"] == (full * Decimal("0.75")).quantize(Decimal("0.01"))
    # новая запись унесёт скидку на ещё одно продление
    assert sub.discount_pct == 25
    assert sub.discount_periods_left == 1


def test_last_discounted_renewal_resets(monkeypatch):
    sub, _ = _charge(monkeypatch, discount_pct=25, carry_periods=0)
    assert sub.discount_pct == 0
    assert sub.discount_periods_left == 0


def test_no_discount_no_carry(monkeypatch):
    sub, kw = _charge(monkeypatch, discount_pct=0, carry_periods=0)
    assert kw["amount"] == PLANS["basic_monthly"].amount
    assert sub.discount_pct == 0
    assert sub.discount_periods_left == 0


def test_yearly_month_free_charges_exact_rubles(monkeypatch):
    month = PLANS["basic_monthly"].amount
    sub, kw = _charge(monkeypatch, discount_pct=10, carry_periods=0,
                      plan_id="basic_yearly", discount_rub=month)
    year = Decimal(str(PLANS["basic_yearly"].amount))
    assert kw["amount"] == (year - Decimal(str(month))).quantize(Decimal("0.01"))
    assert sub.discount_pct == 0 and sub.discount_rub == 0


def test_ruble_discount_larger_than_price_is_ignored(monkeypatch):
    monkeypatch.setattr(service, "test_price_for", lambda user, amount: 3.0)
    prov = _FailingProvider()
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: prov)
    db = _FakeDB()
    user = SimpleNamespace(id=7, email="t@example.com")
    pm = SimpleNamespace(id=3, user_id=7, deleted_at=None, provider="tbank",
                         method_type="card", rebill_id="R1", customer_key="ck")
    service.charge_recurrent(db, user, "basic_yearly", pm, discount_pct=10, discount_rub=2900)
    assert prov.kw["amount"] == 3.0
