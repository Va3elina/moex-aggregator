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
def test_yearly_terms_match_monthly_value(tier):
    monthly = retention_terms(f"{tier}_monthly")
    yearly = retention_terms(f"{tier}_yearly")
    assert yearly["period"] == "yearly"
    assert yearly["periods"] == 1
    # Годовому не 25% от года, а примерно та же сумма, что месячному.
    assert yearly["pct"] < RETENTION_PCT
    assert abs(yearly["discount_rub"] - monthly["discount_rub"]) <= PLANS[f"{tier}_yearly"].amount * 0.01


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


def _charge(monkeypatch, discount_pct, carry_periods):
    prov = _FailingProvider()
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: prov)
    monkeypatch.setattr(service, "test_price_for", lambda user, amount: amount)
    db = _FakeDB()
    user = SimpleNamespace(id=7, email="t@example.com")
    pm = SimpleNamespace(
        id=3, user_id=7, deleted_at=None, provider="tbank", method_type="card",
        rebill_id="R1", customer_key="ck",
    )
    service.charge_recurrent(db, user, "basic_monthly", pm,
                             discount_pct=discount_pct, carry_periods=carry_periods)
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
