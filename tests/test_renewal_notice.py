"""Уведомление за 3 дня до автосписания (api/billing/renewal_notice.py)."""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from api.billing import renewal_notice as rn
from api.billing.service import apply_renewal_discount, renewal_terms
from api.services import email as email_mod

EXP = datetime(2026, 12, 10, 9, 0, tzinfo=timezone.utc)  # конец доступа
CHARGE = EXP - timedelta(hours=rn.RENEW_WINDOW_HOURS)


def _sub(**kw):
    base = dict(expires_at=EXP, plan_id="pro_yearly", discount_pct=0, discount_rub=0,
                discount_periods_left=0, fallback_done=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_charge_is_a_day_before_expiry():
    assert rn.charge_at_for(_sub()) == CHARGE


def test_reminder_window_gives_at_least_three_days():
    sub = _sub()
    # Первый час, когда напоминание «пора», — не меньше 3 дней до списания.
    assert rn.reminder_due(sub, CHARGE - timedelta(hours=rn.REMINDER_LEAD_HOURS))
    assert not rn.reminder_due(sub, CHARGE - timedelta(hours=rn.REMINDER_LEAD_HOURS + 1))
    assert rn.REMINDER_LEAD_HOURS >= 72
    # После момента списания уже не шлём.
    assert not rn.reminder_due(sub, CHARGE + timedelta(minutes=1))
    assert not rn.reminder_due(_sub(expires_at=None), CHARGE)


def test_ru_date_is_moscow_time():
    # 22:30 UTC 25 ноября = 01:30 МСК 26 ноября
    assert rn.ru_date(datetime(2026, 11, 25, 22, 30, tzinfo=timezone.utc)) == "26 ноября"


def test_discount_helper_matches_old_behaviour():
    assert apply_renewal_discount(56640.0) == 56640.0
    assert apply_renewal_discount(56640.0, discount_pct=20) == Decimal("45312.00")
    assert apply_renewal_discount(56640.0, discount_rub=4720) == Decimal("51920.00")
    # вычет больше цены → без скидки
    assert apply_renewal_discount(3.0, discount_rub=4720) == 3.0


def test_renewal_terms_monthly_fallback_drops_discount():
    plan_id, pct, rub, left = renewal_terms(_sub(fallback_done=True, discount_pct=20))
    assert plan_id == "pro_monthly" and pct == 0 and rub == 0 and left == 0


def test_email_has_amount_date_and_how_to_cancel(monkeypatch):
    sent = {}
    monkeypatch.setattr(email_mod, "_send", lambda to, s, t, h: sent.update(to=to, s=s, t=t, h=h) or True)
    ok = email_mod.send_renewal_notice_email(
        "u@example.com", "Pro", "yearly", 56640.0, "9 декабря", "10 декабря", "VISA ····4242",
    )
    assert ok
    assert "56 640 ₽" in sent["s"] and "9 декабря" in sent["s"]
    for part in ("56 640 ₽", "9 декабря", "год", "VISA ····4242", "/profile", "отключите автопродление"):
        assert part in sent["t"]
