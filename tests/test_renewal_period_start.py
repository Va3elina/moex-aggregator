"""Срок продлённой подписки считается от конца оплаченного периода (api/billing/service.py)."""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

from datetime import datetime, timedelta, timezone

from api.billing.service import renewal_period_start

NOW = datetime(2026, 11, 18, 16, 0, tzinfo=timezone.utc)


def test_early_renewal_starts_from_previous_expiry():
    # Списали за 23ч до конца — новый период с конца старого, а не с момента оплаты.
    prev = NOW + timedelta(hours=23)
    assert renewal_period_start(NOW, [prev]) == prev


def test_no_previous_period_starts_now():
    assert renewal_period_start(NOW, []) == NOW
    assert renewal_period_start(NOW, [None]) == NOW


def test_expired_previous_period_starts_now():
    # Оплата прошла после конца доступа — задним числом не компенсируем.
    assert renewal_period_start(NOW, [NOW - timedelta(hours=10)]) == NOW


def test_latest_of_several_previous_periods():
    a, b = NOW + timedelta(days=1), NOW + timedelta(days=5)
    assert renewal_period_start(NOW, [a, b, None]) == b


def test_naive_expiry_treated_as_utc():
    prev = (NOW + timedelta(hours=23)).replace(tzinfo=None)
    assert renewal_period_start(NOW, [prev]) == NOW + timedelta(hours=23)
