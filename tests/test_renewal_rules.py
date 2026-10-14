"""Правила продления без автосписания (api/billing/renewal_rules.py)."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from api.billing.renewal_rules import renewal_start, will_auto_renew

NOW = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


def sub(id=1, tier="basic", status="active", pm=None, cancelled=None, expires_in_days=3, trial=False):
    return SimpleNamespace(
        id=id, tier=tier, status=status, payment_method_id=pm, cancelled_at=cancelled,
        expires_at=NOW + timedelta(days=expires_in_days), is_trial=trial,
    )


def test_will_auto_renew():
    assert will_auto_renew(sub(pm=7))
    assert not will_auto_renew(sub(pm=None))               # СБП на форме банка, без привязки
    assert not will_auto_renew(sub(pm=7, cancelled=NOW))   # сам выключил автопродление


def test_early_renewal_keeps_remaining_days():
    old = sub(id=1, pm=None, expires_in_days=3)
    assert renewal_start(NOW, [old], "basic", exclude_id=2) == old.expires_at


def test_cancelled_autorenew_also_extends():
    old = sub(id=1, pm=7, cancelled=NOW, expires_in_days=10)
    assert renewal_start(NOW, [old], "basic", exclude_id=2) == old.expires_at


def test_cron_renewal_unchanged():
    # Подписка с автопродлением: крон продлевает за сутки, период как раньше с now.
    old = sub(id=1, pm=7, expires_in_days=1)
    assert renewal_start(NOW, [old], "basic", exclude_id=2) == NOW


def test_other_tier_trial_expired_and_self_ignored():
    subs = [
        sub(id=1, tier="pro", pm=None, expires_in_days=20),        # апгрейд/даунгрейд не стыкуем
        sub(id=3, trial=True, pm=None, expires_in_days=20),        # триал не стыкуем
        sub(id=4, status="expired", pm=None, expires_in_days=20),  # не активна
        sub(id=2, pm=None, expires_in_days=40),                    # сама новая подписка
    ]
    assert renewal_start(NOW, subs, "basic", exclude_id=2) == NOW


def test_latest_expiry_wins():
    a = sub(id=1, pm=None, expires_in_days=3)
    b = sub(id=5, pm=None, expires_in_days=33)
    assert renewal_start(NOW, [a, b], "basic", exclude_id=2) == b.expires_at
