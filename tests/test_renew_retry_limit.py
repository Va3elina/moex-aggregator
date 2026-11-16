"""Лимит повторов продления: ≤3 попытки, пауза ≥23ч (api/billing/service.py)."""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")  # файл не создаётся: соединений нет

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from api.billing.service import (
    RENEW_GRACE_HOURS,
    RENEW_MAX_ATTEMPTS,
    RENEW_RETRY_GAP_HOURS,
    _retry_blocked,
    next_renew_attempt_at,
)

T0 = datetime(2026, 11, 14, 6, 0, tzinfo=timezone.utc)  # конец доступа


def _sub(fallback_done=False, period="monthly"):
    return SimpleNamespace(expires_at=T0, fallback_done=fallback_done, period=period)


def _simulate(start_offset_h, end_offset_h, skip_hours=(), sub=None, periods=None):
    """Часовой крон по окну [expires+start, expires+end), банк всегда отказывает."""
    sub = sub or _sub()
    attempts: list[SimpleNamespace] = []
    for h in range(start_offset_h, end_offset_h):
        if h in skip_hours:
            continue  # крон не запускался
        now = T0 + timedelta(hours=h, seconds=0.5)
        if _retry_blocked(sub, attempts, now) is None:
            period = periods[len(attempts)] if periods else "monthly"
            attempts.append(SimpleNamespace(created_at=now, period=period))
    return [a.created_at for a in attempts]


def test_paid_renewal_full_window_makes_three_attempts():
    # Окно крона: за 24ч до конца и grace после — раньше это было ~72 списания.
    got = _simulate(-24, RENEW_GRACE_HOURS)
    assert len(got) == RENEW_MAX_ATTEMPTS
    gaps = [b - a for a, b in zip(got, got[1:])]
    assert all(g >= timedelta(hours=RENEW_RETRY_GAP_HOURS) for g in gaps)


def test_trial_starts_at_expiry_three_attempts_even_with_cron_gaps():
    # Триал списывается только после конца; с окном 72ч третья попытка
    # переживает несколько пропущенных запусков крона.
    assert len(_simulate(0, RENEW_GRACE_HOURS)) == RENEW_MAX_ATTEMPTS
    got = _simulate(0, RENEW_GRACE_HOURS, skip_hours={0, 1, 2, 24, 25, 26, 27})
    assert len(got) == RENEW_MAX_ATTEMPTS


def test_cron_gaps_do_not_add_attempts():
    got = _simulate(-24, RENEW_GRACE_HOURS, skip_hours={-1, 0, 1, 22, 23})
    assert len(got) <= RENEW_MAX_ATTEMPTS


def test_yearly_nsf_then_monthly_next_hour():
    # card-1: годовой отказал по NSF → fallback_done; месячный — на следующем
    # часе, третья попытка — снова через ≥23ч. Всего 3.
    got = _simulate(-24, RENEW_GRACE_HOURS, sub=_sub(fallback_done=True, period="yearly"),
                    periods=["yearly", "monthly", "monthly"])
    assert len(got) == RENEW_MAX_ATTEMPTS
    assert got[1] - got[0] == timedelta(hours=1)
    assert got[2] - got[1] >= timedelta(hours=RENEW_RETRY_GAP_HOURS)


def test_first_attempt_not_blocked():
    assert _retry_blocked(_sub(), [], T0) is None


def test_naive_created_at_treated_as_utc():
    a = [SimpleNamespace(created_at=(T0 - timedelta(hours=1)).replace(tzinfo=None))]
    assert _retry_blocked(_sub(), a, T0) is not None
    a = [SimpleNamespace(created_at=(T0 - timedelta(hours=24)).replace(tzinfo=None))]
    assert _retry_blocked(_sub(), a, T0) is None


def test_limit_blocks_fourth():
    a = [SimpleNamespace(created_at=T0 - timedelta(days=d)) for d in (3, 2, 1)]
    assert _retry_blocked(_sub(), a, T0 + timedelta(days=5)) is not None
    assert next_renew_attempt_at(_sub(), a) is None


def test_next_attempt_none_when_outside_window():
    # Попытка в самом конце окна: следующая не влезет → «больше не пробуем».
    a = [SimpleNamespace(created_at=T0 + timedelta(hours=RENEW_GRACE_HOURS - 2))]
    assert next_renew_attempt_at(_sub(), a) is None
    a = [SimpleNamespace(created_at=T0)]
    assert next_renew_attempt_at(_sub(), a) == T0 + timedelta(hours=RENEW_RETRY_GAP_HOURS)


# ── renewal_attempts: сам запрос к базе (sqlite в памяти) ─────────────────────

@pytest.fixture()
def db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from api.models.subscription import Subscription

    engine = create_engine("sqlite:///:memory:")
    Subscription.__table__.create(engine)
    s = sessionmaker(bind=engine)()
    yield s
    s.close()


def _row(db, created_h, status, *, user=1, tier="pro", pm=10, period="monthly"):
    from api.models.subscription import Subscription
    r = Subscription(
        user_id=user, tier=tier, period=period, plan_id=f"{tier}_{period}",
        amount=100, status=status, payment_method_id=pm,
        created_at=T0 + timedelta(hours=created_h),
        expires_at=T0 if created_h < -100 else None,
    )
    db.add(r)
    db.flush()
    return r


def test_renewal_attempts_counts_only_own_failed_retries(db):
    from api.billing.service import renewal_attempts

    base = _row(db, -720, "active")                 # продлеваемая подписка
    _row(db, -800, "failed")                        # прошлый цикл, до base — не считаем
    a1 = _row(db, -24, "failed")
    a2 = _row(db, -1, "cancelled")                  # отказ, потом webhook отмены
    _row(db, 2, "failed", tier="basic")             # другой тариф
    _row(db, 3, "failed", pm=11)                    # другая карта — свои попытки
    _row(db, 4, "failed", user=2)                   # другой клиент
    _row(db, 5, "pending")                          # в обработке — не отказ
    _row(db, 6, "failed", pm=None)                  # брошенный чекаут без карты
    db.commit()

    got = renewal_attempts(db, base)
    assert [r.id for r in got] == [a1.id, a2.id]


def test_renewal_attempts_empty_without_card(db):
    from api.billing.service import renewal_attempts

    base = _row(db, -720, "active", pm=None)
    _row(db, -1, "failed", pm=None)
    db.commit()
    assert renewal_attempts(db, base) == []
