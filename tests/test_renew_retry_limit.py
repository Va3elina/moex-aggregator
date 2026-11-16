"""Лимит повторов продления: ≤3 попытки, пауза ≥23ч (api/billing/service.py)."""
import os

# api.database создаёт engine при импорте; к БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")  # файл не создаётся: соединений нет

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from api.billing.service import (
    RENEW_MAX_ATTEMPTS,
    RENEW_RETRY_GAP_HOURS,
    _retry_blocked,
)

T0 = datetime(2026, 11, 14, 6, 0, tzinfo=timezone.utc)  # конец доступа


def _simulate(start_offset_h: int, end_offset_h: int, skip_hours=()) -> list[datetime]:
    """Часовой крон по окну [expires+start, expires+end), банк всегда отказывает."""
    attempts: list[SimpleNamespace] = []
    for h in range(start_offset_h, end_offset_h):
        if h in skip_hours:
            continue  # крон не запускался
        now = T0 + timedelta(hours=h, seconds=0.5)
        if _retry_blocked(attempts, now) is None:
            attempts.append(SimpleNamespace(created_at=now))
    return [a.created_at for a in attempts]


def test_paid_renewal_full_window_makes_three_attempts():
    # Окно крона: за 24ч до конца и 48ч после — раньше это было ~72 списания.
    got = _simulate(-24, 48)
    assert len(got) == RENEW_MAX_ATTEMPTS
    gaps = [b - a for a, b in zip(got, got[1:])]
    assert all(g >= timedelta(hours=RENEW_RETRY_GAP_HOURS) for g in gaps)


def test_trial_starts_at_expiry_still_three_attempts():
    # Триал списывается только после конца — 3 попытки должны влезть в 48ч grace.
    assert len(_simulate(0, 48)) == RENEW_MAX_ATTEMPTS


def test_cron_gaps_do_not_add_attempts():
    got = _simulate(-24, 48, skip_hours={-1, 0, 1, 22, 23})
    assert len(got) <= RENEW_MAX_ATTEMPTS


def test_first_attempt_not_blocked():
    assert _retry_blocked([], T0) is None


def test_naive_created_at_treated_as_utc():
    a = [SimpleNamespace(created_at=(T0 - timedelta(hours=1)).replace(tzinfo=None))]
    assert _retry_blocked(a, T0) is not None
    a = [SimpleNamespace(created_at=(T0 - timedelta(hours=24)).replace(tzinfo=None))]
    assert _retry_blocked(a, T0) is None


def test_limit_blocks_fourth():
    a = [SimpleNamespace(created_at=T0 - timedelta(days=d)) for d in (3, 2, 1)]
    assert _retry_blocked(a, T0 + timedelta(days=5)) is not None
