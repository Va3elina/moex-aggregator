#!/usr/bin/env python3
"""Юнит-тесты дожимов бота оплаты (signals/pay_bot_nudges.py). Без БД.

Запуск:
    pytest tests/test_pay_bot_nudges.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DB_URL", "postgresql+pg8000://ci:ci@127.0.0.1:1/ci")

from signals import pay_bot_nudges as nd  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)  # 15:00 МСК


def test_quiet_hours_msk():
    assert not nd.in_quiet_hours(NOW)
    assert nd.in_quiet_hours(datetime(2026, 11, 18, 19, 0, tzinfo=timezone.utc))   # 22:00 МСК
    assert nd.in_quiet_hours(datetime(2026, 11, 18, 5, 59, tzinfo=timezone.utc))   # 08:59 МСК
    assert not nd.in_quiet_hours(datetime(2026, 11, 18, 6, 0, tzinfo=timezone.utc))  # 09:00 МСК


def test_pick_one_per_lead_by_priority():
    cands = [
        {"chat_id": 1, "code": "n2_nopay"},
        {"chat_id": 1, "code": "n5_renew_fail"},
        {"chat_id": 2, "code": "n1_noclaim"},
    ]
    got = nd.pick(cands, {}, NOW)
    assert [(c["chat_id"], c["code"]) for c in got] == [(1, "n5_renew_fail"), (2, "n1_noclaim")]


def test_pick_respects_week_cap_and_daily_gap():
    cands = [{"chat_id": 1, "code": "n2_nopay"}, {"chat_id": 2, "code": "n2_nopay"},
             {"chat_id": 3, "code": "n2_nopay"}]
    hist = {
        1: [NOW - timedelta(days=d) for d in (2, 3, 4)],     # 3 за неделю: лимит
        2: [NOW - timedelta(hours=5)],                        # меньше суток назад
        3: [NOW - timedelta(days=d) for d in (2, 8, 9, 10)],  # старые не считаются
    }
    assert [c["chat_id"] for c in nd.pick(cands, hist, NOW)] == [3]


def test_messages_have_button_and_no_bank_link():
    exp = NOW + timedelta(days=2)
    for code in nd.PRIORITY:
        msg, kb = nd.message({"code": code, "tier": "basic", "expires_at": exp,
                              "url": "https://framedata.ru/pricing?src=tg_bot"})
        assert msg and kb and kb[0][0]["url"].startswith("https://framedata.ru/")
    msg, _ = nd.message({"code": "n4_expiring", "tier": "pro", "expires_at": exp, "url": "u"})
    assert "Pro" in msg and "20.11.2026" in msg
