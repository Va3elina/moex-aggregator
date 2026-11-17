#!/usr/bin/env python3
"""Юнит-тесты бота оплаты (signals/pay_bot.py) и метки источника tg_bot. Без БД.

Запуск:
    pytest tests/test_pay_bot.py
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DB_URL", "postgresql+pg8000://ci:ci@127.0.0.1:1/ci")

from signals import pay_bot as pb  # noqa: E402
from api.billing.source import clean_source  # noqa: E402


def test_start_param_only_telegram_charset():
    assert pb.clean_start_param("p1812") == "p1812"
    assert pb.clean_start_param("  pin ") == "pin"
    assert pb.clean_start_param("") is None
    assert pb.clean_start_param(None) is None
    assert pb.clean_start_param("x" * 65) is None
    assert pb.clean_start_param("p1812<script>") is None
    assert pb.clean_start_param("пост") is None


def test_pricing_url_goes_to_site_fork_not_bank_form():
    url = pb.pricing_url("tok_123", "p1812")
    assert url.startswith("https://framedata.ru/pricing?")
    assert "src=tg_bot" in url and "tgc=tok_123" in url and "feature=p1812" in url
    assert "tinkoff" not in url and "tbank" not in url
    assert "feature" not in pb.pricing_url("tok", None)


def test_paid_text_first_vs_renewal():
    exp = datetime(2026, 12, 17, 21, 30, tzinfo=timezone.utc)  # 00:30 МСК следующего дня
    first = pb.paid_text("basic", "monthly", exp, first=True)
    assert "Доступ открыт" in first and "Basic" in first and "18.12.2026" in first
    renew = pb.paid_text("pro", "yearly", exp, first=False)
    assert "продлена" in renew and "Pro" in renew


def test_source_tg_bot_keeps_campaign():
    assert clean_source("tg_bot", "p1812") == {"source": "tg_bot", "source_feature": "p1812"}
    assert clean_source("nope", "x") == {}


def test_claim_request_rejects_garbage():
    import pytest
    from pydantic import ValidationError
    from api.routers.tg_pay import ClaimRequest

    assert ClaimRequest(token="A" * 32).token == "A" * 32
    for bad in ("short", "A" * 65, "A" * 20 + "'; DROP"):
        with pytest.raises(ValidationError):
            ClaimRequest(token=bad)
