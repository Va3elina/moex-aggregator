"""
Legacy-тир 'premium' должен резолвиться в 'pro' на всех путях гейтинга.

Контекст: premium убран из TIER_LEVELS 2026-05-20, но в проде остались
subscriptions.tier='premium' (в т.ч. у платящего yearly-клиента). До фикса
TIER_LEVELS.get('premium', 0) давал 0 — НИЖЕ free=1, а features._normalize_tier
отправлял такого юзера на free-лимиты.

Намеренно не импортирует api.billing.tiers — он тянет FastAPI-роутеры и БД.
plans.py и features.py самодостаточны.
"""
import pytest

from api.billing.plans import TIER_LEVELS, normalize_tier, tier_level
from api.billing.features import get_common_features, get_indicator_limits


def test_premium_resolves_to_pro():
    assert normalize_tier("premium") == "pro"
    assert tier_level("premium") == TIER_LEVELS["pro"]


def test_premium_is_not_below_free():
    """Регрессия на исходный баг: premium давал 0, ниже free=1."""
    assert tier_level("premium") > tier_level("free")


@pytest.mark.parametrize("value,expected", [
    ("free", "free"),
    ("basic", "basic"),
    ("pro", "pro"),
    ("admin", "admin"),
    ("guest", "guest"),
    ("PREMIUM", "pro"),      # регистр не важен
    ("user", "free"),        # legacy-роль до миграции на 'free'
    ("nonsense", "free"),    # неизвестное — безопасный минимум
    (None, "free"),
])
def test_normalize_tier(value, expected):
    assert normalize_tier(value) == expected


def test_premium_gets_pro_feature_limits():
    """Гейтинг фич, а не только уровни: premium обязан видеть pro-лимиты."""
    for indicator in ("open_interest", "seasonality", "strength", "cbr_flows"):
        assert get_indicator_limits("premium", indicator) == get_indicator_limits("pro", indicator)
    assert get_common_features("premium") == get_common_features("pro")


def test_unknown_tier_still_falls_back_to_free_limits():
    assert get_indicator_limits("nonsense", "seasonality") == get_indicator_limits("free", "seasonality")
