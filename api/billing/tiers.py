"""
Проверки уровня доступа (tier) + FastAPI dependencies.

Главная концепция:
  - Каждый user имеет tier (из users.role)
  - Каждый endpoint требует минимальный tier
  - tier_level(required) <= tier_level(user) → доступ есть

В будущем, когда появятся feature flags (timeframe:5min / indicator:buffett / ...),
это расширится до per-feature проверок. Сейчас — только уровни.

Используется так:

    from api.billing.tiers import require_tier

    @router.get("/api/oi/5min")
    async def oi_5min(user = Depends(require_tier("pro"))):
        # сюда не попадут user'ы с tier < pro
        ...
"""
from fastapi import Depends, HTTPException, status

from api.models.user import User
from api.routers.auth import get_current_user, get_current_user_optional
from api.billing.plans import TIER_LEVELS, normalize_tier, tier_level


def user_tier(user: User | None) -> str:
    """
    Определяет tier пользователя.
    None → 'guest', иначе users.role ('free' / 'basic' / 'pro' / 'admin').
    Legacy-роли резолвятся через normalize_tier: 'user' → 'free',
    'premium' → 'pro' (см. LEGACY_TIER_ALIASES в plans.py).
    """
    if user is None:
        return "guest"
    return normalize_tier(user.role)


def has_tier(user: User | None, min_tier: str) -> bool:
    """True если tier пользователя ≥ min_tier."""
    return tier_level(user_tier(user)) >= tier_level(min_tier)


# ═══════════════════════════════════════════════════════════════════════════════
#  FastAPI dependencies
# ═══════════════════════════════════════════════════════════════════════════════

def require_tier(min_tier: str):
    """
    Dependency factory. Возвращает dependency, которая требует минимальный tier.

    Пример использования:
        @router.get("/api/premium-thing")
        async def premium(user = Depends(require_tier("premium"))):
            ...

    Гостям (не залогинены) возвращает 401, залогиненным но без tier — 403.
    """
    if min_tier not in TIER_LEVELS:
        raise ValueError(f"Unknown tier: {min_tier}")

    async def _check(user: User = Depends(get_current_user)) -> User:
        if not has_tier(user, min_tier):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Требуется тариф {min_tier} или выше",
            )
        return user

    return _check


def optional_tier(user: User | None = Depends(get_current_user_optional)) -> tuple[User | None, str]:
    """
    Dependency для endpoint'ов, которые работают для всех, но дают разные данные
    в зависимости от tier'а (например, больше истории для Pro, real-time для Premium).

    Возвращает (user, tier).
    """
    return user, user_tier(user)
