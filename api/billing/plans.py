"""
Справочник тарифных планов. Источник истины для цен и периодов.

Структура:
  tier      — уровень ("free" / "basic" / "pro" / "premium") — для иерархии доступа
  plan      — период ("monthly" / "yearly") — для расчёта commission-фри периодов
  plan_id   — уникальная связка "{tier}_{plan}" (напр. "pro_monthly")

Frontend берёт отсюда же цены через GET /api/billing/plans.
Меняется цена? → правишь здесь, фронт подхватывает автоматически.
"""
from dataclasses import dataclass


# Уровни доступа (tier). Каждый следующий включает всё предыдущее.
# Используется в api/billing/tiers.py для проверок вида "пользователь >= pro".
TIER_LEVELS: dict[str, int] = {
    "guest": 0,     # не залогинен
    "free": 1,      # залогинен без подписки
    "basic": 2,
    "pro": 3,
    "premium": 4,
    "admin": 99,    # роль из users.role — обходит все проверки
}


@dataclass(frozen=True)
class Plan:
    """Конкретная SKU — комбинация tier + period."""
    plan_id: str              # 'basic_monthly' / 'pro_yearly' / ...
    tier: str                 # 'basic' / 'pro' / 'premium'
    period: str               # 'monthly' / 'yearly'
    title: str                # "Pro — месяц"
    description: str
    amount: float             # сумма в рублях
    duration_days: int        # 30 / 365
    badge: str | None = None  # "Скидка 17%" / "Самое популярное" / None


# Базовые цены (месячные). Годовые = 10×месячных (экономия 2 мес).
BASIC_MONTHLY = 299.00
PRO_MONTHLY = 799.00
PREMIUM_MONTHLY = 1999.00


def _make_pair(tier: str, title_base: str, monthly_price: float, popular: bool = False) -> list[Plan]:
    """Создаёт пару monthly + yearly для одного tier'а. Годовая = 10×месячных."""
    yearly_price = round(monthly_price * 10, 2)  # скидка ~17% = 2 месяца бесплатно
    return [
        Plan(
            plan_id=f"{tier}_monthly",
            tier=tier,
            period="monthly",
            title=f"{title_base} — месяц",
            description="Доступ на 30 дней",
            amount=monthly_price,
            duration_days=30,
            badge="Самый популярный" if popular else None,
        ),
        Plan(
            plan_id=f"{tier}_yearly",
            tier=tier,
            period="yearly",
            title=f"{title_base} — год",
            description=f"Доступ на 365 дней. Экономия {round(monthly_price * 2, 0):.0f} ₽",
            amount=yearly_price,
            duration_days=365,
            badge="Выгодно −2 мес.",
        ),
    ]


# Все платные SKU
PLANS: dict[str, Plan] = {
    p.plan_id: p
    for p in [
        *_make_pair("basic", "Basic", BASIC_MONTHLY),
        *_make_pair("pro", "Pro", PRO_MONTHLY, popular=True),
        *_make_pair("premium", "Premium", PREMIUM_MONTHLY),
    ]
}


def get_plan(plan_id: str) -> Plan | None:
    """Вернёт Plan по id ('pro_monthly' и т.д.) или None."""
    return PLANS.get(plan_id)


def list_public_plans() -> list[Plan]:
    """Все планы для Pricing-страницы (в порядке tier → period)."""
    return list(PLANS.values())


def tiers_grouped() -> list[dict]:
    """
    Структура для UI: сгруппировано по tier'ам.
    Каждый tier отдаёт свои monthly и yearly варианты.
    Плюс фиктивный 'free' без цены — для показа на Pricing.
    """
    result = [
        {
            "tier": "free",
            "title": "Free",
            "description": "Базовый доступ к сайту",
            "monthly": None,
            "yearly": None,
            "is_current_default": True,
        }
    ]
    for tier in ("basic", "pro", "premium"):
        monthly = PLANS.get(f"{tier}_monthly")
        yearly = PLANS.get(f"{tier}_yearly")
        if not monthly:
            continue
        result.append({
            "tier": tier,
            "title": monthly.title.split(" — ")[0],  # "Pro"
            "description": f"Расширенный доступ на уровне {tier.title()}",
            "monthly": {
                "plan_id": monthly.plan_id,
                "amount": monthly.amount,
                "duration_days": monthly.duration_days,
                "badge": monthly.badge,
            },
            "yearly": {
                "plan_id": yearly.plan_id,
                "amount": yearly.amount,
                "duration_days": yearly.duration_days,
                "badge": yearly.badge,
            } if yearly else None,
        })
    return result
