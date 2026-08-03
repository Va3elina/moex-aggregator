"""
Справочник тарифных планов. Источник истины для цен и периодов.

Структура:
  tier      — уровень ("free" / "basic" / "pro" / "premium") — для иерархии доступа
  plan      — период ("monthly" / "yearly") — для расчёта commission-фри периодов
  plan_id   — уникальная связка "{tier}_{plan}" (напр. "pro_monthly")

Frontend берёт отсюда же цены через GET /api/billing/plans.
Меняется цена? → правишь здесь, фронт подхватывает автоматически.
"""
import os
from dataclasses import dataclass


# Уровни доступа (tier). Каждый следующий включает всё предыдущее.
# Используется в api/billing/tiers.py для проверок вида "пользователь >= pro".
#
# Premium убран 2026-05-20 — спецификация тарифов сжата до 3 уровней
# (Free / Basic / Pro). Все бывшие Premium фичи перенесены в Pro.
TIER_LEVELS: dict[str, int] = {
    "guest": 0,     # не залогинен
    "free": 1,      # залогинен без подписки
    "basic": 2,
    "pro": 3,
    "admin": 99,    # роль из users.role — обходит все проверки
}


# Legacy-тиры, которых больше нет в TIER_LEVELS, но которые остались в проде
# (subscriptions.tier / users.role, выданные до 2026-05-20).
#
# Без этого маппинга TIER_LEVELS.get('premium', 0) → 0, что НИЖЕ free=1, а
# features._normalize_tier('premium') → 'free': платящий premium-клиент получил
# бы доступ уровня бесплатного. Premium-фичи были перенесены в Pro, поэтому
# правильный резолв — premium → pro.
#
# Новые premium не появляются: create_invites (invites.py) и PLANS ограничены
# basic/pro. Это исключительно совместимость со старыми строками.
LEGACY_TIER_ALIASES: dict[str, str] = {
    "premium": "pro",
}


def normalize_tier(tier: str | None) -> str:
    """Канонический tier: legacy-алиасы резолвятся, неизвестное → 'free'.

    Единая точка входа для всего, что читает tier из БД (subscriptions.tier,
    users.role). 'guest'/'admin' проходят как есть — они валидные уровни.
    """
    t = (tier or "free").lower()
    t = LEGACY_TIER_ALIASES.get(t, t)
    return t if t in TIER_LEVELS else "free"


def tier_level(tier: str | None) -> int:
    """Числовой уровень tier'а с учётом legacy-алиасов.

    Используй ВМЕСТО TIER_LEVELS.get(tier, 0) везде, где tier приходит из БД:
    голый .get() даёт 0 (ниже free) для legacy-значений вроде 'premium'.
    """
    return TIER_LEVELS[normalize_tier(tier)]


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


# Базовые цены (месячные). Годовые = 9.6×месячных (экономия 20%).
#
# Финальные цены из спецификации тарифов 2026-05-20:
#   Basic — основной платный, realtime, почти весь функционал
#   Pro   — полный функционал + API + Excel/CSV + TradingView/Т-терминал
# Цена basic env-управляема для прод-тестов рекуррента (списание малой суммой
# вместо 2900₽). По умолчанию 2900 → поведение прода без изменений. Для теста:
# BASIC_MONTHLY_OVERRIDE=30 в .env api И orchestrator → recreate. Откат — убрать
# env (без деплоя). Затрагивает цену в /plans, checkout и конверсию триала.
BASIC_MONTHLY = float(os.getenv("BASIC_MONTHLY_OVERRIDE") or 2900.00)
PRO_MONTHLY = 5900.00


# === Бесплатный пробный период (free trial) ===
# Длительность по tier'ам (решение session 2026-06-23): Basic 14 дней, Pro 7.
TRIAL_DAYS: dict[str, int] = {
    "basic": 14,
    "pro": 7,
}

# Привязочный платёж для получения rebill_id (T-Bank выдаёт RebillId только при
# реальном CONFIRMED-платеже с Recurrent='Y'; возвращаем сразу после привязки).
TRIAL_BIND_AMOUNT = 1.00

# Версия текста согласия на автосписание — фиксируем в subscriptions при старте
# триала (доказательство акцепта при споре: ГК ст.438, ЗоЗПП ст.10).
TRIAL_CONSENT_VERSION = "trial-v1"


def trial_days(tier: str) -> int | None:
    """Длительность триала для tier ('basic'→14, 'pro'→7) или None если триала нет."""
    return TRIAL_DAYS.get(tier)


def _make_pair(tier: str, title_base: str, monthly_price: float, popular: bool = False) -> list[Plan]:
    """Создаёт пару monthly + yearly для одного tier'а. Годовая = 9.6×месячных (скидка 20%)."""
    # 20% скидка относительно (monthly × 12). Округление до 10 ₽ для эстетики.
    yearly_price_raw = monthly_price * 12 * 0.8
    yearly_price = round(yearly_price_raw / 10) * 10
    yearly_savings = round(monthly_price * 12 - yearly_price)
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
            description=f"Доступ на 365 дней. Экономия {yearly_savings} ₽",
            amount=yearly_price,
            duration_days=365,
            badge="Скидка 20%",
        ),
    ]


# Все платные SKU (Premium убран 2026-05-20)
PLANS: dict[str, Plan] = {
    p.plan_id: p
    for p in [
        *_make_pair("basic", "Basic", BASIC_MONTHLY),
        *_make_pair("pro", "Pro", PRO_MONTHLY, popular=True),
    ]
}


def get_plan(plan_id: str) -> Plan | None:
    """Вернёт Plan по id ('pro_monthly' и т.д.) или None."""
    return PLANS.get(plan_id)


def monthly_fallback(plan_id: str) -> Plan | None:
    """Для ГОДОВОГО плана вернуть МЕСЯЧНЫЙ того же tier (card-1 fallback при NSF).
    Для не-годовых планов, отсутствующих планов или tier'ов без месячной пары
    (напр. устаревший premium) → None (тогда fallback не делаем)."""
    p = PLANS.get(plan_id)
    if not p or p.period != "yearly":
        return None
    return PLANS.get(f"{p.tier}_monthly")


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
    for tier in ("basic", "pro"):
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
