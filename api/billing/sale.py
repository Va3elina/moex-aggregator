"""
Правила покупки поверх действующей подписки: акция, потолок оплаченного срока,
пересчёт остатка при повышении тарифа.

Чистые функции без БД — их зовёт service.py и покрывают тесты.

АКЦИЯ (выключена, пока не заданы env; код менять не нужно):
  BILLING_SALE_CODE      — метка акции, пишется в subscriptions.promo_code ('bf2026')
  BILLING_SALE_START     — начало, ISO; без пояса — московское время
  BILLING_SALE_END       — конец (не включая), ISO
  BILLING_SALE_TITLE     — подпись на витрине («Чёрная пятница»)
  BILLING_SALE_PCT_BASIC — скидка на годовой Basic, % (1..90)
  BILLING_SALE_PCT_PRO   — скидка на годовой Pro, %
  BILLING_SALE_PCT       — скидка для тарифа, у которого своей не задано
  BILLING_SALE_GIFT_DAYS_BASIC / _PRO / BILLING_SALE_GIFT_DAYS
                         — подарочные дни сверх года (0 — без подарка)
  BILLING_SALE_MONTHLY   — '1' оставить месячные планы в продаже по обычной цене;
                           по умолчанию на время акции купить можно только год

Скидка считается от цены 12 месяцев по месячному тарифу (Basic 2 900 × 12 =
34 800 ₽), как на витрине: −30% → 24 360 ₽. Обычная годовая цена (27 840 ₽,
она уже на 20% ниже) остаётся ценой продления.

Правила акции:
  - скидка только на годовые планы;
  - один аккаунт покупает по акции один раз (любой тариф);
  - действующий подписчик участвует: купленный год прибавляется к оплаченному
    сроку, запрет «этот план уже активен» для него снимается;
  - скидка действует на первую оплату, продление через год — по обычной
    годовой цене.
"""
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from api.billing.plans import PLANS, TIER_LEVELS

MSK = timezone(timedelta(hours=3))

# Потолок оплаченного срока: после покупки доступ не может быть оплачен
# дальше, чем на 3 года вперёд (любые покупки, включая акционные).
MAX_PAID_AHEAD_DAYS = 3 * 365

# Повышение тарифа: сколько дней младшего тарифа равны одному дню старшего.
# Basic 2 900 ₽/мес, Pro 5 900 ₽/мес — соотношение цен 2,03, в оферте «2 к 1».
UPGRADE_DAYS_PER_DAY: dict[tuple[str, str], int] = {
    ("basic", "pro"): 2,
}

SALE_TIERS = ("basic", "pro")


@dataclass(frozen=True)
class Sale:
    code: str
    start: datetime
    end: datetime
    title: str
    monthly_available: bool
    pct_by_tier: dict = field(default_factory=dict)        # {'basic': 30, 'pro': 45}
    gift_days_by_tier: dict = field(default_factory=dict)  # {'basic': 30, 'pro': 30}

    def pct(self, tier: str) -> int:
        return self.pct_by_tier.get(tier, 0)

    def gift_days(self, tier: str) -> int:
        return self.gift_days_by_tier.get(tier, 0)

    @property
    def max_pct(self) -> int:
        return max(self.pct_by_tier.values(), default=0)


def _parse_dt(raw: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=MSK)


def _int_env(*names: str) -> int | None:
    """Первое заданное из env-имён как целое; None — ни одно не задано или мусор."""
    for name in names:
        raw = os.getenv(name, "").strip()
        if raw:
            try:
                return int(raw)
            except ValueError:
                return None
    return None


def configured_sale() -> Sale | None:
    """Акция из env или None, если она не задана либо задана с ошибкой.

    Ошибка в настройке = акции нет: лучше продать по обычной цене, чем по
    случайной.
    """
    code = os.getenv("BILLING_SALE_CODE", "").strip()
    start = _parse_dt(os.getenv("BILLING_SALE_START", ""))
    end = _parse_dt(os.getenv("BILLING_SALE_END", ""))
    if not code or start is None or end is None or end <= start:
        return None
    pct_by_tier, gift_by_tier = {}, {}
    for tier in SALE_TIERS:
        pct = _int_env(f"BILLING_SALE_PCT_{tier.upper()}", "BILLING_SALE_PCT")
        if pct is not None and 1 <= pct <= 90:
            pct_by_tier[tier] = pct
        gift = _int_env(f"BILLING_SALE_GIFT_DAYS_{tier.upper()}", "BILLING_SALE_GIFT_DAYS")
        if gift is not None and 0 < gift <= 366:
            gift_by_tier[tier] = gift
    if not pct_by_tier:
        return None
    return Sale(
        code=code[:32],
        start=start,
        end=end,
        title=os.getenv("BILLING_SALE_TITLE", "").strip() or "Акция",
        monthly_available=os.getenv("BILLING_SALE_MONTHLY", "").strip() == "1",
        pct_by_tier=pct_by_tier,
        gift_days_by_tier=gift_by_tier,
    )


def active_sale(now: datetime | None = None) -> Sale | None:
    """Акция, которая идёт прямо сейчас."""
    sale = configured_sale()
    if sale is None:
        return None
    now = now or datetime.now(timezone.utc)
    return sale if sale.start <= now < sale.end else None


def sale_by_code(code: str | None) -> Sale | None:
    """Настроенная акция с этой меткой — и после её окончания: форму, открытую
    в последний час акции, клиент может оплатить уже после, подарок остаётся."""
    sale = configured_sale()
    return sale if (sale is not None and code and sale.code == code) else None


def is_sale_plan(plan, sale: Sale | None = None) -> bool:
    """Скидка действует только на годовые планы (и только на тарифы с заданным %)."""
    if getattr(plan, "period", None) != "yearly":
        return False
    return sale is None or sale.pct(plan.tier) > 0


def list_price_12m(plan) -> float:
    """Цена 12 месяцев по месячному тарифу — от неё считается скидка акции."""
    monthly = PLANS.get(f"{plan.tier}_monthly")
    return float(monthly.amount * 12) if monthly else float(plan.amount)


def sale_price(plan, sale: Sale) -> float:
    """Цена годового плана по акции, до целого рубля (чек 54-ФЗ бьётся на неё).

    Никогда не выше обычной годовой цены: при маленькой скидке (меньше 20%)
    акция не должна сделать год дороже, чем без неё.
    """
    price = float(round(list_price_12m(plan) * (100 - sale.pct(plan.tier)) / 100))
    return min(price, float(plan.amount))


def upgrade_ratio(from_tier: str, to_tier: str) -> int | None:
    """Сколько дней `from_tier` дают один день `to_tier`; None — пересчёта нет."""
    return UPGRADE_DAYS_PER_DAY.get((from_tier, to_tier))


def bonus_days(from_tier: str, to_tier: str, left: timedelta) -> int:
    """Дополнительные дни нового тарифа за неиспользованный остаток старого.

    Округляем вверх, в пользу клиента: 1 оставшийся день Basic — это 1 день Pro,
    а не 0.
    """
    ratio = upgrade_ratio(from_tier, to_tier)
    seconds = left.total_seconds()
    if not ratio or seconds <= 0:
        return 0
    return math.ceil(seconds / 86400 / ratio)


def same_or_higher_tiers(tier: str) -> list[str]:
    """Платные тарифы не ниже `tier`: их оплаченный срок новая подписка не
    перекрывает, а продолжает (Basic, купленный при действующем Pro, начнётся
    после Pro)."""
    level = TIER_LEVELS.get(tier, 0)
    return [t for t in SALE_TIERS if TIER_LEVELS.get(t, 0) >= level]


def exceeds_paid_ahead_cap(now: datetime, new_expires: datetime) -> bool:
    return new_expires > now + timedelta(days=MAX_PAID_AHEAD_DAYS)
