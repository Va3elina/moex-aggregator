"""
Контроль доступа: tier-aware ограничения для роутеров.

Главная функция: `enforce_tier_limits(user, indicator, **params)`.
`enforce_guest_limits()` оставлен для обратной совместимости (legacy роутеры).

Использование в роутере:

    from api.security.access_control import enforce_tier_limits

    @router.get("/api/openinterest/{sectype}")
    async def get_oi(sectype: str, interval: int, ..., user = Depends(get_current_user_optional)):
        enforce_tier_limits(
            user, "open_interest",
            asset=sectype, interval=interval, period=period,
        )
        ...

Что проверяется:
  - asset / ticker — против whitelist'а (если задан в features matrix)
  - interval — против allowed_intervals
  - period / days — против max_history_days
  - mode — против allowed_modes / modes
  - universe — против universes (strength)
  - timeframe — против allowed_timeframes (funds)
  - clgroup — против clgroups (OI)

При нарушении — HTTPException 403 с человеческим сообщением.

Backend **никогда** не доверяет фронту. Все правила enforced здесь.
"""
from typing import Optional, Any
from datetime import date, timedelta

from fastapi import HTTPException, status

from api.billing.features import get_indicator_limits
from api.billing.tiers import user_tier


# Порядок периодов для сравнения "период ≤ max_history_days"
PERIOD_DAYS = {
    "1d": 1, "1w": 7, "1m": 30, "3m": 90, "6m": 180,
    "1y": 365, "2y": 730, "3y": 1095, "5y": 1825,
    "10y": 3650, "20y": 7300, "all": 99999,
}


def _forbid(msg: str) -> None:
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=msg)


def enforce_tier_limits(
    user,
    indicator: str,
    *,
    asset: Optional[str] = None,
    interval: Optional[int] = None,
    period: Optional[str] = None,
    days: Optional[int] = None,
    mode: Optional[str] = None,
    universe: Optional[str] = None,
    timeframe: Optional[str] = None,
    clgroup: Optional[str] = None,
) -> dict:
    """Проверить ограничения tier'а для запроса к indicator'у.

    Возвращает **активные лимиты** — роутер может использовать чтобы, например,
    подменить `end_date = today() - data_delay_hours` (см. get_effective_end_date).

    Все параметры опциональны — проверяются только переданные.

    Raises HTTPException(403) при нарушении.
    """
    tier = user_tier(user)
    limits = get_indicator_limits(tier, indicator)

    if not limits:
        # Indicator без матрицы — открыт для всех (e.g. funds_catalog)
        return {}

    # ── Asset whitelist (OI: assets_whitelist, funds_money: tickers_whitelist) ──
    if asset is not None:
        whitelist = limits.get("assets_whitelist") or limits.get("tickers_whitelist")
        if whitelist is not None and asset not in whitelist:
            _forbid(
                f"Актив {asset} недоступен на тарифе {tier.title()}. "
                f"Перейдите на Basic или Pro для доступа ко всем инструментам."
            )

    # ── Interval (timeframe в минутах: 5/60/24) ──
    if interval is not None:
        allowed = limits.get("allowed_intervals")
        if allowed is not None and interval not in allowed:
            _forbid(
                f"Таймфрейм {interval}мин недоступен на тарифе {tier.title()}. "
                f"Доступные: {allowed}."
            )

    # ── Timeframe (string, funds_money: 1w/1m/...) ──
    if timeframe is not None:
        allowed = limits.get("allowed_timeframes")
        if allowed is not None and timeframe not in allowed:
            _forbid(
                f"Таймфрейм {timeframe} недоступен на тарифе {tier.title()}. "
                f"Доступные: {allowed}."
            )

    # ── History depth (max_history_days vs period/days) ──
    max_days = limits.get("max_history_days")
    if max_days is not None:
        if period is not None and PERIOD_DAYS.get(period, 0) > max_days:
            _forbid(
                f"Период {period} недоступен на тарифе {tier.title()}. "
                f"Максимум: {max_days} дней."
            )
        if days is not None and days > max_days:
            _forbid(
                f"Глубина {days} дней недоступна на тарифе {tier.title()}. "
                f"Максимум: {max_days} дней."
            )

    # ── Mode (buffett: cap-gdp/cap-m2, seasonality: histogram/price/...) ──
    if mode is not None:
        allowed = limits.get("modes") or limits.get("allowed_modes")
        if allowed is not None and mode not in allowed:
            _forbid(
                f"Режим '{mode}' недоступен на тарифе {tier.title()}. "
                f"Доступные: {allowed}."
            )

    # ── Universe (strength: imoex / all / ...) ──
    if universe is not None:
        allowed = limits.get("universes")
        if allowed is not None and universe not in allowed:
            _forbid(
                f"Вселенная '{universe}' недоступна на тарифе {tier.title()}. "
                f"Доступные: {allowed}."
            )

    # ── clgroup (OI: FIZ/YUR) ──
    if clgroup is not None:
        allowed = limits.get("clgroups")
        if allowed is not None and clgroup not in allowed:
            _forbid(f"Группа {clgroup} недоступна на тарифе {tier.title()}.")

    return limits


def get_effective_end_date(user, indicator: str) -> Optional[date]:
    """Эффективная end_date с учётом задержки tier'а.

    Free для OI и cbr_flows получает данные с задержкой 24 часа.
    Backend подменяет `end_date = today() - delay` ПЕРЕД запросом в БД.
    Не доверять параметрам из фронта — он может прислать today().

    Returns None если задержки нет (актуальный today()).
    """
    tier = user_tier(user)
    limits = get_indicator_limits(tier, indicator)
    delay_h = limits.get("data_delay_hours", 0)
    if not delay_h:
        return None
    # delay измеряется в часах, для daily интервала округляем вниз до days
    days_back = max(1, delay_h // 24) if delay_h >= 24 else 1
    return date.today() - timedelta(days=days_back)


# ═══════════════════════════════════════════════════════════════════════════════
# Legacy compatibility — enforce_guest_limits
# ═══════════════════════════════════════════════════════════════════════════════
# Сохранена для роутеров которые ещё не мигрированы на enforce_tier_limits.
# В новом коде использовать enforce_tier_limits(user, indicator, ...).

GUEST_ALLOWED_INTERVALS = {24}
GUEST_MAX_PERIOD = "1y"
GUEST_MAX_DAYS = 365

GUEST_LIMITS = {
    "allowed_intervals": sorted(GUEST_ALLOWED_INTERVALS),
    "max_period": GUEST_MAX_PERIOD,
    "max_days": GUEST_MAX_DAYS,
}


def enforce_guest_limits(
    user,
    interval: Optional[int] = None,
    period: Optional[str] = None,
    days: Optional[int] = None,
):
    """LEGACY — простая binary guest/auth проверка.

    Используется роутерами которые ещё не мигрированы на enforce_tier_limits.
    Новый код должен использовать enforce_tier_limits(user, indicator, ...).
    """
    if user is not None:
        return

    if interval is not None and interval not in GUEST_ALLOWED_INTERVALS:
        _forbid("Для доступа к интрадей данным необходима авторизация")

    if period is not None and PERIOD_DAYS.get(period, 0) > PERIOD_DAYS.get(GUEST_MAX_PERIOD, 30):
        _forbid("Для доступа к расширенной истории необходима авторизация")

    if days is not None and days > GUEST_MAX_DAYS:
        _forbid("Для доступа к расширенной истории необходима авторизация")
