"""
Контроль доступа для гостевых (неавторизованных) пользователей.

Гости:
- Только дневной таймфрейм (interval=24)
- Только данные за последний месяц (period <= 1m, days <= 30)

Авторизованные пользователи — без ограничений.
"""

from typing import Optional
from fastapi import HTTPException, status


# Разрешённые значения для гостей
GUEST_ALLOWED_INTERVALS = {24}  # Только дневной
GUEST_MAX_PERIOD = "1m"
GUEST_MAX_DAYS = 30

# Порядок периодов для сравнения
PERIOD_ORDER = {
    "1d": 1, "1w": 7, "1m": 30, "3m": 90, "6m": 180,
    "1y": 365, "2y": 730, "3y": 1095, "5y": 1825, "all": 99999,
}

# Ограничения для ответа фронтенду
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
    """
    Проверяет ограничения для неавторизованных пользователей.

    - user: объект User или None (гость)
    - interval: таймфрейм (5, 60, 24)
    - period: период данных ("1w", "1m", "3m", etc.)
    - days: количество дней

    Raises HTTPException(403) если гость запрашивает ограниченные данные.
    Авторизованные пользователи проходят без ограничений.
    """
    if user is not None:
        return  # Авторизован — без ограничений

    if interval is not None and interval not in GUEST_ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Для доступа к интрадей данным необходима авторизация",
        )

    if period is not None and PERIOD_ORDER.get(period, 0) > PERIOD_ORDER.get(GUEST_MAX_PERIOD, 30):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Для доступа к расширенной истории необходима авторизация",
        )

    if days is not None and days > GUEST_MAX_DAYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Для доступа к расширенной истории необходима авторизация",
        )
