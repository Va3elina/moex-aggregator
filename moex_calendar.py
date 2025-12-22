#!/usr/bin/env python3
"""
Календарь торговых дней MOEX.

Содержит:
- Праздничные дни (биржа не работает)
- Функции проверки торговых дней и часов
- Время по Москве

Использование:
    from moex_calendar import is_trading_day, is_trading_hours, get_moscow_time
"""

from datetime import datetime, date, timedelta
from typing import Tuple, List

# ======================================================================
#                    ПРАЗДНИКИ MOEX 2024-2026
# ======================================================================

# Официальные нерабочие дни Московской биржи
# Источник: https://www.moex.com/s2532

MOEX_HOLIDAYS = {
    # 2024
    date(2024, 1, 1),  # Новогодние каникулы
    date(2024, 1, 2),
    date(2024, 1, 3),
    date(2024, 1, 4),
    date(2024, 1, 5),
    date(2024, 1, 8),
    date(2024, 2, 23),  # День защитника Отечества
    date(2024, 3, 8),  # Международный женский день
    date(2024, 5, 1),  # Праздник Весны и Труда
    date(2024, 5, 9),  # День Победы
    date(2024, 6, 12),  # День России
    date(2024, 11, 4),  # День народного единства
    date(2024, 12, 31),  # Новогодние каникулы (перенос)

    # 2025
    date(2025, 1, 1),  # Новогодние каникулы
    date(2025, 1, 2),
    date(2025, 1, 3),
    date(2025, 1, 6),
    date(2025, 1, 7),
    date(2025, 1, 8),
    date(2025, 2, 24),  # День защитника Отечества (перенос с 23.02 вс)
    date(2025, 3, 10),  # Международный женский день (перенос с 08.03 сб)
    date(2025, 5, 1),  # Праздник Весны и Труда
    date(2025, 5, 2),  # Перенос с 04.01
    date(2025, 5, 9),  # День Победы
    date(2025, 6, 12),  # День России
    date(2025, 6, 13),  # Перенос с 05.01
    date(2025, 11, 4),  # День народного единства
    date(2025, 12, 31),  # Новогодние каникулы

    # 2026 (предварительно, может измениться)
    date(2026, 1, 1),  # Новогодние каникулы
    date(2026, 1, 2),
    date(2026, 1, 5),
    date(2026, 1, 6),
    date(2026, 1, 7),
    date(2026, 1, 8),
    date(2026, 2, 23),  # День защитника Отечества
    date(2026, 3, 9),  # Международный женский день (перенос с 08.03 вс)
    date(2026, 5, 1),  # Праздник Весны и Труда
    date(2026, 5, 11),  # День Победы (перенос с 09.05 сб)
    date(2026, 6, 12),  # День России
    date(2026, 11, 4),  # День народного единства
    date(2026, 12, 31),  # Новогодние каникулы
}

# Торговые часы (МСК)
TRADING_START_HOUR = 7
TRADING_END_HOUR = 24

# Рабочие дни недели (0=Пн, 4=Пт)
WORKING_WEEKDAYS = [0, 1, 2, 3, 4]


# ======================================================================
#                         ВРЕМЯ (МСК)
# ======================================================================

def get_moscow_time() -> datetime:
    """Текущее время по Москве"""
    try:
        import pytz
        msk = pytz.timezone('Europe/Moscow')
        return datetime.now(msk).replace(tzinfo=None)
    except ImportError:
        return datetime.utcnow() + timedelta(hours=3)


# ======================================================================
#                    ПРОВЕРКИ ТОРГОВЫХ ДНЕЙ
# ======================================================================

def is_holiday(check_date: date = None) -> bool:
    """
    Проверяет, является ли дата праздником MOEX.

    Args:
        check_date: Дата для проверки (по умолчанию — сегодня по МСК)

    Returns:
        True если праздник, False если рабочий день
    """
    if check_date is None:
        check_date = get_moscow_time().date()

    return check_date in MOEX_HOLIDAYS


def is_weekend(check_date: date = None) -> bool:
    """
    Проверяет, является ли дата выходным (Сб, Вс).

    Args:
        check_date: Дата для проверки (по умолчанию — сегодня по МСК)

    Returns:
        True если выходной, False если будний день
    """
    if check_date is None:
        check_date = get_moscow_time().date()

    return check_date.weekday() not in WORKING_WEEKDAYS


def is_trading_day(check_date: date = None) -> Tuple[bool, str]:
    """
    Проверяет, является ли дата торговым днём MOEX.

    Args:
        check_date: Дата для проверки (по умолчанию — сегодня по МСК)

    Returns:
        (is_trading, reason) — кортеж (торговый день?, причина)
    """
    if check_date is None:
        check_date = get_moscow_time().date()

    # Проверяем выходные
    if check_date.weekday() not in WORKING_WEEKDAYS:
        day_name = check_date.strftime('%A')
        return False, f"Выходной ({day_name})"

    # Проверяем праздники
    if check_date in MOEX_HOLIDAYS:
        return False, f"Праздник MOEX ({check_date})"

    return True, "Торговый день"


def is_trading_hours(check_time: datetime = None) -> Tuple[bool, str]:
    """
    Проверяет, находимся ли в торговых часах MOEX.

    Args:
        check_time: Время для проверки (по умолчанию — сейчас по МСК)

    Returns:
        (is_trading, reason) — кортеж (торговые часы?, причина)
    """
    if check_time is None:
        check_time = get_moscow_time()

    # Сначала проверяем торговый день
    is_trade_day, reason = is_trading_day(check_time.date())
    if not is_trade_day:
        return False, reason

    # Проверяем часы
    if check_time.hour < TRADING_START_HOUR:
        return False, f"До начала торгов ({TRADING_START_HOUR}:00 МСК)"

    if check_time.hour >= TRADING_END_HOUR:
        return False, "Торги завершены"

    return True, "Торговая сессия"


def get_previous_trading_day(from_date: date = None) -> date:
    """
    Возвращает предыдущий торговый день.

    Args:
        from_date: Дата отсчёта (по умолчанию — сегодня)

    Returns:
        Предыдущий торговый день
    """
    if from_date is None:
        from_date = get_moscow_time().date()

    check_date = from_date - timedelta(days=1)

    while True:
        is_trading, _ = is_trading_day(check_date)
        if is_trading:
            return check_date
        check_date -= timedelta(days=1)

        # Защита от бесконечного цикла (макс 30 дней назад)
        if (from_date - check_date).days > 30:
            raise ValueError("Не найден торговый день за последние 30 дней")


def get_next_trading_day(from_date: date = None) -> date:
    """
    Возвращает следующий торговый день.

    Args:
        from_date: Дата отсчёта (по умолчанию — сегодня)

    Returns:
        Следующий торговый день
    """
    if from_date is None:
        from_date = get_moscow_time().date()

    check_date = from_date + timedelta(days=1)

    while True:
        is_trading, _ = is_trading_day(check_date)
        if is_trading:
            return check_date
        check_date += timedelta(days=1)

        # Защита от бесконечного цикла (макс 30 дней вперёд)
        if (check_date - from_date).days > 30:
            raise ValueError("Не найден торговый день в ближайшие 30 дней")


def get_trading_dates(start_date: date, end_date: date) -> List[date]:
    """
    Возвращает список торговых дней в диапазоне.

    Args:
        start_date: Начальная дата
        end_date: Конечная дата

    Returns:
        Список торговых дней
    """
    dates = []
    current = start_date

    while current <= end_date:
        is_trading, _ = is_trading_day(current)
        if is_trading:
            dates.append(current)
        current += timedelta(days=1)

    return dates


# ======================================================================
#                         ТЕСТИРОВАНИЕ
# ======================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("ТЕСТ КАЛЕНДАРЯ MOEX")
    print("=" * 60)

    now = get_moscow_time()
    print(f"\nТекущее время МСК: {now}")

    # Проверка сегодня
    is_trading, reason = is_trading_day()
    print(f"Сегодня торговый день: {is_trading} ({reason})")

    is_trading_now, reason = is_trading_hours()
    print(f"Сейчас торговые часы: {is_trading_now} ({reason})")

    # Праздники 2025
    print(f"\nПраздники MOEX 2025:")
    holidays_2025 = sorted([d for d in MOEX_HOLIDAYS if d.year == 2025])
    for h in holidays_2025:
        print(f"  {h} ({h.strftime('%A')})")

    # Ближайшие торговые дни
    print(f"\nПредыдущий торговый день: {get_previous_trading_day()}")
    print(f"Следующий торговый день: {get_next_trading_day()}")

    # Торговые дни на неделю вперёд
    print(f"\nТорговые дни на 7 дней вперёд:")
    future_dates = get_trading_dates(now.date(), now.date() + timedelta(days=7))
    for d in future_dates:
        print(f"  {d} ({d.strftime('%A')})")