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

# ⚠️ Ручной список ниже ОКАЗАЛСЯ НЕВЕРНЫМ (сверка с календарём ISS 21.10.2026): биржа торгует в
# большинство государственных праздников (в 2026 — 5, 6, 8.01, 23.02, 9.03, 1.05, 11.05, 12.06, 4.11),
# и загрузчики в эти дни ничего не собирали (ОИ за 7 дней 2026 потерян). Теперь для лет из календаря
# ISS (блок ниже) берутся его даты; ручной список — только запасной для лет вне блока.
_LEGACY_HOLIDAYS = {
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

# >>> ISS CALENDAR (генерирует scripts/moex_calendar_refresh.py — руками не править)
# Обновлено 2026-10-21 из apim.moex.com/iss/calendars.json (off_days).
ISS_CALENDAR_YEARS = {2024, 2025, 2026, 2027}
ISS_STOCK_CLOSED = {
    date(2024, 1, 1), date(2024, 1, 2), date(2024, 2, 23), date(2024, 3, 8), date(2024, 5, 1), date(2024, 5, 9),
    date(2024, 6, 12), date(2024, 11, 4), date(2024, 12, 31),
    date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 7), date(2025, 5, 1), date(2025, 5, 9), date(2025, 6, 12),
    date(2025, 11, 4), date(2025, 12, 31),
    date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 7), date(2026, 12, 31),
    date(2027, 1, 2), date(2027, 1, 3), date(2027, 1, 9), date(2027, 1, 10), date(2027, 1, 16), date(2027, 1, 17),
    date(2027, 1, 23), date(2027, 1, 24), date(2027, 1, 30), date(2027, 1, 31), date(2027, 2, 6), date(2027, 2, 7),
    date(2027, 2, 13), date(2027, 2, 14), date(2027, 2, 21), date(2027, 2, 27), date(2027, 2, 28), date(2027, 3, 6),
    date(2027, 3, 7), date(2027, 3, 13), date(2027, 3, 14), date(2027, 3, 20), date(2027, 3, 21), date(2027, 3, 27),
    date(2027, 3, 28), date(2027, 4, 3), date(2027, 4, 4), date(2027, 4, 10), date(2027, 4, 11), date(2027, 4, 17),
    date(2027, 4, 18), date(2027, 4, 24), date(2027, 4, 25), date(2027, 5, 1), date(2027, 5, 2), date(2027, 5, 8),
    date(2027, 5, 9), date(2027, 5, 15), date(2027, 5, 16), date(2027, 5, 22), date(2027, 5, 23), date(2027, 5, 29),
    date(2027, 5, 30), date(2027, 6, 5), date(2027, 6, 6), date(2027, 6, 12), date(2027, 6, 13), date(2027, 6, 19),
    date(2027, 6, 20), date(2027, 6, 26), date(2027, 6, 27), date(2027, 7, 3), date(2027, 7, 4), date(2027, 7, 10),
    date(2027, 7, 11), date(2027, 7, 17), date(2027, 7, 18), date(2027, 7, 24), date(2027, 7, 25), date(2027, 7, 31),
    date(2027, 8, 1), date(2027, 8, 7), date(2027, 8, 8), date(2027, 8, 14), date(2027, 8, 15), date(2027, 8, 21),
    date(2027, 8, 22), date(2027, 8, 28), date(2027, 8, 29), date(2027, 9, 4), date(2027, 9, 5), date(2027, 9, 11),
    date(2027, 9, 12), date(2027, 9, 18), date(2027, 9, 19), date(2027, 9, 25), date(2027, 9, 26), date(2027, 10, 2),
    date(2027, 10, 3), date(2027, 10, 9), date(2027, 10, 10), date(2027, 10, 16), date(2027, 10, 17), date(2027, 10, 23),
    date(2027, 10, 24), date(2027, 10, 30), date(2027, 10, 31), date(2027, 11, 6), date(2027, 11, 7), date(2027, 11, 13),
    date(2027, 11, 14), date(2027, 11, 20), date(2027, 11, 21), date(2027, 11, 27), date(2027, 11, 28), date(2027, 12, 4),
    date(2027, 12, 5), date(2027, 12, 11), date(2027, 12, 12), date(2027, 12, 18), date(2027, 12, 19), date(2027, 12, 25),
    date(2027, 12, 26), date(2027, 12, 31),
}
ISS_FUTURES_CLOSED = {
    date(2024, 1, 1), date(2024, 1, 2), date(2024, 2, 23), date(2024, 3, 8), date(2024, 5, 1), date(2024, 5, 9),
    date(2024, 6, 12), date(2024, 11, 4), date(2024, 12, 31),
    date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 7), date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 15),
    date(2025, 3, 16), date(2025, 3, 29), date(2025, 3, 30), date(2025, 4, 5), date(2025, 4, 6), date(2025, 4, 12),
    date(2025, 4, 13), date(2025, 4, 26), date(2025, 4, 27), date(2025, 5, 1), date(2025, 5, 3), date(2025, 5, 4),
    date(2025, 5, 9), date(2025, 5, 10), date(2025, 5, 11), date(2025, 5, 17), date(2025, 5, 18), date(2025, 5, 31),
    date(2025, 6, 1), date(2025, 6, 7), date(2025, 6, 8), date(2025, 6, 12), date(2025, 6, 14), date(2025, 6, 15),
    date(2025, 6, 28), date(2025, 6, 29), date(2025, 7, 5), date(2025, 7, 6), date(2025, 7, 12), date(2025, 7, 13),
    date(2025, 7, 19), date(2025, 7, 20), date(2025, 7, 26), date(2025, 7, 27), date(2025, 11, 4), date(2025, 12, 31),
    date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 7), date(2026, 12, 31),
    date(2027, 1, 1), date(2027, 2, 23), date(2027, 3, 8), date(2027, 11, 4),
}
# <<< ISS CALENDAR

# Праздник = НЕ торгуют ни акции, ни фьючерсы. Пересечение, а не объединение: если календарь
# неточен (например, предварительный следующего года), загрузчик лишний раз сходит за пустыми
# данными, но торговый день не пропустит. Рынки различаются редко (4.11.2027: фьючерсы закрыты,
# акции торгуют) — для них STOCK_HOLIDAYS / FUTURES_HOLIDAYS.
# Только БУДНИ: выходные обрабатываются отдельно (is_weekend_session), и их поведение не меняем —
# ручной список выходных никогда не содержал, а блок ISS перечисляет и закрытые субботы/воскресенья.
_ISS_YEARS = {int(y) for y in ISS_CALENDAR_YEARS}
_legacy_other_years = {d for d in _LEGACY_HOLIDAYS if d.year not in _ISS_YEARS}
STOCK_HOLIDAYS = {d for d in ISS_STOCK_CLOSED if d.weekday() < 5} | _legacy_other_years
FUTURES_HOLIDAYS = {d for d in ISS_FUTURES_CLOSED if d.weekday() < 5} | _legacy_other_years
MOEX_HOLIDAYS = (STOCK_HOLIDAYS & FUTURES_HOLIDAYS) | _legacy_other_years

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

def is_holiday(check_date: date = None, market: str = None) -> bool:
    """
    Проверяет, является ли дата праздником MOEX.

    Args:
        check_date: Дата для проверки (по умолчанию — сегодня по МСК)
        market: None — закрыты оба рынка; 'stock' / 'futures' — конкретный рынок

    Returns:
        True если праздник, False если рабочий день
    """
    if check_date is None:
        check_date = get_moscow_time().date()

    if market == "stock":
        return check_date in STOCK_HOLIDAYS
    if market == "futures":
        return check_date in FUTURES_HOLIDAYS
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


def is_weekend_session(check_time: datetime = None) -> Tuple[bool, str]:
    """Окно сбора данных для ТОРГОВ ВЫХОДНОГО ДНЯ MOEX (сб/вс).

    True, если сейчас СУББОТА/ВОСКРЕСЕНЬЕ (не праздник) и в пределах торговых
    часов. Отдельная функция (НЕ меняем is_trading_day/is_trading_hours — у них
    десятки вызовов, поведение буден должно остаться идентичным).

    Точные часы выходных сессий MOEX могут отличаться по инструментам и меняться
    (валютные фьючерсы выходного дня — с 18.07.2026; вечные фьючерсы S&P500/Nasdaq —
    июль 2026), поэтому берём ШИРОКОЕ окно (TRADING_START_HOUR..TRADING_END_HOUR).
    Это безопасно: вне реальной сессии ISS/Algopack отдают пустой ответ → no-op.
    """
    if check_time is None:
        check_time = get_moscow_time()
    d = check_time.date()
    if d.weekday() < 5:
        return False, "Будний день (обычная сессия)"
    if d in MOEX_HOLIDAYS:
        return False, f"Праздник MOEX ({d})"
    if check_time.hour < TRADING_START_HOUR:
        return False, f"До начала ({TRADING_START_HOUR}:00 МСК)"
    if check_time.hour >= TRADING_END_HOUR:
        return False, "После закрытия"
    return True, "Сессия выходного дня"


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