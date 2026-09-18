"""Квартальная экспирация фьючерсов Мосбиржи — третий четверг марта, июня, сентября, декабря.

Вадим 15.09: перед каждой экспирацией чистый лонг физлиц снижается по всем активам, и снижение
чистых покупок в эти дни — не сигнал (#1992, #1858 про Яндекс: «экспирация через три дня»).
Строка из expiry_note уходит в ограничения карточек всех трёх конвейеров: находок, связок и
новостей по тикеру.
"""
import numpy as np
import pandas as pd

GEN = ("января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября",
       "октября", "ноября", "декабря")


def next_expiry(t) -> pd.Timestamp:
    """Ближайшая квартальная экспирация в день t или позже."""
    t = pd.Timestamp(t).normalize()
    for y in (t.year, t.year + 1):
        for m in (3, 6, 9, 12):
            first = pd.Timestamp(y, m, 1)
            d = first + pd.Timedelta(days=(3 - first.weekday()) % 7 + 14)
            if d >= t:
                return d
    raise ValueError(t)


def near_expiry(t, before: int = 1, after: int = 1) -> bool:
    """День экспирации или ±1 торговый день от неё: позиции физлиц в эти дни искажены переходом в
    следующий контракт. Вадим 18.09 к #2419 (Мечел, данные 17.09): «надо проверить влияние экспираций»,
    утром после открытия позиции «полетели вниз»."""
    t = pd.Timestamp(t).normalize()
    e = next_expiry(t - pd.Timedelta(days=7))
    for d in (e, next_expiry(t)):
        n = int(np.busday_count(min(d, t).date(), max(d, t).date()))
        if (t <= d and n <= before) or (t > d and n <= after):
            return True
    return False


def expiry_note(t, days: int = 7) -> str:
    """Предупреждение, если до экспирации не больше `days` календарных дней, иначе пустая строка."""
    d = next_expiry(t)
    n = (d - pd.Timestamp(t).normalize()).days
    if n > days:
        return ""
    when = f"{d.day} {GEN[d.month - 1]}"
    lead = (f"{when} - день квартальной экспирации" if n == 0 else
            f"до квартальной экспирации {when} - {n} " + ("день" if n == 1 else "дня" if n < 5 else "дней"))
    return (f"{lead}: перед каждой экспирацией чистый лонг физлиц снижается по всем активам - "
            f"снижение чистых позиций в эти дни не сигнал и не тема поста")
