"""Симулятор исполнения Pine на Python.

ЗАЧЕМ. Обычный расчёт считает каждую сделку НЕЗАВИСИМО: тысячи конфигураций
живут параллельно, ни одна другой не мешает. Pine устроен иначе — счёт один,
позиция одна, сделки выстроены во времени. Почти все пойманные ошибки жили
ровно на этом стыке (ловушка 7 в PROTOCOL.md).

Этот модуль повторяет ПОСЛЕДОВАТЕЛЬНУЮ модель Pine: события внутри дня
обрабатываются в порядке часов, позиция одна, новый вход закрывает старую.
Прогнав обе модели на одних данных, расхождение видно ДО отправки скрипта.
"""
from __future__ import annotations
import datetime
import numpy as np
import pandas as pd


def slot_end(t: datetime.time) -> int:
    """Конец получасового слота в минутах от полуночи. Метка 10:00 -> 10:30."""
    e = t.hour * 60 + t.minute + 30
    return min(e, 1435)


def simulate(px: pd.DataFrame, A, B, C, direction: int, threshold: float = 0.001):
    """Последовательный прогон, как в Pine.

    px         — дни x слоты, цена в КОНЦЕ слота
    direction  — +1 продолжение, -1 разворот
    Возвращает список сделок (вход, выход, доходность со знаком).
    """
    days = list(px.index)
    ev = sorted([("A", slot_end(A)), ("B", slot_end(B)), ("C", slot_end(C))],
                key=lambda x: x[1])

    a_price = np.nan
    a_day = b_day = entry_day = None
    pos, entry_px = 0, np.nan
    trades = []

    for d in days:
        for kind, _ in ev:
            if kind == "A":
                p = px.at[d, A]
                if np.isfinite(p):
                    a_price, a_day = p, d

            elif kind == "B":
                p = px.at[d, B]
                if a_day != d or not np.isfinite(p) or not np.isfinite(a_price):
                    continue
                b_day = d
                chg = p / a_price - 1
                raw = 1 if chg >= threshold else (-1 if chg <= -threshold else 0)
                sig = direction * raw
                if sig == 0:
                    continue
                if pos != 0:                       # перекрытие: закрываем старую
                    trades.append((entry_day, d, pos * (p / entry_px - 1)))
                    pos = 0
                pos, entry_px, entry_day = sig, p, d

            else:                                   # C
                p = px.at[d, C]
                if pos != 0 and d != entry_day and np.isfinite(p):
                    trades.append((entry_day, d, pos * (p / entry_px - 1)))
                    pos, entry_px, entry_day = 0, np.nan, None

    return trades


def vectorized(px: pd.DataFrame, A, B, C, direction: int,
               threshold: float = 0.001, max_gap_days: int = 5):
    """Обычный расчёт: каждая сделка независима, выход на следующий день."""
    days = px.index
    a, b, c = px[A].values, px[B].values, px[C].values
    gap = np.append((days[1:] - days[:-1]).days.values, 10 ** 9)
    c_next = np.roll(c, -1).astype(float)
    c_next[gap > max_gap_days] = np.nan
    c_next[-1] = np.nan

    chg = b / a - 1
    raw = np.where(chg >= threshold, 1.0, np.where(chg <= -threshold, -1.0, 0.0))
    raw = np.nan_to_num(raw)
    sig = direction * raw
    r = sig * (c_next / b - 1)
    ok = (sig != 0) & np.isfinite(r)
    return r[ok]


def compare(px, A, B, C, direction, label=""):
    seq = simulate(px, A, B, C, direction)
    vec = vectorized(px, A, B, C, direction)
    sr = np.array([t[2] for t in seq])
    overlap = sum(1 for i in range(1, len(seq)) if seq[i - 1][1] == seq[i][0])
    return {
        "бумага": label,
        "сделок последовательно": len(sr),
        "сделок независимо": len(vec),
        "на сделку последовательно": round(float(sr.mean() * 100), 3) if len(sr) else None,
        "на сделку независимо": round(float(vec.mean() * 100), 3) if len(vec) else None,
        "суммарно последовательно": round(float(sr.sum() * 100), 1) if len(sr) else None,
        "суммарно независимо": round(float(vec.sum() * 100), 1) if len(vec) else None,
        "перекрытий": overlap,
    }
