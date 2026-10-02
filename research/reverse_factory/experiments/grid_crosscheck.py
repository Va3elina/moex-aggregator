"""Независимая перепроверка движка: тот же бот, написанный иначе — через журнал сделок и список заявок.

Отличия от grid_engine.py: заявки хранятся списком (лимитки докупок, тейк), каждое исполнение пишется в журнал,
прибыль считается по журналу (сумма денег по всем сделкам), а не через среднюю цену. Сигналы входа — из того же
класса Coin (их проверяем отдельно: пересчитываем RSI вручную на 3-мин свечах и сверяем).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import grid_engine as ge  # noqa: E402


def run_journal(coin: ge.Coin, p: ge.P, E0=10_000.0):
    k = coin.k; t = k.index; o, h, l, c = (k[x].values for x in "ohlc")
    sig = coin.signal(p)
    unit_usd = E0 * p.L / ge.SIZES.sum()
    journal = []          # (время, сторона ордера +1 покупка/−1 продажа, количество, цена, комиссия)
    open_q = 0.0; orders = []; entry_i = last_i = None; p0 = None
    fund = 0.0
    for i in range(1, len(c)):
        if open_q > 0 and coin.fund[i]:
            fund -= open_q * c[i - 1] * coin.fund[i]
        if open_q == 0:
            if sig[i - 1]:
                p0 = o[i]; q = unit_usd / p0
                journal.append((t[i], 1, q, p0, q * p0 * p.taker)); open_q = q; entry_i = last_i = i
                orders = [("add", p0 * (1 - lvl / 100), unit_usd / p0 * m) for lvl, m in zip(p.levels, ge.SIZES[1:])]
            continue
        filled = [od for od in orders if od[0] == "add" and l[i] <= od[1]]
        for od in sorted(filled, key=lambda z: -z[1]):
            journal.append((t[i], 1, od[2], od[1], od[2] * od[1] * p.maker)); open_q += od[2]; last_i = i
            orders.remove(od)
        buys = [j for j in journal if j[0] >= t[entry_i] and j[1] == 1]
        cost = sum(j[2] * j[3] for j in buys); qty = sum(j[2] for j in buys)
        n_adds = len(buys) - 1
        tp = p0 * (1 + p.tp1 / 100) if n_adds == 0 else cost / qty * (1 + p.tpn / 100)
        if i > last_i and h[i] >= tp:
            journal.append((t[i], -1, open_q, tp, open_q * tp * p.maker)); open_q = 0.0; orders = []
        elif (i - last_i) >= p.timer_h * 60:
            journal.append((t[i], -1, open_q, c[i], open_q * c[i] * p.taker)); open_q = 0.0; orders = []
    J = pd.DataFrame(journal, columns=["t", "side", "q", "px", "fee"])
    J["cash"] = -J.side * J.q * J.px - J.fee
    return J, fund


if __name__ == "__main__":
    coin = ge.Coin("DOGEUSDT", "2024-03-01", "2026-09-26")
    for filt in (False, True):
        p = ge.P(side=1, L=1.0, btc_filter=filt)
        E, C, liq, tr = ge.run(coin, p)
        J, fund = run_journal(coin, p)
        closed = J  # к концу периода позиция может быть открыта — сравниваем по закрытым
        print(f"фильтр={filt}: движок — итог {E.iloc[-1] - 10_000:,.0f}$, сделок {tr}; журнал — сделок {len(J)}, "
              f"деньги по сделкам {J.cash.sum():,.0f}$ + финансирование {fund:,.0f}$ = {J.cash.sum() + fund:,.0f}$")
    # сверка формулы RSI: вручную по Уайлдеру (полный пересчёт с начала ряда) и функцией движка
    c3 = coin.k.c.resample("3min", label="right", closed="right").last().dropna()
    eng = ge.rsi(c3, 7)
    d = np.diff(c3.values); g = np.where(d > 0, d, 0.0); lo = np.where(d < 0, -d, 0.0)
    ag, al = g[0], lo[0]; manual = [np.nan]
    for a_, b_ in zip(g, lo):
        ag = (ag * 6 + a_) / 7; al = (al * 6 + b_) / 7; manual.append(100 - 100 / (1 + ag / al))
    manual = pd.Series(manual[:len(c3)], c3.index)
    diff = (eng - manual).abs().iloc[500:]
    print(f"RSI7 3м: движок против ручного расчёта — наибольшее расхождение {diff.max():.4f} (после разгона)")
