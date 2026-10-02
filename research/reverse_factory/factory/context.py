"""Стадия 1б. Контекст входа: куда шла цена ПЕРЕД входом — вход по ходу движения или против.

Нужна свечная история (1ч). Для каждой позиции — изменение цены за 4/24/72 ч до последней
закрытой часовой свечи перед входом, умноженное на сторону сделки: >0 — вошёл по ходу
(тренд, пробой), <0 — против хода (контртренд, «ловля ножей», докупка на проливе).
Добавлено 26.09.2026 после калибровки: Knife Catcher («ловит проливы») и Donatello («тренд»)
по одним сделкам были неразличимы («смешанный»).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import market


def run(p: pd.DataFrame, max_pos: int = 600, max_syms: int = 12) -> dict:
    q = p.sort_values("t_open").tail(max_pos)
    top = q.sym.value_counts().index[:max_syms]
    q = q[q.sym.isin(top)]
    rows = []
    for sym, g in q.groupby("sym"):
        k = market.klines(sym, "60", g.t_open.min() - pd.Timedelta(hours=80), g.t_open.max() + pd.Timedelta(hours=1))
        if len(k) < 100:
            continue
        c = k.c.astype(float)
        last = g.t_open.dt.floor("h") - pd.Timedelta(hours=1)      # последняя закрытая часовая свеча
        for h in [4, 24, 72]:
            now = c.reindex(last.values).values
            before = c.reindex((last - pd.Timedelta(hours=h)).values).values
            g = g.assign(**{f"ctx{h}": 100 * g.side.values * (now / before - 1)})
        rows.append(g)
    if not rows:
        return dict(skipped="нет свечей")
    D = pd.concat(rows)
    out = {"n": int(len(D))}
    for h in [4, 24, 72]:
        x = D[f"ctx{h}"].dropna()
        if len(x) >= 10:
            out[f"{h}ч"] = dict(median=round(float(x.median()), 2), with_share=round(float((x > 0).mean()), 2))
    w = [out[f"{h}ч"]["with_share"] for h in [4, 24] if f"{h}ч" in out]
    if w:
        m = float(np.mean(w))
        out["verdict"] = ("вход ПО ХОДУ движения (тренд/пробой)" if m >= 0.62 else
                          "вход ПРОТИВ движения (контртренд/докупка на проливе)" if m <= 0.38 else
                          "вход без выраженной стороны")
        out["with_share"] = round(m, 2)
    return out
