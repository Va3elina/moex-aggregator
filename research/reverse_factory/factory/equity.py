"""Стадия 3. Кривая доходности: итог, годовые, просадки, по годам.

Если источник даёт свою кривую (invvo — полная, Bybit — 90 дней) — берём её.
Иначе строим приближённую по сделкам: каждая закрытая позиция = равная доля
капитала 1/K (K — медиана одновременно открытых), без плеча. Помечается как оценка.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def from_positions(p: pd.DataFrame) -> pd.Series:
    c = p[p.t_close.notna()].copy()
    if c.empty:
        return pd.Series(dtype=float)
    idx = pd.date_range(c.t_open.min().floor("D"), c.t_close.max().ceil("D"), freq="D")
    opened = np.zeros(len(idx))
    for _, r in c.iterrows():
        a, b = idx.searchsorted(r.t_open.floor("D")), idx.searchsorted(r.t_close.floor("D"))
        opened[a:b + 1] += 1
    k = max(np.median(opened[opened > 0]) if (opened > 0).any() else 1, 1)
    day = c.groupby(c.t_close.dt.floor("D")).pnl_pct.sum() / 100 / k
    r = day.reindex(idx, fill_value=0.0)
    return (1 + r).cumprod()


def stats(eq: pd.Series) -> dict:
    eq = eq.dropna()
    eq = eq[eq > 0]
    if len(eq) < 5:
        return {}
    r = eq.pct_change().dropna()
    yrs = max((eq.index[-1] - eq.index[0]).days / 365.25, 1 / 365)
    dd = eq / eq.cummax() - 1
    under = dd < -1e-9
    days_under = int(under[::-1].cumprod().sum())
    ye = eq.resample("YE").last()
    yr = ye.pct_change()
    yr.iloc[0] = ye.iloc[0] / eq.iloc[0] - 1
    m = eq.resample("ME").last().pct_change().dropna()
    pos_m = m[m > 0]
    worst_ratio = float(m.min() / pos_m.mean()) if len(pos_m) and len(m) >= 6 else None
    dd_vol = float(dd.min() / (r.std() * np.sqrt(365))) if r.std() > 0 else None
    # «страховка» по кривой: калибровка 26.09.2026 — Syndicate (докупка) −6.4 / −1.09, Algotoria (тренд) −1.2 / −0.59
    if worst_ratio is not None and (worst_ratio <= -4 or (dd_vol or 0) <= -0.95):
        tail = "продаёт страховку: один плохой месяц съедает много хороших"
    elif worst_ratio is not None and worst_ratio >= -2.5:
        tail = "хвост умеренный: худший месяц сопоставим с обычным хорошим"
    else:
        tail = "не ясно"
    return dict(months_up=round(float((m > 0).mean()), 2) if len(m) else None,
                worst_month_vs_avg_good=round(worst_ratio, 1) if worst_ratio is not None else None,
                dd_to_vol=round(dd_vol, 2) if dd_vol is not None else None, tail_verdict=tail,
                total_x=round(float(eq.iloc[-1] / eq.iloc[0]), 2),
                cagr=round(float((eq.iloc[-1] / eq.iloc[0]) ** (1 / yrs) - 1), 3),
                max_dd=round(float(dd.min()), 3), dd_now=round(float(dd.iloc[-1]), 3), days_in_dd_now=days_under,
                vol=round(float(r.std() * np.sqrt(365)), 3),
                sharpe=round(float(r.mean() / r.std() * np.sqrt(365)), 2) if r.std() > 0 else None,
                by_year={int(k.year): round(float(v), 3) for k, v in yr.items()})
