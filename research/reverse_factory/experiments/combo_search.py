"""Подбор ЕГО набора стратегий: жадно добавляем правила «стоп-и-разворот» (Supertrend, пробой канала,
15м и 1ч), пока растёт совпадение разворотов суммарной позиции с его разворотами (±1ч).
Суммарная позиция = знак среднего направлений выбранных правил (неттинг, как у него).
Подбор — на первой половине его истории, проверка — на второй. До 14 правил (он пишет «14 стратегий»).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market                                         # noqa: E402
from factory.schema import load_target                              # noqa: E402
from sar_search import donchian_dir, supertrend_dir, match          # noqa: E402

DT = np.timedelta64(1, "h")


def candidates(sym, a, b, grid):
    C = {}
    for iv, stepm in [("15", 15), ("60", 60), ("240", 240)]:
        k = market.klines(sym, iv, a, b)
        step = pd.Timedelta(minutes=stepm)
        def put(name, d):
            d = d.copy(); d.index = d.index + step / 2                # разворот случается внутри свечи
            C[name] = d.reindex(grid, method="ffill").fillna(1)
        for n in [10, 20]:
            for m in [2, 3, 4, 5]:
                put(f"ST {iv}м ATR{n}×{m}", supertrend_dir(k, n, m))
        for N in [24, 48, 96]:
            put(f"канал {iv}м N{N}", donchian_dir(k, N))
    return pd.DataFrame(C)


def flips_of(sgn: pd.Series) -> pd.DataFrame:
    ch = sgn.ne(sgn.shift()) & (sgn != 0)
    return pd.DataFrame(dict(t=sgn.index[ch][1:], side=sgn[ch].values[1:]))


def score(C, cols, E, lo, hi):
    s = np.sign(C[cols].mean(axis=1)).replace(0, np.nan).ffill().fillna(1)
    Eo = flips_of(s.loc[lo:hi])
    Et = E[(E.t >= lo) & (E.t < hi)]
    r, p = match(Et, Eo, DT)
    return (2 * r * p / (r + p) if r + p else 0.0), r, p, len(Eo), len(Et)


if __name__ == "__main__":
    tr, _, _ = load_target("algotoria")
    for sym in ["BTCUSDT", "ETHUSDT"]:
        t = tr[tr.sym == sym].sort_values("t_open")
        E = pd.DataFrame(dict(t=t.t_open.values, side=t.side.values))
        a, b = t.t_open.min() - pd.Timedelta(days=15), t.t_close.max()
        grid = pd.date_range(t.t_open.min().floor("15min"), b, freq="15min")
        C = candidates(sym, a, b, grid)
        mid = t.t_open.iloc[len(t) // 2]
        chosen, best = [], 0.0
        while len(chosen) < 14:
            trial = {c: score(C, chosen + [c], E, grid[0], mid)[0] for c in C.columns if c not in chosen or True}
            c, f = max(trial.items(), key=lambda kv: kv[1])
            if f <= best + 0.002:
                break
            chosen.append(c); best = f
        f_in, r_in, p_in, no, nt = score(C, chosen, E, grid[0], mid)
        f_out, r_out, p_out, no2, nt2 = score(C, chosen, E, mid, grid[-1])
        print(f"\n=== {sym}: выбрано {len(chosen)} правил (повторы = больший вес): {chosen}")
        print(f"   подбор   ({str(grid[0])[:10]}…{str(mid)[:10]}): его→наш ±1ч {r_in:.2f}, наш→его {p_in:.2f}, F1 {f_in:.2f} | разворотов наших {no}, его {nt}")
        print(f"   ПРОВЕРКА ({str(mid)[:10]}…{str(grid[-1])[:10]}): его→наш ±1ч {r_out:.2f}, наш→его {p_out:.2f}, F1 {f_out:.2f} | разворотов наших {no2}, его {nt2}")
        s = np.sign(C[chosen].mean(axis=1)).replace(0, np.nan).ffill().fillna(1)
        their = pd.Series(0.0, grid)
        for x in t.dropna(subset=["t_close"]).itertuples():
            their[(grid >= x.t_open) & (grid < x.t_close)] += x.side
        m = their != 0
        print(f"   совпадение направления на каждой 15-минутке: {(s[m] == np.sign(their[m])).mean():.0%} (было 78%/75% у v1)")
