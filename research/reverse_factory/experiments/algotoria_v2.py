"""v2: ставка по согласию + перекос в лонг. Подбор — по совпадению с ЕГО позицией (размер в размер), не по доходности.

Его позиция по монете на каждой 15-минутке = сумма (сторона × объём в $) открытых записей.
Наша = f(согласие 14 стратегий): размер = знак × |среднее|^p, лонг умножается на L.
Метрики: связь размера (корреляция позиций), совпадение знака, связь дневной доходности, доходность.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al                       # noqa: E402
from factory.schema import load_target            # noqa: E402

FEE = 0.0002


def their_size(tr, sym, grid):
    t = tr[(tr.sym == sym)].dropna(subset=["t_close"])
    s = np.zeros(len(grid))
    for x in t.itertuples():
        a, b = grid.searchsorted(x.t_open), grid.searchsorted(x.t_close)
        s[a:b] += x.side * (x.cost if pd.notna(x.cost) else 0)
    return pd.Series(s, grid)


def shape(net: pd.Series, p: float, L: float) -> pd.Series:
    x = np.sign(net) * net.abs() ** p
    return x.where(x < 0, x * L)


if __name__ == "__main__":
    tr, _, eq = load_target("algotoria")
    ya = eq[eq > 0].resample("D").last().pct_change().dropna()["2023-10-10":]
    base = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        net, r, _ = al.coin(sym, pd.Timestamp.now(tz="UTC").tz_localize(None))
        t = tr[tr.sym == sym]
        a, b = t.t_open.min().floor("15min"), t.t_close.max()
        grid = net.loc[a:b].index
        base[sym] = dict(net=net.loc[a:b] / net.loc[a:b].abs().max(), r=r.loc[a:b], their=their_size(tr, sym, grid))
    rows = []
    for p in [1, 2, 3]:
        for L in [1, 1.5, 2, 3]:
            day, corr_size, agree = [], [], []
            for sym, d in base.items():
                pos = shape(d["net"], p, L)
                corr_size.append(pos.corr(d["their"]))
                m = d["their"] != 0
                agree.append((np.sign(pos[m]) == np.sign(d["their"][m])).mean())
                ret = pos.shift(1).fillna(0) * d["r"] - pos.diff().abs().fillna(0) * FEE
                day.append(ret.groupby(ret.index.floor("D")).sum())
            port = (day[0] + day[1]) / 2
            i = port.index.intersection(ya.index)
            port = port[i] * (ya[i].std() / port[i].std())
            rows.append(dict(p=p, L=L, связь_размера=round(float(np.mean(corr_size)), 3), совпадение_знака=round(float(np.mean(agree)), 3),
                             связь_дохода=round(float(port.corr(ya[i])), 3), наша=al.stats(port)))
    R = pd.DataFrame(rows).sort_values("связь_размера", ascending=False)
    pd.set_option("display.width", 230); pd.set_option("display.max_colwidth", 90)
    print(R.to_string(index=False))
    print(f"\nAlgotoria: {al.stats(ya[i])}")
