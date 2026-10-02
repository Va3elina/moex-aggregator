"""Разбор 2025 года сделка в сделку: что делал Algotoria, пока наша трендовая версия теряла.

По месяцам (2024 для сравнения, 2025 подробно): ход BTC/ETH, его позиция (лонг/шорт в $, доля времени вне
рынка), его прибыль по закрытым сделкам, наша позиция и результат. Плюс худшие для нас дни 2025
относительно него — как он стоял в эти дни.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al                       # noqa: E402
from algotoria_v2 import shape, their_size        # noqa: E402
from factory.schema import load_target            # noqa: E402

FEE = 0.0002

tr, _, eq = load_target("algotoria")
ya = eq[eq > 0].resample("D").last().pct_change().dropna()
end = pd.Timestamp.now(tz="UTC").tz_localize(None)
C = {}
for sym in ["BTCUSDT", "ETHUSDT"]:
    net, r, _ = al.coin(sym, end)
    net = net["2023-10-10":]
    pos = shape(net / net.abs().max(), 1, 2)
    grid = pos.index
    theirs = their_size(tr, sym, grid)
    ret = pos.shift(1).fillna(0) * r.loc[grid] - pos.diff().abs().fillna(0) * FEE
    price = (1 + r.loc[grid]).cumprod()
    C[sym] = pd.DataFrame(dict(pos=pos, theirs=theirs, ret=ret, price=price))

# масштаб нашей доходности к его волатильности (как в v2)
our_day = sum(C[s].ret.groupby(C[s].index.floor("D")).sum() for s in C) / 2
i = our_day.index.intersection(ya.index)
k = ya[i].std() / our_day[i].std()
our_day = our_day * k

c = tr.dropna(subset=["t_close", "cost", "pnl_pct"]).copy()
c["usd"] = c.cost * c.pnl_pct / 100
c["m"] = c.t_close.dt.to_period("M")

rows = []
for m in pd.period_range("2024-01", "2025-12", freq="M"):
    a, b = m.start_time, m.end_time
    row = dict(месяц=str(m))
    for sym, tag in [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]:
        d = C[sym].loc[a:b]
        if d.empty:
            continue
        row[f"{tag} ход"] = f"{d.price.iloc[-1] / d.price.iloc[0] - 1:+.0%}"
        th = d.theirs
        row[f"{tag} его лонг $к"] = round(th.clip(lower=0).mean() / 1e3)
        row[f"{tag} его шорт $к"] = round(-th.clip(upper=0).mean() / 1e3)
        row[f"{tag} вне рынка"] = f"{(th == 0).mean():.0%}"
        row[f"{tag} наш знак"] = f"{(np.sign(d.pos) == np.sign(th))[th != 0].mean():.0%} совп."
    cm = c[c.m == m]
    row["его $ BTC/ETH"] = round(cm[cm.sym.isin(["BTCUSDT", "ETHUSDT"])].usd.sum())
    row["его $ альты"] = round(cm[~cm.sym.isin(["BTCUSDT", "ETHUSDT"])].usd.sum())
    yam = ya[a:b]
    row["он, %"] = f"{(1 + yam).prod() - 1:+.0%}"
    row["мы, %"] = f"{(1 + our_day[a:b]).prod() - 1:+.0%}"
    rows.append(row)
R = pd.DataFrame(rows)
pd.set_option("display.width", 260); pd.set_option("display.max_columns", 30)
print(R.to_string(index=False))

# худшие для нас дни 2025 относительно него
diff = (our_day - ya).dropna()["2025"]
worst = diff.sort_values().head(12).index
print("\nДни 2025, где мы потеряли больше всего относительно него:")
out = []
for dday in worst:
    x = dict(день=str(dday.date()), мы=f"{our_day[dday]:+.1%}", он=f"{ya[dday]:+.1%}")
    for sym, tag in [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]:
        d = C[sym].loc[dday:dday + pd.Timedelta("1D") - pd.Timedelta("1min")]
        x[f"{tag} ход"] = f"{d.price.iloc[-1] / d.price.iloc[0] - 1:+.1%}" if len(d) else ""
        x[f"{tag} наш"] = round(float(d.pos.mean()), 2) if len(d) else None
        x[f"{tag} его $к"] = round(float(d.theirs.mean() / 1e3), 1) if len(d) else None
    out.append(x)
print(pd.DataFrame(out).to_string(index=False))
