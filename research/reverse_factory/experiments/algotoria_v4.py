"""v4: 14 медленных трендовых правил v2 (размер = согласие) + быстрые правила разворота (момент смены стороны).
Суммарная позиция = среднее всех (неттинг); лонг ×L. Сверка: развороты ±1ч, направление, размер в размер, доходность."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al
from algotoria_v2 import their_size
from combo_search import flips_of
from factory import market
from factory.schema import load_target
from sar_search import donchian_dir, match, supertrend_dir

FEE = 0.0002
tr, _, eq = load_target("algotoria")
ya = eq[eq > 0].resample("D").last().pct_change().dropna()
end = pd.Timestamp.now(tz="UTC").tz_localize(None)
base = {}
for sym in ["BTCUSDT", "ETHUSDT"]:
    _, r, P = al.coin(sym, end)                       # 14 медленных (1ч/4ч/1д), позиции ±w на 15м
    t = tr[tr.sym == sym]
    grid = P.loc[t.t_open.min().floor("15min"):t.t_close.max()].index
    a, b = grid[0] - pd.Timedelta(days=15), grid[-1]
    k60, k15 = market.klines(sym, "60", a, b), market.klines(sym, "15", a, b)
    fast = {}
    for name, d, off in [("ST1h 20×4", supertrend_dir(k60, 20, 4), "30min"), ("ST15 20×5", supertrend_dir(k15, 20, 5), "7min"),
                         ("канал15 N96", donchian_dir(k15, 96), "7min"), ("канал1h N48", donchian_dir(k60, 48), "30min")]:
        d.index = d.index + pd.Timedelta(off); fast[name] = d.reindex(grid, method="ffill").fillna(1)
    base[sym] = dict(slow=P.loc[grid] / 3, fast=pd.DataFrame(fast), r=r.loc[grid], theirs=their_size(tr, sym, grid),
                     E=pd.DataFrame(dict(t=t.sort_values("t_open").t_open.values, side=t.sort_values("t_open").side.values)))
rows = []
for wf in [0, 0.5, 1, 2, 4]:                 # вес быстрых правил относительно медленных
    for L in [1, 2]:
        cs, day, f1s, ag = [], [], [], []
        for sym, x in base.items():
            net = pd.concat([x["slow"], x["fast"] * wf], axis=1).mean(axis=1) if wf else x["slow"].mean(axis=1)
            pos = net.where(net < 0, net * L)
            cs.append(pos.corr(x["theirs"]))
            s = np.sign(net).replace(0, np.nan).ffill().fillna(1)
            rr, pp = match(x["E"], flips_of(s), np.timedelta64(1, "h")); f1s.append(2 * rr * pp / (rr + pp) if rr + pp else 0)
            m = x["theirs"] != 0; ag.append((s[m] == np.sign(x["theirs"][m])).mean())
            ret = pos.shift(1).fillna(0) * x["r"] - pos.diff().abs().fillna(0) * FEE
            day.append(ret.groupby(ret.index.floor("D")).sum())
        port = (day[0] + day[1]).dropna(); i = port.index.intersection(ya.index)
        pv = port[i] * (ya[i].std() / port[i].std())
        rows.append(dict(вес_быстрых=wf, лонг_x=L, развороты_F1=round(float(np.mean(f1s)), 2), направление=round(float(np.mean(ag)), 2),
                         связь_размера=round(float(np.mean(cs)), 2), связь_дохода=round(float(pv.corr(ya[i])), 2), наша=al.stats(pv)))
pd.set_option("display.width", 240); pd.set_option("display.max_colwidth", 100)
print(pd.DataFrame(rows).to_string(index=False))
print(f"\nAlgotoria: {al.stats(ya[i])}")
