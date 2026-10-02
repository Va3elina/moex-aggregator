"""Слой исполнения для прогнозов big_search: позиция меняется только при заметном сдвиге прогноза (порог),
чтобы не платить комиссию за мелкую дрожь. Сравниваем: без комиссий, оборот, с комиссией 0.02%."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al
import big_search as bs
from algotoria_v2 import their_size
from combo_search import flips_of
from factory.schema import load_target
from sar_search import match
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

def hysteresis(p: pd.Series, thr: float) -> pd.Series:
    v, out, cur = p.values, np.zeros(len(p)), 0.0
    for i, x in enumerate(v):
        if abs(x - cur) >= thr or np.sign(x) != np.sign(cur) and abs(x) >= thr / 2:
            cur = x
        out[i] = cur
    return pd.Series(out, p.index)

tr, _, eq = load_target("algotoria")
ya = eq[eq > 0].resample("D").last().pct_change().dropna()
G = pd.read_csv(Path(__file__).with_name("algotoria_daily.csv"), index_col=0, parse_dates=True)
mb = G.mb.where(G.mb > 100).ffill()
data = {}
for sym in ["BTCUSDT", "ETHUSDT"]:
    t = tr[tr.sym == sym]
    grid = pd.date_range(t.t_open.min().floor("15min"), t.t_close.max(), freq="15min")
    X, r = bs.build(sym, grid, grid[0] - pd.Timedelta(days=40), grid[-1])
    y = (their_size(tr, sym, grid) / mb.reindex(grid.floor("D")).values).clip(-3, 3).fillna(0)
    E = pd.DataFrame(dict(t=t.sort_values("t_open").t_open.values, side=t.sort_values("t_open").side.values))
    data[sym] = (X, y, r, E)
split = pd.Timestamp("2025-04-01")
Xtr = pd.concat([d[0][:split] for d in data.values()]); ytr = pd.concat([d[1][:split] for d in data.values()])
rule_cols = [c for c in Xtr.columns if c.split()[0] in bs.TF]
M = {"линейная": (Ridge(alpha=10.0).fit(Xtr[rule_cols], ytr), rule_cols),
     "бустинг": (HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=200, l2_regularization=1.0).fit(Xtr, ytr), list(Xtr.columns))}
rows = []
for mname, (mdl, cols) in M.items():
    for thr in [0.0, 0.1, 0.2, 0.4]:
        for fee in [0.0, 0.0002, 0.00055]:
            day, turn, f1s, ag = [], [], [], []
            for sym, (X, y, r, E) in data.items():
                p = pd.Series(mdl.predict(X[cols]), X.index)[split:]
                q = hysteresis(p, thr) if thr else p
                rr = r[split:]
                ret = q.shift(1).fillna(0) * rr - q.diff().abs().fillna(0) * fee
                day.append(ret.groupby(ret.index.floor("D")).sum())
                turn.append(q.diff().abs().sum() / ((q.index[-1] - q.index[0]).days / 365))
                s = np.sign(q).replace(0, np.nan).ffill().fillna(1); yt = y[split:]; m = yt != 0
                ag.append((s[m] == np.sign(yt[m])).mean())
                Et = E[E.t >= split]; rc, pc = match(Et, flips_of(s), np.timedelta64(1, "h")); f1s.append(2 * rc * pc / (rc + pc) if rc + pc else 0)
            port = (day[0] + day[1]).dropna(); i = port.index.intersection(ya.index)
            rows.append(dict(модель=mname, порог=thr, комиссия=f"{fee*100:.3f}%", оборот_в_год=round(float(np.mean(turn))), направление=round(float(np.mean(ag)), 3),
                             развороты_F1=round(float(np.mean(f1s)), 3), связь_дохода=round(float(port[i].corr(ya[i])), 2), наша=al.stats(port[i])))
pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 110)
R = pd.DataFrame(rows); print(R.to_string(index=False))
print(f"\nего на проверке: {al.stats(ya[ya.index >= split])}")
