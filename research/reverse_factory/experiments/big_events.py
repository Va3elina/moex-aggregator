"""Исполнение рывками (как он): разворот только когда прогноз ушёл за порог θ на другую сторону;
размер ступенями δ: доливка при росте уверенности на шаг, сокращение — только при падении на 2 шага.
Модели — как в big_search (обучение до 04.2025, проверка после). Смотрим оборот, сверку и деньги с комиссией."""
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


def events(p: pd.Series, theta: float, delta: float, max_lev: float = 3.0) -> pd.Series:
    v, out = p.values, np.zeros(len(p))
    side, size = 1.0 if v[0] >= 0 else -1.0, 0.0
    for i, x in enumerate(v):
        if side > 0 and x < -theta or side < 0 and x > theta:          # разворот
            side, size = -side, delta
        conf = max(side * x, 0.0)
        if conf >= size + delta:                                         # доливка ступенью
            size = min(max_lev, size + delta * np.floor((conf - size) / delta))
        elif conf <= size - 2 * delta:                                   # сокращение, только заметное
            size = max(delta, size - delta * np.floor((size - conf) / delta - 1))
        out[i] = side * max(size, delta)
    return pd.Series(out, p.index)


if __name__ == "__main__":
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
         "бустинг": (HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=200,
                                                   l2_regularization=1.0).fit(Xtr, ytr), list(Xtr.columns))}
    # его собственный оборот (для ориентира): сумма |изменений плеча| по его позиции
    his_turn = np.mean([(d[1][split:].diff().abs().sum()) / ((d[1].index[-1] - split).days / 365) for d in data.values()])
    rows = []
    for mname, (mdl, cols) in M.items():
        preds = {s: pd.Series(mdl.predict(d[0][cols]), d[0].index)[split:] for s, d in data.items()}
        for theta in [0.05, 0.1, 0.2, 0.3]:
            for delta in [0.1, 0.2, 0.3, 0.5]:
                for fee in [0.0002, 0.00055]:
                    day, turn, f1s, ag, cs = [], [], [], [], []
                    for sym, (X, y, r, E) in data.items():
                        q = events(preds[sym], theta, delta)
                        rr, yt = r[split:], y[split:]
                        ret = q.shift(1).fillna(0) * rr - q.diff().abs().fillna(0) * fee
                        day.append(ret.groupby(ret.index.floor("D")).sum())
                        turn.append(q.diff().abs().sum() / ((q.index[-1] - q.index[0]).days / 365))
                        s = np.sign(q); m = yt != 0
                        ag.append((s[m] == np.sign(yt[m])).mean()); cs.append(q.corr(yt))
                        rc, pc = match(E[E.t >= split], flips_of(s), np.timedelta64(1, "h")); f1s.append(2 * rc * pc / (rc + pc) if rc + pc else 0)
                    port = (day[0] + day[1]).dropna(); i = port.index.intersection(ya.index)
                    rows.append(dict(модель=mname, θ=theta, δ=delta, комиссия=f"{fee * 100:.3f}%", оборот=round(float(np.mean(turn))),
                                     направление=round(float(np.mean(ag)), 3), развороты=round(float(np.mean(f1s)), 3), размер=round(float(np.mean(cs)), 3),
                                     связь=round(float(port[i].corr(ya[i])), 2), наша=al.stats(port[i]), _cagr=float((1 + port[i]).prod() ** (365 / len(i)) - 1)))
    R = pd.DataFrame(rows)
    pd.set_option("display.width", 260); pd.set_option("display.max_colwidth", 100)
    for fee in ["0.020%", "0.055%"]:
        print(f"\n=== комиссия {fee}: лучшие по доходности, и рядом — по сверке")
        sub = R[R.комиссия == fee]
        print(sub.sort_values("_cagr", ascending=False).head(6).drop(columns="_cagr").to_string(index=False))
    print(f"\nего оборот (изменения плеча по BTC/ETH): ~{his_turn:.0f} в год; его на проверке: {al.stats(ya[ya.index >= split])}")
