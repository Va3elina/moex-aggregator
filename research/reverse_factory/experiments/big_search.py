"""Большой одновременный перебор: все семейства правил × все таймфреймы × условия рынка → ЕГО позиция.

Цель на каждой 15-минутке и монете: его плечо по монете = (сторона × объём $) / баланс счёта.
Признаки (только прошлое, известное на момент решения): ~80 правил на монету (ход, EMA, пробой канала,
Supertrend, MACD, наклон регрессии, Хейкен-Аши, Боллинджер, RSI на 15м/1ч/4ч/1д) + условия
(волатильность, режим EMA50/200, час, день недели, ход за 1ч/4ч/24ч/7д, место в канале, согласие групп).
Модели: линейная (веса правил — толкуемо) и градиентный бустинг (все условия, взаимодействия).
Обучение — первая половина истории, проверка — вторая. Метрики проверки: направление, развороты ±1ч,
размер в размер, доходность (прогноз = плечо, торгуем им напрямую), по годам. + где совпадает, где нет.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.linear_model import Ridge

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al                                   # noqa: E402
from algotoria_v2 import their_size                           # noqa: E402
from combo_search import flips_of                             # noqa: E402
from factory import market                                    # noqa: E402
from factory.schema import load_target                        # noqa: E402
from sar_search import donchian_dir, match, supertrend_dir    # noqa: E402

FEE = 0.0002
TF = {"15м": ("15", 15), "1ч": ("60", 60), "4ч": ("240", 240), "1д": ("D", 1440)}


def rules_tf(k: pd.DataFrame, tag: str, st=True) -> dict[str, pd.Series]:
    c, h, l, o = (k[x].astype(float) for x in ["c", "h", "l", "o"])
    R = {}
    for L in [4, 12, 24, 48, 96]:
        R[f"{tag} ход{L}"] = np.sign(c / c.shift(L) - 1)
    for f, s in [(5, 20), (10, 50), (20, 100)]:
        R[f"{tag} EMA{f}/{s}"] = np.sign(c.ewm(span=f).mean() - c.ewm(span=s).mean())
    for N in [20, 55, 100]:
        R[f"{tag} канал{N}"] = donchian_dir(k, N)
    if st:
        for n, m in [(10, 2), (10, 3), (20, 4), (20, 5)]:
            R[f"{tag} ST{n}×{m}"] = supertrend_dir(k, n, m)
    m_ = c.ewm(span=12).mean() - c.ewm(span=26).mean()
    R[f"{tag} MACD"] = np.sign(m_ - m_.ewm(span=9).mean())
    for N in [20, 50]:
        kern = np.arange(N) - (N - 1) / 2                        # наклон регрессии окна = Σ(j−x̄)·v_j (знак)
        num = np.convolve(c.values, kern[::-1], mode="valid")
        R[f"{tag} регрессия{N}"] = pd.Series(np.r_[np.full(N - 1, np.nan), np.sign(num)], c.index)
    ha_c = (o + h + l + c) / 4
    ha_o = ha_c.copy()
    for i in range(1, len(ha_o)):
        ha_o.iat[i] = (ha_o.iat[i - 1] + ha_c.iat[i - 1]) / 2
    R[f"{tag} ХейкенАши"] = np.sign(ha_c - ha_o).rolling(3).mean().apply(np.sign)
    mid, sd = c.rolling(20).mean(), c.rolling(20).std()
    R[f"{tag} Боллинджер-пробой"] = pd.Series(np.where(c > mid + 2 * sd, 1, np.where(c < mid - 2 * sd, -1, np.nan)), c.index).ffill()
    d = c.diff()
    rsi = 100 - 100 / (1 + d.clip(lower=0).ewm(alpha=1 / 14).mean() / (-d.clip(upper=0)).ewm(alpha=1 / 14).mean())
    R[f"{tag} RSI>50"] = np.sign(rsi - 50)
    return R


def build(sym: str, grid: pd.DatetimeIndex, a, b) -> pd.DataFrame:
    cols = {}
    for tag, (iv, m) in TF.items():
        k = market.klines(sym, iv, a, b)
        for name, s in rules_tf(k, tag, st=(tag != "1д")).items():
            s = s.copy(); s.index = s.index + pd.Timedelta(minutes=m)       # известно на закрытии свечи
            cols[name] = s.reindex(grid, method="ffill")
    X = pd.DataFrame(cols).fillna(0)
    # условия рынка
    k15 = market.klines(sym, "15", a, b); c = k15.c.astype(float)
    c.index = c.index + pd.Timedelta("15min"); c = c.reindex(grid, method="ffill")
    r = c.pct_change()
    kd = market.klines(sym, "D", a - pd.Timedelta(days=300), b).c.astype(float)
    e50, e200 = kd.ewm(span=50).mean(), kd.ewm(span=200).mean()
    reg = pd.DataFrame(dict(над_EMA50=(kd > e50).astype(float), над_EMA200=(kd > e200).astype(float),
                            вол_20д=kd.pct_change().rolling(20).std() * np.sqrt(365)))
    reg.index = reg.index + pd.Timedelta("1D")
    X = X.join(reg.reindex(grid, method="ffill"))
    for h_, n in [("1ч", 4), ("4ч", 16), ("24ч", 96), ("7д", 672)]:
        X[f"ход {h_} %"] = 100 * (c / c.shift(n) - 1)
    X["вол_1д_внутри"] = r.rolling(96).std() * np.sqrt(96 * 365)
    hi, lo = c.rolling(96 * 7).max(), c.rolling(96 * 7).min()
    X["место в недельном канале"] = (c - lo) / (hi - lo)
    X["час"] = grid.hour
    X["день недели"] = grid.dayofweek
    slow = [x for x in X.columns if x.startswith(("4ч", "1д"))]
    fast = [x for x in X.columns if x.startswith(("15м", "1ч"))]
    X["согласие медленных"] = X[slow].mean(axis=1)
    X["согласие быстрых"] = X[fast].mean(axis=1)
    return X.fillna(0), r.fillna(0)


def evaluate(name, pred: pd.Series, y: pd.Series, r: pd.Series, E: pd.DataFrame, lo, hi) -> dict:
    p, t_, rr = pred.loc[lo:hi], y.loc[lo:hi], r.loc[lo:hi]
    s = np.sign(p).replace(0, np.nan).ffill().fillna(1)
    m = t_ != 0
    Et = E[(E.t >= lo) & (E.t < hi)]
    rec, prec = match(Et, flips_of(s), np.timedelta64(1, "h"))
    ret = p.shift(1).fillna(0) * rr - p.diff().abs().fillna(0) * FEE
    return dict(модель=name, направление=round(float((s[m] == np.sign(t_[m])).mean()), 3),
                развороты_F1=round(2 * rec * prec / (rec + prec), 3) if rec + prec else 0.0, наших_разворотов=int(len(flips_of(s))),
                размер=round(float(p.corr(t_)), 3), ret=ret)


if __name__ == "__main__":
    tr, _, eq = load_target("algotoria")
    ya = eq[eq > 0].resample("D").last().pct_change().dropna()
    G = pd.read_csv(Path(__file__).with_name("algotoria_daily.csv"), index_col=0, parse_dates=True)
    mb = G.mb.where(G.mb > 100).ffill()
    data = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        t = tr[tr.sym == sym]
        grid = pd.date_range(t.t_open.min().floor("15min"), t.t_close.max(), freq="15min")
        a, b = grid[0] - pd.Timedelta(days=40), grid[-1]
        X, r = build(sym, grid, a, b)
        y = (their_size(tr, sym, grid) / mb.reindex(grid.floor("D")).values).clip(-3, 3).fillna(0)
        E = pd.DataFrame(dict(t=t.sort_values("t_open").t_open.values, side=t.sort_values("t_open").side.values))
        data[sym] = (X, y, r, E)
        print(f"{sym}: признаков {X.shape[1]}, строк {len(X)}")
    split = pd.Timestamp("2025-04-01")
    Xtr = pd.concat([d[0][:split] for d in data.values()]); ytr = pd.concat([d[1][:split] for d in data.values()])
    rule_cols = [c for c in Xtr.columns if c.split()[0] in TF]
    models = {}
    lin = Ridge(alpha=10.0).fit(Xtr[rule_cols], ytr); models["линейная: только правила"] = (lin, rule_cols)
    gb = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=200, l2_regularization=1.0)
    gb.fit(Xtr, ytr); models["бустинг: правила + условия"] = (gb, list(Xtr.columns))
    rows, rets = [], {}
    for mname, (mdl, cols) in models.items():
        day = []
        for sym, (X, y, r, E) in data.items():
            pred = pd.Series(mdl.predict(X[cols]), X.index)
            for part, lo, hi in [("подбор", X.index[0], split), ("ПРОВЕРКА", split, X.index[-1])]:
                ev = evaluate(mname, pred, y, r, E, lo, hi)
                if part == "ПРОВЕРКА":
                    day.append(ev["ret"].groupby(ev["ret"].index.floor("D")).sum())
                rows.append(dict(монета=sym[:3], часть=part, **{k: v for k, v in ev.items() if k != "ret"}))
        port = (day[0] + day[1]).dropna()
        i = port.index.intersection(ya.index)
        rets[mname] = (port[i], ya[i])
    R = pd.DataFrame(rows)
    pd.set_option("display.width", 230); pd.set_option("display.max_colwidth", 120)
    print(R.to_string(index=False))
    for mname, (p, t_) in rets.items():
        print(f"\nПРОВЕРКА, деньги — {mname}: связь дневной доходности {p.corr(t_):.2f}\n   наша: {al.stats(p)}\n   его:  {al.stats(t_)}")
    # что важно (линейная — веса; бустинг — перестановочная важность на проверке)
    w = pd.Series(lin.coef_, rule_cols).sort_values()
    print("\nлинейная, крупнейшие + веса:", {k: round(v, 3) for k, v in w.tail(10)[::-1].items()})
    print("линейная, крупнейшие − веса:", {k: round(v, 3) for k, v in w.head(6).items()})
    Xte = pd.concat([d[0][split:] for d in data.values()]).reset_index(drop=True)
    yte = pd.concat([d[1][split:] for d in data.values()]).reset_index(drop=True)
    samp = Xte.sample(min(40000, len(Xte)), random_state=0)
    pi = permutation_importance(gb, samp, yte.loc[samp.index], n_repeats=3, random_state=0, n_jobs=-1)
    imp = pd.Series(pi.importances_mean, samp.columns).sort_values(ascending=False)
    print("\nбустинг, что важнее всего на проверке:", {k: round(v, 4) for k, v in imp.head(15).items()})
    # где совпадает, где нет (бустинг, проверка)
    out = []
    for sym, (X, y, r, E) in data.items():
        Xt, yt = X[split:], y[split:]
        s = np.sign(pd.Series(gb.predict(Xt), Xt.index)); m = yt != 0
        ok = (s[m] == np.sign(yt[m]))
        C = Xt[m].assign(ok=ok.values)
        C["волатильность"] = pd.qcut(C["вол_1д_внутри"], 3, labels=["тихо", "средне", "бурно"])
        C["режим"] = np.where(C["над_EMA50"] > 0, "над EMA50", "под EMA50")
        C["сессия"] = pd.cut(C["час"], [-1, 7, 13, 20, 24], labels=["Азия 0–7", "Европа 8–13", "США 14–20", "ночь 21–23"])
        C["согласие"] = pd.cut(C["согласие медленных"].abs(), [-0.01, 0.3, 0.6, 1.01], labels=["спорят", "частично", "единодушно"])
        C["монета"] = sym[:3]
        out.append(C)
    C = pd.concat(out)
    for col in ["волатильность", "режим", "сессия", "согласие"]:
        print(f"\nсовпадение направления по «{col}»:", C.groupby(col, observed=True).ok.mean().round(3).to_dict(),
              "| доля времени:", C[col].value_counts(normalize=True).round(2).to_dict())
