"""CryptosMX: поиск условий по всем таймфреймам сразу — «клон поведения» на минутах.

Каждая минута окна DOGE 16.01 09:10 – 03.02 05:00 (его позиция = позиция мастера): купил ли он в эту минуту,
продал ли. Признаки (всё известно на начало минуты):
  цена: ход за 1/3/5/10/15/30/60/120/240/480/1440 мин; новый минимум/максимум за 5/15/60/240 мин;
        место в диапазоне и расстояние до минимума/максимума 15/60/240/1440 мин; разброс 15/60/240;
        RSI 1м/5м/15м/1ч; отклонение от EMA20/50/200 на 1м/15м/1ч; объём к среднему 60 мин;
  биткоин: ход за 5/15/60/240 мин;
  его состояние: позиция к максимуму, цена к средней, к прошлой покупке и продаже, минут с них,
        сколько покупок/продаж за последний час, час суток.
Модель — градиентный бустинг, проверка блоками по времени (учимся на трёх четвертях, проверяем на четвёртой),
плюс монета, которой не было в обучении (PEPE), и дни после 03.02. Совпадение сделок — как раньше (±15/±60 мин).
→ ml_oof.parquet, ml_importance.csv
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402

OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"


def rsi(c: pd.Series, n: int = 14) -> pd.Series:
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def market_features(sym: str, a: str, b: str) -> pd.DataFrame:
    k = market.klines(sym, "1", a, b).astype(float)
    btc = market.klines("BTCUSDT", "1", a, b).astype(float).c.reindex(k.index).ffill()
    c, h, l, v = k.c, k.h, k.l, k.v
    X = pd.DataFrame(index=k.index)
    for n in (1, 3, 5, 10, 15, 30, 60, 120, 240, 480, 1440):
        X[f"ход_{n}м"] = (c / c.shift(n) - 1) * 100
    for n in (5, 15, 60, 240):
        X[f"новый_лой_{n}м"] = (l <= l.shift(1).rolling(n).min()).astype(float)
        X[f"новый_хай_{n}м"] = (h >= h.shift(1).rolling(n).max()).astype(float)
    for n in (15, 60, 240, 1440):
        lo, hi = l.rolling(n).min(), h.rolling(n).max()
        X[f"место_{n}м"] = (c - lo) / (hi - lo)
        X[f"от_лоя_{n}м"] = (c / lo - 1) * 100
        X[f"от_хая_{n}м"] = (c / hi - 1) * 100
    r1 = c.pct_change()
    for n in (15, 60, 240):
        X[f"разброс_{n}м"] = r1.rolling(n).std() * 100
    X["объём_к_60м"] = v / v.rolling(60).mean()
    X["объём5_к_60м"] = v.rolling(5).sum() / v.rolling(60).sum() * 12
    X["rsi_1м"] = rsi(c)
    for rule, name in (("5min", "5м"), ("15min", "15м"), ("1h", "1ч")):
        cc = c.resample(rule, label="right", closed="right").last()
        X[f"rsi_{name}"] = rsi(cc).reindex(k.index, method="ffill")
        for span in (20, 50, 200):
            e = cc.ewm(span=span, adjust=False).mean().reindex(k.index, method="ffill")
            X[f"к_ema{span}_{name}"] = (c / e - 1) * 100
    for span in (20, 50, 200):
        X[f"к_ema{span}_1м"] = (c / c.ewm(span=span, adjust=False).mean() - 1) * 100
    for n in (5, 15, 60, 240):
        X[f"btc_ход_{n}м"] = (btc / btc.shift(n) - 1) * 100
    # биткоин: где он в своём диапазоне и у каких уровнях (автор решает по «зонам» биткоина)
    bk = market.klines("BTCUSDT", "1", a, b).astype(float).reindex(k.index).ffill()
    for n in (60, 240, 1440, 4320, 10080):
        lo, hi = bk.l.rolling(n).min(), bk.h.rolling(n).max()
        X[f"btc_место_{n}м"] = (bk.c - lo) / (hi - lo)
        X[f"btc_от_хая_{n}м"] = (bk.c / hi - 1) * 100
        X[f"btc_от_лоя_{n}м"] = (bk.c / lo - 1) * 100
    X["btc_к_круглой_1000"] = (bk.c / 1000 - np.round(bk.c / 1000)) * 1000 / bk.c * 100
    X["btc_к_круглой_5000"] = (bk.c / 5000 - np.round(bk.c / 5000)) * 5000 / bk.c * 100
    for rule, name in (("1h", "1ч"), ("4h", "4ч")):
        cc = bk.c.resample(rule, label="right", closed="right").last()
        X[f"btc_rsi_{name}"] = rsi(cc).reindex(k.index, method="ffill")
    # вторая монета (PEPE) — общий фон мем-монет
    pe = market.klines("1000PEPEUSDT", "1", a, b).astype(float).c.reindex(k.index).ffill()
    for n in (15, 60, 240):
        X[f"pepe_ход_{n}м"] = (pe / pe.shift(n) - 1) * 100
    # структура: пробой минимумов/максимумов за N часов (прошлый экстремум — с зазором 30 мин) у DOGE и биткоина
    for nm, kk in (("", k), ("btc_", bk)):
        for N in (2, 6, 12, 24, 48):
            plo = kk.l.shift(30).rolling(N * 60).min(); phi = kk.h.shift(30).rolling(N * 60).max()
            X[f"{nm}ниже_лоя_{N}ч"] = (kk.c / plo - 1) * 100
            X[f"{nm}выше_хая_{N}ч"] = (kk.c / phi - 1) * 100
            newlo = (kk.l <= plo).astype(float); newhi = (kk.h >= phi).astype(float)
            grp_lo = newlo.cumsum(); grp_hi = newhi.cumsum()
            X[f"{nm}мин_с_нового_лоя_{N}ч"] = newlo.groupby(grp_lo).cumcount().clip(upper=1440)
            X[f"{nm}мин_с_нового_хая_{N}ч"] = newhi.groupby(grp_hi).cumcount().clip(upper=1440)
    X["час"] = X.index.hour
    # всё известно на НАЧАЛО минуты: сдвигаем на 1
    X = X.shift(1)
    X["px_open"] = k.o; X["px_low"] = l; X["px_high"] = h
    return X


def state_features(X: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
    """Его состояние на начало каждой минуты (по его исполнениям до этой минуты)."""
    f = fills.sort_values("t", kind="stable").copy(); f["m"] = f.t.dt.floor("min")
    g = f.groupby("m")
    st = pd.DataFrame(dict(pos=g.pos_after.last(), avg=g.avg_after.last()))
    idx = X.index
    S = pd.DataFrame(index=idx)
    S["pos"] = st.pos.reindex(idx).shift(1).ffill().fillna(0)
    S["avg"] = st.avg.reindex(idx).shift(1).ffill()
    lb = f[f.side == "buy"].groupby("m").mpx.last(); ls = f[f.side == "sell"].groupby("m").mpx.last()
    tb = pd.Series(lb.index, lb.index); ts = pd.Series(ls.index, ls.index)
    S["last_buy"] = lb.reindex(idx).shift(1).ffill(); S["last_sell"] = ls.reindex(idx).shift(1).ffill()
    S["t_buy"] = tb.reindex(idx).shift(1).ffill(); S["t_sell"] = ts.reindex(idx).shift(1).ffill()
    px = X.px_open
    out = pd.DataFrame(index=idx)
    out["позиция_к_макс"] = S.pos / S.pos.cummax().replace(0, np.nan)
    out["к_средней"] = (px / S.avg - 1) * 100
    out["к_прошлой_покупке"] = (px / S.last_buy - 1) * 100
    out["к_прошлой_продаже"] = (px / S.last_sell - 1) * 100
    out["мин_с_покупки"] = (idx - S.t_buy).dt.total_seconds() / 60
    out["мин_с_продажи"] = (idx - S.t_sell).dt.total_seconds() / 60
    nb = f[f.side == "buy"].groupby("m").size().reindex(idx, fill_value=0)
    ns = f[f.side == "sell"].groupby("m").size().reindex(idx, fill_value=0)
    out["покупок_за_60м"] = nb.shift(1).rolling(60, min_periods=1).sum()
    out["продаж_за_60м"] = ns.shift(1).rolling(60, min_periods=1).sum()
    out["y_buy"] = (nb > 0).astype(int); out["y_sell"] = (ns > 0).astype(int)
    return out


def load_fills(sym: str) -> pd.DataFrame:
    T = pd.read_parquet(OUT / "trades_full.parquet")
    T = T[(T.sym == sym) & (~T.manual)].copy()
    T["mpx"] = T.master_px.fillna(T.px)
    return T


def match_f1(t_his, t_mod, tol):
    import cryptos_replica as cr
    r, p = cr.match(np.sort(t_his), np.sort(t_mod), tol)
    return 2 * r * p / (r + p) if r + p else 0.0, r, p


def events_from_prob(p: pd.Series, thr: float, refractory: int) -> np.ndarray:
    """События: минуты, где вероятность выше порога; после события — пауза refractory минут."""
    v = p.values; out = []; last = -10 ** 9
    for i in np.where(v >= thr)[0]:
        if i - last >= refractory:
            out.append(i); last = i
    return p.index[out].values.astype("datetime64[m]").astype(np.int64)


if __name__ == "__main__":
    from sklearn.ensemble import HistGradientBoostingClassifier
    W0, W1 = pd.Timestamp("2025-01-16 09:10"), pd.Timestamp("2025-02-03 05:00")
    X = market_features("DOGEUSDT", "2025-01-13", "2025-02-12")
    F = load_fills("DOGEUSDT")
    S = state_features(X, F[F.t < W1])
    D = pd.concat([X, S], axis=1).loc[W0:W1 - pd.Timedelta(minutes=1)]
    D = D[D.позиция_к_макс.notna() | (D.index >= W0)]
    feats = [c for c in D.columns if c not in ("y_buy", "y_sell", "px_open", "px_low", "px_high")]
    blocks = np.array_split(np.arange(len(D)), 4)
    rows, imp = [], {}
    oof = pd.DataFrame(index=D.index, columns=["p_buy", "p_sell"], dtype=float)
    for side in ("buy", "sell"):
        y = D[f"y_{side}"].values
        for bi, test in enumerate(blocks):
            gap = 240
            train = np.concatenate([b for j, b in enumerate(blocks) if j != bi])
            train = train[(train < test[0] - gap) | (train > test[-1] + gap)]
            mdl = HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, max_leaf_nodes=15, min_samples_leaf=40,
                                                 l2_regularization=1.0, class_weight="balanced", random_state=0)
            mdl.fit(D[feats].iloc[train], y[train])
            oof.iloc[test, 0 if side == "buy" else 1] = mdl.predict_proba(D[feats].iloc[test])[:, 1]
    oof.to_parquet(OUT / "ml_oof.parquet")
    his = {s: D.index[D[f"y_{s}"] == 1].values.astype("datetime64[m]").astype(np.int64) for s in ("buy", "sell")}
    best = {}
    for side in ("buy", "sell"):
        p = oof[f"p_{side}"]
        for thr in (0.5, 0.6, 0.7, 0.8, 0.9, 0.95):
            for refr in (1, 3, 5, 10, 20):
                ev = events_from_prob(p, thr, refr)
                f15, r15, p15 = match_f1(his[side], ev, 15)
                f60, _, _ = match_f1(his[side], ev, 60)
                rows.append(dict(сторона=side, порог=thr, пауза=refr, событий=len(ev), его=len(his[side]), F1_15=round(f15, 3),
                                 полнота_15=round(r15, 3), точность_15=round(p15, 3), F1_60=round(f60, 3)))
    R = pd.DataFrame(rows)
    pd.set_option("display.width", 200)
    for side in ("buy", "sell"):
        print(f"\n=== {side}: проверка вне обучения (блоки по времени), лучшие пороги")
        print(R[R.сторона == side].sort_values("F1_15", ascending=False).head(6).to_string(index=False))
    # важность признаков: модель на всём окне, перестановкой
    from sklearn.inspection import permutation_importance
    for side in ("buy", "sell"):
        y = D[f"y_{side}"].values
        mdl = HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, max_leaf_nodes=15, min_samples_leaf=40,
                                             l2_regularization=1.0, class_weight="balanced", random_state=0).fit(D[feats], y)
        pi = permutation_importance(mdl, D[feats], y, n_repeats=3, random_state=0, scoring="average_precision")
        imp[side] = pd.Series(pi.importances_mean, feats).sort_values(ascending=False)
        print(f"\nважные признаки ({side}):", imp[side].head(15).round(4).to_dict())
    pd.DataFrame(imp).to_csv(OUT / "ml_importance.csv")
