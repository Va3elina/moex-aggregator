"""Kamaz A, вход — широкий перебор: таймфрейм RSI (1/3/5/15 мин), период, порог, «пересечение вниз» или «ниже уровня»,
падение за час (или без него), фильтр тренда старшего ТФ (цена к EMA 1ч/4ч), пауза после выхода.
Метрика та же: в каждой свободной паузе первый сигнал = старт (±6 мин)."""
import itertools, sys
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from factory import market  # noqa

def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)

T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
K = T[(T.acc == "Kamaz") & T.sym.isin(["1000PEPE", "DOGE"]) & (T.t >= pd.Timestamp("2024-11-19"))]
D = {}
for sym in ["1000PEPE", "DOGE"]:
    g = K[K.sym == sym]
    camps = g.groupby("camp").agg(t0=("t", "min"), t1=("t", "max")).sort_values("t0")
    k = market.klines(sym + "USDT", "1", "2024-11-10", "2025-01-17").astype(float)
    feats = {}
    for tf in (1, 3, 5, 15):
        c = k.c.resample(f"{tf}min", label="right", closed="right").last().dropna()
        for n in (6, 7, 9, 14):
            r = rsi(c, n)
            feats[(tf, n)] = (r.reindex(k.index + pd.Timedelta(minutes=1), method="ffill").values, r.shift(1).reindex(k.index + pd.Timedelta(minutes=1), method="ffill").values)
    t_idx = k.index + pd.Timedelta(minutes=1)          # момент закрытия минуты
    drop = ((k.c / k.c.shift(60) - 1) * 100).values
    ema = {}
    for rule, span in (("1h", 50), ("1h", 200), ("4h", 50)):
        cc = k.c.resample(rule, label="right", closed="right").last()
        e = cc.ewm(span=span, adjust=False).mean().reindex(t_idx, method="ffill").values
        ema[(rule, span)] = (k.c.values / e - 1) * 100
    D[sym] = (camps, t_idx, feats, drop, ema)
rows = []
for tf, n, thr, cross, dthr, trend, cool in itertools.product((1, 3, 5, 15), (6, 7, 9, 14), (20, 25, 30), (False, True), (None, 1.5, 2.0, 2.5),
                                                            (None, ("1h", 50), ("1h", 200), ("4h", 50)), (0, 30, 120)):
    hit = fp = miss = tot = 0
    for sym, (camps, t_idx, feats, drop, ema) in D.items():
        r, rprev = feats[(tf, n)]
        sig = (r <= thr)
        if cross: sig &= (rprev > thr)
        if dthr is not None: sig &= (drop <= -dthr)
        if trend is not None: sig &= (ema[trend] > -99)  # заглушка ниже
        if trend is not None: sig = sig & (ema[trend] >= -3.0)   # не глубже 3% под EMA — «рынок растёт»
        st = t_idx[sig]
        prev_end = pd.Timestamp("2024-11-19")
        for c in camps.itertuples():
            lo = prev_end + pd.Timedelta(minutes=cool)
            s = st[(st > lo) & (st <= c.t0 + pd.Timedelta(minutes=6))]
            tot += 1
            if len(s) and abs((s[0] - c.t0).total_seconds()) <= 360: hit += 1
            elif len(s): fp += 1
            else: miss += 1
            prev_end = c.t1
    rows.append(dict(tf=tf, n=n, порог=thr, пересечение=cross, падение=dthr, тренд=str(trend), пауза=cool, совпал=hit, ложный=fp, нет_сигнала=miss, доля=round(hit / tot, 3)))
R = pd.DataFrame(rows).sort_values(["доля", "ложный"], ascending=[False, True])
pd.set_option("display.width", 220)
print(R.head(20).to_string(index=False))
R.to_csv(ROOT / "inbox/private/work/itek/kamaz_entry_search.csv", index=False)
