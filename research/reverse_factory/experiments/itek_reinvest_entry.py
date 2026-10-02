"""REINVEST: условие входа по монете — отступ ниже скользящей средней на 1-мин свечах и др., с учётом занятости.
Монеты января 2025 (18.01–03.02 04:25). Метрика — первый сигнал в свободное время совпал со входом (±5 мин)."""
import itertools, sys
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from factory import market  # noqa

T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
R = T[(T.acc == "REINVEST") & (T.t < pd.Timestamp("2025-02-03 04:25")) & (T.sym != "1000PEPE")]
coins = [s for s, g in R.groupby("sym") if g.camp.nunique() >= 3]
D = {}
for sym in coins:
    g = R[R.sym == sym]
    camps = g.groupby("camp").agg(t0=("t", "min"), t1=("t", "max")).sort_values("t0")
    k = market.klines(sym + "USDT", "1", "2025-01-16", "2025-02-04").astype(float)
    tc = k.index + pd.Timedelta(minutes=1)
    C = {}
    for n in (30, 60, 120, 240):
        sma = k.c.rolling(n).mean()
        dev = ((k.c / sma - 1) * 100).values
        for d in (1.0, 1.4, 1.8, 2.2, 2.6, 3.0):
            C[f"ниже SMA{n} на {d}%"] = dev <= -d
    for n in (15, 60):
        ch = ((k.c / k.c.shift(n) - 1) * 100).values
        for d in (1.0, 1.5, 2.0, 2.5):
            C[f"ход {n}м ≤−{d}%"] = ch <= -d
    c3 = k.c.resample("3min", label="right", closed="right").last()
    d_ = c3.diff(); up = d_.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean(); dn = (-d_.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    r3 = (100 - 100 / (1 + up / dn)).reindex(tc, method="ffill").values
    for x in (25, 30, 35):
        C[f"RSI14 3м ≤{x}"] = r3 <= x
    D[sym] = (camps, tc, {kk: np.nan_to_num(v).astype(bool) for kk, v in C.items()})
names = list(D[coins[0]][2])

def score(combo):
    hit = fp = miss = 0
    for sym, (camps, tc, C) in D.items():
        sig = np.logical_and.reduce([C[x] for x in combo]); st = tc[sig]
        prev = pd.Timestamp("2025-01-18")
        for c in camps.itertuples():
            s = st[(st > prev) & (st <= c.t0 + pd.Timedelta(minutes=5))]
            if len(s) and abs((s[0] - c.t0).total_seconds()) <= 300: hit += 1
            elif len(s): fp += 1
            else: miss += 1
            prev = c.t1
    return hit, fp, miss

rows = [(x, "", *score((x,))) for x in names]
rows += [(a, b, *score((a, b))) for a, b in itertools.combinations(names, 2)]
S = pd.DataFrame(rows, columns=["у1", "у2", "совпал", "ложный", "нет"])
S["цель"] = S.совпал - 0.5 * S.ложный
pd.set_option("display.width", 200)
print("монет:", coins, "кампаний:", int(sum(len(v[0]) for v in D.values())))
print(S.sort_values("цель", ascending=False).head(12).to_string(index=False))
