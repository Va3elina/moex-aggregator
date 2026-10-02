"""Kamaz A — поиск условия входа: пересечения условий на разных таймфреймах.

Библиотека одиночных условий (значения закрытых свечей своего ТФ, известны на закрытии минуты):
  RSI(n) на 1/3/5/15/30/60 мин ≤ x; ход цены за 5–240 мин ≤ −y; падение от максимума 60/240 мин;
  Боллинджер %B ≤ 0 (5/15/60 мин); CCI(20) ≤ −100/−150/−200 (5/15 мин); MFI(14) ≤ 20/30 (5/15);
  Стохастик %K ≤ 10/20 (3/5/15); всплеск объёма; биткоин за час; положение к EMA50/200 на 1ч.
Метрика: кампании, где ПЕРВЫЙ сигнал в свободное время (после выхода прошлой кампании) совпал со стартом ±6 мин;
«ложные» — первый сигнал раньше старта. Перебираем пары, лучшие пары дополняем третьим условием.
"""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402


def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def bars(k, tf):
    return k.resample(f"{tf}min", label="right", closed="right").agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna()


def conditions(k: pd.DataFrame, btc: pd.Series) -> dict[str, np.ndarray]:
    t_close = k.index + pd.Timedelta(minutes=1)
    C: dict[str, np.ndarray] = {}

    def put(name, series_on_tf, op, thr):
        v = series_on_tf.reindex(t_close, method="ffill").values
        C[name] = (v <= thr) if op == "<=" else (v >= thr)

    for tf in (1, 3, 5, 15, 30, 60):
        b = bars(k, tf)
        for n in (6, 7, 9, 14):
            r = rsi(b.c, n)
            for x in (15, 20, 25, 30, 35):
                put(f"RSI{n} {tf}м ≤{x}", r, "<=", x)
        if tf in (5, 15, 60):
            m = b.c.rolling(20).mean(); s = b.c.rolling(20).std()
            pb = (b.c - (m - 2 * s)) / (4 * s)
            put(f"%B {tf}м ≤0", pb, "<=", 0.0); put(f"%B {tf}м ≤0.1", pb, "<=", 0.1)
        if tf in (5, 15):
            tp = (b.h + b.l + b.c) / 3; ma = tp.rolling(20).mean(); md = (tp - ma).abs().rolling(20).mean()
            cci = (tp - ma) / (0.015 * md)
            for x in (-100, -150, -200):
                put(f"CCI20 {tf}м ≤{x}", cci, "<=", x)
            mf = tp * b.v; pos = mf.where(tp > tp.shift(1), 0).rolling(14).sum(); neg = mf.where(tp < tp.shift(1), 0).rolling(14).sum()
            mfi = 100 - 100 / (1 + pos / neg)
            for x in (20, 30):
                put(f"MFI14 {tf}м ≤{x}", mfi, "<=", x)
        if tf in (3, 5, 15):
            ll, hh = b.l.rolling(14).min(), b.h.rolling(14).max()
            kk = ((b.c - ll) / (hh - ll) * 100).rolling(3).mean()
            for x in (10, 20):
                put(f"Стох {tf}м ≤{x}", kk, "<=", x)
    c = k.c
    for n in (5, 15, 30, 60, 120, 240):
        ch = (c / c.shift(n) - 1) * 100; ch.index = t_close
        for y in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0):
            C[f"ход {n}м ≤−{y}%"] = (ch.values <= -y)
    for n in (60, 240):
        dd = (c / k.h.rolling(n).max() - 1) * 100; dd.index = t_close
        for y in (2, 3, 4, 5):
            C[f"от хая {n}м ≤−{y}%"] = (dd.values <= -y)
    vz = (k.v / k.v.rolling(60).mean()); vz.index = t_close
    for y in (2, 3, 5):
        C[f"объём ×{y}"] = (vz.values >= y)
    b1 = (btc / btc.shift(60) - 1) * 100; b1.index = t_close
    C["биткоин за час ≤−0.5%"] = b1.reindex(t_close).values <= -0.5
    C["биткоин за час ≤−1%"] = b1.reindex(t_close).values <= -1.0
    for span in (50, 200):
        e = bars(k, 60).c.ewm(span=span, adjust=False).mean().reindex(t_close, method="ffill").values
        C[f"цена ≥ EMA{span} 1ч −3%"] = (k.c.values / e - 1) * 100 >= -3
    return {kk: np.nan_to_num(v.astype(bool)) for kk, v in C.items()}


def evaluate(sig: np.ndarray, t_close: pd.DatetimeIndex, camps, t_first):
    st = t_close[sig]
    hit = fp = miss = 0
    prev = t_first
    for c in camps.itertuples():
        s = st[(st > prev) & (st <= c.t0 + pd.Timedelta(minutes=6))]
        if len(s) and abs((s[0] - c.t0).total_seconds()) <= 360:
            hit += 1
        elif len(s):
            fp += 1
        else:
            miss += 1
        prev = c.t1
    return hit, fp, miss


if __name__ == "__main__":
    T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
    K = T[(T.acc == "Kamaz") & T.sym.isin(["1000PEPE", "DOGE"]) & (T.t >= pd.Timestamp("2024-11-19"))]
    btc = market.klines("BTCUSDT", "1", "2024-11-10", "2025-01-17").astype(float).c
    D = {}
    for sym in ["1000PEPE", "DOGE"]:
        g = K[K.sym == sym]
        camps = g.groupby("camp").agg(t0=("t", "min"), t1=("t", "max")).sort_values("t0")
        k = market.klines(sym + "USDT", "1", "2024-11-10", "2025-01-17").astype(float)
        D[sym] = (camps, k.index + pd.Timedelta(minutes=1), conditions(k, btc.reindex(k.index).ffill()))
    names = list(D["DOGE"][2].keys())
    print("условий:", len(names))
    t_first = pd.Timestamp("2024-11-19")

    def score(combo):
        h = f = m = 0
        for sym, (camps, tc, C) in D.items():
            sig = np.logical_and.reduce([C[x] for x in combo])
            a, b, c = evaluate(sig, tc, camps, t_first); h += a; f += b; m += c
        return h, f, m

    single = []
    for x in names:
        h, f, m = score((x,))
        single.append((x, h, f, m))
    S = pd.DataFrame(single, columns=["условие", "совпал", "ложный", "нет"]).sort_values("совпал", ascending=False)
    print(S.head(10).to_string(index=False))
    # пары: берём условия, у которых «нет сигнала» мало (иначе они не могут быть частью правила)
    cand = S[S.нет <= 25].условие.tolist()
    print("кандидатов для пар:", len(cand))
    pairs = []
    for a, b in itertools.combinations(cand, 2):
        h, f, m = score((a, b))
        pairs.append((a, b, h, f, m))
    P = pd.DataFrame(pairs, columns=["у1", "у2", "совпал", "ложный", "нет"])
    P["цель"] = P.совпал - 0.5 * P.ложный
    P = P.sort_values("цель", ascending=False)
    pd.set_option("display.width", 220)
    print(P.head(15).to_string(index=False))
    top = P.head(40)
    triples = []
    for r in top.itertuples():
        for c in cand:
            if c in (r.у1, r.у2):
                continue
            h, f, m = score((r.у1, r.у2, c))
            triples.append((r.у1, r.у2, c, h, f, m))
    Tr = pd.DataFrame(triples, columns=["у1", "у2", "у3", "совпал", "ложный", "нет"])
    Tr["цель"] = Tr.совпал - 0.5 * Tr.ложный
    Tr = Tr.sort_values("цель", ascending=False).drop_duplicates(subset=["совпал", "ложный", "нет"])
    print(Tr.head(15).to_string(index=False))
    P.to_csv(ROOT / "inbox/private/work/itek/kamaz_trigger_pairs.csv", index=False)
    Tr.to_csv(ROOT / "inbox/private/work/itek/kamaz_trigger_triples.csv", index=False)
