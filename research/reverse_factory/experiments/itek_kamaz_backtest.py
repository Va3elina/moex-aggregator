"""Бот по найденным правилам ITEK Kamaz (слой A) на всей истории 03.2024–09.2026: заработали бы мы?

Правила: вход — RSI7 на 3-мин ≤ 15, цена ≥3% ниже максимума часа, за 2 ч −2% (лучшая тройка из перебора),
только когда по монете нет открытой кампании; докупки лимитками от цены входа
(PEPE −1.0/−2.4/−3.9/−5.5/−7.0/−8.5%, DOGE −1.4/−2.8/−4.5/−6.0/−7.8/−9.0%), объёмы 1 : 3.08 : 4.19 : 4.82 : 5.54 : 6.37 : 7.33;
выход — вся позиция лимиткой +1.94% (одна покупка) / +1.6% от средней (после докупок), через 12 ч после последней
покупки — по рынку. Счёт общий на две монеты (кросс-маржа), ликвидация при капитале ≤ 0.5% от позиций.
Деньги: полная лесенка по монете = L × половина капитала (L — «плечо на всю лесенку»); кусок фиксирован от стартового
депозита (спокойно) или растёт с капиталом (как витрина). Комиссии: лимитки 0.02%, рыночные 0.055%; финансирование — фактическое.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402

LV = {"1000PEPEUSDT": [-1.0, -2.4, -3.9, -5.5, -7.0, -8.5], "DOGEUSDT": [-1.4, -2.8, -4.5, -6.0, -7.8, -9.0]}
SIZES = np.array([1, 3.08, 4.19, 4.82, 5.54, 6.37, 7.33])
FULL = SIZES.sum()
MAKER, TAKER, MMR = 0.0002, 0.00055, 0.005
T0, T1 = pd.Timestamp("2024-03-01"), pd.Timestamp("2026-09-26")


def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def load():
    D = {}
    idx = None
    for s in LV:
        k = market.klines(s, "1", T0 - pd.Timedelta(days=1), T1).astype(float)
        idx = k.index if idx is None else idx.union(k.index)
        D[s] = k
    idx = idx[(idx >= T0) & (idx <= T1)]
    out = {}
    for s, k in D.items():
        k = k.reindex(idx.union(k.index)).sort_index(); k["c"] = k.c.ffill()
        for x in "ohl":
            k[x] = k[x].fillna(k.c)
        c3 = k.c.resample("3min", label="right", closed="right").last().dropna()
        r = rsi(c3, 7).reindex(k.index + pd.Timedelta(minutes=1), method="ffill").values
        hi60 = k.h.rolling(60).max().values; ch120 = (k.c / k.c.shift(120) - 1).values * 100
        sig = (r <= 15) & ((k.c.values / hi60 - 1) * 100 <= -3) & (ch120 <= -2)
        k["sig"] = sig
        k = k.reindex(idx)
        fr = funding(s, T0, T1)
        fm = np.zeros(len(idx)); pos = np.searchsorted(idx.values, fr.index.values); ok = pos < len(idx); fm[pos[ok]] = fr.values[ok]
        out[s] = dict(o=k.o.values, h=k.h.values, l=k.l.values, c=k.c.values, sig=k.sig.fillna(False).values.astype(bool), fund=fm)
    return idx, out


def run(idx, D, L=1.0, compound=False, E0=10_000.0, timer_h=12.0):
    S = list(D)
    st = {s: None for s in S}
    cash = E0; n = len(idx); eq = np.zeros(n); notl = np.zeros(n); liq = []; trades = 0; timers = 0
    for i in range(1, n):
        for s in S:
            x = st[s]
            if x and D[s]["fund"][i]:
                cash -= x["Q"] * D[s]["c"][i - 1] * D[s]["fund"][i]
        for s in S:
            d = D[s]; x = st[s]; o, h, l, c = d["o"][i], d["h"][i], d["l"][i], d["c"][i]
            if x is None:
                if d["sig"][i - 1]:
                    E = cash + sum(st[t]["Q"] * D[t]["c"][i - 1] - st[t]["cost"] for t in S if st[t])
                    base = (E if compound else E0) * 0.5 * L / FULL
                    if base <= 0:
                        continue
                    q = base / o
                    st[s] = x = dict(Q=q, cost=q * o, p0=o, k=0, last=i, unit=q)
                    cash -= q * o * TAKER; trades += 1
                continue
            lv = np.array(LV[s]) / 100
            while x["k"] < len(lv) and l <= x["p0"] * (1 + lv[x["k"]]):
                px = x["p0"] * (1 + lv[x["k"]]); q = x["unit"] * SIZES[x["k"] + 1]
                x["Q"] += q; x["cost"] += q * px; x["k"] += 1; x["last"] = i; cash -= q * px * MAKER; trades += 1
            avg = x["cost"] / x["Q"]
            tp = x["p0"] * 1.0194 if x["k"] == 0 else avg * 1.016
            if i > x["last"] and h >= tp:
                cash += (tp - avg) * x["Q"] - tp * x["Q"] * MAKER; st[s] = None; trades += 1; continue
            if (i - x["last"]) >= timer_h * 60:
                cash += (c - avg) * x["Q"] - c * x["Q"] * TAKER; st[s] = None; trades += 1; timers += 1
        Nl = sum(st[s]["Q"] * D[s]["l"][i] for s in S if st[s])
        El = cash + sum(st[s]["Q"] * D[s]["l"][i] - st[s]["cost"] for s in S if st[s])
        if Nl > 0 and El <= MMR * Nl:
            liq.append(idx[i]); cash = 0.0; st = {s: None for s in S}
            eq[i:] = 0; break
        eq[i] = cash + sum(st[s]["Q"] * D[s]["c"][i] - st[s]["cost"] for s in S if st[s])
        notl[i] = sum(st[s]["Q"] * D[s]["c"][i] for s in S if st[s])
    eq[0] = E0
    return pd.DataFrame(dict(equity=eq, notional=notl), idx), liq, trades, timers


if __name__ == "__main__":
    idx, D = load()
    rows = []
    for compound in (False, True):
        for L in (1, 2, 3, 5):
            R, liq, tr, tm = run(idx, D, L=L, compound=compound)
            d = R.resample("D").last().equity
            yr = {}
            for y in (2024, 2025, 2026):
                e = d[d.index.year == y]
                s0 = d[d.index < e.index[0]].iloc[-1] if (d.index < e.index[0]).any() else 10_000.0
                yr[str(y)] = f"{(e.iloc[-1] / s0 - 1) * 100:+.0f}%" if s0 > 0 else "—"
            dd = (d / d.cummax() - 1).min() * 100
            rows.append(dict(кусок="растёт с капиталом" if compound else "от стартового", плечо_лесенки=f"x{L}", **yr,
                             итог=f"{d.iloc[-1]:,.0f}$", просадка=f"{dd:.0f}%", ликвидация=liq[0].strftime("%d.%m.%y") if liq else "нет",
                             сделок=tr, по_таймеру=tm))
            print(rows[-1], flush=True)
    pd.set_option("display.width", 200)
    print(pd.DataFrame(rows).to_string(index=False))
