"""ITEK Kamaz, слой A (1000PEPE, DOGE) — полный бот и сверка сделка в сделку.

Правила (из разбора копий):
  вход: падение за час ≥ D% и RSI(n) на 3-мин ≤ R (лучшее из перебора itek_kamaz_entry2.py), только когда по монете нет кампании;
  докупки: лимитки от цены входа — PEPE −1.0/−2.4/−3.9/−5.5/−7.0/−8.5%, DOGE −1.4/−2.8/−4.5/−6.0/−7.8/−9.0%;
           объёмы к первому: ×3.08, затем ×1.36, ×1.15, ×1.15, ×1.15;
  выход: вся позиция одной лимиткой +1.94% от цены входа (без докупок) или +1.6% от средней (после докупок);
         через 12 ч после последней докупки — по рынку.
Два режима сверки:
  «вход как у него» — старт кампании берём из его сделок (проверяем докупки и выходы);
  «полный бот» — вход тоже по правилу.
Совпадение: его событие и наше той же стороны в ±5 мин (вход/докупка/выход) и цена ±0.3%.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

LV = {"1000PEPE": [-1.0, -2.4, -3.9, -5.5, -7.0, -8.5], "DOGE": [-1.4, -2.8, -4.5, -6.0, -7.8, -9.0]}
MULT = [3.08, 1.36, 1.15, 1.15, 1.15, 1.15]


def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def run(sym, k, entries=None, D=2.5, R=20, n=7, tf=5, tp1=1.94, tpn=1.6, timer_h=12.0):
    """entries — список времён входа (режим «вход как у него») или None (по правилу)."""
    o, h, l, c = (k[x].values for x in "ohlc")
    idx = k.index
    ctf = k.c.resample(f"{tf}min", label="right", closed="right").last().dropna()
    r = rsi(ctf, n).reindex(idx + pd.Timedelta(minutes=1), method="ffill").values
    drop = ((k.c / k.c.shift(60) - 1) * 100).values
    want = {t.floor("min"): px for t, px in entries} if entries is not None else None
    ev = []
    i, N = 61, len(c)
    lv = np.array(LV[sym]) / 100
    while i < N - 1:
        start = (idx[i] in want) if want is not None else (drop[i] <= -D and r[i] <= R)
        if not start:
            i += 1; continue
        p0 = c[i] if want is None else None
        if want is not None:                                      # его вход — по его цене
            p0 = want[idx[i]]
        q, cost, kf, last_add = 1.0, p0, 0, i
        ev.append(("buy", idx[i], p0))
        sizes = np.cumprod([1.0] + MULT)
        j = i + 1
        while j < N:
            while kf < len(lv) and l[j] <= p0 * (1 + lv[kf]):
                px = p0 * (1 + lv[kf]); qq = sizes[kf + 1] - sizes[kf] if False else sizes[kf + 1]
                q += qq; cost += px * qq; kf += 1; last_add = j
                ev.append(("buy", idx[j], px))
            avg = cost / q
            tp = p0 * (1 + tp1 / 100) if kf == 0 else avg * (1 + tpn / 100)
            if j > last_add and h[j] >= tp:
                ev.append(("sell", idx[j], tp)); break
            if kf > 0 and (j - last_add) >= timer_h * 60:
                ev.append(("sell", idx[j], c[j])); break
            j += 1
        i = j + 1
    return pd.DataFrame(ev, columns=["side", "t", "px"])


def match(his: pd.DataFrame, mod: pd.DataFrame, tol_min=5, tol_px=0.3):
    mod = mod.reset_index(drop=True)
    used = np.zeros(len(mod), bool); ok = 0
    for r in his.itertuples():
        m = mod[(mod.side == r.side) & ((mod.t - r.t).abs() <= pd.Timedelta(minutes=tol_min)) & ((mod.px / r.px - 1).abs() * 100 <= tol_px)]
        m = m[~used[m.index]]
        if len(m):
            used[m.index[0]] = True; ok += 1
    return ok / len(his), used.sum() / max(len(mod), 1)


if __name__ == "__main__":
    T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
    K = T[(T.acc == "Kamaz") & T.sym.isin(["1000PEPE", "DOGE"]) & (T.t >= pd.Timestamp("2024-11-19"))].sort_values("t", kind="stable")
    rows = []
    for sym in ["1000PEPE", "DOGE"]:
        his = K[K.sym == sym][["side", "t", "px", "k_buy"]].reset_index(drop=True)
        k = market.klines(sym + "USDT", "1", "2024-11-18", "2025-01-17").astype(float)
        k = k[k.index >= pd.Timestamp("2024-11-19")]
        ent = list(zip(his[(his.side == "buy") & (his.k_buy == 1)].t, his[(his.side == "buy") & (his.k_buy == 1)].px))
        for mode, e in (("вход как у него", ent), ("полный бот", None)):
            mod = run(sym, k, entries=e).reset_index(drop=True)
            for kind, hh in (("входы", his[(his.side == "buy") & (his.k_buy == 1)]), ("докупки", his[(his.side == "buy") & (his.k_buy > 1)]),
                             ("выходы", his[his.side == "sell"]), ("все", his)):
                mm = mod if kind == "все" else mod[mod.side == ("sell" if kind == "выходы" else "buy")]
                rc, pr = match(hh, mm)
                rows.append(dict(монета=sym, режим=mode, что=kind, его=len(hh), наших=len(mm), совпало_его=f"{rc:.0%}", точность=f"{pr:.0%}"))
    pd.set_option("display.width", 200)
    print(pd.DataFrame(rows).to_string(index=False))
