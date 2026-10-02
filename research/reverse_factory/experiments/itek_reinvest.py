"""ITEK REINVEST (январь 2025, до ручного закрытия Вадима 03.02 04:29): докупки и выходы при его входах.

Модель (входы берём его — проверяем, как бот ведёт кампанию):
  докупка: при первом касании «прошлая покупка × (1 − s%)», кусок = первый кусок кампании (объём одного ордера не растёт);
  выход: вся позиция на «средняя × (1 + tp%)», tp своя для монеты (подбираем по монете) или общая;
  таймер: через H часов после последней покупки — по рынку (H = 12 или без таймера).
Сверка: его события и наши той же стороны в ±5 мин и ±0.3% по цене.
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

T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
R = T[(T.acc == "REINVEST") & (T.t < pd.Timestamp("2025-02-03 04:25")) & (~T.manual) & (~T.liq)].copy()
KL = {}


def kl(sym):
    if sym not in KL:
        KL[sym] = market.klines(sym + "USDT", "1", "2025-01-17", "2025-02-04").astype(float)
    return KL[sym]


def sim(c: pd.DataFrame, s, tp, H):
    sym = c.sym.iloc[0]; k = kl(sym); idx = k.index; l, h, cl = k.l.values, k.h.values, k.c.values
    b0 = c[c.side == "buy"].iloc[0]
    i = idx.searchsorted(b0.t.floor("min")); end = min(idx.searchsorted(c.t.max().floor("min")) + 720, len(idx) - 1)
    Q, cost, last_px, last_i = b0.qty, b0.qty * b0.px, b0.px, i
    ev = [("buy", b0.t, b0.px)]
    for j in range(i + 1, end):
        while l[j] <= last_px * (1 - s / 100):
            px = last_px * (1 - s / 100); Q += b0.qty; cost += b0.qty * px; last_px = px; last_i = j; ev.append(("buy", idx[j], px))
        avg = cost / Q
        if h[j] >= avg * (1 + tp / 100) and j > last_i:
            ev.append(("sell", idx[j], avg * (1 + tp / 100))); break
        if H and (j - last_i) >= H * 60:
            ev.append(("sell", idx[j], cl[j])); break
    return ev


def match(his, mod, tol_min=5, tol_px=0.3):
    used = [False] * len(mod); hit = 0
    for side, t, px in his:
        for q, (s2, t2, p2) in enumerate(mod):
            if not used[q] and s2 == side and abs((t2 - t).total_seconds()) <= tol_min * 60 and abs(p2 / px - 1) * 100 <= tol_px:
                used[q] = True; hit += 1; break
    return hit, sum(used)


if __name__ == "__main__":
    camps = [c for _, c in R.groupby("camp") if (c.side == "buy").any() and (c.side == "sell").any()]
    rows = []
    for s, tp, H in itertools.product([1.4, 1.8, 2.1, 2.5, 3.0], [1.0, 1.5, 2.0, 2.5], [0, 12]):
        per = {}
        for c in camps:
            his_b = [("buy", r.t, r.px) for r in c[c.side == "buy"].iloc[1:].itertuples()]
            his_s = [("sell", r.t, r.px) for r in c[c.side == "sell"].itertuples()]
            mod = sim(c, s, tp, H)
            mb = [m for m in mod[1:] if m[0] == "buy"]; ms = [m for m in mod if m[0] == "sell"]
            hb, ub = match(his_b, mb); hs, us = match(his_s, ms)
            sym = c.sym.iloc[0]
            a = per.setdefault(sym, [0, 0, 0, 0, 0, 0])
            a[0] += len(his_b); a[1] += hb; a[2] += len(mb); a[3] += len(his_s); a[4] += hs; a[5] += len(ms)
        for sym, a in per.items():
            rows.append(dict(монета=sym, шаг=s, тейк=tp, таймер=H, докупок_его=a[0], докупок_совпало=a[1], докупок_наших=a[2],
                             продаж_его=a[3], продаж_совпало=a[4], продаж_наших=a[5]))
    D = pd.DataFrame(rows)
    D["F1_докупки"] = 2 * D.докупок_совпало / (D.докупок_его + D.докупок_наших).clip(lower=1)
    D["F1_выходы"] = 2 * D.продаж_совпало / (D.продаж_его + D.продаж_наших).clip(lower=1)
    D["цель"] = D.F1_докупки * D.докупок_его + D.F1_выходы * D.продаж_его
    best = D.sort_values("цель", ascending=False).groupby("монета").head(1).sort_values("докупок_его", ascending=False)
    pd.set_option("display.width", 220)
    print(best.round(2).to_string(index=False))
    tot = best[["докупок_его", "докупок_совпало", "продаж_его", "продаж_совпало"]].sum()
    print(f"\nвсего: докупки {tot.докупок_совпало}/{tot.докупок_его}, продажи {tot.продаж_совпало}/{tot.продаж_его}")
    D.to_csv(ROOT / "inbox/private/work/itek/reinvest_fit.csv", index=False)
