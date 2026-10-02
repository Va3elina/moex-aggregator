"""CryptosMX-лесенка на длинной истории: DOGE + 1000PEPE одним счётом (кросс-маржа), 03.2024 – 09.2026.

Лесенка — подобранная на его сделках (cryptos_replica_fit.py). Деньги:
  * кусок покупки = k × капитал (в момент покупки), «ведро» ×m;
  * плечо L = потолок: сумма позиций ≤ L × капитал, иначе заявка не ставится;
  * ликвидация (кросс): капитал на минимуме минуты ≤ 0.5% от суммы позиций → счёт обнуляется;
    после ликвидации — новый депозит (считаем, сколько раз пришлось «заводить заново»);
  * комиссия лимиток 0.02%, финансирование — фактическое Bybit раз в 8 ч.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402

SYMS = ["DOGEUSDT", "1000PEPEUSDT"]
T0, T1 = pd.Timestamp("2024-03-01"), pd.Timestamp("2026-09-26")
MAKER, MMR = 0.0002, 0.005
BEST = dict(sb=0.75, a=2.5, sd=0.35, f=0.10, rs=2.5, z=5, m=15, trig=2.0)


def load(syms=SYMS):
    ks = {s: market.klines(s, "1", T0 - pd.Timedelta(hours=2), T1) for s in syms}
    idx = ks[syms[0]].index
    for s in syms[1:]:
        idx = idx.union(ks[s].index)
    idx = idx[(idx >= T0) & (idx <= T1)]
    D = {}
    for s, k in ks.items():
        k = k.reindex(idx.union(k.index)).sort_index()
        k["c"] = k.c.ffill(); k["o"] = k.o.fillna(k.c); k["h"] = k.h.fillna(k.c); k["l"] = k.l.fillna(k.c)
        r1h = (k.c / k.c.shift(60) - 1) * 100
        k = k.reindex(idx)
        fr = funding(s, T0, T1)
        fmask = np.zeros(len(idx)); pos = np.searchsorted(idx.values, fr.index.values)
        ok = pos < len(idx); fmask[pos[ok]] = fr.values[ok]
        D[s] = dict(o=k.o.values, h=k.h.values, l=k.l.values, c=k.c.values, r1h=r1h.reindex(idx).values, fund=fmask)
    return idx, D


def run(idx, D, p=BEST, k=0.01, L=3.0, E0=10_000.0, refill=True, compound=True, through=0.0, fee=MAKER, one_side=False):
    sb, a, sd, f, rs = p["sb"] / 100, p["a"] / 100, p["sd"] / 100, p["f"], p["rs"] / 100
    z, m, trig = p["z"] / 100, p["m"], p["trig"]
    S = list(D)
    st = {s: dict(Q=0.0, cost=0.0, bn=np.nan, sn=np.nan, Qref=None, started=False) for s in S}
    cash, deposits = E0, E0
    n = len(idx)
    eq = np.zeros(n); notl = np.zeros(n)
    liq, skipped, fills = [], 0, 0
    for i in range(1, n):
        # 1) финансирование
        for s in S:
            fr = D[s]["fund"][i]
            if fr and st[s]["Q"] > 0:
                cash -= st[s]["Q"] * D[s]["c"][i - 1] * fr
        # 2) лесенки
        for s in S:
            x, d = st[s], D[s]
            o, h, l, c = d["o"][i], d["h"][i], d["l"][i], d["c"][i]
            if not x["started"]:
                if d["r1h"][i - 1] <= -trig:
                    x["started"] = True; x["bn"] = o; x["sn"] = np.inf
                else:
                    continue
            if x["Q"] <= 0 and x["started"]:                   # вне позиции: вход на откате rs% от хая после выхода
                x["hflat"] = max(x.get("hflat", 0.0), d["h"][i - 1])
                x["bn"] = max(x["bn"], x["hflat"] * (1 - rs))
            acted = False
            for side in (("s", "b") if c < o else ("b", "s")):
                if one_side and acted:                           # строгий режим: в одной минуте только одна сторона
                    break
                f0 = fills
                if side == "b":
                    while l <= x["bn"] * (1 - through):
                        px = x["bn"]
                        E = cash + sum(st[t]["Q"] * (px if t == s else D[t]["c"][i - 1]) - st[t]["cost"] for t in S)
                        N = sum(st[t]["Q"] * (px if t == s else D[t]["c"][i - 1]) for t in S)
                        avg = x["cost"] / x["Q"] if x["Q"] > 0 else px
                        usd = k * (E if compound else E0) * (m if (x["Q"] > 0 and px <= avg * (1 - z)) else 1)
                        if E > 0 and N + usd <= L * E:
                            q = usd / px
                            x["Q"] += q; x["cost"] += px * q; cash -= usd * fee; fills += 1
                            x["sn"] = x["cost"] / x["Q"] * (1 + a); x["Qref"] = None
                        else:
                            skipped += 1
                        x["bn"] = px * (1 - sb)
                else:
                    while x["Q"] > 0 and h >= x["sn"] * (1 + through):
                        if x["Qref"] is None:
                            x["Qref"] = x["Q"]
                        px = x["sn"]; avg = x["cost"] / x["Q"]
                        q = min(x["Q"], f * x["Qref"])
                        if x["Q"] - q < 0.02 * x["Qref"]:
                            q = x["Q"]
                        cash += (px - avg) * q - px * q * fee; fills += 1
                        x["Q"] -= q; x["cost"] = avg * x["Q"]; x["hflat"] = px
                        x["sn"] = px * (1 + sd); x["bn"] = px * (1 - rs)
                acted = acted or fills > f0
        # 3) ликвидация (кросс) по минимумам минуты
        Nlow = sum(st[s]["Q"] * D[s]["l"][i] for s in S)
        Elow = cash + sum(st[s]["Q"] * D[s]["l"][i] - st[s]["cost"] for s in S)
        if Nlow > 0 and Elow <= MMR * Nlow:
            liq.append((idx[i], cash + sum(st[s]["Q"] * D[s]["c"][i - 1] - st[s]["cost"] for s in S), Nlow))
            for s in S:
                st[s].update(Q=0.0, cost=0.0, Qref=None, started=False, bn=np.nan, sn=np.nan, hflat=0.0)
            cash = 0.0
            if refill:
                cash = E0; deposits += E0
            else:                                              # один депозит: счёт умер, дальше нули
                eq[i:] = 0.0; notl[i:] = 0.0; eq[0] = E0
                return pd.DataFrame(dict(equity=eq, notional=notl), idx), liq, deposits, skipped, fills
        eq[i] = cash + sum(st[s]["Q"] * D[s]["c"][i] - st[s]["cost"] for s in S)
        notl[i] = sum(st[s]["Q"] * D[s]["c"][i] for s in S)
    eq[0] = E0
    return pd.DataFrame(dict(equity=eq, notional=notl), idx), liq, deposits, skipped, fills


def stats(R, liq, deposits, E0=10_000.0):
    """Деньги: чистый итог с учётом довнесений после ликвидаций; по годам — прибыль к капиталу на начало года
    (плюс довнесения в году); просадка — по капиталу внутри жизни одного депозита."""
    d = R.resample("D").last()
    liq_days = {t.normalize() for t, *_ in liq}
    out = dict(итог_чистыми=round(d.equity.iloc[-1] - deposits), внесено=round(deposits), ликвидаций=len(liq))
    for y in sorted(set(d.index.year)):
        e = d.equity[d.index.year == y]
        start = d.equity[d.index < e.index[0]].iloc[-1] if (d.index < e.index[0]).any() else E0
        dep = E0 * sum(1 for t in liq_days if t.year == y)
        out[str(y)] = f"{(e.iloc[-1] - start - dep) / (start + dep) * 100:+.0f}%"
    peak = d.equity.cummax()
    seg = np.cumsum([1 if t in liq_days else 0 for t in d.index])       # отрезки жизни депозита
    dd = min((d.equity[seg == g] / d.equity[seg == g].cummax() - 1).min() for g in set(seg))
    lev = d.notional / d.equity.clip(lower=1)
    out.update(просадка_макс=f"{dd * 100:.0f}%", плечо_среднее=round(float(lev.mean()), 2), плечо_макс=round(float(lev.max()), 1),
               в_рынке=f"{(d.notional > 0).mean() * 100:.0f}%")
    return out


if __name__ == "__main__":
    idx, D = load()
    print("минут:", len(idx), idx[0], idx[-1])
    rows = []
    for k in [0.005, 0.01, 0.02]:
        for L in [1, 2, 3, 5]:
            R, liq, dep, sk, fills = run(idx, D, BEST, k=k, L=L)
            s = stats(R, liq, dep)
            y = R.equity.resample("YE").last()
            rows.append(dict(кусок=f"{k * 100:.1f}%", плечо=f"x{L}", **s, пропущено_заявок=sk, сделок=fills,
                             даты_ликвидаций=", ".join(t.strftime("%d.%m.%y") for t, *_ in liq)))
            R.to_parquet(Path(__file__).resolve().parents[1] / "inbox" / "private" / "work" / "cryptosmx" / f"bt_k{k}_L{L}.parquet")
            print(rows[-1], flush=True)
    pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 80)
    print(pd.DataFrame(rows).to_string(index=False))
