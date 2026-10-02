"""CryptosMX — повтор «как он делает» и жёсткая сверка с его сделками (копии Вадима, DOGE 16.01–03.02.2025).

Модель (гипотеза по копиям): лесенка лимиток вокруг средней цены, без стопа.
  * Покупки: одинаковые куски u каждые sb% вниз от последней покупки.
    После фазы продаж лесенка покупок начинается заново: от последней продажи −rs% (rule="sell")
    или от средней цены −ra% (rule="avg").
    «Ведро»: если цена ниже средней на z% — кусок ×m.
  * Продажи: кусками f от позиции; первая — на a% выше средней, дальше каждые +sd%.
    После любой покупки лесенка продаж пересчитывается от новой средней.
  * Позиция меньше половины куска = вышел; новый вход — по сигналу «пролив» (ход за час ≤ −trig%) или сразу.

Сверка (то же, что у Algotoria, но для лестницы): совпадение покупок/продаж по времени (±15 мин, ±60 мин),
действие в 15-минутке (купил/продал/ничего), связь пути позиции. База — модель со случайным сдвигом во времени.
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

PRIV = ROOT / "inbox" / "private"
W0, W1 = pd.Timestamp("2025-01-16 09:10"), pd.Timestamp("2025-02-03 05:00")   # чистое окно DOGE (до ручного закрытия Вадима)


def his_events(sym="DOGEUSDT", t0=W0, t1=W1) -> pd.DataFrame:
    L = pd.read_parquet(PRIV / "work" / "cryptosmx" / "ledger.parquet")
    L = L[(L.sym == sym) & (L.t >= t0) & (L.t < t1)].copy()
    L["q"] = np.where(L.kind == "open", L.qty, -L.qty)
    return L[["t", "kind", "q", "px", "pos_after", "avg"]].reset_index(drop=True)


def simulate(o, h, l, c, r1h, p: dict, start_px=None, i0=1):
    """Минутный прогон. Возвращает события (i, +1/−1, qty, price) и путь позиции.
    Позиция в ноль не «выключает» модель: лесенка покупок остаётся стоять (от последней продажи / средней).
    Если start_px не задан — первый вход по сигналу «пролив» (ход за час ≤ −trig%)."""
    sb, a, sd, f = p["sb"] / 100, p["a"] / 100, p["sd"] / 100, p["f"]
    rule, rs, ra = p["rule"], p.get("rs", 3) / 100, p.get("ra", 0) / 100
    z, m, trig = p.get("z", 99) / 100, p.get("m", 1), p.get("trig", 2.0)
    u = 1.0
    n = len(c)
    Q = cost = 0.0
    buy_next = sell_next = np.nan
    Qref = None
    hflat = 0.0
    ev, pos = [], np.zeros(n)
    if start_px is not None:                                    # вход в ту же минуту, что и он
        Q, cost = u, start_px * u
        buy_next, sell_next = start_px * (1 - sb), start_px * (1 + a)
        ev.append((i0 - 1, 1, u, start_px))
    for i in range(i0, n):
        if Q <= 1e-12 and np.isnan(buy_next):                   # ещё не входили: ждём пролив
            if r1h[i - 1] <= -trig:
                px = o[i]; Q, cost = u, px * u
                buy_next, sell_next = px * (1 - sb), px * (1 + a)
                ev.append((i, 1, u, px))
            pos[i] = Q
            continue
        if Q <= 1e-12 and rule == "sell":                       # вне позиции: лесенка подтягивается за ценой
            hflat = max(hflat, h[i - 1])                        # (вход на откате rs% от хая после выхода)
            buy_next = max(buy_next, hflat * (1 - rs))
        seq = ("s", "b") if c[i] < o[i] else ("b", "s")         # падающая минута: сначала хай, потом лой
        for side in seq:
            if side == "b":
                while l[i] <= buy_next:
                    px = buy_next
                    avg = cost / Q if Q > 1e-12 else px
                    q = u * (m if (Q > 1e-12 and px <= avg * (1 - z)) else 1)
                    Q += q; cost += px * q
                    ev.append((i, 1, q, px))
                    buy_next = px * (1 - sb)
                    sell_next = cost / Q * (1 + a)
                    Qref = None
            else:
                while Q > 1e-12 and h[i] >= sell_next:
                    if Qref is None:
                        Qref = Q
                    px = sell_next; avg = cost / Q
                    q = min(Q, f * Qref)
                    if Q - q < 0.05 * u:
                        q = Q
                    Q -= q; cost = avg * Q
                    ev.append((i, -1, q, px))
                    hflat = px
                    sell_next = px * (1 + sd)
                    buy_next = px * (1 - rs) if rule == "sell" else avg * (1 - ra)
        pos[i] = Q
    return ev, pos


def match(t_his, t_mod, tol):
    """Жадное сопоставление один-к-одному по времени: доля его событий с парой и доля наших с парой."""
    if len(t_his) == 0 or len(t_mod) == 0:
        return 0.0, 0.0
    used = np.zeros(len(t_mod), bool); hit = 0
    for t in t_his:
        j = np.searchsorted(t_mod, t)
        best, bd = -1, tol + 1
        for k in (j - 2, j - 1, j, j + 1):
            if 0 <= k < len(t_mod) and not used[k]:
                d = abs(t_mod[k] - t)
                if d <= tol and d < bd:
                    best, bd = k, d
        if best >= 0:
            used[best] = True; hit += 1
    return hit / len(t_his), used.sum() / len(t_mod)


def f1(r, p):
    return 2 * r * p / (r + p) if r + p else 0.0


def score(ev, pos, idx, H, shift=0):
    """Сверка с его событиями H. idx — минутный индекс окна. shift — сдвиг наших событий (база)."""
    tm = np.array([idx[min(max(i + shift, 0), len(idx) - 1)].value // 60_000_000_000 for i, *_ in ev])
    sm = np.array([s for _, s, *_ in ev])
    th = H.t.values.astype("datetime64[m]").astype(np.int64)
    out = {}
    for side, nm in [(1, "покупки"), (-1, "продажи")]:
        hs = np.sort(th[(H.q > 0).values if side == 1 else (H.q < 0).values])
        ms = np.sort(tm[sm == side])
        for tol in (15, 60):
            r, p = match(hs, ms, tol)
            out[f"{nm}±{tol}м"] = round(f1(r, p), 3)
            out[f"{nm}±{tol}м_r"], out[f"{nm}±{tol}м_p"] = round(r, 2), round(p, 2)
        out[f"{nm}_n"] = len(ms)
    # 15-минутки: действие купил/продал/ничего
    bars = pd.date_range(idx[0].floor("15min"), idx[-1], freq="15min")
    hb = pd.Series(H.q.values, H.t.dt.floor("15min")).groupby(level=0).sum().reindex(bars, fill_value=0)
    mb = pd.Series([s * q for _, s, q, _ in ev], idx[[min(max(i + shift, 0), len(idx) - 1) for i, *_ in ev]].floor("15min")).groupby(level=0).sum().reindex(bars, fill_value=0)
    act = (hb != 0) | (mb != 0)
    out["действие_15м"] = round(float((np.sign(hb[act]) == np.sign(mb[act])).mean()), 3) if act.any() else 0
    hp = H.set_index("t").pos_after.resample("15min").last().ffill().reindex(bars).ffill().fillna(0)
    mp = pd.Series(pos, idx).resample("15min").last().reindex(bars).ffill().fillna(0)
    out["позиция_связь"] = round(float(hp.corr(mp)), 3)
    return out


def grid():
    for sb, a, sd, f, rr, zm in itertools.product([0.25, 0.4, 0.55, 0.75, 1.0], [0.25, 0.5, 1.0, 1.5, 2.5], [0.1, 0.2, 0.35, 0.6],
                                                   [0.02, 0.04, 0.08, 0.15, 0.3],
                                                   [("sell", 1), ("sell", 2), ("sell", 3.5), ("sell", 5), ("avg", -1), ("avg", 0), ("avg", 1), ("avg", 2.5)],
                                                   [(99, 1), (5, 5), (5, 15)]):
        p = dict(sb=sb, a=a, sd=sd, f=f, rule=rr[0], z=zm[0], m=zm[1])
        p["rs" if rr[0] == "sell" else "ra"] = rr[1]
        yield p


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "DOGEUSDT"
    H = his_events(sym)
    k = market.klines(sym, "1", W0 - pd.Timedelta(hours=2), W1)
    k = k[k.index >= W0.floor("min")]
    o, h, l, c = (k[x].values.astype(float) for x in "ohlc")
    r1h = np.r_[np.full(60, np.nan), c[60:] / c[:-60] - 1] * 100
    start_px = float(H.px.iloc[0])
    rows = []
    for p in grid():
        ev, pos = simulate(o, h, l, c, r1h, p, start_px)
        s = score(ev, pos, k.index, H.iloc[1:])
        s["цель"] = (s["покупки±15м"] + s["продажи±15м"]) / 2 * 0.7 + 0.3 * max(s["позиция_связь"], 0)
        rows.append({**p, **s})
    R = pd.DataFrame(rows).sort_values("цель", ascending=False)
    R.to_csv(PRIV / "work" / "cryptosmx" / "replica_grid.csv", index=False)
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
    cols = ["sb", "a", "sd", "f", "rule", "rs", "ra", "z", "m", "покупки±15м", "покупки±60м", "продажи±15м", "продажи±60м",
            "покупки_n", "продажи_n", "действие_15м", "позиция_связь", "цель"]
    print(f"его событий: покупок {int((H.q > 0).sum()) - 1}, продаж {int((H.q < 0).sum())}; вариантов {len(R)}")
    print(R[cols].head(15).to_string(index=False))
    best = R.iloc[0].to_dict()
    p = {k2: best[k2] for k2 in ["sb", "a", "sd", "f", "rule", "z", "m"]}
    p["rs" if p["rule"] == "sell" else "ra"] = best["rs" if p["rule"] == "sell" else "ra"]
    ev, pos = simulate(o, h, l, c, r1h, p, start_px)
    rng = np.random.default_rng(0)
    base = [score(ev, pos, k.index, H.iloc[1:], shift=int(rng.integers(-720, 720))) for _ in range(30)]
    print("\nбаза (наши события сдвинуты на случайные ±12 ч):",
          {kk: round(float(np.mean([b[kk] for b in base])), 3) for kk in ["покупки±15м", "покупки±60м", "продажи±15м", "продажи±60м", "действие_15м"]})
