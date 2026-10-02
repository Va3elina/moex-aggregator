"""ITEK: общий пусковой сигнал «рынок пролился» для всех ботов (основной счёт, Kamaz, REINVEST).

Входы трёх ботов склеиваем в «события рынка» (входы в пределах 3 мин — одно событие). Ищем условие на уровне рынка:
  биткоин: ход за 5/15/30/60 мин, RSI 3м/5м; корзина 8 альтов (DOGE, PEPE, SOL, XRP, BONK, SHIB, ADA, WIF) — средний ход
  за 5/15/30/60 мин; «широта» — сколько из 8 монет за 15 мин упали на 1.5%+; самая просевшая монета корзины.
Сигнал = условие выполнено (с паузой 30 мин между сигналами). Совпадение: событие ITEK в ±5 мин от сигнала.
Полнота — доля событий ITEK с сигналом рядом; точность — доля сигналов, рядом с которыми был вход ITEK.
Проверка: подбор на 19.10–15.12.2024, проверка на 16.12.2024–03.02.2025.
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

A, B = pd.Timestamp("2024-10-19"), pd.Timestamp("2025-02-03 04:25")
SPLIT = pd.Timestamp("2024-12-16")
BASKET = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT"]


def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def features():
    btc = market.klines("BTCUSDT", "1", A - pd.Timedelta(days=2), B).astype(float)
    idx = btc.index
    F = pd.DataFrame(index=idx)
    for n in (5, 15, 30, 60):
        F[f"btc_{n}"] = (btc.c / btc.c.shift(n) - 1) * 100
    for tf in (3, 5):
        c = btc.c.resample(f"{tf}min", label="right", closed="right").last()
        F[f"btc_rsi{tf}"] = rsi(c, 7).reindex(idx + pd.Timedelta(minutes=1), method="ffill").values
    rets = {}
    for s in BASKET:
        c = market.klines(s, "1", A - pd.Timedelta(days=2), B).astype(float).c.reindex(idx).ffill()
        for n in (5, 15, 30, 60):
            rets[(s, n)] = (c / c.shift(n) - 1) * 100
    for n in (5, 15, 30, 60):
        F[f"корзина_{n}"] = pd.concat([rets[(s, n)] for s in BASKET], axis=1).mean(axis=1)
        F[f"худшая_{n}"] = pd.concat([rets[(s, n)] for s in BASKET], axis=1).min(axis=1)
    F["широта_15"] = (pd.concat([rets[(s, 15)] for s in BASKET], axis=1) <= -1.5).sum(axis=1)
    return F.shift(1).loc[A:B]


def events():
    T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
    E = T[(T.side == "buy") & (T.k_buy == 1) & (T.t >= A) & (T.t <= B)].sort_values("t")
    grp = (E.t.diff() > pd.Timedelta(minutes=3)).cumsum()
    ev = E.groupby(grp).agg(t=("t", "min"), n=("t", "size"), счета=("acc", lambda s: ",".join(sorted(set(s)))))
    return ev


def evaluate(sig: pd.Series, ev_t: np.ndarray, refr=30):
    t = sig.index[sig.values]
    keep, last = [], None
    for x in t:
        if last is None or (x - last) >= pd.Timedelta(minutes=refr):
            keep.append(x); last = x
    st = np.array(keep, dtype="datetime64[ns]")
    if len(st) == 0:
        return 0.0, 0.0, 0
    evn = np.sort(ev_t)
    j = np.searchsorted(st, evn); d1 = np.abs(evn - st[np.clip(j, 0, len(st) - 1)]); d2 = np.abs(evn - st[np.clip(j - 1, 0, len(st) - 1)])
    rec = (np.minimum(d1, d2) <= np.timedelta64(5, "m")).mean()
    j = np.searchsorted(evn, st); d1 = np.abs(st - evn[np.clip(j, 0, len(evn) - 1)]); d2 = np.abs(st - evn[np.clip(j - 1, 0, len(evn) - 1)])
    prec = (np.minimum(d1, d2) <= np.timedelta64(5, "m")).mean()
    return rec, prec, len(st)


if __name__ == "__main__":
    F = features(); ev = events()
    print("событий рынка (входы ботов ITEK, склеенные по 3 мин):", len(ev), "из них с несколькими входами:", int((ev.n > 1).sum()))
    conds = {}
    for n in (5, 15, 30, 60):
        for y in (0.5, 1.0, 1.5, 2.0, 3.0):
            conds[f"биткоин {n}м ≤−{y}%"] = F[f"btc_{n}"] <= -y
            conds[f"корзина {n}м ≤−{y}%"] = F[f"корзина_{n}"] <= -y
            conds[f"худшая монета {n}м ≤−{y * 1.5:g}%"] = F[f"худшая_{n}"] <= -y * 1.5
    for k in (2, 3, 4, 5, 6):
        conds[f"широта: ≥{k} из 8 упали на 1.5% за 15м"] = F["широта_15"] >= k
    for x in (20, 25, 30):
        conds[f"RSI7 биткоина 3м ≤{x}"] = F["btc_rsi3"] <= x
        conds[f"RSI7 биткоина 5м ≤{x}"] = F["btc_rsi5"] <= x
    tr = (F.index < SPLIT); te = ~tr
    evt_tr = ev[ev.t < SPLIT].t.values; evt_te = ev[ev.t >= SPLIT].t.values
    rows = []
    names = list(conds)
    for a in names:
        r, p, n = evaluate(conds[a][tr], evt_tr)
        rows.append((a, "", r, p, n))
    for a, b in itertools.combinations(names, 2):
        s = conds[a] & conds[b]
        r, p, n = evaluate(s[tr], evt_tr)
        rows.append((a, b, r, p, n))
    Rr = pd.DataFrame(rows, columns=["у1", "у2", "полнота", "точность", "сигналов"])
    Rr["F1"] = 2 * Rr.полнота * Rr.точность / (Rr.полнота + Rr.точность).replace(0, np.nan)
    top = Rr.sort_values("F1", ascending=False).head(12).copy()
    out = []
    for r in top.itertuples():
        s = conds[r.у1] & (conds[r.у2] if r.у2 else True)
        rr, pp, nn = evaluate(s[te], evt_te)
        out.append(dict(условие=r.у1 + (" И " + r.у2 if r.у2 else ""), подбор_полнота=round(r.полнота, 2), подбор_точность=round(r.точность, 2),
                        проверка_полнота=round(rr, 2), проверка_точность=round(pp, 2), сигналов_проверка=nn))
    pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 90)
    print(pd.DataFrame(out).to_string(index=False))
