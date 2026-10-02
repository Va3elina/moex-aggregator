"""Защиты от обвала монеты для «спокойного Kamaz» (лонг):
- жёсткий стоп: закрыть всю позицию, если цена ушла на X% ниже цены первой покупки (с поправкой на размах монеты),
  исполнение по стоп-заявке с проскальзыванием 0,5% (при гэпе — от цены открытия минуты), после стопа пауза 24 ч;
- пропуск: не входить, если монета за сутки упала на 25% и больше.
Цена защиты в обычные годы (2021–2024 — 6 монет, 2024–2026 — 10 монет) и эффект в обвалах: LUNA 05.2022, FTT и SRM
11.2022 (минутки из тиков), искусственные −50% на DOGE, 10.10.2025 и он же вдвое глубже для портфеля 10 монет.
"""
from __future__ import annotations

import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import grid_engine as ge  # noqa: E402
from factory import market  # noqa: E402

V = {"без защиты": {},
     "стоп 15%": dict(hard_stop=15.0, stop_cooldown_h=24),
     "стоп 20%": dict(hard_stop=20.0, stop_cooldown_h=24),
     "стоп 25%": dict(hard_stop=25.0, stop_cooldown_h=24),
     "стоп 30%": dict(hard_stop=30.0, stop_cooldown_h=24),
     "стоп 40%": dict(hard_stop=40.0, stop_cooldown_h=24),
     "пропуск −25%": dict(crash_skip=25.0),
     "пропуск −15%": dict(crash_skip=15.0),
     "не больше счёта": dict(cap_to_equity=True),
     "фильтр биткоина": dict(btc_filter=True),
     "фильтр + пропуск −25%": dict(btc_filter=True, crash_skip=25.0),
     "фильтр + пропуск + стоп 30% + не больше счёта": dict(btc_filter=True, crash_skip=25.0, hard_stop=30.0, stop_cooldown_h=24, cap_to_equity=True)}
LS = (1.0, 3.0)
OLD = ["DOGEUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "BTCUSDT", "ETHUSDT"]
NEW = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT", "BTCUSDT", "ETHUSDT"]
PERIODS = [("2021-07-01", "2024-03-01", OLD), ("2024-03-01", "2026-09-27 11:00", NEW)]
TEN = NEW


def drange(sym, a):
    d = market.klines(sym, "D", pd.Timestamp(a) - pd.Timedelta(days=90), pd.Timestamp(a)).astype(float)
    return float(((d.h - d.l) / d.c).median())


def normal_job(args):
    """Одна монета за период: итог по годам и разбивка выходов для каждой защиты и плеча."""
    a, b, sym = args
    c = ge.Coin(sym, a, b)
    sc = min(1.0, c.daily_range / drange("DOGEUSDT", a)) if sym in ("BTCUSDT", "ETHUSDT") else 1.0
    out = {}
    for name, kw in V.items():
        for L in LS:
            E, C, liq, tr = ge.run(c, ge.P(side=1, L=L, scale=sc, **{"btc_filter": False, **kw}))
            y = ge.yearly(E)
            out[(name, L)] = dict(years={k: v for k, v in y.items() if k.isdigit()}, liq=liq is not None,
                                  stops=int((C.exit == "стоп").sum()), stop_pnl=float(C.pnl[C.exit == "стоп"].sum()),
                                  n=len(C), worst=float(C.pnl.min()) if len(C) else 0.0,
                                  days=E.resample("D").last().dropna().diff().fillna(E.resample("D").last().dropna().iloc[0] - 10_000))
    return a, sym, out


def crash_frames():
    import ticks_to_1m as t1
    cases = []
    for sym, a, b, a0, b0 in (("LUNAUSDT", "2022-04-28", "2022-05-12 10:00", "2022-04-26", "2022-05-12"),
                              ("FTTUSDT", "2022-10-30", "2022-11-13 11:00", "2022-10-28", "2022-11-13"),
                              ("SRMUSDT", "2022-10-30", "2022-11-15 02:00", "2022-10-28", "2022-11-15")):
        cases.append((sym.replace("USDT", ""), sym, t1.build(sym, a0, b0), a, b))
    base = market.klines("DOGEUSDT", "1", "2025-06-29", "2025-07-31").astype(float)
    T = pd.Timestamp("2025-07-10 12:00")
    for nm, hours in (("DOGE −50% за 12 ч", 12), ("DOGE −50% за 1 ч", 1)):
        k = base.copy(); t = (k.index - T).total_seconds() / 3600; f = np.ones(len(k))
        ramp = (t >= 0) & (t <= hours); f[ramp] = 1 - 0.5 * t[ramp] / hours; f[t > hours] = 0.5
        for col in "ohlc":
            k[col] = k[col] * f
        k["l"] = k[["o", "h", "l", "c"]].min(axis=1); k["h"] = k[["o", "h", "l", "c"]].max(axis=1)
        cases.append((nm, "DOGEUSDT", k, "2025-07-01", "2025-07-31"))
    return cases


def crash_job(case):
    nm, sym, k, a, b = case
    out = {}
    for L in LS:
        for vn, kw in V.items():
            E, C, liq, tr = ge.run(ge.FrameCoin(sym, k, a, b), ge.P(side=1, L=L, **{"btc_filter": False, **kw}))
            out[(vn, L)] = f"{E.iloc[-1] - 10_000:+,.0f}" + (" Л" if liq is not None else "")
    return nm, out


def oct10_job(args):
    """10.10.2025 для одной монеты: как было и вдвое глубже (ходы 20:00–24:00 в квадрате, ниже остаёмся)."""
    sym, deeper = args
    a, b = "2025-10-08", "2025-10-14"
    k = market.klines(sym, "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
    btc = market.klines("BTCUSDT", "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
    if deeper:
        w0, w1 = pd.Timestamp("2025-10-10 20:00"), pd.Timestamp("2025-10-11 00:00")
        p0 = k.c.asof(w0 - pd.Timedelta(minutes=1)); win = (k.index >= w0) & (k.index <= w1)
        end_ratio = k.c[win].iloc[-1] / p0
        for col in "ohlc":
            r = k[col] / p0
            k.loc[win, col] = p0 * r[win] ** 2
            k.loc[k.index > w1, col] = k.loc[k.index > w1, col] * end_ratio
        k["l"] = k[["o", "h", "l", "c"]].min(axis=1); k["h"] = k[["o", "h", "l", "c"]].max(axis=1)
    sc = 0.45 if sym in ("BTCUSDT", "ETHUSDT") else 1.0
    out = {}
    for vn, kw in V.items():
        for L in LS:
            E, C, liq, tr = ge.run(ge.FrameCoin(sym, k, a, b, btc_full=btc if sym != "BTCUSDT" else k), ge.P(side=1, L=L, scale=sc, **{"btc_filter": False, **kw}))
            out[(vn, L)] = (E.iloc[-1] - 10_000, liq is not None)
    return deeper, sym, out


if __name__ == "__main__":
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
    ctx = mp.get_context("spawn")
    with ctx.Pool(5) as pool:
        res_n = pool.map(normal_job, [(a, b, s) for a, b, syms in PERIODS for s in syms], chunksize=1)
        res_o = pool.map(oct10_job, [(s, d) for d in (False, True) for s in TEN], chunksize=1)
        res_c = pool.map(crash_job, crash_frames(), chunksize=1)

    print("=== Обычные годы: итог портфеля по годам, $ (2021–2023 — 6 монет по $10 000; 2024–2026 — 10 монет по $10 000)")
    rows = []
    for name in V:
        for L in LS:
            tot = {}; stops = 0; spnl = 0.0; n = 0; liqs = 0; worst = 0.0; days = []
            for a, sym, out in res_n:
                o = out[(name, L)]
                for y, v in o["years"].items():
                    tot[y] = tot.get(y, 0) + v
                stops += o["stops"]; spnl += o["stop_pnl"]; n += o["n"]; liqs += o["liq"]; worst = min(worst, o["worst"])
                days.append(o["days"])
            D = pd.concat(days, axis=1).sum(axis=1).sort_index()
            worst_day = D.min()
            rows.append({"защита": name, "плечо": f"x{L:g}", **{y: f"{v:+,}" for y, v in sorted(tot.items())},
                         "всего": f"{sum(tot.values()):+,}", "стопов": stops, "итог стопов": f"{spnl:+,.0f}",
                         "худшая сделка": f"{worst:+,.0f}", "худший день": f"{worst_day:+,.0f} ({D.idxmin():%d.%m.%y})", "ликвидаций": liqs})
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== Обвалы одной монеты: ячейка $10 000, итог, $ (Л — ликвидация); 10.10 — портфель 10 монет")
    for L in LS:
        rows = []
        for vn in V:
            r = {"защита": vn}
            for nm, out in res_c:
                r[nm] = out[(vn, L)]
            for deeper in (False, True):
                tot = sum(o[(vn, L)][0] for d, s_, o in res_o if d == deeper); nl = sum(o[(vn, L)][1] for d, s_, o in res_o if d == deeper)
                r["10.10" + (" ×2" if deeper else "")] = f"{tot:+,.0f}" + (f" ({nl} Л)" if nl else "")
            rows.append(r)
        print(f"плечо x{L:g}"); print(pd.DataFrame(rows).to_string(index=False))
