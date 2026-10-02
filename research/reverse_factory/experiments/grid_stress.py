"""Тяжёлые сценарии для «спокойного Kamaz» (лонг, таймер 12 ч, кусок от стартовой ячейки $10 000).

1. Настоящие обвалы монет (как если бы бот их торговал): LUNA май 2022, FTT ноябрь 2022, SRM ноябрь 2022.
2. Искусственный обвал одной монеты на спокойном месяце DOGE (июль 2025): −50% за 12 ч без отскока; −50% за 1 ч;
   прокол −30% за 5 минут с полным возвратом.
3. Биржа не работает в худший день (10.10.2025) для портфеля 10 монет: окно 21:00–23:00 UTC без исполнений;
   проскальзывание 3% на рыночных заявках; оба сразу.
4. Обвал 10.10.2025 вдвое глубже для всех 10 монет (ходы с 20:00 до 24:00 удвоены, ниже остаёмся и потом).
Для каждого — плечо x1 и x3, с фильтром по биткоину и без.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import grid_engine as ge  # noqa: E402
from factory import market  # noqa: E402

TEN = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT", "BTCUSDT", "ETHUSDT"]


def run_frame(sym, k, a, b, L, filt, **kw):
    c = ge.FrameCoin(sym, k, a, b)
    E, C, liq, tr = ge.run(c, ge.P(side=1, L=L, btc_filter=filt, **kw))
    return E, C, liq


def show(name, E, C, liq, E0=10_000.0):
    worst = C.pnl.min() if len(C) else 0.0
    print(f"  {name:48s} итог {E.iloc[-1] - E0:+9,.0f}$ | худшая сделка {worst:+8,.0f}$ | мин. капитал {E.min() - E0:+9,.0f}$ | "
          f"{'ЛИКВИДАЦИЯ ' + liq.strftime('%d.%m %H:%M') if liq is not None else 'без ликвидации'}")


def real_collapses():
    print("\n=== 1. Настоящие обвалы монет (одна ячейка $10 000)")
    for sym, a, b in (("LUNAUSDT", "2022-05-01", "2022-05-14"), ("FTTUSDT", "2022-11-01", "2022-11-20"), ("SRMUSDT", "2022-11-01", "2022-11-20")):
        k = market.klines(sym, "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
        if k.empty:
            print("  нет данных", sym); continue
        print(f" {sym}: цена {k.c.iloc[0]:.4g} → {k.c.iloc[-1]:.4g} ({(k.c.iloc[-1] / k.c.iloc[0] - 1) * 100:+.0f}%)")
        for L in (1.0, 3.0):
            for filt in (False, True):
                E, C, liq = run_frame(sym, k, a, b, L, filt)
                show(f"x{L:g}, {'фильтр' if filt else 'без фильтра'}, сделок {len(C)}", E, C, liq)


def synthetic():
    print("\n=== 2. Искусственный обвал одной монеты (DOGE, июль 2025, одна ячейка)")
    a, b = "2025-07-01", "2025-07-31"
    base = market.klines("DOGEUSDT", "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
    T = pd.Timestamp("2025-07-10 12:00")
    scen = {}
    for name, hours, keep in (("−50% за 12 ч, без отскока", 12, True), ("−50% за 1 ч, без отскока", 1, True), ("прокол −30% за 5 мин, возврат", 5 / 60, False)):
        k = base.copy()
        t = (k.index - T).total_seconds() / 3600
        depth = 0.5 if keep else 0.3
        f = np.ones(len(k))
        ramp = (t >= 0) & (t <= hours)
        f[ramp] = 1 - depth * (t[ramp] / hours)
        if keep:
            f[t > hours] = 1 - depth
        else:
            back = (t > hours) & (t <= 2 * hours)
            f[back] = 1 - depth * (1 - (t[back] - hours) / hours)
        for col in "ohlc":
            k[col] = k[col] * f
        k["l"] = np.minimum(k.l, np.minimum(k.o, k.c)); k["h"] = np.maximum(k.h, np.maximum(k.o, k.c))
        scen[name] = k
    for name, k in scen.items():
        for L in (1.0, 3.0):
            E, C, liq = run_frame("DOGEUSDT", k, a, b, L, False)
            show(f"{name}, x{L:g}", E, C, liq)


def exchange_failures():
    print("\n=== 3. Биржа не работает в худший день (10.10.2025), портфель 10 монет по $10 000, без фильтра")
    a, b = "2025-10-08", "2025-10-14"
    coins = {s: ge.Coin(s, a, b) for s in TEN}
    doge = ge.Coin("DOGEUSDT", "2025-06-01", "2025-10-08").daily_range
    for name, kw in (("как было", {}), ("21:00–23:00 без исполнений", dict(outages=(("2025-10-10 21:00", "2025-10-10 23:00"),))),
                     ("проскальзывание 3% на рыночных", dict(slip=3.0)),
                     ("окно + проскальзывание 3%", dict(outages=(("2025-10-10 21:00", "2025-10-10 23:00"),), slip=3.0))):
        for L in (1.0, 3.0):
            tot = 0.0; liqs = 0; worst = 0.0
            for s, c in coins.items():
                sc = min(1.0, c.daily_range / doge) if s in ("BTCUSDT", "ETHUSDT") else 1.0
                E, C, liq = ge.run(c, ge.P(side=1, L=L, btc_filter=False, scale=sc, **kw))[:3]
                tot += E.iloc[-1] - 10_000; liqs += liq is not None; worst = min(worst, C.pnl.min() if len(C) else 0)
            print(f"  {name:36s} x{L:g}: портфель за 08–14.10 {tot:+9,.0f}$ ({tot / 1000:+.1f}% от $100k) | худшая сделка {worst:+,.0f}$ | ликвидаций {liqs}")


def deeper_crash():
    print("\n=== 4. Обвал 10.10.2025 вдвое глубже для всех 10 монет")
    a, b = "2025-10-08", "2025-10-14"
    w0, w1 = pd.Timestamp("2025-10-10 20:00"), pd.Timestamp("2025-10-11 00:00")
    btc_k = market.klines("BTCUSDT", "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
    for filt in (False, True):
        for L in (1.0, 3.0):
            tot = 0.0; liqs = 0; worst = 0.0
            for s in TEN:
                k = market.klines(s, "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
                p0 = k.c.asof(w0 - pd.Timedelta(minutes=1))
                win = (k.index >= w0) & (k.index <= w1)
                end_ratio = k.c[win].iloc[-1] / p0
                for col in "ohlc":
                    r = k[col] / p0
                    k.loc[win, col] = p0 * r[win] ** 2
                    k.loc[k.index > w1, col] = k.loc[k.index > w1, col] * end_ratio      # остаёмся ниже на ту же долю
                k["l"] = k[["o", "h", "l", "c"]].min(axis=1); k["h"] = k[["o", "h", "l", "c"]].max(axis=1)
                bk = btc_k.copy() if s != "BTCUSDT" else k
                c = ge.FrameCoin(s, k, a, b, btc_full=bk)
                E, C, liq, tr = ge.run(c, ge.P(side=1, L=L, btc_filter=filt, scale=0.45 if s in ("BTCUSDT", "ETHUSDT") else 1.0))
                tot += E.iloc[-1] - 10_000; liqs += liq is not None; worst = min(worst, C.pnl.min() if len(C) else 0)
            print(f"  {'фильтр' if filt else 'без фильтра':11s} x{L:g}: портфель за 08–14.10 {tot:+9,.0f}$ ({tot / 1000:+.1f}%) | худшая сделка {worst:+,.0f}$ | ликвидаций {liqs}")


if __name__ == "__main__":
    real_collapses()
    synthetic()
    exchange_failures()
    deeper_crash()
