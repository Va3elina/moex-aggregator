"""Проверка «спокойного Kamaz» на прочность: другие монеты, дорогие комиссии, по годам.

Каждая монета — отдельный счёт $10 000, кусок от стартового депозита, таймер 12 ч, лесенка x1 и x3.
Два способа перенести правила на новую монету:
  «как есть» — уровни докупок и пороги входа как у DOGE (без подстройки);
  «под размах» — уровни и пороги умножены на отношение дневного размаха монеты к DOGE (по первым 3 месяцам 2024).
Комиссии: обычные (лимитки 0.02%, рыночные 0.055%) и дорогие (0.04% / 0.10% + проскальзывание 0.1% на выходе по таймеру).
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
import itek_kamaz_backtest as kb  # noqa: E402

COINS = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT"]
DOGE_LV = np.array([-1.4, -2.8, -4.5, -6.0, -7.8, -9.0])


def coin_data(sym, scale):
    k = market.klines(sym, "1", kb.T0 - pd.Timedelta(days=1), kb.T1).astype(float)
    k = k[(k.index >= kb.T0 - pd.Timedelta(hours=3)) & (k.index <= kb.T1)]
    c3 = k.c.resample("3min", label="right", closed="right").last().dropna()
    r = kb.rsi(c3, 7).reindex(k.index + pd.Timedelta(minutes=1), method="ffill").values
    hi60 = k.h.rolling(60).max().values; ch120 = (k.c / k.c.shift(120) - 1).values * 100
    sig = (r <= 15) & ((k.c.values / hi60 - 1) * 100 <= -3 * scale) & (ch120 <= -2 * scale)
    k = k[k.index >= kb.T0]; sig = sig[-len(k):]
    fr = funding(sym, kb.T0, kb.T1)
    fm = np.zeros(len(k)); pos = np.searchsorted(k.index.values, fr.index.values); ok = pos < len(k); fm[pos[ok]] = fr.values[ok]
    return k.index, dict(o=k.o.values, h=k.h.values, l=k.l.values, c=k.c.values, sig=np.nan_to_num(sig).astype(bool), fund=fm)


def daily_range(sym):
    d = market.klines(sym, "D", "2024-03-01", "2024-06-01").astype(float)
    return float(((d.h - d.l) / d.c).median())


if __name__ == "__main__":
    base_rng = daily_range("DOGEUSDT")
    rows = []
    for sym in COINS:
        sc_v = daily_range(sym) / base_rng
        for mode, sc in (("как есть", 1.0), ("под размах", sc_v)):
            idx, d = coin_data(sym, sc)
            kb.LV[sym] = list(DOGE_LV * sc)
            for fees in ("обычные", "дорогие"):
                kb.MAKER, kb.TAKER = (0.0002, 0.00055) if fees == "обычные" else (0.0004, 0.0011)
                for L in (1, 3):
                    R, liq, tr, tm = kb.run(idx, {sym: d}, L=2 * L, compound=False)   # один счёт на одну монету: половина → вся
                    e = R.equity.resample("D").last()
                    ye = e.resample("YE").last(); prev = [10_000.0] + list(ye.values[:-1])
                    pnl = {f"{y.year}": int(v - p) for y, v, p in zip(ye.index, ye.values, prev)}
                    rows.append(dict(монета=sym.replace("USDT", ""), перенос=mode, размах=round(sc, 2), комиссии=fees, плечо=f"x{L}", **pnl,
                                     просадка=f"{(e / e.cummax() - 1).min() * 100:.0f}%", ликвидация=liq[0].strftime("%d.%m.%y") if liq else "нет",
                                     сделок=tr))
        print(sym, "готово", flush=True)
    R = pd.DataFrame(rows)
    R.to_csv(ROOT / "inbox/private/work/itek/kamaz_stress.csv", index=False)
    pd.set_option("display.width", 220); pd.set_option("display.max_rows", 200)
    print(R.to_string(index=False))
