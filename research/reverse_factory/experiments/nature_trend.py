"""Эксперимент 26.09.2026: можно ли торговать «природой» Algotoria / Quant Hill самим.

1. «По мотивам» — БЕЗ подгонки: равные веса простых трендовых правил (ход N свечей, EMA-пересечения,
   пробой) на BTC/ETH/SOL, дневные и 4-часовые, лонг и шорт, выравнивание по волатильности,
   комиссия Bybit тейкер 0.055% за сторону от оборота. 2021–2026.
2. «Копия» — веса NNLS подобраны под кривую цели на 1-й половине её истории, торговля на 2-й половине
   (вне подбора) с теми же комиссиями; сравнение с самой целью на том же отрезке.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import nnls

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market                      # noqa: E402
from factory.schema import load_target           # noqa: E402

FEE = 0.00055          # тейкер Bybit за сторону
SYMS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def components(k: pd.DataFrame, tag: str, horizons, emas, breakouts) -> dict[str, tuple[pd.Series, pd.Series]]:
    c, r = k.c.astype(float), k.c.astype(float).pct_change()
    vol = r.rolling(20).std()
    w = (vol.median() / vol).clip(upper=3).shift(1)
    out = {}
    for L in horizons:
        out[f"{tag} ход {L}"] = np.sign(c / c.shift(L) - 1)
    for f, s in emas:
        out[f"{tag} EMA{f}/{s}"] = np.sign(c.ewm(span=f).mean() - c.ewm(span=s).mean())
    for N in breakouts:
        hi, lo = k.h.rolling(N).max().shift(1), k.l.rolling(N).min().shift(1)
        out[f"{tag} пробой {N}"] = pd.Series(np.where(c > hi, 1, np.where(c < lo, -1, np.nan)), c.index).ffill().fillna(0)
    res = {}
    for name, sig in out.items():
        pos = (sig.shift(1) * w).fillna(0)              # сигнал на закрытии → позиция на следующей свече
        gross = pos * r
        cost = pos.diff().abs().fillna(0) * FEE
        res[name] = (gross.fillna(0), (gross - cost).fillna(0))
    return res


def daily(s: pd.Series) -> pd.Series:
    return s.groupby(s.index.floor("D")).sum()


def library(start, end):
    G, N = {}, {}
    for sym in SYMS:
        tag = sym.replace("USDT", "")
        kd = market.klines(sym, "D", start, end)
        k4 = market.klines(sym, "240", start, end)
        for name, (g, n) in components(kd, f"{tag} [день]", [3, 5, 10, 20], [(5, 20), (10, 50)], [20]).items():
            G[name], N[name] = g, n
        for name, (g, n) in components(k4, f"{tag} [4ч]", [5, 10, 20], [(5, 20)], []).items():
            G[name], N[name] = daily(g), daily(n)
    return pd.DataFrame(G), pd.DataFrame(N)


def stats(r: pd.Series) -> dict:
    r = r.dropna()
    e = (1 + r).cumprod()
    yrs = (r.index[-1] - r.index[0]).days / 365.25
    ye = e.resample("YE").last()
    yr = ye.pct_change()
    yr.iloc[0] = ye.iloc[0] - 1
    return dict(годовых=f"{e.iloc[-1] ** (1 / yrs) - 1:+.0%}", просадка=f"{(e / e.cummax() - 1).min():.0%}",
                Шарп=round(float(r.mean() / r.std() * np.sqrt(365)), 2),
                по_годам=" ".join(f"{k.year}:{v:+.0%}" for k, v in yr.items()))


def scale(r: pd.Series, vol=0.30) -> pd.Series:
    return r * (vol / (r.std() * np.sqrt(365)))


if __name__ == "__main__":
    G, N = library("2020-06-01", pd.Timestamp.now(tz="UTC").tz_localize(None))
    G, N = G["2021-01-01":].fillna(0), N["2021-01-01":].fillna(0)
    ens_g, ens_n = G.mean(axis=1), N.mean(axis=1)
    k = 0.30 / (ens_g.std() * np.sqrt(365))            # к 30% годовой волатильности (плечо ≈ k)
    print(f"=== 1. «По мотивам»: {G.shape[1]} правил BTC/ETH/SOL равными долями, 2021–2026, волатильность 30%/год (плечо ×{k:.1f})")
    print("   без комиссий:", stats(ens_g * k))
    print("   с комиссиями:", stats(ens_n * k))
    fam = {"ход": [c for c in N if " ход " in c], "EMA": [c for c in N if "EMA" in c], "пробой": [c for c in N if "пробой" in c]}
    for f, cols in fam.items():
        print(f"   только «{f}»:", stats(N[cols].mean(axis=1) * 0.30 / (N[cols].mean(axis=1).std() * np.sqrt(365))))
    for a in ["BTC", "ETH", "SOL"]:
        cols = [c for c in N if c.startswith(a)]
        print(f"   только {a}:", stats(N[cols].mean(axis=1) * 0.30 / (N[cols].mean(axis=1).std() * np.sqrt(365))))

    print("\n=== 2. Копия под конкретную стратегию: подбор на 1-й половине, торговля на 2-й (с комиссиями)")
    for slug in ["quant-hill", "algotoria", "syndicate"]:
        _, meta, eq = load_target(slug)
        y = eq[eq > 0].resample("D").last().pct_change().dropna()
        y = y[y.abs() < 0.9]
        idx = y.index.intersection(G.index)
        y, g, n = y[idx], G.loc[idx], N.loc[idx]
        half = idx[len(idx) // 2]
        w, _ = nnls(g[:half].values, y[:half].values)
        copy_oos = (n[half:] * w).sum(axis=1)
        tgt_oos = y[half:]
        copy_oos = copy_oos * (tgt_oos.std() / copy_oos.std())
        corr = float(tgt_oos.corr(copy_oos))
        ens_oos = ens_n[half:] * (tgt_oos.std() / ens_n[half:].std())
        print(f"   {meta['name']} ({half.date()} → {idx[-1].date()}): связь копии с оригиналом {corr:.2f}; "
              f"связь «по мотивам» с оригиналом {float(tgt_oos.corr(ens_oos)):.2f}")
        print(f"      оригинал:      {stats(tgt_oos)}")
        print(f"      копия:         {stats(copy_oos)}")
        print(f"      «по мотивам»:  {stats(ens_oos)}")
    (N.mean(axis=1) * k).to_frame("по_мотивам").to_csv(Path(__file__).with_suffix(".csv"))
