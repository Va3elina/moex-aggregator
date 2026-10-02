"""CryptosMX, «метод из канала»: можно ли вычислить его зоны машиной?

Его сетап (2023–2024): после импульса тянет Фибо от основания до хая и ставит зоны на 0.5 / 0.382 / 0.236
(с декабря 2024 — от 0.618). Проверка: для каждого его Фибо-сетапа на лонг берём 1ч свечи Bybit ДО поста,
находим «последний импульс» (хай за N часов, основание — минимум за M часов до хая) и считаем середину трёх зон.
Сравниваем с серединой его зон (emid из разбора картинок). База — «зоны» на той же глубине от цены, но случайные:
середина = цена × (1 − его типичная глубина), то есть без знания графика.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market  # noqa: E402

PRIV = Path(__file__).resolve().parents[1] / "inbox" / "private"
LEVELS = {"старые 0.5/0.382/0.236": (0.5, 0.382, 0.236), "новые 0.618/0.5/0.382": (0.618, 0.5, 0.382)}


def fibo_mid(k: pd.DataFrame, t: pd.Timestamp, N: int, M: int, lv) -> tuple[float, float, float]:
    """k — 1ч свечи (индекс = открытие). Берём только закрытые до t."""
    w = k[k.index + pd.Timedelta("1h") <= t]
    if len(w) < M + N:
        return np.nan, np.nan, np.nan
    hi_win = w.iloc[-N:]
    jh = hi_win.h.values.argmax(); H = hi_win.h.values[jh]; tH = hi_win.index[jh]
    lo_win = w[(w.index <= tH) & (w.index > tH - pd.Timedelta(hours=M))]
    L = lo_win.l.min()
    zones = [L + x * (H - L) for x in lv]
    return float(np.mean(zones)), float(H / L - 1) * 100, float(w.c.iloc[-1])


if __name__ == "__main__":
    S = pd.read_csv(PRIV / "work" / "photos" / "setups_parsed.csv", parse_dates=["t"])
    X = S[(S.fibo) & (S.dir == "лонг") & (S.y >= 2023) & S.emid.notna()].copy()
    X["sym"] = X.base.str.upper().map(lambda b: ("1000" + b if b in {"PEPE", "BONK", "FLOKI", "LUNC", "SATS"} else b) + "USDT")
    rows = []
    cache = {}
    for r in X.itertuples():
        if r.sym not in cache:
            cache[r.sym] = market.klines(r.sym, "60", "2022-10-01", "2025-12-31")
        k = cache[r.sym]
        if k.empty or r.t < k.index.min() + pd.Timedelta(days=10):
            continue
        emid = r.emid * (1000 if r.sym.startswith("1000") and r.emid < 0.01 else 1)
        cp = float(k[k.index + pd.Timedelta("1h") <= r.t].c.iloc[-1])
        if abs(emid / cp - 1) > 0.6:                          # единицы не сошлись (разбор картинки)
            continue
        row = dict(t=r.t, sym=r.sym, tf=r.tf, emid=emid, cp=cp, depth=(1 - emid / cp) * 100)
        for name, lv in LEVELS.items():
            for N in (24, 48, 96, 168):
                for M in (48, 120, 240):
                    mid, imp, _ = fibo_mid(k, r.t, N, M, lv)
                    row[f"{name}|N{N}|M{M}"] = (mid / emid - 1) * 100 if mid == mid else np.nan
        rows.append(row)
    R = pd.DataFrame(rows)
    print(f"сетапов с данными: {len(R)} из {len(X)}; тикеров {R.sym.nunique()}")
    cols = [c for c in R.columns if "|" in c]
    res = []
    for c in cols:
        d = R[c].abs()
        res.append(dict(вариант=c, в_пределах_1проц=(d <= 1).mean(), в_пределах_2проц=(d <= 2).mean(), медиана_откл=d.median()))
    res = pd.DataFrame(res).sort_values("в_пределах_2проц", ascending=False)
    # база: середина = цена × (1 − медианная глубина его зон), без графика
    med_depth = R.depth.median()
    base = ((R.cp * (1 - med_depth / 100)) / R.emid - 1).abs() * 100
    pd.set_option("display.width", 200)
    print(res.head(10).round(3).to_string(index=False))
    print(f"\nбаза без графика (глубина {med_depth:.1f}% от цены у всех): в пределах 1% — {(base <= 1).mean():.2f}, 2% — {(base <= 2).mean():.2f}, медиана {base.median():.2f}%")
    best = res.iloc[0].вариант
    R["best"] = R[best]
    print("\nпо годам, лучший вариант:", best)
    print(R.groupby(R.t.dt.year).best.agg(n="size", в_1проц=lambda s: (s.abs() <= 1).mean(), в_2проц=lambda s: (s.abs() <= 2).mean(), медиана=lambda s: s.abs().median()).round(2).to_string())
    R.to_csv(PRIV / "work" / "cryptosmx" / "fibo_check.csv", index=False)
