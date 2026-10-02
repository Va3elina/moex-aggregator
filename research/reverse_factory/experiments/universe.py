"""Отбор монет правилом, как в реальном времени, — пересчёт каждый день.

Каждый день D (UTC): из бессрочных USDT-фьючерсов Bybit берём те, что торгуются ≥ 90 дней (первая дневная свеча
не позже D−90), убираем «неживые» (медианный дневной размах за 30 дней < 1% — стейблкоины, золото, токенизированные
акции), ранжируем по среднему дневному обороту за [D−30, D−1] и берём первые N.
Ограничение: API Bybit отдаёт только монеты, которые торгуются сейчас; снятые с торгов (LUNA 2022, FTT и др.) в
выборку не попадают — это завышает результат, оговариваем отдельно.
→ data/universe_daily_top<N>.parquet (дата × монета, 1 если в списке)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory.schema import DATA  # noqa: E402


# не крипта: металлы, токенизированные акции и фонды (Bybit TradFi)
NON_CRYPTO = {"XAU", "XAUT", "PAXG", "XAG", "XPT", "SOXL", "SNDK", "CSCO", "TSLA", "NVDA", "AAPL", "AMZN", "GOOGL", "GOOG", "META", "MSFT",
              "MSTR", "COIN", "HOOD", "SPY", "QQQ", "TQQQ", "SQQQ", "NFLX", "AMD", "INTC", "PLTR", "CRCL", "BABA", "ORCL", "AVGO", "MU", "SMCI"}


def select(N: int = 10, start="2021-07-01", end="2026-09-27", perps: list[str] | None = None) -> pd.DataFrame:
    """Десятка на каждый день [start, end] по дневным свечам из кэша (без записи на диск)."""
    if perps is None:
        perps = [p[0] for p in json.load(open(DATA / "universe_perps.json"))] + json.load(open(DATA / "universe_delisted.json"))
    turn, rng, first = {}, {}, {}
    for s in perps:
        f = DATA / "klines" / f"{s}_D.parquet"
        if not f.exists():
            continue
        d = pd.read_parquet(f).astype(float)
        if d.empty:
            continue
        first[s] = d.index.min()
        turn[s] = d.v * d.c
        rng[s] = (d.h - d.l) / d.c
    T = pd.DataFrame(turn).sort_index(); Rg = pd.DataFrame(rng).sort_index()
    days = pd.date_range(start, end, freq="D")
    avg_turn = T.rolling(30, min_periods=20).mean().shift(1).reindex(days)
    med_rng = Rg.rolling(30, min_periods=20).median().shift(1).reindex(days)
    first_s = pd.Series(first)
    out = pd.DataFrame(0, index=days, columns=T.columns, dtype=np.int8)
    for D in days:
        ok = (first_s <= D - pd.Timedelta(days=90))
        cand = avg_turn.loc[D][ok.reindex(avg_turn.columns).fillna(False).values]
        cand = cand[(med_rng.loc[D].reindex(cand.index) >= 0.01)].dropna()
        cand = cand[[c for c in cand.index if c.replace("USDT", "") not in NON_CRYPTO]]
        top = cand.sort_values(ascending=False).head(N).index
        out.loc[D, top] = 1
    return out.loc[:, out.sum() > 0]


def build(N: int = 10, start="2021-07-01", end="2026-09-27") -> pd.DataFrame:
    out = select(N, start, end)
    out.to_parquet(DATA / f"universe_daily_top{N}.parquet")
    return out


if __name__ == "__main__":
    U = build()
    print("монет, хоть раз попавших в десятку:", U.shape[1])
    days_in = U.sum().sort_values(ascending=False)
    print((days_in / len(U) * 100).round(0).astype(int).to_string())
    for y in range(2021, 2027):
        u = U[U.index.year == y]
        print(y, "разных монет:", int((u.sum() > 0).sum()), "— чаще всего:", ", ".join(u.sum().sort_values(ascending=False).head(12).index.str.replace("USDT", "")))
