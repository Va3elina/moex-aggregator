"""«Спокойный Kamaz» + биткоин и эфир в наборе монет + фильтры по биткоину на вход.

Монеты: 8 мемов/альтов + BTC и ETH (для них — правила «под размах»: уровни и пороги × размах монеты / размах DOGE).
Фильтры нового входа (докупки в открытой кампании идут как обычно):
  нет;
  «пробой биткоина» — не входить, пока биткоин ниже своего минимума за 12 ч (обвал рынка идёт);
  «биткоин падает» — не входить, если биткоин за час −1.5% и хуже;
  «предохранитель» — после падения биткоина на 5% за сутки новые входы на 24 ч запрещены;
  «тренд биткоина» — входить только когда биткоин выше своей EMA200 на 4-часовых свечах.
Каждая монета — отдельный счёт $10 000, кусок от стартового, таймер 12 ч, лесенка x1 и x3. Итог портфеля — сумма.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
import itek_kamaz_backtest as kb  # noqa: E402
import itek_kamaz_stress as ks  # noqa: E402

COINS = ks.COINS + ["BTCUSDT", "ETHUSDT"]
FILTERS = ["нет", "пробой биткоина", "биткоин падает", "предохранитель", "тренд биткоина"]


def btc_blocks(idx: pd.DatetimeIndex) -> dict[str, np.ndarray]:
    b = market.klines("BTCUSDT", "1", kb.T0 - pd.Timedelta(days=40), kb.T1).astype(float)
    lo12 = b.l.shift(30).rolling(12 * 60).min()
    r60 = (b.c / b.c.shift(60) - 1) * 100
    r24 = (b.c / b.c.shift(1440) - 1) * 100
    crash = (r24 <= -5).astype(float).rolling(1440, min_periods=1).max() > 0
    e200 = b.c.resample("4h", label="right", closed="right").last().ewm(span=200, adjust=False).mean().reindex(b.index, method="ffill")
    return {"нет": np.zeros(len(idx), bool),
            "пробой биткоина": (b.c <= lo12).reindex(idx).fillna(False).values,
            "биткоин падает": (r60 <= -1.5).reindex(idx).fillna(False).values,
            "предохранитель": crash.reindex(idx).fillna(False).values,
            "тренд биткоина": (b.c < e200).reindex(idx).fillna(False).values}


if __name__ == "__main__":
    base_rng = ks.daily_range("DOGEUSDT")
    rows, curves = [], {}
    for sym in COINS:
        sc = ks.daily_range(sym) / base_rng if sym in ("BTCUSDT", "ETHUSDT") else 1.0
        idx, d = ks.coin_data(sym, sc)
        kb.LV[sym] = list(ks.DOGE_LV * sc)
        kb.MAKER, kb.TAKER = 0.0002, 0.00055
        blocks = btc_blocks(idx)
        for fname, blk in blocks.items():
            dd = dict(d); dd["sig"] = d["sig"] & ~blk
            for L in (1, 3):
                R, liq, tr, tm = kb.run(idx, {sym: dd}, L=2 * L, compound=False)
                e = R.equity.resample("D").last()
                curves[f"{sym}|{fname}|{L}"] = e
                ye = e.resample("YE").last(); prev = [10_000.0] + list(ye.values[:-1])
                rows.append(dict(монета=sym.replace("USDT", ""), размах=round(sc, 2), фильтр=fname, плечо=f"x{L}",
                                 **{f"{y.year}": int(v - p) for y, v, p in zip(ye.index, ye.values, prev)},
                                 просадка=round((e / e.cummax() - 1).min() * 100), ликвидация=liq[0].strftime("%d.%m.%y") if liq else "нет", сделок=tr))
        print(sym, "готово", flush=True)
    R = pd.DataFrame(rows)
    R.to_csv(ROOT / "inbox/private/work/itek/kamaz_filters.csv", index=False)
    Cv = pd.DataFrame(curves); Cv.to_parquet(ROOT / "inbox/private/work/itek/kamaz_filters_curves.parquet")
    pd.set_option("display.width", 220); pd.set_option("display.max_rows", 300)
    print(R[R.монета.isin(["BTC", "ETH"])].to_string(index=False))
    for L in (1, 3):
        for coins, name in ((ks.COINS, "8 мемов/альтов"), (COINS, "10 монет с BTC и ETH")):
            names = [c.replace("USDT", "") for c in coins]
            print(f"\n=== портфель {name}, x{L}")
            for fname in FILTERS:
                x = R[(R.плечо == f"x{L}") & (R.фильтр == fname) & (R.монета.isin(names))]
                tot = Cv[[f"{c}|{fname}|{L}" for c in coins]].sum(axis=1)
                ddp = (tot / tot.cummax() - 1).min() * 100
                print(f"  {fname:18s} 2024 {x['2024'].sum():+8,.0f}$  2025 {x['2025'].sum():+8,.0f}$  2026 {x['2026'].fillna(0).sum():+8,.0f}$  "
                      f"просадка портфеля {ddp:.0f}%  ликвидаций {int((x.ликвидация != 'нет').sum())}")
