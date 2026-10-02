"""Проверка ядра бота против движка бэктеста (должно совпасть до копейки):
1) бумажная книга, получая минутки по одной, даёт те же сделки, что grid_engine.run на всём периоде;
2) сигнал по скользящему окну (как считает бот вживую) совпадает с сигналом движка на тех же минутках.
Запуск: .venv/bin/python bot/test_core.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core  # noqa: E402
from core import ge  # noqa: E402

P = dict(side=1, L=1.0, btc_filter=False, crash_skip=25.0, cap_to_equity=True)
CASES = [("DOGEUSDT", "2024-03-01", "2026-09-27 11:00"), ("NEARUSDT", "2026-06-01", "2026-09-27 11:00"),
         ("SOLUSDT", "2022-04-20", "2022-12-31"), ("XRPUSDT", "2025-09-20", "2025-10-20")]


def book_vs_engine():
    doge = ge.range_series("DOGEUSDT", "2021-01-01", "2026-09-28")
    ok = True
    for sym, a, b in CASES:
        for filt in (False, True):
            coin = ge.Coin(sym, a, b)
            scale = ge.daily_scale(sym, coin.k.index, doge)
            p = ge.P(scale=scale, **{**P, "btc_filter": filt})
            E, C, liq, tr = ge.run(coin, p)
            sig = coin.signal(p)
            book = core.PaperBook(sym, ge.P(**{**P, "btc_filter": filt}))
            k = coin.k
            for i, t in enumerate(k.index):
                book.step(t, k.o.iat[i], k.h.iat[i], k.l.iat[i], k.c.iat[i], coin.fund[i])
                book.set_signal(sig[i], scale[i])
            B = pd.DataFrame(book.camps, columns=C.columns)
            same_n = len(B) == len(C)
            same = same_n and (B.t0.values == C.t0.values).all() and (B.t1.values == C.t1.values).all() and np.allclose(B.pnl, C.pnl) \
                and (B.adds.values == C.adds.values).all()
            ok &= bool(same)
            last_eq = book.equity(k.c.iat[-1])
            print(f"  {sym:9s} {a}…{b[:10]} фильтр={filt!s:5s}: движок {len(C)} сделок, книга {len(B)}; совпадение {'ДА' if same else 'НЕТ'}; "
                  f"счёт движка {E.iloc[-1]:,.2f} / книги {last_eq:,.2f}")
            if not same and same_n:
                d = np.where((B.t0.values != C.t0.values) | ~np.isclose(B.pnl, C.pnl))[0][:3]
                print(C.iloc[d]); print(B.iloc[d])
    return ok


def window_vs_engine(n_rand=300):
    ok = True
    doge = ge.range_series("DOGEUSDT", "2021-01-01", "2026-09-28")
    for sym, a, b in (("NEARUSDT", "2026-07-01", "2026-09-27 11:00"), ("DOGEUSDT", "2025-10-05", "2025-10-15")):
        for filt in (False, True):
            coin = ge.Coin(sym, a, b)
            scale = ge.daily_scale(sym, coin.k.index, doge)
            p = ge.P(scale=scale, **{**P, "btc_filter": filt})
            sig = coin.signal(p)
            full = ge.market.klines(sym, "1", pd.Timestamp(a) - pd.Timedelta(days=4), b).astype(float)
            btc = ge.market.klines("BTCUSDT", "1", pd.Timestamp(a) - pd.Timedelta(days=4), b).astype(float)
            k = coin.k
            rng = np.random.default_rng(0)
            pos = np.r_[np.where(sig)[0], rng.choice(np.arange(1500, len(k)), size=min(n_rand, len(k) - 1500), replace=False)]
            pos = np.unique(pos[pos >= 1500])
            r3_full = coin.r3[coin.sel]
            bad = 0; bad_r3 = 0
            for i in pos:
                t = k.index[i]
                w = core.Window(full[full.index <= t]); bw = core.Window(btc[btc.index <= t])
                s = core.signal_now(w, bw, ge.P(**{**P, "btc_filter": filt}), float(scale[i]), True)
                bad += s["sig"] != bool(sig[i])
                bad_r3 += not np.isclose(s["r3"], r3_full[i], atol=1e-9)
            ok &= bad == 0 and bad_r3 == 0
            print(f"  {sym:9s} {a}…{b[:10]} фильтр={filt!s:5s}: проверено минуток {len(pos)} (сигналов {int(sig.sum())}), "
                  f"расхождений сигнала {bad}, RSI {bad_r3}")
    return ok


if __name__ == "__main__":
    print("1) бумажная книга против движка:")
    a = book_vs_engine()
    print("2) сигнал по скользящему окну против движка:")
    b = window_vs_engine()
    print("ИТОГ:", "всё совпало" if a and b else "ЕСТЬ РАСХОЖДЕНИЯ")
