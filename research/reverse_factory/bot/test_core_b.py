"""Проверка варианта B (вход внутри минутки) против движка бэктеста:
1) бумажная книга PaperBookB, получая минутки по одной, даёт те же сделки, что grid_engine.run на монете, где «сигнал» i−1 =
   «минимум i ≤ цены срабатывания i», а открытие i заменено на min(открытие, цена) — так считал experiments/kamaz_missed_entries.py;
2) цена срабатывания по скользящему окну (как считает бот вживую, core.trigger_now) = векторной trigger_price на тех же минутках.
Запуск: .venv/bin/python bot/test_core_b.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core  # noqa: E402
from core import ge  # noqa: E402

sys.path.insert(0, str(core.ROOT / "experiments"))
import kamaz_missed_entries as km  # noqa: E402

P = dict(side=1, L=1.0, btc_filter=False, crash_skip=25.0, cap_to_equity=True, tp1=3.9, tpn=3.0)
CASES = [("NEARUSDT", "2026-06-01", "2026-09-27 11:00"), ("DOGEUSDT", "2025-09-20", "2025-10-20"), ("SUIUSDT", "2026-08-15", "2026-10-02 08:00")]


def engine_b(sym, a, b, doge):
    coin = ge.Coin(sym, a, b)
    full = ge.market.klines(sym, "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
    F = km.feats(full)
    sc_full = ge.daily_scale(sym, full.index, doge)
    trig_full = pd.Series(km.trigger_price(F, sc_full), full.index)
    crash_ok = pd.Series(np.nan_to_num(F.ch1440.values, nan=0.0) > -25.0 * sc_full, full.index)
    k = coin.k
    trig = trig_full.reindex(k.index).values; ok = crash_ok.reindex(k.index).values
    hit = k.l.values <= trig
    sig = np.zeros(len(k), bool); sig[:-1] = hit[1:] & ok[:-1]
    kB = k.copy(); kB["o"] = np.where(np.r_[False, sig[:-1]], np.minimum(k.o.values, trig), k.o.values)
    raw_o = k.o.values.copy()
    coin.k = kB; coin.signal = lambda p, _s=sig: _s
    scale = ge.daily_scale(sym, k.index, doge)
    return coin, scale, trig, raw_o, full, sc_full, ok


def book_vs_engine(doge):
    ok = True
    for sym, a, b in CASES:
        coin, scale, trig, raw_o, _, _, okc = engine_b(sym, a, b, doge)
        E, C, liq, tr = ge.run(coin, ge.P(scale=scale, **P))
        book = core.PaperBookB(sym, ge.P(**P))
        k = coin.k
        for i, t in enumerate(k.index):
            tg = trig[i] if (i > 0 and okc[i - 1]) else np.nan        # пропуск после обвала в боте делает trigger_now (NaN)
            book.step(t, raw_o[i], k.h.iat[i], k.l.iat[i], k.c.iat[i], coin.fund[i], trig=tg)
            book.set_signal(False, scale[i])
        B = pd.DataFrame(book.camps, columns=C.columns)
        same = len(B) == len(C) and (B.t0.values == C.t0.values).all() and (B.t1.values == C.t1.values).all() and np.allclose(B.pnl, C.pnl) \
            and (B.adds.values == C.adds.values).all()
        ok &= bool(same)
        print(f"  {sym:9s} {a}…{b[:10]}: движок {len(C)} сделок, книга B {len(B)}; совпадение {'ДА' if same else 'НЕТ'}; "
              f"счёт движка {E.iloc[-1]:,.2f} / книги {book.equity(k.c.iat[-1]):,.2f}; по цели {(C.exit == 'цель').sum()}, по таймеру {(C.exit == 'таймер').sum()}")
        if not same:
            n = min(len(B), len(C)); d = np.where((B.t0.values[:n] != C.t0.values[:n]) | ~np.isclose(B.pnl.values[:n], C.pnl.values[:n]))[0][:3]
            print(C.iloc[d]); print(B.iloc[d])
    return ok


def window_vs_vector(doge, n_rand=300):
    ok = True
    for sym, a, b in CASES[:2]:
        coin, scale, trig, raw_o, full, sc_full, okc = engine_b(sym, a, b, doge)
        k = coin.k
        rng = np.random.default_rng(0)
        hits = np.where(k.l.values <= trig)[0]
        pos = np.unique(np.r_[hits, rng.choice(np.arange(1500, len(k)), size=min(n_rand, len(k) - 1500), replace=False)])
        pos = pos[pos >= 1500]
        bad = 0
        for i in pos:
            t = k.index[i]
            w = core.Window(full[full.index <= t])
            tw = core.trigger_now(w, ge.P(**P), float(sc_full[full.index.get_loc(t)]))
            exp = trig[i] if okc[i - 1] else np.nan
            bad += not ((np.isnan(tw) and np.isnan(exp)) or (not np.isnan(tw) and not np.isnan(exp) and np.isclose(tw, exp, rtol=1e-9)))
        ok &= bad == 0
        print(f"  {sym:9s} {a}…{b[:10]}: проверено минуток {len(pos)} (срабатываний {len(hits)}), расхождений цены срабатывания {bad}")
    return ok


if __name__ == "__main__":
    doge = ge.range_series("DOGEUSDT", "2021-01-01", "2026-10-03")
    print("1) бумажная книга B против движка (цель 3.9/3.0):")
    a = book_vs_engine(doge)
    print("2) цена срабатывания по окну против векторной:")
    b = window_vs_vector(doge)
    print("ИТОГ:", "всё совпало" if a and b else "ЕСТЬ РАСХОЖДЕНИЯ")
