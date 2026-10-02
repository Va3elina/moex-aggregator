"""Проверка исполнителя настоящих заявок на имитации биржи: те же сигналы, что у бумажной книги, —
кампании должны открываться и закрываться в те же минутки (размеры другие: у исполнителя равные минимальные куски).
Запуск: .venv/bin/python bot/test_live.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core  # noqa: E402
import live  # noqa: E402
from bybit import Rest  # noqa: E402
from core import ge  # noqa: E402
from sim_exchange import SimExchange  # noqa: E402

import numpy as np  # noqa: E402
P = dict(side=1, L=1.0, btc_filter=False, crash_skip=25.0, cap_to_equity=True, sizes=np.ones(7))
CASES = [("NEARUSDT", "2026-07-01", "2026-09-27 11:00"), ("DOGEUSDT", "2025-09-20", "2025-10-20"), ("XRPUSDT", "2026-05-01", "2026-09-27 11:00")]

if __name__ == "__main__":
    EQUAL = "--equal" in sys.argv                   # по умолчанию — минимальные заявки с целью по весам лесенки; --equal — 7 равных покупок
    SLOT = "--slot" in sys.argv                     # --slot — настоящая лесенка на ячейку, цель от реальной средней
    if not EQUAL:
        P.pop("sizes")
    inst = {i["symbol"]: i for i in Rest().instruments()}
    doge = ge.range_series("DOGEUSDT", "2021-01-01", "2026-09-28")
    all_ok = True
    for sym, a, b in CASES:
        coin = ge.Coin(sym, a, b)
        scale = ge.daily_scale(sym, coin.k.index, doge)
        sig = coin.signal(ge.P(scale=scale, **P))
        book = core.PaperBook(sym, ge.P(**P))
        sim = SimExchange()
        J = []
        if SLOT:
            ex = live.LiveExec(sim, {sym: live.Spec.from_instrument(inst[sym])}, ge.P(**P), J.append, max_camps=1, capital=3.2e7, sizing="slot")
        else:
            ex = live.LiveExec(sim, {sym: live.Spec.from_instrument(inst[sym])}, ge.P(**P), J.append, max_camps=100, piece_usd=1e6, equal=EQUAL)
        k = coin.k
        for i, t in enumerate(k.index):
            o, h, l, c = k.o.iat[i], k.h.iat[i], k.l.iat[i], k.c.iat[i]
            book.step(t, o, h, l, c, coin.fund[i]); book.set_signal(sig[i], scale[i])
            sim.bar_t = t
            for e in sim.run_bar(sym, t, o, h, l, c):
                ex.on_execution(e, t)
            ex.on_bar(sym, t, o, h, l, c)
            if sig[i]:
                ex.on_signal(sym, t, c, float(scale[i]))
        errs = [j for j in J if j["kind"] == "ошибка"]
        if errs:
            print("  ошибок исполнителя:", len(errs), errs[:3])
        B = pd.DataFrame(book.camps, columns=["t0", "t1", "pnl", "adds", "exit", "p0", "avg", "px"])
        L = pd.DataFrame([dict(t0=next(o.filled_at for o in cp.orders.values() if o.kind == "вход"), t1=cp.closed_at, adds=len(cp.fills) - 1,
                               exit="цель" if any(o.kind == "цель" and o.filled_at is not None for o in cp.orders.values()) else "таймер")
                          for cp in ex.done])
        m = B.merge(L, on="t0", how="outer", suffixes=("_книга", "_реал"), indicator=True)
        both = m[m._merge == "both"]
        same_adds = (both.adds_книга == both.adds_реал).mean() * 100 if len(both) else 0
        same_exit = (both.exit_книга == both.exit_реал).mean() * 100 if len(both) else 0
        dt = (both.t1_реал - both.t1_книга).dt.total_seconds().div(60)
        close_ok = ((dt >= 0) & (dt <= 1)).mean() * 100 if len(both) else 0
        print(f"{sym}: книга {len(B)} кампаний, исполнитель {len(L)} (+ открыто {len(ex.camps)}); совпали по входу {len(both)}; "
              f"число докупок совпало {same_adds:.0f}%, тип выхода {same_exit:.0f}%, выход в ту же/следующую минутку {close_ok:.0f}%")
        bad = m[(m._merge != "both")]
        if len(bad):
            print("  без пары:", bad[["t0", "_merge"]].head(6).to_string(index=False))
        diff = both[(both.adds_книга != both.adds_реал) | (both.exit_книга != both.exit_реал) | ~((dt >= 0) & (dt <= 1))]
        if len(diff):
            print("  расхождения:\n", diff[["t0", "adds_книга", "adds_реал", "exit_книга", "exit_реал", "t1_книга", "t1_реал"]].head(6).to_string(index=False))
        all_ok &= len(bad) <= 1 and len(diff) <= max(1, len(both) // 50)
        st = ex.fill_stats()
        if len(st):
            s = st[st.kind == "ступень"]
            print(f"  журнал касаний (имитация, всегда «исполнилось»): ступеней поставлено {len(s)}, коснулись {s.touched.sum()}, исполнились {s.filled.sum()}")
    print("ИТОГ:", "исполнитель повторяет книгу" if all_ok else "ЕСТЬ РАСХОЖДЕНИЯ")
