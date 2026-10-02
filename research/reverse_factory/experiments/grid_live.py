"""Живой прогон «спокойного Kamaz» за последние дни — вручную, по свежим свечам Bybit. Ничего не копит и не торгует.

Правила (рабочий вариант после проверок 27.09): лонг, плечо x1 (лесенка целиком = ячейка $10 000), каждый день десятка
монет по обороту за 30 дней (торгуются ≥ 90 дней), вход только в дни, когда монета в десятке; вход после пролива
(RSI7 на 3-мин ≤ 15, цена ≥ 3% ниже максимума часа, за 2 ч −2%), докупки −1.4/−2.8/−4.5/−6.0/−7.8/−9.0% (под размах монеты),
выход +1.94% / +1.6% от средней, таймер 12 ч после последней покупки; не входить после −25% за сутки; лесенка не больше
текущего счёта; стопа нет. Фильтр по биткоину — второй вариант (не входить, пока биткоин ниже минимума 12 ч).

Запуск: .venv/bin/python experiments/grid_live.py [начало, по умолчанию 7 дней назад]
"""
from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
from factory.schema import DATA  # noqa: E402
import grid_engine as ge  # noqa: E402
import universe  # noqa: E402

NOW = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("min") - pd.Timedelta(minutes=1)   # последняя закрытая минута
CFG = dict(L=1.0, crash_skip=25.0, cap_to_equity=True)
TZ = __import__("os").environ.get("LIVE_TZ", "Europe/Berlin")


def berlin(t: pd.Timestamp) -> str:
    return t.tz_localize("UTC").tz_convert(TZ).strftime("%d.%m %H:%M")


def refresh_daily():
    """Свежие дневные свечи всех торгуемых сейчас бессрочных контрактов (последний день перекачивается целиком)."""
    perps = [p[0] for p in json.load(open(DATA / "universe_perps.json"))]
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda s: market.klines(s, "D", NOW - pd.Timedelta(days=200), NOW + pd.Timedelta(days=3)), perps))
    return perps


class LiveCoin(ge.FrameCoin):
    def __init__(self, sym, k, a, b, btc, allowed):
        super().__init__(sym, k, a, b, btc_full=btc)
        self.allowed = allowed

    def signal(self, p):
        return super().signal(p) & self.allowed


def drange(sym: str, a: pd.Timestamp) -> float:
    d = market.klines(sym, "D", a - pd.Timedelta(days=95), a - pd.Timedelta(days=1)).astype(float)
    d = d[d.index < a]
    return float(((d.h - d.l) / d.c).tail(90).median()) if len(d) >= 20 else np.nan


def main(start: str | None):
    A = pd.Timestamp(start) if start else (NOW - pd.Timedelta(days=7)).floor("D")
    perps = refresh_daily()
    U = universe.select(10, A.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"), perps=perps)
    print(f"Живой прогон с {berlin(A)} по {berlin(NOW)} (время берлинское)\n\nДесятка по дням:")
    prev = None
    for d, r in U.iterrows():
        cur = [c.replace("USDT", "") for c in r.index[r == 1]]
        if prev is None:
            note = ""
        elif prev == cur:
            note = "  (без изменений)"
        else:
            note = "  (" + ", ".join([f"+{c}" for c in cur if c not in prev] + [f"−{c}" for c in prev if c not in cur]) + ")"
        print(f"  {d:%d.%m}: {', '.join(cur)}{note}")
        prev = cur

    btc = market.klines("BTCUSDT", "1", A - pd.Timedelta(days=3), NOW).astype(float)
    btc = btc[btc.index <= NOW]
    doge_rng = drange("DOGEUSDT", A)
    doge_series = ge.range_series("DOGEUSDT", A, NOW)
    results = {}
    for filt in (False, True):
        trades, opens, eqs = [], [], []
        for sym in U.columns:
            k = market.klines(sym, "1", A - pd.Timedelta(days=3), NOW).astype(float)
            k = k[k.index <= NOW]
            allowed = U[sym].reindex(k.index.floor("D")).fillna(0).values.astype(bool)[(k.index >= A)]
            c = LiveCoin(sym, k, A, NOW, btc if sym != "BTCUSDT" else k, allowed)
            scale = ge.daily_scale(sym, c.k.index, doge_series)     # пересчёт каждый день, как в бэктесте
            st = {}
            E, C, liq, tr = ge.run(c, ge.P(side=1, scale=scale, btc_filter=filt, **CFG), state_out=st)
            for r in C.itertuples():
                before = E[E.index < r.t0]
                net = E.asof(r.t1) - (before.iloc[-1] if len(before) else 10_000.0)
                trades.append(dict(монета=sym.replace("USDT", ""), t0=r.t0, t1=r.t1, вход=r.p0, докупок=r.adds, средняя=r.avg,
                                   выход=r.px_out, как=r.exit, итог=net))
            if st["open"]:
                o = st["open"]
                opens.append(dict(монета=sym.replace("USDT", ""), с=o["t0"], вход=o["p0"], докупок=o["adds"], средняя=o["avg"], цена=o["px"],
                                  в_позиции=o["Q"] * o["avg"], сейчас=(o["px"] - o["avg"]) * o["Q"],
                                  цель=o["avg"] * (1 + (ge.P().tp1 if o["adds"] == 0 else ge.P().tpn) / 100),
                                  таймер=o["last"] + pd.Timedelta(hours=12)))
            eqs.append(E - 10_000.0)
        T = pd.DataFrame(trades).sort_values("t0") if trades else pd.DataFrame()
        results[filt] = (T, opens, pd.concat(eqs, axis=1).sum(axis=1))

    T0 = results[False][0]
    T1 = results[True][0]
    blocked = set()
    if len(T0):
        keys1 = set(zip(T1.монета, T1.t0)) if len(T1) else set()
        blocked = {(m, t) for m, t in zip(T0.монета, T0.t0) if (m, t) not in keys1}
    print(f"\nСделки (без фильтра по биткоину; «ф» — фильтр такую сделку не открыл бы). Ячейка $10 000 на монету, x1:")
    if len(T0):
        for r in T0.itertuples():
            mark = "ф" if (r.монета, r.t0) in blocked else " "
            dur = (r.t1 - r.t0).total_seconds() / 3600
            print(f"  {mark} {r.монета:6s} вход {berlin(r.t0)} по {r.вход:.6g}, докупок {r.докупок}, средняя {r.средняя:.6g} → "
                  f"{r.как} {berlin(r.t1)} по {r.выход:.6g} ({dur:.1f} ч): {r.итог:+,.0f}$")
    else:
        print("  сделок не было")
    for filt in (False, True):
        T, opens, pnl = results[filt]
        name = "с фильтром по биткоину" if filt else "без фильтра"
        if len(T):
            print(f"\nИтог {name}: сделок {len(T)}, прибыльных {(T.итог > 0).mean() * 100:.0f}%, закрытые {T.итог.sum():+,.0f}$, "
                  f"со всеми открытыми {pnl.iloc[-1]:+,.0f}$ ({pnl.iloc[-1] / 1000:+.2f}% от $100 000), худший момент {pnl.min():+,.0f}$")
        else:
            print(f"\nИтог {name}: сделок нет, открытые {pnl.iloc[-1]:+,.0f}$")
        for o in opens:
            print(f"  открыта {o['монета']}: вход {berlin(o['с'])} по {o['вход']:.6g}, докупок {o['докупок']}, средняя {o['средняя']:.6g}, "
                  f"цена {o['цена']:.6g}, в позиции ${o['в_позиции']:,.0f}, сейчас {o['сейчас']:+,.0f}$, цель {o['цель']:.6g}, таймер до {berlin(o['таймер'])}")

    print(f"\nЧто бот видит сейчас ({berlin(NOW)}):")
    lo12 = btc.l.shift(30).rolling(720).min()
    print(f"  биткоин {btc.c.iloc[-1]:,.0f}, минимум 12 ч {lo12.iloc[-1]:,.0f} → фильтр {'ВКЛЮЧЁН (новые входы запрещены)' if btc.c.iloc[-1] <= lo12.iloc[-1] else 'выключен'}")
    today = U.index.max()
    for sym in U.columns[U.loc[today] == 1]:
        k = market.klines(sym, "1", NOW - pd.Timedelta(days=2), NOW).astype(float)
        k = k[k.index <= NOW]
        c3 = k.c.resample("3min", label="right", closed="right").last().dropna()
        r3 = ge.rsi(c3, 7).iloc[-1]
        sc = float(ge.daily_scale(sym, k.index[-1:], doge_series)[-1])
        px = k.c.iloc[-1]; hi = k.h.iloc[-60:].max()
        ch2 = (px / k.c.iloc[-121] - 1) * 100; ch24 = (px / k.c.iloc[-1441] - 1) * 100
        print(f"  {sym.replace('USDT', ''):5s} {px:<10.6g} RSI {r3:5.1f} (вход ≤15) | от максимума часа {(px / hi - 1) * 100:+5.2f}% (≤{-3 * sc:.1f}%) | "
              f"за 2 ч {ch2:+5.2f}% (≤{-2 * sc:.1f}%) | за сутки {ch24:+5.1f}% (пропуск ≤{-25 * sc:.0f}%)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
