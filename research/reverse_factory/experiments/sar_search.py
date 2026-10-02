"""Поиск его правила разворота: «всегда в рынке, стоп-и-разворот» на внутридневных свечах.

Кандидаты: Supertrend (ATR n, множитель k) и пробой канала N (разворот при пробое N-свечного
максимума/минимума) на 15м и 1ч. Каждый кандидат даёт моменты разворотов; сверяем с ЕГО разворотами
(начала его записей) по BTC и ETH: доля его разворотов, у которых наш разворот в ту же сторону
в пределах ±1ч и ±4ч, и обратная точность. Выбор — по совпадению, не по доходности.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market                      # noqa: E402
from factory.schema import load_target           # noqa: E402


def atr(k, n):
    tr = pd.concat([k.h - k.l, (k.h - k.c.shift()).abs(), (k.l - k.c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def supertrend_dir(k, n, m):
    a = atr(k, n).values
    hl = ((k.h + k.l) / 2).values
    up, dn = hl - m * a, hl + m * a
    h, l, c = k.h.values, k.l.values, k.c.values
    fu, fd, d = up.copy(), dn.copy(), np.ones(len(c))
    for i in range(1, len(c)):
        fu[i] = max(up[i], fu[i - 1]) if c[i - 1] > fu[i - 1] else up[i]
        fd[i] = min(dn[i], fd[i - 1]) if c[i - 1] < fd[i - 1] else dn[i]
        # разворот по касанию уровня внутри свечи (как стоп-заявка), а не по закрытию
        if d[i - 1] == 1:
            d[i] = -1 if l[i] < fu[i - 1] else 1
        else:
            d[i] = 1 if h[i] > fd[i - 1] else -1
    return pd.Series(d, k.index)


def donchian_dir(k, N):
    hi, lo = k.h.rolling(N).max().shift(1), k.l.rolling(N).min().shift(1)
    d = pd.Series(np.where(k.h > hi, 1, np.where(k.l < lo, -1, np.nan)), k.index).ffill().fillna(1)
    return d


def flips(d: pd.Series, step: pd.Timedelta) -> pd.DataFrame:
    ch = d.ne(d.shift())
    return pd.DataFrame(dict(t=d.index[ch][1:] + step / 2, side=d[ch].values[1:]))   # середина свечи разворота


def match(E_their, E_our, dt):
    if E_our.empty:
        return 0.0, 0.0
    ot, os_ = E_our.t.values, E_our.side.values
    tt, ts = E_their.t.values, E_their.side.values
    rec = np.mean([np.any((os_ == s) & (np.abs(ot - t) <= dt)) for t, s in zip(tt, ts)])
    prec = np.mean([np.any((ts == s) & (np.abs(tt - t) <= dt)) for t, s in zip(ot, os_)])
    return rec, prec


if __name__ == "__main__":
    tr, _, _ = load_target("algotoria")
    rows = []
    for sym in ["BTCUSDT", "ETHUSDT"]:
        t = tr[tr.sym == sym].sort_values("t_open")
        E = pd.DataFrame(dict(t=t.t_open.values, side=t.side.values))
        a, b = t.t_open.min() - pd.Timedelta(days=10), t.t_close.max()
        for iv, stepm in [("15", 15), ("60", 60)]:
            k = market.klines(sym, iv, a, b)
            step = pd.Timedelta(minutes=stepm)
            cands = {}
            for n in [10, 14, 20]:
                for m in [1.5, 2, 2.5, 3, 4, 5]:
                    cands[f"Supertrend {iv}м ATR{n}×{m}"] = supertrend_dir(k, n, m)
            for N in [12, 24, 48, 96, 192]:
                cands[f"пробой канала {iv}м N={N}"] = donchian_dir(k, N)
            for name, d in cands.items():
                Eo = flips(d.loc[t.t_open.min():], step)
                r1, p1 = match(E, Eo, np.timedelta64(1, "h"))
                r4, p4 = match(E, Eo, np.timedelta64(4, "h"))
                rows.append(dict(монета=sym[:3], правило=name, наших=len(Eo), его=len(E),
                                 **{"его→наш ±1ч": round(r1, 2), "наш→его ±1ч": round(p1, 2), "его→наш ±4ч": round(r4, 2), "наш→его ±4ч": round(p4, 2)}))
    R = pd.DataFrame(rows)
    R["F1 ±1ч"] = (2 * R["его→наш ±1ч"] * R["наш→его ±1ч"] / (R["его→наш ±1ч"] + R["наш→его ±1ч"])).round(3)
    pd.set_option("display.width", 220)
    for coin in ["BTC", "ETH"]:
        print(f"\n=== {coin}: лучшие по совпадению разворотов ±1ч")
        print(R[R.монета == coin].sort_values("F1 ±1ч", ascending=False).head(10).to_string(index=False))
    R.to_csv(Path(__file__).with_suffix(".csv"), index=False)
