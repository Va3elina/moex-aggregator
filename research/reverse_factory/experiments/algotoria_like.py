"""Своя версия Algotoria: «14 некоррелированных лонг-шорт стратегий на BTC и ETH» (его описание).

v1 (26.09.2026): на каждую монету 14 простых трендовых стратегий на 1ч / 4ч / 1д свечах, каждая —
«в рынке всегда»: +1 или −1, объём выровнен по волатильности. Позиции 14 стратегий СКЛАДЫВАЮТСЯ
в одну позицию по монете (неттинг, как у него на бирже); решение на закрытии 15-минутки,
исполнение на следующей. Комиссия — от оборота СУММАРНОЙ позиции.
Похожесть на оригинал: связь дневной доходности (без подгонки весов!), доходность по годам, почерк сделок.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market                      # noqa: E402
from factory.schema import load_target           # noqa: E402

START, TARGET_VOL = "2023-06-01", 0.58            # у оригинала годовая волатильность 58%


def trend_signals(k: pd.DataFrame, tf: str) -> dict[str, pd.Series]:
    """Сигналы ±1 на свечах таймфрейма tf (закрытие свечи = момент решения)."""
    c = k.c.astype(float)
    s = {}
    def ema_x(f, sl): return np.sign(c.ewm(span=f).mean() - c.ewm(span=sl).mean())
    def ret(L): return np.sign(c / c.shift(L) - 1)
    def donch(N):
        hi, lo = k.h.rolling(N).max().shift(1), k.l.rolling(N).min().shift(1)
        return pd.Series(np.where(c > hi, 1, np.where(c < lo, -1, np.nan)), c.index).ffill()
    if tf == "1h":
        s.update({"1ч EMA12/48": ema_x(12, 48), "1ч пробой 24": donch(24), "1ч пробой 72": donch(72), "1ч ход 24": ret(24)})
    elif tf == "4h":
        s.update({"4ч EMA6/24": ema_x(6, 24), "4ч пробой 20": donch(20), "4ч пробой 55": donch(55), "4ч ход 6": ret(6), "4ч ход 30": ret(30)})
    else:
        s.update({"1д ход 3": ret(3), "1д ход 10": ret(10), "1д ход 20": ret(20), "1д EMA5/20": ema_x(5, 20), "1д EMA10/50": ema_x(10, 50)})
    return s


def vol_weight(k: pd.DataFrame) -> pd.Series:
    r = k.c.astype(float).pct_change()
    v = r.rolling(20).std()
    return (v.median() / v).clip(upper=3)


def coin(sym: str, end) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    """→ (позиция на 15-минутках, доходность 15-мин свечи, позиции по стратегиям)."""
    k15 = market.klines(sym, "15", START, end)
    grid = k15.index
    parts = {}
    for tf, iv, step in [("1h", "60", "1h"), ("4h", "240", "4h"), ("1d", "D", "1D")]:
        k = market.klines(sym, iv, pd.Timestamp(START) - pd.Timedelta(days=120), end)
        w = vol_weight(k)
        for name, sig in trend_signals(k, tf).items():
            pos = (sig * w).clip(-3, 3)
            pos.index = pos.index + pd.Timedelta(step)            # известно на закрытии свечи
            parts[name] = pos.reindex(grid, method="ffill")
    P = pd.DataFrame(parts).fillna(0)
    net = P.mean(axis=1)
    r = k15.c.astype(float).pct_change().fillna(0)
    return net, r, P


def run(fee: float, end=None):
    end = end or pd.Timestamp.now(tz="UTC").tz_localize(None)
    out, detail = {}, {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        net, r, P = coin(sym, end)
        pos = net.shift(1).fillna(0)                          # исполнение на следующей 15-минутке
        gross = pos * r
        cost_net = net.diff().abs().fillna(0) * fee          # оборот суммарной позиции (неттинг)
        cost_sep = P.diff().abs().mean(axis=1).fillna(0) * fee   # если бы 14 стратегий торговали отдельно
        out[sym] = pd.DataFrame(dict(gross=gross, net=gross - cost_net, net_sep=gross - cost_sep, pos=net, r=r))
        detail[sym] = P
    return out, detail


def daily(x: pd.Series) -> pd.Series:
    return x.groupby(x.index.floor("D")).sum()


def stats(r: pd.Series) -> str:
    e = (1 + r).cumprod()
    yrs = (r.index[-1] - r.index[0]).days / 365.25
    ye = e.resample("YE").last()
    yr = ye.pct_change()
    yr.iloc[0] = ye.iloc[0] - 1
    return (f"{e.iloc[-1] ** (1 / yrs) - 1:+.0%}/год, просадка {(e / e.cummax() - 1).min():.0%}, Шарп {r.mean() / r.std() * np.sqrt(365):.2f} | "
            + " ".join(f"{k.year}:{v:+.0%}" for k, v in yr.items()))


def cycles(pos: pd.Series, price: pd.Series) -> pd.DataFrame:
    """Сделки как у неттингованного счёта: от смены знака суммарной позиции до следующей смены."""
    sgn = np.sign(pos.round(3))
    ch = sgn.ne(sgn.shift()).cumsum()
    rows = []
    for _, g in sgn.groupby(ch):
        side = g.iloc[0]
        if side == 0 or len(g) < 1:
            continue
        t0, t1 = g.index[0], g.index[-1]
        p0, p1 = price.asof(t0), price.asof(t1)
        rows.append(dict(t0=t0, t1=t1, side=side, pnl=100 * side * (p1 / p0 - 1), hold_h=(t1 - t0).total_seconds() / 3600))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    _, meta, eq = load_target("algotoria")
    ya = eq[eq > 0].resample("D").last().pct_change().dropna()
    ya = ya["2023-10-10":]
    for fee, label in [(0.00055, "тейкер 0.055%"), (0.0002, "лимитки 0.02%")]:
        out, detail = run(fee)
        port = sum(daily(out[s]["net"]) for s in out) / 2
        port_sep = sum(daily(out[s]["net_sep"]) for s in out) / 2
        gross = sum(daily(out[s]["gross"]) for s in out) / 2
        i = port.index.intersection(ya.index)
        k = TARGET_VOL / (port[i].std() * np.sqrt(365))
        print(f"\n=== {label}: 14 стратегий × BTC/ETH, неттинг, к волатильности 58% (множитель ×{k:.1f})")
        print(f"   связь дневной доходности с Algotoria: {port[i].corr(ya[i]):.2f}  (без подгонки весов)")
        print(f"   Algotoria:           {stats(ya[i])}")
        print(f"   наша, неттинг:       {stats(port[i] * k)}")
        print(f"   наша, без неттинга:  {stats(port_sep[i] * k)}")
        print(f"   наша, без комиссий:  {stats(gross[i] * k)}")
        turn = sum(out[s]['pos'].diff().abs().sum() for s in out) / 2 / ((i[-1] - i[0]).days / 365)
        turn_sep = sum(detail[s].diff().abs().mean(axis=1).sum() for s in detail) / 2 / ((i[-1] - i[0]).days / 365)
        print(f"   оборот в год (в объёмах позиции): неттинг {turn:.0f} против {turn_sep:.0f} у 14 отдельных")
    # почерк наших «сделок» против его
    price = market.klines("BTCUSDT", "15", START).c.astype(float)
    cy = cycles(out["BTCUSDT"]["pos"], price)
    cy = cy[cy.t0 >= "2023-10-10"]
    w, l = cy[cy.pnl > 0].pnl, cy[cy.pnl < 0].pnl
    print(f"\nпочерк BTC: сделок {len(cy)} ({len(cy) / ((cy.t0.max() - cy.t0.min()).days / 30.4):.0f}/мес), в плюс {len(w) / len(cy):.0%}, "
          f"плюс/минус {w.mean() / -l.mean():.2f}, держит медиана {cy.hold_h.median():.0f} ч, шорты {(cy.side < 0).mean():.0%}")
    print("почерк Algotoria по BTC: 533 сделки (~15/мес), в плюс ~31%, плюс/минус ~2.0, держит медиана 30 ч, шорты 50%")
