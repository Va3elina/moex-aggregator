"""Ядро бота «спокойный Kamaz»: сигнал по скользящему окну минуток и бумажная книга, повторяющая
experiments/grid_engine.run шаг в шаг (одна минутка за раз). Равенство с движком проверяет bot/test_core.py.

Время — наивное UTC, минутка помечена временем ОТКРЫТИЯ и обрабатывается после закрытия.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "experiments"))
import grid_engine as ge  # noqa: E402

KEEP = 3 * 1440 + 60          # сколько минуток держать в окне: RSI разгоняется, сутки нужны для пропуска после обвала


class Window:
    """Скользящее окно минуток одной монеты (o h l c v, индекс — время открытия)."""

    def __init__(self, frame: pd.DataFrame):
        self.k = frame[["o", "h", "l", "c", "v"]].astype(float).sort_index()
        self.k = self.k[~self.k.index.duplicated(keep="last")].iloc[-KEEP:]

    @property
    def last_t(self) -> pd.Timestamp | None:
        return self.k.index[-1] if len(self.k) else None

    def append(self, t: pd.Timestamp, o: float, h: float, l: float, c: float, v: float = 0.0):
        if len(self.k) and t <= self.k.index[-1]:
            self.k.loc[t] = [o, h, l, c, v]              # поправка уже записанной минутки
            return
        self.k.loc[t] = [o, h, l, c, v]
        if len(self.k) > KEEP + 500:
            self.k = self.k.iloc[-KEEP:]


def signal_now(coin: Window, btc: Window | None, p: ge.P, scale: float, allowed: bool) -> dict:
    """Сигнал входа на только что закрытой минутке (последней в окне) — те же формулы, что Coin.signal."""
    k = coin.k
    out = dict(sig=False, r3=np.nan, from_hi=np.nan, ch120=np.nan, ch1440=np.nan, btc_low=False)
    if len(k) < 1441 + 1:
        return out
    c = k.c.values
    r3 = ge.rsi3(k.c)[-1]
    hi60 = k.h.values[-60:].max()
    from_hi = (c[-1] / hi60 - 1) * 100
    ch120 = (c[-1] / c[-121] - 1) * 100
    ch1440 = (c[-1] / c[-1441] - 1) * 100
    sig = (r3 <= p.rsi_thr) and (from_hi <= -p.drop1h * scale) and (ch120 <= -p.drop2h * scale)
    btc_low = False
    if btc is not None and len(btc.k) >= 751:
        b = btc.k.reindex(k.index[-751:]).ffill()          # как в Coin: биткоин приведён к минуткам монеты
        btc_low = bool(b.c.values[-1] <= b.l.values[-750:-30].min())
    if p.btc_filter:
        sig = sig and not btc_low
    if p.crash_skip is not None:
        sig = sig and (ch1440 > -p.crash_skip * scale)
    out.update(sig=bool(sig and allowed), r3=float(r3), from_hi=float(from_hi), ch120=float(ch120), ch1440=float(ch1440), btc_low=btc_low)
    return out


def trigger_now(coin: Window, p: ge.P, scale: float) -> float:
    """Вариант B (вход внутри минуты): цена срабатывания для ПОСЛЕДНЕЙ минутки окна по данным на её открытие —
    максимум часа по 59 прошлым минуткам и открытию, закрытие 120 минут назад, RSI7 по формирующейся 3-минутке
    (состояние после последней закрытой 3-минутки). Самая высокая цена, при которой выполнены все три условия Kamaz A.
    Вход, если минимум минутки ≤ этой цены, по min(открытие, цена). NaN — нет данных или пропуск после обвала.
    Совпадает с experiments/kamaz_missed_entries.trigger_price (проверка — bot/test_core_b.py)."""
    k = coin.k
    if len(k) < 1442 + 1 or p.btc_filter:                        # фильтр по биткоину в варианте B не поддержан
        return float("nan")
    t = k.index[-1]
    o_t = float(k.o.values[-1]); h = k.h.values[:-1]; c = k.c.values[:-1]
    if p.crash_skip is not None and not ((c[-1] / c[-1441] - 1) * 100 > -p.crash_skip * scale):
        return float("nan")
    H = max(float(h[-59:].max()), o_t)
    c120 = float(c[-120])
    prev_lbl = t.floor("3min") - pd.Timedelta(minutes=3)
    c3 = k.c.iloc[:-1].resample("3min", label="left", closed="left").last().dropna()
    c3 = c3[c3.index <= prev_lbl]
    if not len(c3) or c3.index[-1] != prev_lbl:
        return float("nan")
    a = 1 / 7
    d = c3.diff()
    up = float(d.clip(lower=0).ewm(alpha=a, adjust=False).mean().iloc[-1]); dn = float((-d.clip(upper=0)).ewm(alpha=a, adjust=False).mean().iloc[-1])
    cp = float(c3.iloc[-1]); kk = p.rsi_thr / (100 - p.rsi_thr)
    X = ((1 - a) * up / kk - (1 - a) * dn) / a
    p_rsi = cp - X if X >= 0 else cp + (kk * (1 - a) * dn - (1 - a) * up) / a
    return float(min(H * (1 - p.drop1h * scale / 100), c120 * (1 - p.drop2h * scale / 100), p_rsi))


@dataclass
class Campaign:
    t0: pd.Timestamp
    p0: float
    Q: float
    cost: float
    bu: float
    sc: float
    lv: np.ndarray
    c0: float
    k: int = 0
    last: pd.Timestamp = None

    @property
    def avg(self) -> float:
        return self.cost / self.Q

    def target(self, p: ge.P) -> float:
        return self.p0 * (1 + p.tp1 / 100) if self.k == 0 else self.avg * (1 + p.tpn / 100)


@dataclass
class PaperBook:
    """Бумажная ячейка одной монеты: лонг, лесенка, цель, таймер — ровно как grid_engine.run (без стопа и простоев)."""
    sym: str
    p: ge.P
    E0: float = 10_000.0
    cash: float = None
    x: Campaign | None = None
    prev_sig: bool = False
    prev_scale: float = 1.0
    prev_c: float | None = None
    camps: list = field(default_factory=list)
    events: list = field(default_factory=list)

    def __post_init__(self):
        if self.cash is None:
            self.cash = self.E0

    def step(self, t: pd.Timestamp, o: float, h: float, l: float, c: float, fund: float = 0.0):
        """Обработать закрытую минутку t (вход — по сигналу ПРЕДЫДУЩЕЙ минутки, как sig[i-1] в движке)."""
        p = self.p; x = self.x; ev = []
        if x is not None and fund and self.prev_c is not None:
            self.cash -= x.Q * self.prev_c * fund
        if x is None:
            if self.prev_sig:
                FULL = p.sizes[:len(p.levels) + 1].sum()
                bu = min(self.E0, self.cash) * p.L / FULL if p.cap_to_equity else self.E0 * p.L / FULL
                q = bu / o
                sc = self.prev_scale
                self.x = x = Campaign(t0=t, p0=o, Q=q, cost=q * o, bu=bu, sc=sc, lv=p.levels * sc / 100, c0=self.cash, last=t)
                self.cash -= q * o * p.taker
                ev.append(dict(kind="вход", t=t, price=o, usd=q * o))
        else:
            while x.k < len(x.lv):
                lvl = x.p0 * (1 - x.lv[x.k])
                if not (l <= lvl * (1 - p.through / 100)):
                    break
                q = x.bu / x.p0 * p.sizes[x.k + 1]
                x.Q += q; x.cost += q * lvl; x.k += 1; x.last = t
                self.cash -= q * lvl * p.maker
                ev.append(dict(kind=f"докупка {x.k}", t=t, price=lvl, usd=q * lvl))
            avg = x.avg
            tp = x.target(p)
            if t > x.last and h >= tp * (1 + p.through / 100):
                pnl = (tp - avg) * x.Q
                self.cash += pnl - tp * x.Q * p.maker
                self.camps.append((x.t0, t, pnl, x.k, "цель", x.p0, avg, tp)); ev.append(dict(kind="цель", t=t, price=tp, pnl=pnl))
                self.x = x = None
            elif (t - x.last) >= pd.Timedelta(hours=p.timer_h):
                pnl = (c - avg) * x.Q
                self.cash += pnl - c * x.Q * p.taker
                self.camps.append((x.t0, t, pnl, x.k, "таймер", x.p0, avg, c)); ev.append(dict(kind="таймер", t=t, price=c, pnl=pnl))
                self.x = x = None
        self.prev_c = c
        self.events += ev
        return ev

    def set_signal(self, sig: bool, scale: float):
        """Сигнал и масштаб закрытой минутки — для решения на следующей."""
        self.prev_sig = bool(sig); self.prev_scale = float(scale)

    def equity(self, price: float) -> float:
        return self.cash + ((price - self.x.avg) * self.x.Q if self.x is not None else 0.0)


@dataclass
class PaperBookB(PaperBook):
    """Бумажная ячейка варианта B: вход внутри минутки по цене срабатывания trig (trigger_now), а не по сигналу прошлой минутки.
    Всё остальное (лесенка от цены входа × масштаб прошлой минутки, цель, таймер, комиссии, финансирование) — как PaperBook,
    то есть как grid_engine.run на монете, где сигнал i−1 = «минимум i ≤ trig i», а открытие i заменено на min(открытие, trig)."""

    def step(self, t: pd.Timestamp, o: float, h: float, l: float, c: float, fund: float = 0.0, trig: float = float("nan")):
        if self.x is None:
            hit = trig == trig and l <= trig
            self.prev_sig = bool(hit)                    # для базового шага: «сигнал» = срабатывание в этой минутке
            return super().step(t, min(o, trig) if hit else o, h, l, c, fund)
        return super().step(t, o, h, l, c, fund)
