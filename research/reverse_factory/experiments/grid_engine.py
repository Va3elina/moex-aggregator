"""Движок «спокойного Kamaz» в обе стороны (лонг / шорт) — для проверки на прочность и шортов.

Лонг: вход после пролива (RSI7 на 3-мин ≤ 15, цена ≥ 3% ниже максимума часа, за 2 ч −2%), докупки лимитками ниже входа,
      выход всей позицией на +1.94% (одна покупка) / +1.6% от средней, через 12 ч после последней покупки — по рынку.
Шорт (зеркало): вход после выстрела (RSI ≥ 85, цена ≥ 3% выше минимума часа, за 2 ч +2%), досдачи лимитками выше входа,
      выход на −1.94% / −1.6% от средней, тот же таймер.
Фильтр по биткоину (зеркальный): лонг не открываем, пока биткоин ниже своего минимума за 12 ч; шорт — пока выше максимума.
Кусок фиксирован от стартового депозита (лесенка целиком = L × депозит), счёт на одну монету, ликвидация при капитале
≤ 0.5% позиции (для лонга по минимуму минуты, для шорта по максимуму). Финансирование: лонг платит, шорт получает.
Проверочные режимы: вход наугад (та же частота сигналов), исполнение только при проходе цены сквозь уровень.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402

SIZES = np.array([1, 3.08, 4.19, 4.82, 5.54, 6.37, 7.33])
DOGE_LV = np.array([1.4, 2.8, 4.5, 6.0, 7.8, 9.0])


@dataclass
class P:
    side: int = 1                  # 1 — лонг, −1 — шорт
    L: float = 1.0                 # лесенка целиком = L × депозит
    scale: float | np.ndarray = 1.0  # множитель уровней и порогов (под размах монеты); массив — своё значение на каждую минуту
    rsi_thr: float = 15.0          # для шорта используется 100 − rsi_thr
    drop1h: float = 3.0
    drop2h: float = 2.0
    tp1: float = 1.94
    tpn: float = 1.6
    timer_h: float = 12.0
    btc_filter: bool = True
    maker: float = 0.0002
    taker: float = 0.00055
    through: float = 0.0           # исполнение лимиток только при проходе на through%
    random_entry: bool = False
    random_onsets: bool = False    # вход наугад с частотой НАЧАЛ сигнала (≈ то же число сделок), а не минут сигнала
    regime: str = ""               # "медвежий": только когда биткоин ниже дневной EMA50; "бычий": только выше
    funding_min: float | None = None  # шорт только при ставке финансирования ≥ этого значения (последняя известная)
    slip: float = 0.0              # доп. проскальзывание рыночных заявок, % (вход, выход по таймеру)
    outages: tuple = ()            # окна (начало, конец): биржа не исполняет ни лимитки, ни рыночные
    hard_stop: float | None = None  # жёсткий стоп: закрыть кампанию, если цена ушла на X% от цены входа против нас
    crash_skip: float | None = None  # не входить, если монета за сутки упала на X% и больше (лонг) / выросла (шорт)
    stop_slip: float = 0.5         # проскальзывание стоп-заявки, % (сверх slip); при гэпе — от цены открытия минуты
    stop_cooldown_h: float = 0.0   # после стопа не входить в эту монету N часов
    cap_to_equity: bool = False    # кусок от меньшего из (стартовый депозит, текущий капитал): после убытков лесенка не больше счёта
    seed: int = 0
    levels: np.ndarray = field(default_factory=lambda: DOGE_LV.copy())
    sizes: np.ndarray = field(default_factory=lambda: SIZES.copy())   # объёмы: вход + по одному на каждый уровень
    funding_on: bool = True        # False — спот: ставки финансирования нет


def range_series(sym: str, a, b) -> pd.Series:
    """Медианный дневной размах (макс − мин) / закрытие за 90 дней ДО каждого дня — известен в начале дня."""
    dd = market.klines(sym, "D", pd.Timestamp(a) - pd.Timedelta(days=100), b).astype(float)
    return ((dd.h - dd.l) / dd.c).rolling(90, min_periods=20).median().shift(1)


def daily_scale(sym: str, idx: pd.DatetimeIndex, doge: pd.Series) -> np.ndarray:
    """Масштаб уровней на каждую минуту: размах монеты / размах DOGE, пересчёт раз в день, в пределах 0.4…1.0."""
    r = range_series(sym, idx.min(), idx.max())
    ratio = (r / doge.reindex(r.index)).clip(0.4, 1.0)
    return ratio.reindex(idx.floor("D")).ffill().fillna(1.0).values


def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


RSI_MODE = os.environ.get("RSI_MODE", "bybit")


def rsi3(c: pd.Series, mode: str | None = None) -> np.ndarray:
    """RSI7 на 3-минутках, известный на ЗАКРЫТИИ каждой минутки (индекс c — время открытия минутки).
    bybit — стандартные 3-минутки Bybit (открываются в :00, :03, :06…), берётся последняя закрытая;
    fixed — прежняя фаза (3-минутка из минуток L−2, L−1, L), без заглядывания;
    old — как было до 27.09: в каждой третьей минуте брала закрытие СЛЕДУЮЩЕЙ минутки (заглядывание на минуту)."""
    mode = mode or RSI_MODE
    if mode in ("bybit", "ph0", "ph1", "ph2"):                 # ph0 = bybit; ph1 = fixed; ph2 — третья фаза сетки 3-минуток
        ph = 0 if mode == "bybit" else int(mode[-1])
        c3 = c.resample("3min", label="left", closed="left", offset=f"{ph}min").last().dropna()   # метка = открытие 3-минутки
        return rsi(c3, 7).reindex(c.index - pd.Timedelta(minutes=2), method="ffill").values
    c3 = c.resample("3min", label="right", closed="right").last().dropna()
    shift = pd.Timedelta(minutes=1) if mode == "old" else pd.Timedelta(0)
    return rsi(c3, 7).reindex(c.index + shift, method="ffill").values


class Coin:
    """Данные монеты и готовые сигналы на период."""

    def __init__(self, sym: str, a: str, b: str):
        self.sym = sym
        k = market.klines(sym, "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float)
        btc = market.klines("BTCUSDT", "1", pd.Timestamp(a) - pd.Timedelta(days=2), b).astype(float).reindex(k.index).ffill()
        self.r3 = rsi3(k.c)
        self.hi60 = k.h.rolling(60).max().values; self.lo60 = k.l.rolling(60).min().values
        self.ch120 = ((k.c / k.c.shift(120) - 1) * 100).values
        self.ch1440 = ((k.c / k.c.shift(1440) - 1) * 100).values
        self.btc_lo12 = (btc.c <= btc.l.shift(30).rolling(720).min()).values
        self.btc_hi12 = (btc.c >= btc.h.shift(30).rolling(720).max()).values
        bd = market.klines("BTCUSDT", "D", pd.Timestamp(a) - pd.Timedelta(days=120), b).astype(float)
        e50 = bd.c.ewm(span=50, adjust=False).mean(); e50.index = e50.index + pd.Timedelta(days=1)   # известна после закрытия дня
        self.btc_bear = (btc.c < e50.reindex(k.index, method="ffill")).values
        sel = (k.index >= pd.Timestamp(a)) & (k.index <= pd.Timestamp(b))
        self.k = k[sel]; self.sel = sel
        fr = funding(sym, a, b)
        fm = np.zeros(len(self.k)); pos = np.searchsorted(self.k.index.values, fr.index.values); ok = pos < len(self.k)
        fm[pos[ok]] = fr.values[ok]; self.fund = fm
        self.fund_last = pd.Series(fr.values, fr.index).reindex(self.k.index, method="ffill").fillna(0).values
        rng = market.klines(sym, "D", pd.Timestamp(a) - pd.Timedelta(days=90), pd.Timestamp(a)).astype(float)
        self.daily_range = float(((rng.h - rng.l) / rng.c).median()) if len(rng) else np.nan

    def signal(self, p: P) -> np.ndarray:
        s = self.sel; c = self.k.c.values
        if p.side == 1:
            sig = (self.r3[s] <= p.rsi_thr) & ((c / self.hi60[s] - 1) * 100 <= -p.drop1h * p.scale) & (self.ch120[s] <= -p.drop2h * p.scale)
            if p.btc_filter:
                sig &= ~self.btc_lo12[s]
        else:
            sig = (self.r3[s] >= 100 - p.rsi_thr) & ((c / self.lo60[s] - 1) * 100 >= p.drop1h * p.scale) & (self.ch120[s] >= p.drop2h * p.scale)
            if p.btc_filter:
                sig &= ~self.btc_hi12[s]
        if p.crash_skip is not None:
            ch1d = self.ch1440[s] if hasattr(self, "ch1440") else np.zeros(len(sig))
            sig &= (ch1d > -p.crash_skip * p.scale) if p.side == 1 else (ch1d < p.crash_skip * p.scale)
        if p.regime == "медвежий":
            sig &= self.btc_bear[s]
        elif p.regime == "бычий":
            sig &= ~self.btc_bear[s]
        if p.funding_min is not None:
            sig &= self.fund_last >= p.funding_min
        sig = np.nan_to_num(sig).astype(bool)
        if p.random_entry:
            rng = np.random.default_rng(p.seed)
            freq = (sig & ~np.r_[False, sig[:-1]]).mean() if p.random_onsets else sig.mean()
            sig = rng.random(len(sig)) < freq
        return sig


def run(coin: Coin, p: P, E0: float = 10_000.0, state_out: dict | None = None, expo_out: dict | None = None):
    k = coin.k; o, h, l, c = (k[x].values for x in "ohlc"); n = len(c)
    sig = coin.signal(p); sd = p.side
    scale_arr = isinstance(p.scale, np.ndarray)
    FULL = p.sizes[:len(p.levels) + 1].sum(); base_usd = E0 * p.L / FULL
    cash = E0; x = None; eq = np.empty(n); eq[0] = E0; liq = None; trades = 0; camps = []; cool = -1
    nt = np.zeros(n, np.float32) if expo_out is not None else None   # объём позиции по закрытию минуты, $
    ur = np.zeros(n, np.float32) if expo_out is not None else None   # открытый результат по худшей цене минуты, $
    thr = p.through / 100
    slip = p.slip / 100
    down = np.zeros(n, bool)
    for a_, b_ in p.outages:
        down |= (k.index >= pd.Timestamp(a_)) & (k.index <= pd.Timestamp(b_))
    for i in range(1, n):
        if down[i]:
            if x is not None:
                worst = l[i] if sd == 1 else h[i]
                e_w = cash + sd * (worst - x["cost"] / x["Q"]) * x["Q"]
                if e_w <= 0.005 * x["Q"] * worst:
                    camps.append((k.index[x["t0"]], k.index[i], -x["c0"], x["k"], "ликвидация", x["p0"], x["cost"] / x["Q"], worst))
                    liq = k.index[i]; cash = 0.0; x = None; eq[i:] = 0.0
                    break
            eq[i] = cash + (sd * (c[i] - x["cost"] / x["Q"]) * x["Q"] if x is not None else 0.0)
            continue
        if x is not None and p.funding_on and coin.fund[i]:
            cash -= sd * x["Q"] * c[i - 1] * coin.fund[i]
        if x is None:
            if sig[i - 1] and i >= cool:
                px_in = o[i] * (1 + sd * slip)
                bu = min(E0, cash) * p.L / FULL if p.cap_to_equity else base_usd
                q = bu / px_in
                sc = p.scale[i - 1] if scale_arr else p.scale          # масштаб на минуту сигнала
                x = dict(Q=q, cost=q * px_in, p0=px_in, k=0, last=i, t0=i, c0=cash, bu=bu, sc=sc, lv=p.levels * sc / 100)
                cash -= q * px_in * p.taker; trades += 1
        else:
            # докупки / досдачи
            while x["k"] < len(x["lv"]):
                lvl = x["p0"] * (1 - sd * x["lv"][x["k"]])
                hit = (l[i] <= lvl * (1 - thr)) if sd == 1 else (h[i] >= lvl * (1 + thr))
                if not hit:
                    break
                q = x["bu"] / x["p0"] * p.sizes[x["k"] + 1]
                x["Q"] += q; x["cost"] += q * lvl; x["k"] += 1; x["last"] = i; cash -= q * lvl * p.maker; trades += 1
            avg = x["cost"] / x["Q"]
            if p.hard_stop is not None:
                stop_px = x["p0"] * (1 - sd * p.hard_stop * x["sc"] / 100)
                if (l[i] <= stop_px) if sd == 1 else (h[i] >= stop_px):
                    gap = min(stop_px, o[i]) if sd == 1 else max(stop_px, o[i])
                    px_out = gap * (1 - sd * (slip + p.stop_slip / 100))
                    # биржа ликвидирует раньше, чем сработает стоп, если цена ликвидации ближе (счёт меньше позиции)
                    liq_px = (avg * x["Q"] - cash) / (0.995 * x["Q"]) if sd == 1 else (cash + avg * x["Q"]) / (1.005 * x["Q"])
                    if (px_out <= liq_px) if sd == 1 else (px_out >= liq_px):
                        camps.append((k.index[x["t0"]], k.index[i], -x["c0"], x["k"], "ликвидация", x["p0"], avg, liq_px))
                        liq = k.index[i]; cash = 0.0; x = None; eq[i:] = 0.0
                        break
                    pnl = sd * (px_out - avg) * x["Q"]
                    cash += pnl - px_out * x["Q"] * p.taker; trades += 1
                    camps.append((k.index[x["t0"]], k.index[i], pnl, x["k"], "стоп", x["p0"], avg, px_out)); x = None
                    cool = i + int(p.stop_cooldown_h * 60)
                    eq[i] = cash
                    continue
            tp = x["p0"] * (1 + sd * p.tp1 / 100) if x["k"] == 0 else avg * (1 + sd * p.tpn / 100)
            tp_hit = (h[i] >= tp * (1 + thr)) if sd == 1 else (l[i] <= tp * (1 - thr))
            if i > x["last"] and tp_hit:
                pnl = sd * (tp - avg) * x["Q"]
                cash += pnl - tp * x["Q"] * p.maker; trades += 1
                camps.append((k.index[x["t0"]], k.index[i], pnl, x["k"], "цель", x["p0"], avg, tp)); x = None
            elif (i - x["last"]) >= p.timer_h * 60:
                px_out = c[i] * (1 - sd * slip)
                pnl = sd * (px_out - avg) * x["Q"]
                cash += pnl - px_out * x["Q"] * p.taker; trades += 1
                camps.append((k.index[x["t0"]], k.index[i], pnl, x["k"], "таймер", x["p0"], avg, px_out)); x = None
        if x is not None:
            worst = l[i] if sd == 1 else h[i]
            e_w = cash + sd * (worst - x["cost"] / x["Q"]) * x["Q"]
            if e_w <= 0.005 * x["Q"] * worst:
                camps.append((k.index[x["t0"]], k.index[i], -x["c0"], x["k"], "ликвидация", x["p0"], x["cost"] / x["Q"], worst))
                liq = k.index[i]; cash = 0.0; x = None; eq[i:] = 0.0
                break
            if nt is not None:
                nt[i] = x["Q"] * c[i]; ur[i] = sd * (worst - x["cost"] / x["Q"]) * x["Q"]
        eq[i] = cash + (sd * (c[i] - x["cost"] / x["Q"]) * x["Q"] if x is not None else 0.0)
    E = pd.Series(eq, k.index)
    C = pd.DataFrame(camps, columns=["t0", "t1", "pnl", "adds", "exit", "p0", "avg", "px_out"])
    if expo_out is not None:
        m = nt > 0
        expo_out["notional"] = pd.Series(nt[m], k.index[m]); expo_out["unreal"] = pd.Series(ur[m], k.index[m])
    if state_out is not None:                     # открытая позиция на конец прогона (для живого прогона)
        state_out["open"] = None if x is None else dict(t0=k.index[x["t0"]], last=k.index[x["last"]], p0=x["p0"], adds=x["k"],
                                                         avg=x["cost"] / x["Q"], Q=x["Q"], px=c[-1], cash=cash)
    return E, C, liq, trades


def yearly(E: pd.Series, E0=10_000.0) -> dict:
    d = E.resample("D").last().dropna(); ye = d.resample("YE").last(); prev = [E0] + list(ye.values[:-1])
    out = {f"{y.year}": int(round(v - pv)) for y, v, pv in zip(ye.index, ye.values, prev)}
    dd = (d / d.cummax() - 1).min() * 100
    out["просадка"] = int(round(dd)) if dd == dd else 0
    return out


class FrameCoin(Coin):
    """Монета из готового кадра минуток (например, с искусственным обвалом). Биткоин-фильтр — по настоящему биткоину."""

    def __init__(self, sym: str, k_full: pd.DataFrame, a, b, btc_full: pd.DataFrame | None = None):
        self.sym = sym
        k = k_full.astype(float)
        btc = (btc_full if btc_full is not None else market.klines("BTCUSDT", "1", k.index.min(), k.index.max())).astype(float).reindex(k.index).ffill()
        self.r3 = rsi3(k.c)
        self.hi60 = k.h.rolling(60).max().values; self.lo60 = k.l.rolling(60).min().values
        self.ch120 = ((k.c / k.c.shift(120) - 1) * 100).values
        self.ch1440 = ((k.c / k.c.shift(1440) - 1) * 100).values
        self.btc_lo12 = (btc.c <= btc.l.shift(30).rolling(720).min()).values
        self.btc_hi12 = (btc.c >= btc.h.shift(30).rolling(720).max()).values
        self.btc_bear = np.zeros(len(k), bool)
        sel = (k.index >= pd.Timestamp(a)) & (k.index <= pd.Timestamp(b))
        self.k = k[sel]; self.sel = sel
        try:
            fr = funding(sym, a, b)
        except Exception:
            fr = pd.Series(dtype=float)
        fm = np.zeros(len(self.k))
        if len(fr):
            pos = np.searchsorted(self.k.index.values, fr.index.values); ok = pos < len(self.k); fm[pos[ok]] = fr.values[ok]
        self.fund = fm; self.fund_last = np.zeros(len(self.k))
        self.daily_range = np.nan
