"""Стадия 4. Копия по кривой доходности (как реплицируют хедж-фонды).

Идея (сработала на Algotoria 26.09.2026, корр. 0,65 вне подбора): не угадывать
сделки, а подобрать неотрицательную смесь простых стратегий, дневная доходность
которой лучше всего повторяет дневную доходность цели. Подбор на первой половине,
проверка на второй. Веса по семействам = «природа» стратегии.

Библиотека (на каждую монету, дневные и 4-часовые свечи):
  тренд       — «ход L»: знак изменения цены за L свечей; EMA f/s; пробой канала N
  возврат     — против изменения за L свечей
  держание    — просто лонг (бета к рынку)
Копия строится ТОЛЬКО из торгуемых правил (сигнал на закрытии вчера → доходность сегодня).
Отдельно — «форма доходности»: участие цели в росте и падении рынка (BTC+ETH).
  участие в падениях > в росте — продаёт страховку (докупщики, сетки);
  участие в росте > в падениях — покупает страховку (тренд, пробой).
Не торгуемые формы (−|r|, max(r,0)…) в копию не входят — они завышали совпадение.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import nnls

from . import market

FAMILY = [("докупка", "докупка (продажа страховки)"),("продажа волатильности", "продажа страховки"), ("покупка волатильности", "покупка страховки"),
          ("только падения", "продажа страховки"), ("только рост", "покупка страховки"),
          ("возврат", "возврат к среднему"), ("держать", "держание (бета рынка)"),
          ("ход", "тренд"), ("EMA", "тренд"), ("пробой", "тренд")]


def family(name: str) -> str:
    for key, fam in FAMILY:
        if key in name:
            return fam
    return "прочее"


def library(k: pd.DataFrame, tag: str) -> pd.DataFrame:
    c = k.c.astype(float)
    r = c.pct_change()
    vol = r.rolling(20).std()
    w = (vol.median() / vol).clip(upper=3)
    out = {}

    def add(name, pos):
        out[f"{tag} {name}"] = pos.clip(-1, 1).shift(1) * w.shift(1) * r

    for L in [1, 2, 3, 5, 10, 20, 40, 60, 100, 200]:
        add(f"ход {L}", np.sign(c / c.shift(L) - 1))
    for f, s in [(5, 20), (10, 50), (20, 100), (50, 200)]:
        add(f"EMA{f}/{s}", np.sign(c.ewm(span=f).mean() - c.ewm(span=s).mean()))
    for N in [10, 20, 55]:
        hi, lo = k.h.rolling(N).max().shift(1), k.l.rolling(N).min().shift(1)
        add(f"пробой {N}", pd.Series(np.where(c > hi, 1, np.where(c < lo, -1, np.nan)), c.index).ffill().fillna(0))
    for L in [1, 2, 3, 5]:
        add(f"возврат {L}", -np.sign(c / c.shift(L) - 1))
    out[f"{tag} держать"] = r
    return pd.DataFrame(out)


def sim_dca(k: pd.DataFrame, step: float, tp: float, levels: int = 5, mult: float = 2.0) -> pd.Series:
    """Бот-докупка (лонг): базовый ордер по открытию, доборы ×mult на шагах вниз, тейк от средней.
    Торгуемая стратегия — решения только по прошлому; доходность = изменение стоимости / объём лесенки."""
    o, h, l, c = (k[x].values.astype(float) for x in ["o", "h", "l", "c"])
    sizes = mult ** np.arange(levels)
    F = sizes.sum()
    cash = qty = cost = base = 0.0
    filled, prev_eq, out = 0, 0.0, np.zeros(len(c))
    for i in range(len(c)):
        if qty == 0:
            base = o[i]; q = sizes[0] / base; qty, cost, filled = q, sizes[0], 1; cash -= sizes[0]
        elif qty > 0 and h[i] >= (cost / qty) * (1 + tp):          # тейк (с прошлой свечи)
            px = (cost / qty) * (1 + tp); cash += qty * px; qty = cost = 0.0; filled = 0
        while qty > 0 and filled < levels and l[i] <= base * (1 - step * filled):
            px = base * (1 - step * filled); q = sizes[filled] / px; qty += q; cost += sizes[filled]; cash -= sizes[filled]; filled += 1
        eq = cash + qty * c[i]
        out[i] = (eq - prev_eq) / F
        prev_eq = eq
    return pd.Series(out, k.index)


def shapes(k: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Формы доходности — НЕ торгуемые (берут доходность того же дня), только для описания природы."""
    r = k.c.astype(float).pct_change()
    out = {f"{tag} продажа волатильности (форма)": -(r.abs() - r.abs().rolling(60).mean().shift(1)),
           f"{tag} покупка волатильности (форма)": r.abs() - r.abs().rolling(60).mean().shift(1),
           f"{tag} только падения (форма)": r.clip(upper=0),
           f"{tag} только рост (форма)": r.clip(lower=0)}
    return pd.DataFrame(out)


def universe(p: pd.DataFrame, extra: int = 5) -> list[str]:
    base = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    vc = p.sym.value_counts(normalize=True)
    add = [s for s in vc.index if s not in base and vc[s] >= 0.05][:extra]
    return base + add


def run(eq: pd.Series, p: pd.DataFrame, intraday: bool = True) -> dict:
    eq = eq.dropna()
    eq = eq[eq > 0]
    y = eq.resample("D").last().pct_change().dropna()
    y = y[y.abs() < 0.9]
    if len(y) < 60:
        return dict(skipped=f"кривая короче 60 дней ({len(y)})")
    weak = len(y) < 180
    start = y.index.min() - pd.Timedelta(days=260)
    cols, shp = [], []
    for sym in universe(p):
        kd = market.klines(sym, "D", start, y.index.max() + pd.Timedelta(days=1))
        if len(kd) < 60:
            continue
        tag = sym.replace("USDT", "")
        cols.append(library(kd, f"{tag} [день]"))
        shp.append(shapes(kd, f"{tag} [день]"))
        if intraday:
            k4 = market.klines(sym, "240", y.index.min() - pd.Timedelta(days=60), y.index.max() + pd.Timedelta(days=1))
            if len(k4) > 200:
                lib4 = library(k4, f"{tag} [4ч]")
                for st, tp in [(0.01, 0.01), (0.02, 0.02), (0.03, 0.03)]:
                    lib4[f"{tag} [4ч] докупка шаг {int(st * 100)}% тейк {int(tp * 100)}%"] = sim_dca(k4, st, tp)
                cols.append(lib4.groupby(lib4.index.floor("D")).sum(min_count=1))
    if not cols:
        return dict(skipped="нет свечей")
    X = pd.concat(cols, axis=1).reindex(y.index)
    X = X.loc[:, X.notna().mean() > 0.9].fillna(0.0)
    half = y.index[len(y) // 2]

    def fit(a, b):
        w, _ = nnls(X[a:b].values, y[a:b].values)
        return pd.Series(w, X.columns)

    w_is = fit(None, half)
    pred_is = X @ w_is
    w = fit(None, None)
    pred = X @ w
    corr_is = float(y[:half].corr(pred_is[:half]))
    corr_oos = float(y[half:].corr(pred_is[half:]))
    corr_full = float(y.corr(pred))
    wn = (w / w.sum()) if w.sum() > 0 else w
    fams = wn.groupby(wn.index.map(family)).sum().sort_values(ascending=False)
    # форма доходности: участие в росте/падении рынка (среднее BTC и ETH)
    mk = pd.concat([pd.concat(shp, axis=1).filter(like=f"{t} [день] только")for t in ["BTC", "ETH"]], axis=1).reindex(y.index).fillna(0)
    up = mk.filter(like="только рост").mean(axis=1)
    dn = mk.filter(like="только падения").mean(axis=1)
    A = np.column_stack([np.ones(len(y)), up.values, dn.values])
    coef = np.linalg.lstsq(A, y.values, rcond=None)[0]
    beta_up, beta_dn = float(coef[1]), float(coef[2])
    shape = dict(beta_up=round(beta_up, 2), beta_down=round(beta_dn, 2),
                 verdict=("в обычные дни в падениях участвует сильнее, чем в росте" if beta_dn - beta_up > 0.1 else
                          "в обычные дни в росте участвует сильнее, чем в падениях (редкие обвалы тут не видны — см. «хвосты»)" if beta_up - beta_dn > 0.1 else
                          "в обычные дни симметрично"))
    single = pd.Series({c: y.corr(X[c]) for c in X.columns}).dropna().sort_values()
    # копия с той же волатильностью, что у цели
    vm = lambda s: s * (y.std() / s.std()) if s.std() > 0 else s
    curves = pd.DataFrame({"цель": (1 + y).cumprod(), "копия": (1 + vm(pred)).cumprod(),
                           "копия (подбор до середины)": (1 + vm(pred_is)).cumprod()})
    return dict(weak=weak, shape=shape, corr_in=round(corr_is, 2), corr_out=round(corr_oos, 2), corr_full=round(corr_full, 2),
                r2_full=round(corr_full ** 2, 2), split=str(half.date()), days=int(len(y)),
                families={k: round(float(v), 2) for k, v in fams.items() if v >= 0.02},
                top_weights={k: round(float(v), 3) for k, v in wn.sort_values(ascending=False).head(10).items() if v > 0.01},
                best_single={k: round(float(v), 2) for k, v in single.tail(6)[::-1].items()},
                worst_single={k: round(float(v), 2) for k, v in single.head(4).items()},
                _curves=curves)
