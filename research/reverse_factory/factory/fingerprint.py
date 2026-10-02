"""Стадия 1. Почерк: всё, что видно по сделкам без свечей.

Каждый признак — отдельная функция, возвращает dict. Итог — один dict `fp`,
его читают classify.py и report.py. Пороги подобраны на целях, разобранных
вручную 26.09.2026 (Meridian, Syndicate, Algotoria, HYPE-мартингейл, XRP-сетка).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .schema import MAJORS

GRID_TF = [5, 15, 30, 60, 120, 240, 360, 480, 720, 1440]   # минуты


def basics(p: pd.DataFrame) -> dict:
    c = p[p.t_close.notna()]
    w, l = c[c.pnl_pct > 0].pnl_pct, c[c.pnl_pct < 0].pnl_pct
    span_d = max((p.t_open.max() - p.t_open.min()).total_seconds() / 86400, 1)
    sym = p.sym.value_counts()
    return dict(
        n_pos=int(len(p)), n_closed=int(len(c)), start=str(p.t_open.min())[:10], end=str(p.t_open.max())[:10],
        span_days=round(span_d), per_week=round(len(p) / (span_d / 7), 2),
        n_sym=int(sym.size), top_sym={k.replace("USDT", ""): int(v) for k, v in sym.head(6).items()},
        majors_share=round(p.sym.isin(MAJORS).mean(), 2), short_share=round((p.side < 0).mean(), 2),
        win=round(len(w) / max(len(c), 1), 3), avg_win=round(w.mean(), 2) if len(w) else 0.0,
        avg_loss=round(l.mean(), 2) if len(l) else 0.0,
        payoff=round(w.mean() / -l.mean(), 2) if len(w) and len(l) else None,
        expectancy=round(c.pnl_pct.mean(), 3) if len(c) else None,
        worst=round(c.pnl_pct.min(), 2) if len(c) else None, best=round(c.pnl_pct.max(), 2) if len(c) else None,
        hold_h=dict(zip(["p10", "p50", "p90"], np.round(c.hold_h.quantile([.1, .5, .9]).values, 1).tolist())) if len(c) else {},
        open_now=int(p.t_close.isna().sum()))


def timing(p: pd.DataFrame) -> dict:
    """Сетка свечей: доля входов в первые минуты после закрытия свечи таймфрейма."""
    t = p.t_open
    res = {}
    for tf in GRID_TF:
        since = (t - t.dt.floor(f"{tf}min")).dt.total_seconds() / 60
        win = max(2.0, min(7.0, 0.1 * tf))
        share = float((since < win).mean())
        res[tf] = dict(share=round(share, 3), ratio=round(share / (win / tf), 1), lag_min=round(float(since[since < win].median()), 1) if share else None)
    best = None
    for tf in GRID_TF:
        r = res[tf]
        if r["share"] >= 0.5 and r["ratio"] >= 3:
            best = tf
    sec = t.dt.second
    sec_share = float((sec <= 10).mean()) if sec.max() > 0 else None     # у invvo секунд нет
    hours = t.dt.hour.value_counts(normalize=True)
    return dict(grid_tf_min=best, grid=res[best] if best else None, grid_all={k: v["share"] for k, v in res.items()},
                sec_le10_share=round(sec_share, 2) if sec_share is not None else None,
                top_hours={int(k): round(v, 2) for k, v in hours.head(4).items()},
                top4_hours_share=round(float(hours.head(4).sum()), 2))


def batching(p: pd.DataFrame, gap_s: int = 120) -> dict:
    t = p.sort_values("t_open")
    ts = t.t_open.values.astype("datetime64[s]").astype(np.int64)
    syms = t.sym.values
    near = np.zeros(len(t), bool)
    for i in range(len(t)):
        lo, hi = np.searchsorted(ts, ts[i] - gap_s), np.searchsorted(ts, ts[i] + gap_s, side="right")
        near[i] = any(syms[j] != syms[i] for j in range(lo, hi))
    groups = (np.diff(ts, prepend=ts[0] - 10 * gap_s) > gap_s).cumsum()
    sizes = pd.Series(groups).value_counts()
    return dict(batch_share=round(float(near.mean()), 2), max_batch=int(sizes.max()) if len(sizes) else 0,
                batches_ge3=int((sizes >= 3).sum()))


def _modes(x: pd.Series, top=3) -> list[dict]:
    if len(x) < 5:
        return []
    out, rest = [], x.copy()
    for _ in range(top):
        if len(rest) < 3:
            break
        m = rest.round(1).mode().iloc[0]
        tol = max(0.15, 0.04 * abs(m))
        hit = (rest - m).abs() <= tol
        out.append(dict(level=float(m), share=round(float(hit.sum() / len(x)), 2)))
        rest = rest[~hit]
    return out


def exits(p: pd.DataFrame) -> dict:
    c = p[p.t_close.notna()]
    w, l = c[c.pnl_pct > 0].pnl_pct, c[c.pnl_pct < 0].pnl_pct
    tp, sl = _modes(w), _modes(l)
    fixed_tp = tp[0] if tp and tp[0]["share"] >= 0.35 and len(w) >= 8 else None
    fixed_sl = sl[0] if sl and sl[0]["share"] >= 0.35 and len(l) >= 8 else None
    # выход = одновременно новый вход по той же монете (переворот / перезапуск)
    starts = p.groupby("sym").t_open.apply(lambda s: np.sort(s.values.astype("datetime64[s]").astype(np.int64)))
    rev = 0
    for _, r in c.iterrows():
        s = starts.get(r.sym)
        if s is None:
            continue
        t1 = np.int64(pd.Timestamp(r.t_close).value // 10**9)
        k = np.searchsorted(s, t1 - 5)
        rev += int(k < len(s) and s[k] <= t1 + 90)
    return dict(tp_modes=tp, sl_modes=sl, fixed_tp=fixed_tp, fixed_sl=fixed_sl,
                reentry_after_exit_share=round(rev / max(len(c), 1), 2))


def averaging(p: pd.DataFrame, tr: pd.DataFrame) -> dict:
    multi = p[p.n_orders > 1]
    out = dict(multi_order_share=round(len(multi) / max(len(p), 1), 2),
               size_ratio=round(float(multi.size_ratio.median()), 2) if len(multi) and multi.size_ratio.notna().any() else None,
               add_dir=None, max_orders=int(p.n_orders.max()))
    if len(multi) and multi.add_dir.notna().any():
        d = multi.add_dir.mean()
        out["add_dir"] = "усреднение вниз" if d < -0.3 else "пирамида по ходу" if d > 0.3 else "в обе стороны"
    # без разбивки на ордера (invvo): разброс объёмов
    cost = p.cost.dropna()
    if len(cost) >= 10 and cost.quantile(.25) > 0:
        out["cost_mult_max"] = round(float(cost.max() / cost.quantile(.25)), 1)
        out["cost_cv"] = round(float(cost.std() / cost.mean()), 2)
    return out


def price_levels(tr: pd.DataFrame) -> dict:
    """Сетка уровней и круглые цены (признак лимиток руками или сетки)."""
    px = tr.order_price.where(tr.order_price.notna(), tr.p_open).dropna()
    px = px[px > 0]
    if px.empty:
        return {}
    mag = np.floor(np.log10(px))
    rounded = np.round(px / 10 ** (mag - 2)) * 10 ** (mag - 2)
    round_share = float((np.abs(rounded - px) / px < 1e-9).mean())
    # Сетка = ПОВТОРЯЮЩИЕСЯ уровни: бот раз за разом ставит заявки на одни и те же цены.
    # Одиночные цены (ручные или рыночные входы) в расчёт шага не берём.
    grid, repeat_orders = None, 0
    for sym, g in tr.groupby("sym"):
        x = pd.Series(np.round(g.order_price.where(g.order_price.notna(), g.p_open).dropna().values, 8))
        vc = x.value_counts()
        rep = np.sort(vc[vc >= 2].index.values)
        repeat_orders += int(vc[vc >= 2].sum())
        if len(rep) < 3:
            continue
        d = np.diff(rep)
        step = d.min()
        on = float(np.mean(np.abs(d / step - np.round(d / step)) < 0.05))
        if on >= 0.8 and (grid is None or len(rep) > grid["levels"]):
            grid = dict(sym=sym.replace("USDT", ""), step=float(round(step, 8)), step_pct=round(100 * step / float(np.median(rep)), 2),
                        levels=int(len(rep)), on_grid=round(on, 2))
    return dict(round_price_share=round(round_share, 2), repeat_price_share=round(repeat_orders / max(len(px), 1), 2), grid=grid)


def concurrency(p: pd.DataFrame) -> dict:
    t1 = p.t_close.fillna(pd.Timestamp.now(tz="UTC").tz_localize(None))
    ev = pd.concat([pd.DataFrame({"t": p.t_open, "d": 1}), pd.DataFrame({"t": t1, "d": -1})]).sort_values(["t", "d"])
    overlap_same = 0
    for _, g in p.assign(t1=t1).groupby("sym"):
        g = g.sort_values("t_open")
        overlap_same += int((g.t_open.values[1:] < np.maximum.accumulate(g.t1.values)[:-1]).sum())
    return dict(max_open=int(ev.d.cumsum().max()), same_symbol_overlaps=overlap_same)


def run(tr: pd.DataFrame, p: pd.DataFrame) -> dict:
    fp = dict(basics=basics(p), timing=timing(p), batching=batching(p), exits=exits(p),
              averaging=averaging(p, tr), levels=price_levels(tr), concurrency=concurrency(p))
    return fp
