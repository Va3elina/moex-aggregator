"""Стадия 6. Как выходит: фиксированный %, в долях ATR, по времени, по обратному сигналу.

Логика: у какого способа измерения выходы «ровнее» (меньше разброс), тот и ближе к правилу.
  * фиксированный % — кластеры в распределении хода (fingerprint.exits);
  * ATR — ход / ATR14 на свече входа: если разброс этого отношения заметно меньше
    разброса самого хода — выход привязан к волатильности;
  * время — мода длительности удержания + выходы ровно на границе свечи;
  * переворот — сразу после выхода новый вход по той же монете.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import market
from .entry_rules import atr


def run(p: pd.DataFrame, fp: dict, tf_min: int | None = None, max_n: int = 300) -> dict:
    c = p[p.t_close.notna()].copy()
    ex = fp["exits"]
    out = dict(fixed_tp=ex.get("fixed_tp"), fixed_sl=ex.get("fixed_sl"), reentry=ex.get("reentry_after_exit_share"))
    if c.empty:
        return out
    # выходы на границе свечей
    grid = {}
    for tf in [15, 60, 240, 1440]:
        since = (c.t_close - c.t_close.dt.floor(f"{tf}min")).dt.total_seconds() / 60
        grid[tf] = round(float((since < 2).mean()), 2)
    out["exit_on_candle_close"] = grid
    hm = c.hold_h.round(2)
    mode = hm.mode().iloc[0] if len(hm) else None
    out["hold_mode_h"], out["hold_mode_share"] = mode, round(float((hm == mode).mean()), 2) if mode is not None else None
    # ATR на свече входа
    interval = market.tf_to_interval(tf_min) if tf_min else "60"
    step = pd.Timedelta(minutes=market.MIN[interval])
    top = c.sym.value_counts().index[:8]
    s = c[c.sym.isin(top)].tail(max_n)
    ratios = []
    for sym, g in s.groupby("sym"):
        k = market.klines(sym, interval, g.t_open.min() - step * 40, g.t_open.max() + step)
        if len(k) < 30:
            continue
        a = 100 * atr(k, 14) / k.c
        at = a.reindex(g.t_open.dt.floor(step) - step).values
        ratios += list(zip(g.pnl_pct.values, at))
    if len(ratios) >= 20:
        R = pd.DataFrame(ratios, columns=["pnl", "atr"]).dropna()
        R = R[R.atr > 0]
        R["k"] = R.pnl / R.atr
        cv = lambda x: float(x.std() / abs(x.mean())) if len(x) > 4 and x.mean() else np.nan
        w, l = R[R.pnl > 0], R[R.pnl < 0]
        out["atr"] = dict(interval=interval, win_pct_cv=round(cv(w.pnl), 2), win_atr_cv=round(cv(w.k), 2),
                          loss_pct_cv=round(cv(l.pnl), 2), loss_atr_cv=round(cv(l.k), 2),
                          win_k_median=round(float(w.k.median()), 2) if len(w) else None,
                          loss_k_median=round(float(l.k.median()), 2) if len(l) else None)
    # вердикт
    v = []
    if out["fixed_tp"]:
        v.append(f"тейк фиксированный ≈ +{out['fixed_tp']['level']}%")
    if out["fixed_sl"]:
        v.append(f"стоп фиксированный ≈ {out['fixed_sl']['level']}%")
    A = out.get("atr")
    if A and not out["fixed_tp"] and A["win_atr_cv"] and A["win_pct_cv"] and A["win_atr_cv"] < 0.75 * A["win_pct_cv"]:
        v.append(f"тейк похож на ≈{A['win_k_median']}×ATR")
    if A and not out["fixed_sl"] and A["loss_atr_cv"] and A["loss_pct_cv"] and A["loss_atr_cv"] < 0.75 * A["loss_pct_cv"]:
        v.append(f"стоп похож на ≈{A['loss_k_median']}×ATR")
    if (out.get("reentry") or 0) >= 0.3:
        v.append(f"{int(100 * out['reentry'])}% выходов сразу сменяются новым входом — переворот или перезапуск бота")
    if (out.get("hold_mode_share") or 0) >= 0.3 and (out.get("hold_mode_h") or 0) > 0.05:
        v.append(f"выход по времени ≈ через {out['hold_mode_h']} ч")
    if not v:
        v.append("выход плавающий: по сигналу или скользящему стопу")
    out["verdict"] = v
    return out
