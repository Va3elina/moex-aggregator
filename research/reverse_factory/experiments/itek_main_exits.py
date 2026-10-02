"""ITEK основной счёт: выходы — лесенка продаж кусками + трейлинг-стоп остатка. Сверка при его реальных покупках.

Модель выхода (покупки берём его — проверяем только продажи):
  первая продажа фазы: цена ≥ средняя × (1 + a%); дальше каждая следующая ≥ прошлая продажа × (1 + s%);
  кусок = f × позиция на начало фазы продаж; после первой продажи следим за максимумом цены —
  откат на tr% от максимума → продать весь остаток. Любая его покупка обновляет среднюю и начинает фазу заново.
Совпадение: его продажа и наша в ±5 мин и ±0.2% по цене. Перебор a, s, f, tr по монетам.
"""
from __future__ import annotations

import itertools
import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
M = T[(T.acc == "основной") & T.sym.isin(["1000PEPE", "DOGE", "SOL"]) & (~T.manual) & (~T.liq)].copy()
K = {s: market.klines(s + "USDT", "1", "2024-10-18", "2025-02-04").astype(float) for s in ["1000PEPE", "DOGE", "SOL"]}


def sim_camp(k: pd.DataFrame, buys: pd.DataFrame, t_end, a, s, f, tr):
    """Продажи модели для одной кампании при его покупках."""
    idx = k.index; o, h, l, c = (k[x].values for x in "ohlc")
    i0 = idx.searchsorted(buys.t.iloc[0].floor("min")); i1 = min(idx.searchsorted(t_end.floor("min")) + 180, len(idx) - 1)
    bt = buys.t.dt.floor("min").values; bq = buys.qty.values; bp = buys.px.values
    Q = cost = 0.0; nxt = np.inf; piece = 0.0; peak = None; out = []; bi = 0
    for i in range(i0, i1):
        while bi < len(bt) and bt[bi] <= idx[i].to_datetime64():
            Q += bq[bi]; cost += bq[bi] * bp[bi]; bi += 1
            avg = cost / Q; nxt = avg * (1 + a / 100); piece = f * Q; peak = None
        if Q <= 1e-12:
            if bi >= len(bt):
                break
            continue
        avg = cost / Q
        while Q > 1e-12 and h[i] >= nxt:
            px = nxt; q = min(piece, Q)
            if Q - q < 0.01 * piece: q = Q
            Q -= q; cost = avg * Q; out.append((idx[i], px)); nxt = px * (1 + s / 100)
            peak = px if peak is None else max(peak, px)
        if peak is not None and Q > 1e-12:
            peak = max(peak, h[i])
            if l[i] <= peak * (1 - tr / 100):
                out.append((idx[i], peak * (1 - tr / 100))); Q = cost = 0.0
        if Q <= 1e-12 and bi >= len(bt):
            break
    return out


def score(args):
    sym, a, s, f, tr = args
    g = M[M.sym == sym]; k = K[sym]
    his_n = hit = mod_n = 0
    for cid, c in g.groupby("camp"):
        buys = c[c.side == "buy"]; sells = c[c.side == "sell"]
        if buys.empty or sells.empty:
            continue
        mod = sim_camp(k, buys, sells.t.iloc[-1], a, s, f, tr)
        mt = np.array([m[0].value for m in mod]); mp_ = np.array([m[1] for m in mod])
        used = np.zeros(len(mod), bool)
        for r in sells.itertuples():
            if len(mt) == 0: break
            dt = np.abs(mt - r.t.floor("min").value) / 6e10; dp = np.abs(mp_ / r.px - 1) * 100
            cand = np.where((dt <= 5) & (dp <= 0.2) & ~used)[0]
            if len(cand): used[cand[0]] = True; hit += 1
        his_n += len(sells); mod_n += len(mod)
    return dict(монета=sym, a=a, s=s, f=f, tr=tr, его=his_n, наших=mod_n, совпало=hit, полнота=hit / his_n, точность=hit / max(mod_n, 1))


if __name__ == "__main__":
    grid = [(sym, a, s, f, tr) for sym in ["1000PEPE", "DOGE", "SOL"] for a, s, f, tr in itertools.product(
        [0.0, 0.3, 0.6, 1.0, 1.5], [0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5], [0.015, 0.025, 0.04, 0.06], [0.8, 1.2, 1.6, 2.0, 2.4])]
    with mp.get_context("fork").Pool(8) as pool:
        rows = pool.map(score, grid, chunksize=10)
    R = pd.DataFrame(rows)
    R["F1"] = 2 * R.полнота * R.точность / (R.полнота + R.точность)
    pd.set_option("display.width", 200)
    for sym, x in R.groupby("монета"):
        print(x.sort_values("F1", ascending=False).head(5).round(3).to_string(index=False))
    R.to_csv(ROOT / "inbox/private/work/itek/main_exits_fit.csv", index=False)
