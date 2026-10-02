"""Тип каждой его заявки по ленте: лимитная (цена пришла к заявке) или рыночная (он «ударил» сам).

Для каждого исполнения копии:
  * всплеск подписчиков — плотная серия сделок той же стороны (≥25 за 20 мс) рядом с временем копии;
  * перед всплеском (300 мс и 2 с): чья сторона давила и куда шла цена;
  * «спусковая» сделка — последняя сделка перед всплеском.
Лимитная: спусковая сделка — ПРОТИВОПОЛОЖНОЙ стороны (кто-то продал в его заявку на покупку), цена шла к заявке;
  цена мастера = цена этой сделки (для покупки — минимальная из встречных за 300 мс, для продажи — максимальная).
Рыночная: перед всплеском — сделка той же стороны без встречного давления; цена мастера = её цена.
→ inbox/private/work/cryptosmx/tape_class.parquet
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"

if __name__ == "__main__":
    W = pd.read_parquet(OUT / "tape_windows.parquet")
    L = pd.read_parquet(OUT / "ledger.parquet")
    L = L[L.sym.isin(["DOGEUSDT", "1000PEPEUSDT"])]
    B = pd.read_parquet(OUT / "tape_bursts.parquet")
    rows = []
    for (sym, t), w in W.groupby(["sym", "fill_t"]):
        rr = L[(L.sym == sym) & (L.t == t)]
        for r in rr.itertuples():
            s = "Buy" if r.kind == "open" else "Sell"; o = "Sell" if s == "Buy" else "Buy"
            w = w.sort_values("ts", kind="stable")
            ww = w[(w.side == s) & (w.ts >= t - pd.Timedelta(seconds=5)) & (w.ts <= t + pd.Timedelta(seconds=2))]
            b = ww.groupby(ww.ts.dt.floor("20ms")).size(); b = b[b >= 25]
            row = dict(t=t, sym=sym, side="buy" if s == "Buy" else "sell", copy_px=r.px, copy_qty=r.qty)
            if b.empty:
                rows.append({**row, "order": "не найден"}); continue
            cand = b.index[np.argmin(np.abs((b.index - t).total_seconds()))]
            tb = cand
            while (tb - pd.Timedelta("20ms")) in b.index:
                tb -= pd.Timedelta("20ms")
            first = ww[ww.ts >= tb].iloc[0]
            # размер всплеска и отношение к копии — по записи всплесков
            bb = B[(B.sym == sym) & (B.side == s) & (B.t >= first.ts - pd.Timedelta("400ms")) & (B.t <= first.ts + pd.Timedelta("400ms"))]
            burst_n = int(bb.n.sum()) if len(bb) else np.nan
            burst_ratio = float(bb.qty.sum() / r.qty) if len(bb) else np.nan
            pre = w[(w.ts < first.ts) & (w.ts >= first.ts - pd.Timedelta(seconds=2))]
            p3 = pre[pre.ts >= first.ts - pd.Timedelta("300ms")]
            opp3 = p3[p3.side == o]
            last = pre.iloc[-1] if len(pre) else None
            opp_share = pre[pre.side == o]["size"].sum() / pre["size"].sum() if len(pre) else np.nan
            dpx = (pre.price.iloc[-1] / pre.price.iloc[0] - 1) * 100 if len(pre) > 1 else 0.0
            toward = -dpx if s == "Buy" else dpx                       # >0 — цена шла к его заявке
            is_limit = last is not None and last.side == o and (toward >= 0 or opp_share >= 0.6)
            if is_limit:
                mpx = opp3.price.min() if s == "Buy" else opp3.price.max()
                if not mpx == mpx:
                    mpx = last.price
                order = "лимитная"
            elif last is not None and last.side == s:
                mpx = last.price; order = "рыночная"
            else:
                mpx = first.price; order = "неясно"
            rows.append({**row, "order": order, "master_px": float(mpx), "burst_t": first.ts,
                         "lag_ms": (first.ts - t).total_seconds() * 1000, "gap_ms": (first.ts - last.ts).total_seconds() * 1000 if last is not None else np.nan,
                         "opp_share": opp_share, "toward": toward, "burst_n": burst_n, "burst_ratio": burst_ratio,
                         "slip": (r.px / mpx - 1) * 100 * (1 if s == "Buy" else -1)})
    C = pd.DataFrame(rows).drop_duplicates(["t", "sym", "side", "copy_px", "copy_qty"])
    C.to_parquet(OUT / "tape_class.parquet", index=False)
    pd.set_option("display.width", 200)
    print(C.groupby(["sym", "side", "order"]).size().unstack(fill_value=0))
    print("\nпроскальзывание копии относительно цены мастера, %:")
    print(C.groupby(["side", "order"]).slip.describe().round(3))
    print("\nотношение всплеска к копии:", C.burst_ratio.describe().round(0).to_dict())
