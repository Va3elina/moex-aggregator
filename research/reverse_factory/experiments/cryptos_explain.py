"""CryptosMX: разбор каждой сделки — лесенки и ручные заявки.

1) Склеиваем частичные исполнения одной секунды в одно событие.
2) Фазы: подряд идущие события одной стороны (покупки / продажи) по монете.
3) В фазе события одного размера (±6%) — одна «лесенка» (он ставит пачку одинаковых лимиток);
   одиночные размеры и крупные куски — отдельные ручные заявки.
4) Для каждой лесенки: число исполнений, размер к позиции, диапазон цен к средней, шаг между уровнями.
5) Для каждого события — строка «почему»: какая лесенка / ручная заявка, где цена к средней, что было с ценой перед этим.
→ trades_explained.csv, ladders.csv и печать по фазам.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"


def events() -> pd.DataFrame:
    T = pd.read_parquet(OUT / "trades_full.parquet")
    T = T[~T.manual].copy()
    agg = dict(qty=("qty", "sum"), px=("px", "mean"), master_px=("master_px", "mean"), order=("order", "first"),
               pos_before=("pos_before", "first"), pos_after=("pos_after", "last"), avg_before=("avg_before", "first"),
               avg_after=("avg_after", "last"))
    keep = ["r5", "r15", "r60", "r240", "r1440", "btc60", "from_hi4", "from_lo4", "from_hi24", "from_lo24", "bar_pos",
            "vs_last_buy", "min_since_buy", "vs_last_sell", "min_since_sell", "round_dist", "post_t", "post", "hrs_after_post", "opp_share", "toward"]
    for k in keep:
        agg[k] = (k, "first")
    E = T.groupby(["sym", "t", "side"], sort=False).agg(**agg).reset_index().sort_values(["sym", "t"], kind="stable")
    E["mpx"] = E.master_px.fillna(E.px)
    E["vs_avg"] = (E.mpx / E.avg_before - 1) * 100
    E["frac_pos"] = np.where(E.side == "buy", E.qty / E.pos_after, E.qty / E.pos_before) * 100
    # фазы
    E["phase"] = (E.side != E.groupby("sym").side.shift()).groupby(E.sym).cumsum()
    return E.reset_index(drop=True)


def ladders(E: pd.DataFrame) -> pd.DataFrame:
    E["ladder"] = ""
    rows = []
    lid = 0
    for (sym, ph), g in E.groupby(["sym", "phase"]):
        sizes = g.qty.values
        used = np.zeros(len(g), bool)
        for i in range(len(g)):
            if used[i]:
                continue
            same = (~used) & (np.abs(sizes - sizes[i]) <= 0.06 * sizes[i] + 1)
            if same.sum() >= 3:
                lid += 1
                idx = g.index[same]
                used |= same
                x = E.loc[idx].sort_values("mpx", ascending=(g.side.iloc[0] == "sell"))
                steps = np.abs(np.diff(x.mpx.values)) / x.mpx.values[:-1] * 100
                name = f"Л{lid}"
                E.loc[idx, "ladder"] = name
                rows.append(dict(лесенка=name, монета=sym.replace("USDT", ""), сторона=g.side.iloc[0], фаза=ph,
                                 с=x.t.min(), по=x.t.max(), исполнений=len(x), кусок=int(np.median(x.qty)),
                                 кусок_к_позиции=f"{np.median(x.frac_pos):.1f}%",
                                 от_средней=f"{x.vs_avg.iloc[0]:+.2f}%", до_средней=f"{x.vs_avg.iloc[-1]:+.2f}%",
                                 шаг_медиана=f"{np.median(steps):.2f}%" if len(steps) else "", шаг_разброс=f"{np.percentile(steps,25):.2f}–{np.percentile(steps,75):.2f}%" if len(steps) else "",
                                 лимитных=f"{(x.order == 'лимитная').mean():.0%}"))
    return pd.DataFrame(rows)


def why(r) -> str:
    move = f"за час {r.r60:+.1f}%, за 5 мин {r.r5:+.1f}%"
    where = f"{r.vs_avg:+.2f}% к средней" if r.vs_avg == r.vs_avg else "первая покупка"
    kind = "лимитка" if r.order == "лимитная" else ("рыночная" if r.order == "рыночная" else "тип неясен")
    if r.ladder:
        return f"{r.ladder}: {kind}, {where}; {move}"
    big = "крупный кусок " if r.frac_pos >= 10 else ""
    return f"ручная {big}{kind} ({r.frac_pos:.0f}% позиции), {where}; {move}"


if __name__ == "__main__":
    E = events()
    Ld = ladders(E)
    E["почему"] = [why(r) for r in E.itertuples()]
    E.to_csv(OUT / "trades_explained.csv", index=False)
    Ld.to_csv(OUT / "ladders.csv", index=False)
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 500); pd.set_option("display.max_colwidth", 120)
    print(f"событий {len(E)}: в лесенках {(E.ladder != '').mean():.0%}, ручных {(E.ladder == '').mean():.0%}")
    print(E.groupby(["side", E.ladder != ""]).agg(n=("qty", "size"), доля_объёма=("qty", "sum")).to_string())
    print("\nЛЕСЕНКИ"); print(Ld.to_string(index=False))
