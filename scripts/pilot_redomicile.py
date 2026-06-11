#!/usr/bin/env python3
"""ПИЛОТ #3 — Навес расконвертации (редомициляция) — event-study.

Гипотеза: после переезда в РФ / возобновления торгов конвертированные «бывшие
нерезы» получают возможность продать → навес предложения → акция недоперформит
IMOEX и свой сектор в окне после события (ЦБ-лимит 5%/10% растягивает навес).

Даты возобновления торгов получены ЭМПИРИЧЕСКИ из дыр в свечах (>20 дней) — кроме
новых листингов (YDEX/HEAD/X5), где день 0 = первая свеча. Малая выборка (N=8),
поэтому печатаем каждое событие + агрегат. Метрика — market-adjusted доходность
(vs IMOEX и vs сектор) в окнах [0,+20], [0,+40], [0,+60] торговых дней.
"""
import os
from datetime import date
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])

# (secid, дата события, сектор-бенчмарк, описание)
EVENTS = [
    ("VKCO", date(2023, 10, 20), "MOEXIT", "VK МКАО (возобновление)"),
    ("GEMC", date(2024, 2, 1),  "MOEXCN", "ЮМГ/EMC (редомициляция)"),
    ("T",    date(2024, 3, 18), "MOEXFN", "Т-Технологии (ex-TCSG)"),
    ("MDMG", date(2024, 6, 17), "MOEXCN", "Мать и дитя (редомициляция)"),
    ("YDEX", date(2024, 7, 24), "MOEXIT", "Яндекс МКПАО (новый листинг)"),
    ("HEAD", date(2024, 9, 26), "MOEXIT", "Хэдхантер (новый листинг)"),
    ("X5",   date(2025, 1, 9),  "MOEXCN", "ИКС 5 (новый листинг)"),
    ("OZON", date(2025, 11, 11), "MOEXIT", "OZON МКПАО (возобновление)"),
]
WINS = [(0, 20), (0, 40), (0, 60)]


def series(secid, table="candles"):
    if table == "candles":
        df = pd.read_sql(text(
            "SELECT begin_time::date d, close FROM candles WHERE secid=:s AND type='stock' "
            "AND interval=24 AND close>0 ORDER BY begin_time"),
            eng, params={"s": secid}, parse_dates=["d"])
    else:
        df = pd.read_sql(text(
            "SELECT trade_date d, close FROM index_data WHERE secid=:s AND close>0 ORDER BY trade_date"),
            eng, params={"s": secid}, parse_dates=["d"])
    return df.set_index("d")["close"]


def car(stock, bench, cal, ev, lo, hi):
    pos = cal.searchsorted(np.datetime64(ev))
    i1, i2 = pos + lo, pos + hi
    if i1 < 0 or i2 >= len(cal):
        return None
    d1, d2 = cal[i1], cal[i2]
    s1, s2 = stock.asof(d1), stock.asof(d2)
    b1, b2 = bench.asof(d1), bench.asof(d2)
    if not all(np.isfinite(x) for x in (s1, s2, b1, b2)) or s1 <= 0 or b1 <= 0:
        return None
    return (s2 / s1 - 1.0) - (b2 / b1 - 1.0)


def main():
    print("=" * 86)
    print("ПИЛОТ #3 — Навес редомициляции (event-study, market-adjusted)")
    print("=" * 86)
    imoex = series("IMOEX", "index")
    cal = imoex.index.values.astype("datetime64[D]"); cal.sort()
    sect_cache = {}

    agg = {w: {"imoex": [], "sect": []} for w in WINS}
    print(f"\n{'Событие':32s} {'дата':11s} │ vs IMOEX [0,+20]/[+40]/[+60] │ vs Сектор")
    print("-" * 86)
    for secid, ev, sec, desc in EVENTS:
        st = series(secid)
        if sec not in sect_cache:
            sect_cache[sec] = series(sec, "index")
        sb = sect_cache[sec]
        line_i, line_s = [], []
        for w in WINS:
            ci = car(st, imoex, cal, ev, *w)
            cs = car(st, sb, cal, ev, *w)
            if ci is not None:
                agg[w]["imoex"].append(ci); line_i.append(f"{ci*100:+5.1f}%")
            else:
                line_i.append("  n/a")
            if cs is not None:
                agg[w]["sect"].append(cs); line_s.append(f"{cs*100:+5.1f}%")
            else:
                line_s.append("  n/a")
        print(f"{desc:32s} {str(ev):11s} │ {' '.join(line_i):24s} │ {' '.join(line_s)}")

    print("-" * 86)
    print("АГРЕГАТ (среднее по событиям, отрицательное = навес давит вниз):")
    for w in WINS:
        ai = np.array(agg[w]["imoex"], float); as_ = np.array(agg[w]["sect"], float)
        si = f"vs IMOEX: сред={ai.mean()*100:+5.1f}% медиана={np.median(ai)*100:+5.1f}% доля<0={(ai<0).mean()*100:.0f}% (n={len(ai)})"
        ss = f"vs Сектор: сред={as_.mean()*100:+5.1f}% медиана={np.median(as_)*100:+5.1f}% доля<0={(as_<0).mean()*100:.0f}% (n={len(as_)})"
        print(f"  окно [0,+{w[1]}]: {si}")
        print(f"  {'':12s}  {ss}")
    print("\n[N=8 — это event-study, не статистика; смотрим знак и согласованность. "
          "Навес подтверждён, если большинство событий недоперформят (отрицательно) "
          "и в [0,+40/+60], когда лимиты ЦБ растягивают продажи]")


if __name__ == "__main__":
    main()
