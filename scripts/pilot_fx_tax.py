#!/usr/bin/env python3
"""ПИЛОТ #2 — Налоговый период / экспортёры ↔ OI ЮР.

Гипотеза: экспортёры вынужденно продают валюту к единому налоговому дню (28-е,
НДД/НДПИ/налог на прибыль) → спрос на рубль → USDRUB/CNYRUB слабеют в окне ДО 28-го
и отыгрывают после. Подтверждение: юрлица (ЮР) на Si/CR двигают нетто-позицию в
сторону рубля (шорт USD/CNY-фьючерса) к 28-му.

Метрика — сырой ход курса (FX это сам актив, бенчмарк не нужен) в окнах относительно
последнего торгового дня <= 28-го. Режимы обязательной продажи выручки размечены
хардкодом (амплитуда эффекта режимо-зависима).
"""
import os
from datetime import date
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])

W_PRE = (-7, -1)    # до 28-го: ожидаем укрепление рубля (FX вниз)
W_POST = (1, 7)     # после: ожидаем откат вверх
W_CTRL = (-18, -12)  # контроль (середина прошлого периода)

# Режимы обязательной продажи валютной выручки (грубо, по указам)
def regime(d: date) -> str:
    if d < date(2022, 6, 10):
        return "2022 жёсткий мандат (80%→искл.)"
    if d < date(2023, 10, 11):
        return "2022-09..2023-09 без мандата"
    if d < date(2024, 7, 1):
        return "2023-10..2024-06 указ 771 (жёсткий)"
    if d < date(2025, 9, 1):
        return "2024-07..2025-08 смягчённый"
    return "2025-09+ мандат снят"


def tax_anchors(cal, start="2022-01-01", end="2026-06-01"):
    """Последний торговый день <= 28-го для каждого месяца."""
    out = []
    for ym in pd.period_range(start, end, freq="M"):
        d28 = np.datetime64(date(ym.year, ym.month, 28))
        pos = cal.searchsorted(d28, side="right") - 1
        if 0 <= pos < len(cal):
            out.append(cal[pos])
    return out


def fx_series(secid):
    df = pd.read_sql(text(
        "SELECT trade_date, close FROM index_data WHERE secid=:s AND close>0 ORDER BY trade_date"),
        eng, params={"s": secid}, parse_dates=["trade_date"])
    return df.set_index("trade_date")["close"]


def oi_net(sectype, clgroup):
    df = pd.read_sql(text(
        "SELECT tradedate, pos FROM open_interest WHERE sectype=:s AND clgroup=:c "
        "AND interval=24 AND pos IS NOT NULL ORDER BY tradedate"),
        eng, params={"s": sectype, "c": clgroup}, parse_dates=["tradedate"])
    return df.set_index("tradedate")["pos"]


def win_ret(s, cal, anchor, lo, hi):
    pos = cal.searchsorted(np.datetime64(anchor))
    i1, i2 = pos + lo, pos + hi
    if i1 < 0 or i2 >= len(cal):
        return None
    v1 = s.asof(cal[i1]); v2 = s.asof(cal[i2])
    if not (np.isfinite(v1) and np.isfinite(v2)) or v1 <= 0:
        return None
    return v2 / v1 - 1.0


def stat(arr):
    a = np.asarray(arr, float); a = a[np.isfinite(a)]
    if len(a) < 5:
        return "n<5"
    m, sd = a.mean(), a.std(ddof=1)
    t = m / (sd / np.sqrt(len(a))) if sd > 0 else 0
    return f"n={len(a):3d}  сред={m*100:+5.2f}%  медиана={np.median(a)*100:+5.2f}%  t={t:+4.1f}  доля<0={(a<0).mean()*100:4.0f}%"


def run(secid, fut, label):
    print("\n" + "=" * 78)
    print(f"{label}: {secid} (фьючерс {fut})")
    print("=" * 78)
    fx = fx_series(secid)
    cal = fx.index.values.astype("datetime64[D]"); cal.sort()
    anchors = tax_anchors(cal)
    rows = []
    for a in anchors:
        ad = pd.Timestamp(a).date()
        rows.append(dict(anchor=ad, reg=regime(ad),
                         pre=win_ret(fx, cal, a, *W_PRE),
                         post=win_ret(fx, cal, a, *W_POST),
                         ctrl=win_ret(fx, cal, a, *W_CTRL)))
    ev = pd.DataFrame(rows)
    print(f"налоговых окон: {len(ev)}")
    print(f"  ПРЕ  [{W_PRE[0]:+},{W_PRE[1]:+}] (укрепление руб?): {stat(ev['pre'])}")
    print(f"  КОНТРОЛЬ[{W_CTRL[0]:+},{W_CTRL[1]:+}]:              {stat(ev['ctrl'])}")
    print(f"  ПОСТ [{W_POST[0]:+},{W_POST[1]:+}] (откат?):        {stat(ev['post'])}")
    print("  — по режимам (ПРЕ-окно):")
    for rg, g in ev.groupby("reg"):
        print(f"    {rg:38s} {stat(g['pre'])}")

    # OI ЮР: меняется ли нетто-позиция юрлиц к 28-му (шорт USD = укрепление руб)
    yur = oi_net(fut, "YUR")
    fiz = oi_net(fut, "FIZ")
    if len(yur) > 50:
        dys, dfs = [], []
        for a in anchors:
            pos = cal.searchsorted(np.datetime64(a))
            if pos + W_PRE[0] < 0 or pos >= len(cal):
                continue
            y0 = yur.asof(cal[pos + W_PRE[0]]); y1 = yur.asof(cal[pos + W_PRE[1]])
            f0 = fiz.asof(cal[pos + W_PRE[0]]); f1 = fiz.asof(cal[pos + W_PRE[1]])
            if np.isfinite(y0) and np.isfinite(y1):
                dys.append(y1 - y0)
            if np.isfinite(f0) and np.isfinite(f1):
                dfs.append(f1 - f0)
        ay = np.array(dys, float); af = np.array(dfs, float)
        print("  — OI в ПРЕ-окне (Δ нетто-позиции, лоты; <0 = разворот в сторону рубля):")
        print(f"    ЮР: сред Δ={ay.mean():+.0f}  медиана={np.median(ay):+.0f}  доля<0={(ay<0).mean()*100:4.0f}%  (n={len(ay)})")
        print(f"    ФИЗ: сред Δ={af.mean():+.0f}  медиана={np.median(af):+.0f}  доля<0={(af<0).mean()*100:4.0f}%  (n={len(af)})")


def main():
    print("ПИЛОТ #2 — Налоговый период / экспортёры ↔ OI ЮР (28-е число)")
    run("USD000UTSTOM", "Si", "USDRUB ↔ Si")
    run("CNYRUB_TOM", "CR", "CNYRUB ↔ CR")
    print("\n[гипотеза в плюс, если ПРЕ-окно курса в среднем ОТРИЦАТЕЛЬНО (руб крепнет) "
          "сильнее контроля, ПОСТ — разворот вверх, и ЮР в ПРЕ-окне снижает нетто "
          "(разворот в сторону рубля); сильнее в режимах жёсткого мандата]")


if __name__ == "__main__":
    main()
