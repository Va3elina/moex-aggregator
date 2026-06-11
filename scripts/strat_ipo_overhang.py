#!/usr/bin/env python3
"""ИССЛЕДОВАНИЕ — навес IPO: дебют, lock-up, Cash-out vs Cash-in.

Маленькая выборка (10 IPO 2024-2026) → ИЛЛЮСТРАЦИЯ, не статистика. Считаем:
  (1) дебютную динамику от цены размещения к +20/+60 торг. дней (vs IMOEX);
  (2) реакцию вокруг истечения lock-up: окно [-10,-1] (до) и [0,+10] (после),
      рыночно-скорректированно. Гипотеза навеса: после lock-up бумага слабее рынка.
  (3) разрез Cash-out (акционеры продают) vs Cash-in (деньги в компанию).
"""
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])


def main():
    ipo = pd.read_sql("SELECT * FROM ipo_deals", eng, parse_dates=["placement_date", "lockup_expiry"])
    imoex = pd.read_sql("SELECT trade_date, close FROM index_data WHERE secid='IMOEX' AND close>0 ORDER BY trade_date",
                        eng, parse_dates=["trade_date"]).set_index("trade_date")["close"]
    cal = imoex.index.values.astype("datetime64[D]"); cal.sort()
    tickers = ipo["ticker"].tolist()
    px = pd.read_sql(text("SELECT secid, begin_time::date d, close FROM candles "
                          "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
                     eng, params={"s": tickers}, parse_dates=["d"])
    closes = {sid: g.sort_values("d").set_index("d")["close"] for sid, g in px.groupby("secid")}

    def at(s, i):
        if s is None or i < 0 or i >= len(cal):
            return None
        v = s.asof(cal[i]); return v if (v is not None and np.isfinite(v)) else None

    def mret(s, i0, i1):
        a, b = at(s, i0), at(s, i1)
        m0, m1 = at(imoex, i0), at(imoex, i1)
        if not all(x and x > 0 for x in (a, b, m0, m1)):
            return None
        return (b / a - 1.0) - (m1 / m0 - 1.0)

    rows = []
    for _, r in ipo.iterrows():
        cl = closes.get(r["ticker"])
        if cl is None or len(cl) < 5:
            continue
        p0 = cal.searchsorted(np.datetime64(r["placement_date"].date()))
        first = cl.iloc[0]
        deb20 = (at(cl, p0 + 20) / first - 1.0) if at(cl, p0 + 20) else None  # vs цена 1-го дня (груб)
        # lock-up event (если прошёл и есть данные вокруг)
        pre = post = None
        if pd.notna(r["lockup_expiry"]):
            pl = cal.searchsorted(np.datetime64(r["lockup_expiry"].date()))
            pre = mret(cl, pl - 10, pl - 1)
            post = mret(cl, pl, pl + 10)
        rows.append(dict(ticker=r["ticker"], typ=r["deal_type"], lvl=r["listing_level"],
                         flt=r["float_pct"], deb20=deb20, pre=pre, post=post))
    df = pd.DataFrame(rows)

    def fmt(x):
        return f"{x*100:+5.1f}%" if x is not None and np.isfinite(x) else "  n/a"

    print("=" * 78)
    print("(1) ПО БУМАГАМ: дебют +20дн (от 1-го дня) | lock-up [-10,-1] | lock-up [0,+10]")
    print("=" * 78)
    print(f"{'тик':6}{'тип':10}{'эш':3}{'fl%':6}{'деб+20':>9}{'до lockup':>11}{'после':>9}")
    for _, r in df.iterrows():
        print(f"{r.ticker:6}{r.typ:10}{int(r.lvl):<3}{r.flt:<6.1f}{fmt(r.deb20):>9}{fmt(r.pre):>11}{fmt(r.post):>9}")

    def agg(mask, col):
        v = df.loc[mask, col].dropna()
        v = v[np.isfinite(v)]
        return (len(v), v.mean()) if len(v) else (0, np.nan)

    print("\n" + "=" * 78)
    print("(2) НАВЕС ПОСЛЕ LOCK-UP [0,+10] (рыночно-скорр.) — Cash-out vs Cash-in")
    print("=" * 78)
    for typ in ["Cash-out", "Cash-in"]:
        n, mn = agg((df.typ == typ), "post")
        npre, mpre = agg((df.typ == typ), "pre")
        print(f"  {typ:9}: после n={n} сред {fmt(mn)} | до n={npre} сред {fmt(mpre)}")
    n, mn = agg(df.index >= 0, "post")
    print(f"  ВСЕ      : после n={n} сред {fmt(mn)}")
    print("\n[выборка крошечная (n≈9) — читаем как иллюстрацию знака, не как t-стат]")


if __name__ == "__main__":
    main()
