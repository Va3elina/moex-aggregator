#!/usr/bin/env python3
"""Историческая капитализация акций → stock_market_cap (помесячно).

cap = close(конец месяца) × ISSUESIZE (акций в обращении из ISS). У нас в БД cap
был только за 3 мес — этот скрипт достраивает историю для разрезов стратегий по
размеру компании. ISSUESIZE берётся ТЕКУЩИЙ (ISS не отдаёт историю share count);
для классификации крупная/средняя/малая этого достаточно (ранг стабилен), но для
бумаг с эмиссиями/байбэками абсолютная капа прошлого приблизительна.

Запуск: python Candles/backfill_market_cap.py
"""
import os
import json
import urllib.request
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
ISS = ("https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/"
       "securities.json?iss.meta=off&iss.only=securities")
UA = "Mozilla/5.0 (compatible; FrameBot/1.0)"


def fetch_issuesize() -> dict:
    req = urllib.request.Request(ISS, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    c = d["securities"]["columns"]
    si, ii = c.index("SECID"), c.index("ISSUESIZE")
    out = {}
    for row in d["securities"]["data"]:
        if row[ii]:
            out[row[si]] = float(row[ii])
    return out


def main():
    eng = create_engine(DB_URL)
    issize = fetch_issuesize()
    print(f"ISSUESIZE получено по {len(issize)} бумагам с TQBR")

    # месячные close по всем нашим акциям
    secids = list(issize.keys())
    px = pd.read_sql(text(
        "SELECT secid, begin_time::date d, close FROM candles "
        "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
        eng, params={"s": secids}, parse_dates=["d"])
    px["ym"] = px["d"].dt.to_period("M")
    # последняя свеча каждого месяца
    mon = px.sort_values("d").groupby(["secid", "ym"]).tail(1)

    n = 0
    with eng.begin() as conn:
        for _, r in mon.iterrows():
            sz = issize.get(r["secid"])
            if not sz:
                continue
            cap = float(r["close"]) * sz
            conn.execute(text("""
                INSERT INTO stock_market_cap (sec_id, period_date, market_cap)
                VALUES (:s, :d, :c)
                ON CONFLICT (sec_id, period_date) DO UPDATE SET market_cap = EXCLUDED.market_cap
            """), {"s": r["secid"], "d": r["d"].date(), "c": round(cap, 2)})
            n += 1
    print(f"upsert: {n} месячных строк капитализации в stock_market_cap "
          f"по {mon['secid'].nunique()} бумагам")


if __name__ == "__main__":
    main()
