#!/usr/bin/env python3
"""Точечный backfill дневного OI (физ/юр) по диапазону дат из ISS MOEX.

Нужен потому что штатный fetch_oi_daily_realtime.py инкрементален
(start_date = max(tradedate)+1) и НЕ закрывает дыры РАНЬШЕ последней даты
(напр. апрельская регрессия источника при наличии майских/июньских данных).

Маппинг колонок и знак pos_short (-abs) — 1:1 с fetch_oi_daily_realtime.py.
Идемпотентно (ON CONFLICT DO UPDATE). Выходные/неторговые дни ISS отдаёт
пустыми — пропускаем.

Usage: DB_URL=... python backfill_oi_range.py SECTYPE ISS_CODE FROM TILL
   напр.  ... backfill_oi_range.py PI PIKK 2026-04-07 2026-05-03
"""
import sys
import os
import json
import time
import urllib.request
from datetime import date, timedelta

from sqlalchemy import create_engine, text

SECTYPE, ISS_CODE, FROM, TILL = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
BASE = "https://iss.moex.com/iss/statistics/engines/futures/markets/forts/openpositions"

UP = text("""
    INSERT INTO open_interest
      (sectype, tradedate, tradetime, clgroup, pos, pos_long, pos_short,
       pos_long_num, pos_short_num, interval)
    VALUES
      (:sectype, :tradedate, '00:00:00', :clgroup, :pos, :pos_long, :pos_short,
       :pos_long_num, :pos_short_num, 24)
    ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) DO UPDATE SET
      pos = EXCLUDED.pos,
      pos_long = EXCLUDED.pos_long,
      pos_short = EXCLUDED.pos_short,
      pos_long_num = EXCLUDED.pos_long_num,
      pos_short_num = EXCLUDED.pos_short_num
""")


def fetch(d: str):
    url = f"{BASE}/{ISS_CODE}.json?date={d}&iss.meta=off"
    with urllib.request.urlopen(url, timeout=30) as r:
        j = json.load(r)
    op = j.get("open_positions", {})
    cols = op.get("columns", [])
    return [dict(zip(cols, row)) for row in op.get("data", [])]


def main():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    d0 = date.fromisoformat(FROM)
    d1 = date.fromisoformat(TILL)
    days = 0
    rows = 0
    with engine.begin() as conn:
        d = d0
        while d <= d1:
            if d.weekday() < 5:  # пн-пт; выходные ISS всё равно отдаёт пусто
                try:
                    recs = fetch(d.isoformat())
                except Exception as e:
                    print(f"{d} FETCH ERROR: {type(e).__name__}: {e}")
                    recs = []
                if recs:
                    days += 1
                    for rec in recs:
                        clgroup = "FIZ" if rec.get("is_fiz") == 1 else "YUR"
                        pl = rec.get("open_position_long", 0) or 0
                        ps = rec.get("open_position_short", 0) or 0
                        conn.execute(UP, {
                            "sectype": SECTYPE,
                            "tradedate": d,
                            "clgroup": clgroup,
                            "pos": pl + ps,
                            "pos_long": pl,
                            "pos_short": -abs(ps),
                            "pos_long_num": rec.get("persons_long", 0) or 0,
                            "pos_short_num": rec.get("persons_short", 0) or 0,
                        })
                        rows += 1
                    print(f"{d}: {len(recs)} rec(s) upserted")
                time.sleep(0.15)
            d += timedelta(days=1)
    print(f"DONE {SECTYPE} ({ISS_CODE}) {FROM}..{TILL}: {days} trading days, {rows} rows")


if __name__ == "__main__":
    main()
