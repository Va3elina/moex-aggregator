"""Fetch 1H candles for IMOEX + 8 mid/small-cap RU stocks via MOEX ISS API.

Pagination: max 500 candles per page, use `start` param.
"""
import csv
import time
from datetime import datetime
from pathlib import Path

import requests

OUT_DIR = Path(__file__).parent / "moex_1h"
OUT_DIR.mkdir(exist_ok=True)

INSTRUMENTS = {
    "IMOEX": ("index",  "IMOEX",  "2007-01-01"),
    "AFKS":  ("shares", "AFKS",   "2010-01-01"),
    "SMLT":  ("shares", "SMLT",   "2020-10-01"),
    "POSI":  ("shares", "POSI",   "2021-12-01"),
    "ASTR":  ("shares", "ASTR",   "2023-10-01"),
    "VKCO":  ("shares", "VKCO",   "2022-09-01"),
    "SOFL":  ("shares", "SOFL",   "2023-09-01"),
    "WUSH":  ("shares", "WUSH",   "2022-12-01"),
    "DELI":  ("shares", "DELI",   "2024-02-01"),
}


def fetch_pair(market, secid, start_date):
    url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/securities/{secid}/candles.json"
    today = datetime.now().strftime("%Y-%m-%d")
    all_rows = []
    start = 0
    while True:
        for attempt in range(5):
            try:
                r = requests.get(url, timeout=30,
                                 params={"interval": 60, "from": start_date,
                                         "till": today, "start": start})
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(2 ** attempt)
        else:
            break
        rows = r.json()["candles"]["data"]
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 500:
            break
        start += len(rows)
        time.sleep(0.1)
    return all_rows


def main():
    for sec, (market, sid, start_date) in INSTRUMENTS.items():
        out = OUT_DIR / f"{sec.lower()}_1h.csv"
        if out.exists():
            print(f"  {sec}: cached")
            continue
        print(f"  fetching {sec} ({market}, from {start_date})...")
        rows = fetch_pair(market, sid, start_date)
        if not rows:
            print(f"    no data")
            continue
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["datetime", "open", "high", "low", "close", "value", "volume"])
            for row in rows:
                op, cl, hi, lo, val, vol, begin, _ = row
                w.writerow([begin, op, hi, lo, cl, val, vol])
        first = rows[0][6]; last = rows[-1][6]
        print(f"    saved {len(rows)} bars  ({first[:10]} → {last[:10]})")


if __name__ == "__main__":
    main()
