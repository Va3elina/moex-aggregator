"""Download ETH-USD 1H candles from Coinbase Exchange."""
import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PRODUCT = "ETH-USD"
GRANULARITY = 3600
URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
CHUNK_HOURS = 290
OUT_PATH = Path(__file__).parent / "eth_usd_hourly_coinbase.csv"

START = datetime(2016, 5, 18, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def fetch_chunk(start, end):
    params = {"granularity": GRANULARITY,
              "start": start.isoformat(),
              "end": end.isoformat()}
    for attempt in range(4):
        try:
            r = requests.get(URL, params=params, timeout=30,
                             headers={"User-Agent": "eth-backtest/1.0"})
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 ** attempt); continue
            if r.status_code >= 400:
                return []  # tolerate gaps in early history
            return r.json()
        except Exception:
            time.sleep(2 ** attempt)
    return []  # tolerate persistent failures


def main():
    all_rows = {}
    cursor = START
    chunk_n = 0
    while cursor < END:
        chunk_end = min(cursor + timedelta(hours=CHUNK_HOURS), END)
        candles = fetch_chunk(cursor, chunk_end)
        for row in candles:
            all_rows[int(row[0])] = row
        chunk_n += 1
        if chunk_n % 30 == 0:
            print(f"  chunk {chunk_n}: {cursor.isoformat()}  total={len(all_rows)}")
        cursor = chunk_end
        time.sleep(0.35)

    sorted_rows = sorted(all_rows.values(), key=lambda r: r[0])
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datetime","open","high","low","close","volume"])
        for ts, low, high, open_, close, volume in sorted_rows:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            w.writerow([dt, open_, high, low, close, volume])

    if sorted_rows:
        first = datetime.fromtimestamp(sorted_rows[0][0], tz=timezone.utc)
        last = datetime.fromtimestamp(sorted_rows[-1][0], tz=timezone.utc)
        print(f"\nSaved {len(sorted_rows)} hourly rows to {OUT_PATH.name}")
        print(f"Range: {first.isoformat()} .. {last.isoformat()}")


if __name__ == "__main__":
    main()
