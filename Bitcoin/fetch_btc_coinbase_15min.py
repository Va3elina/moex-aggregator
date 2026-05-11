"""Download BTC-USD 15-min candles from Coinbase Exchange. ~378K bars."""
import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PRODUCT = "BTC-USD"
GRANULARITY = 900  # 15 min
URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
CHUNK_MIN = 290 * 15  # 290 bars * 15 min
OUT_PATH = Path(__file__).parent / "btc_usd_15min_coinbase.csv"

START = datetime(2015, 7, 20, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(minute=(datetime.now(timezone.utc).minute // 15) * 15,
                                          second=0, microsecond=0)


def fetch_chunk(start, end):
    last_err = None
    for attempt in range(10):
        try:
            r = requests.get(URL,
                             params={"granularity": GRANULARITY, "start": start.isoformat(),
                                     "end": end.isoformat()},
                             timeout=30,
                             headers={"User-Agent": "btc-backtest/1.0"})
        except requests.RequestException as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
            continue
        if r.status_code == 200:
            return r.json()
        last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"Failed: {start} .. {end}  | last: {last_err}")


def main():
    all_rows = {}
    cursor = START
    chunk_n = 0
    while cursor < END:
        chunk_end = min(cursor + timedelta(minutes=CHUNK_MIN), END)
        for row in fetch_chunk(cursor, chunk_end):
            all_rows[int(row[0])] = row
        chunk_n += 1
        if chunk_n % 50 == 0:
            print(f"  chunk {chunk_n}: {cursor.isoformat()} total={len(all_rows)}")
        cursor = chunk_end
        time.sleep(0.5)

    rows = sorted(all_rows.values(), key=lambda r: r[0])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datetime", "open", "high", "low", "close", "volume"])
        for ts, low, high, op, cl, vol in rows:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            w.writerow([dt, op, high, low, cl, vol])

    if rows:
        first = datetime.fromtimestamp(rows[0][0], tz=timezone.utc)
        last = datetime.fromtimestamp(rows[-1][0], tz=timezone.utc)
        print(f"\nSaved {len(rows)} 15-min rows to {OUT_PATH}")
        print(f"Range: {first.isoformat()} .. {last.isoformat()}")


if __name__ == "__main__":
    main()
