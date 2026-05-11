"""Generic Coinbase daily-candle fetcher. Usage: python ... ETH-USD"""
import csv
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

GRANULARITY = 86400
CHUNK_DAYS = 290
START = datetime(2014, 12, 1, tzinfo=timezone.utc)


def fetch_chunk(url, start, end):
    last_err = None
    for attempt in range(10):
        try:
            r = requests.get(url, params={"granularity": GRANULARITY,
                                          "start": start.isoformat(),
                                          "end": end.isoformat()},
                             timeout=30,
                             headers={"User-Agent": "crypto-backtest/1.0"})
        except requests.RequestException as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
            continue
        if r.status_code == 200:
            return r.json()
        last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"Failed {start} .. {end}  | last: {last_err}")


def main():
    product = sys.argv[1] if len(sys.argv) > 1 else "ETH-USD"
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    out_path = Path(__file__).parent / f"{product.lower().replace('-', '_')}_daily_coinbase.csv"

    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    all_rows = {}
    cursor = START
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), end)
        for row in fetch_chunk(url, cursor, chunk_end):
            all_rows[int(row[0])] = row
        cursor = chunk_end
        time.sleep(0.4)

    rows = sorted(all_rows.values(), key=lambda r: r[0])
    if not rows:
        print(f"No data returned for {product}")
        return
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for ts, low, high, op, cl, vol in rows:
            d = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            w.writerow([d, op, high, low, cl, vol])
    first = datetime.fromtimestamp(rows[0][0], tz=timezone.utc).date()
    last = datetime.fromtimestamp(rows[-1][0], tz=timezone.utc).date()
    print(f"{product}: saved {len(rows)} rows to {out_path}  ({first} .. {last})")


if __name__ == "__main__":
    main()
