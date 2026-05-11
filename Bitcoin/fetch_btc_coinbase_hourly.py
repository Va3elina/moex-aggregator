"""Download BTC-USD 1H candles from Coinbase Exchange.

Same approach as the daily fetcher but with granularity=3600. Each request
returns up to 300 candles = 12.5 hours days. Full history ~95K bars.
"""
import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PRODUCT = "BTC-USD"
GRANULARITY = 3600  # 1 hour
URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
CHUNK_HOURS = 290  # under 300 limit, leaves margin
OUT_PATH = Path(__file__).parent / "btc_usd_hourly_coinbase.csv"

START = datetime(2015, 7, 20, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def fetch_chunk(start: datetime, end: datetime) -> list[list]:
    params = {
        "granularity": GRANULARITY,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    for attempt in range(6):
        r = requests.get(URL, params=params, timeout=30,
                         headers={"User-Agent": "btc-backtest/1.0"})
        if r.status_code in (429, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Failed too many times: {start} .. {end}")


def main() -> None:
    all_rows: dict[int, list] = {}
    cursor = START
    chunk_n = 0
    while cursor < END:
        chunk_end = min(cursor + timedelta(hours=CHUNK_HOURS), END)
        candles = fetch_chunk(cursor, chunk_end)
        for row in candles:
            all_rows[int(row[0])] = row
        chunk_n += 1
        if chunk_n % 20 == 0:
            print(f"  chunk {chunk_n}: {cursor.isoformat()} .. {chunk_end.isoformat()}  "
                  f"total={len(all_rows)}")
        cursor = chunk_end
        time.sleep(0.35)

    sorted_rows = sorted(all_rows.values(), key=lambda r: r[0])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datetime", "open", "high", "low", "close", "volume"])
        for ts, low, high, open_, close, volume in sorted_rows:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            w.writerow([dt, open_, high, low, close, volume])

    if sorted_rows:
        first = datetime.fromtimestamp(sorted_rows[0][0], tz=timezone.utc)
        last = datetime.fromtimestamp(sorted_rows[-1][0], tz=timezone.utc)
        print(f"\nSaved {len(sorted_rows)} hourly rows to {OUT_PATH}")
        print(f"Range: {first.isoformat()} .. {last.isoformat()}")


if __name__ == "__main__":
    main()
