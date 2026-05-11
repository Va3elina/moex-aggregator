"""Download BTC-USD daily candles from Coinbase Exchange and save to CSV.

API: https://api.exchange.coinbase.com/products/BTC-USD/candles
Returns rows of [time, low, high, open, close, volume], up to 300 per request.
"""
import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PRODUCT = "BTC-USD"
GRANULARITY = 86400  # 1 day in seconds
URL = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
CHUNK_DAYS = 290  # under 300-candle limit, leaves margin
OUT_PATH = Path(__file__).parent / "btc_usd_daily_coinbase.csv"

# Coinbase Exchange (GDAX) BTC-USD started trading 2015. Start a bit earlier
# to let the API return whatever it has.
START = datetime(2014, 12, 1, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def fetch_chunk(start: datetime, end: datetime) -> list[list]:
    params = {
        "granularity": GRANULARITY,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    for attempt in range(5):
        r = requests.get(URL, params=params, timeout=30,
                         headers={"User-Agent": "btc-backtest/1.0"})
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Rate-limited too many times for {start} .. {end}")


def main() -> None:
    all_rows: dict[int, list] = {}  # dedupe by timestamp
    cursor = START
    while cursor < END:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), END)
        candles = fetch_chunk(cursor, chunk_end)
        for row in candles:
            ts = int(row[0])
            all_rows[ts] = row
        print(f"  {cursor.date()} .. {chunk_end.date()}: +{len(candles)} "
              f"(total {len(all_rows)})")
        cursor = chunk_end
        time.sleep(0.35)  # be polite (Coinbase public: ~3 rps)

    sorted_rows = sorted(all_rows.values(), key=lambda r: r[0])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for ts, low, high, open_, close, volume in sorted_rows:
            date_str = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            w.writerow([date_str, open_, high, low, close, volume])

    if sorted_rows:
        first = datetime.fromtimestamp(sorted_rows[0][0], tz=timezone.utc).date()
        last = datetime.fromtimestamp(sorted_rows[-1][0], tz=timezone.utc).date()
        print(f"\nSaved {len(sorted_rows)} rows to {OUT_PATH}")
        print(f"Range: {first} .. {last}")
    else:
        print("No data returned.")


if __name__ == "__main__":
    main()
