"""Fetch BTC global daily OHLCV (aggregate across exchanges) from CryptoCompare.

Free public API. limit=2000 days per request, paginate with toTs.
volumefrom = BTC qty traded globally, volumeto = USD volume globally.
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://min-api.cryptocompare.com/data/v2/histoday"
OUT_PATH = Path(__file__).parent / "btc_daily_global_cryptocompare.csv"


def fetch_chunk(to_ts):
    for attempt in range(8):
        r = requests.get(URL, timeout=30,
                         params={"fsym": "BTC", "tsym": "USD",
                                 "limit": 2000, "toTs": to_ts},
                         headers={"User-Agent": "btc-research/1.0"})
        if r.status_code == 200:
            return r.json()
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"Failed at toTs={to_ts}")


def main():
    print("Fetching CryptoCompare global BTC daily history...")
    all_rows = {}
    to_ts = int(datetime.now(timezone.utc).timestamp())
    earliest_target = int(datetime(2014, 1, 1, tzinfo=timezone.utc).timestamp())

    while to_ts > earliest_target:
        data = fetch_chunk(to_ts)
        rows = data.get("Data", {}).get("Data", [])
        if not rows:
            break
        new = 0
        for row in rows:
            ts = row["time"]
            if ts not in all_rows:
                all_rows[ts] = row
                new += 1
        if new == 0:
            break
        earliest_in_chunk = rows[0]["time"]
        print(f"  chunk up to {datetime.fromtimestamp(to_ts, tz=timezone.utc).date()} "
              f"({len(rows)} rows; total so far {len(all_rows)})")
        # Step back: next chunk ends at earliest_in_chunk - 1 day
        to_ts = earliest_in_chunk - 86400
        if earliest_in_chunk <= earliest_target:
            break
        time.sleep(0.3)

    sorted_rows = sorted(all_rows.values(), key=lambda r: r["time"])
    # Filter to >= 2014
    sorted_rows = [r for r in sorted_rows if r["time"] >= earliest_target]

    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume_btc", "volume_usd"])
        for r in sorted_rows:
            d = datetime.fromtimestamp(r["time"], tz=timezone.utc).date().isoformat()
            w.writerow([d, r["open"], r["high"], r["low"], r["close"],
                        r["volumefrom"], r["volumeto"]])

    print(f"\nSaved {len(sorted_rows)} rows to {OUT_PATH}")
    if sorted_rows:
        first = datetime.fromtimestamp(sorted_rows[0]["time"], tz=timezone.utc).date()
        last = datetime.fromtimestamp(sorted_rows[-1]["time"], tz=timezone.utc).date()
        print(f"Range: {first} .. {last}")


if __name__ == "__main__":
    main()
