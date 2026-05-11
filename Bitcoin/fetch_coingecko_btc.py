"""Fetch BTC daily market cap, price, and total_volume from CoinGecko.

⚠ As of late 2024 CoinGecko's free public API restricts historical data to
the last 365 days (returns HTTP 401 / error_code 10012 for older ranges).
For full BTC history (2010+) use fetch_coinmetrics_btc.py instead, which
hits the free CoinMetrics community API.

This script is kept as a reference / fallback if you have a Pro key in
the future.
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
OUT_PATH = Path(__file__).parent / "btc_daily_marketcap_coingecko.csv"

START = int(datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp())
END = int(datetime.now(timezone.utc).timestamp())


def fetch(start_ts, end_ts):
    last_err = None
    for attempt in range(8):
        try:
            r = requests.get(URL,
                             params={"vs_currency": "usd",
                                     "from": start_ts, "to": end_ts},
                             timeout=60,
                             headers={"User-Agent": "btc-research/1.0"})
        except requests.RequestException as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
            continue
        if r.status_code == 200:
            return r.json()
        last_err = f"HTTP {r.status_code}: {r.text[:300]}"
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"Failed: {last_err}")


def main():
    print(f"Fetching CoinGecko BTC daily from {datetime.fromtimestamp(START, tz=timezone.utc).date()} ..")
    data = fetch(START, END)
    prices = data.get("prices", [])
    mcaps = data.get("market_caps", [])
    vols = data.get("total_volumes", [])
    n = min(len(prices), len(mcaps), len(vols))
    print(f"  prices={len(prices)} mcaps={len(mcaps)} vols={len(vols)}  using n={n}")
    rows = []
    for i in range(n):
        ts_ms, p = prices[i]
        _, mc = mcaps[i]
        _, v = vols[i]
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()
        rows.append((dt, p, mc, v))
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "price_usd", "market_cap_usd", "total_volume_usd"])
        # de-dupe by date, keep last
        last_by_date = {}
        for r in rows:
            last_by_date[r[0]] = r
        for r in sorted(last_by_date.values()):
            w.writerow(r)
    print(f"\nSaved {len(last_by_date)} unique daily rows to {OUT_PATH}")
    if last_by_date:
        first = min(last_by_date)
        last = max(last_by_date)
        print(f"Range: {first} .. {last}")


if __name__ == "__main__":
    main()
