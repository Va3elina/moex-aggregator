"""Fetch BTC daily market-cap & supply from CoinMetrics community API.

Free, no API key needed. Provides:
  PriceUSD       — reference price in USD
  CapMrktCurUSD  — circulating market cap in USD
  SplyCur        — circulating supply (BTC)
  TxTfrValAdjUSD — on-chain transfer value in USD (proxy for "real" usage)
  TxCnt          — transaction count
  AdrActCnt      — active address count

Paginate by start_time/next_page_token. Daily granularity, full BTC history.
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
OUT_PATH = Path(__file__).parent / "btc_daily_coinmetrics.csv"
METRICS = ["PriceUSD", "CapMrktCurUSD", "SplyCur"]


def fetch_page(params):
    for attempt in range(8):
        try:
            r = requests.get(URL, params=params, timeout=60,
                             headers={"User-Agent": "btc-research/1.0"})
        except requests.RequestException as e:
            time.sleep(min(60, 2 ** attempt))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(min(60, 2 ** attempt))
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")


def main():
    print(f"Fetching CoinMetrics BTC daily metrics: {METRICS}")
    all_rows = []
    next_token = None
    page = 0
    while True:
        params = {
            "assets": "btc",
            "metrics": ",".join(METRICS),
            "frequency": "1d",
            "start_time": "2010-07-17",
            "end_time": datetime.now(timezone.utc).date().isoformat(),
            "page_size": 10000,
        }
        if next_token:
            params["next_page_token"] = next_token
        data = fetch_page(params)
        rows = data.get("data", [])
        all_rows.extend(rows)
        page += 1
        next_token = data.get("next_page_token")
        if not next_token:
            break
        print(f"  page {page}: total rows so far = {len(all_rows)}")
        time.sleep(0.3)

    print(f"  total rows fetched: {len(all_rows)}")
    with OUT_PATH.open("w", newline="") as f:
        cols = ["date"] + METRICS
        w = csv.writer(f)
        w.writerow(cols)
        for row in all_rows:
            date = row["time"][:10]
            out = [date]
            for m in METRICS:
                v = row.get(m)
                out.append(v if v is not None else "")
            w.writerow(out)
    print(f"Saved to {OUT_PATH}")
    if all_rows:
        print(f"Range: {all_rows[0]['time'][:10]} .. {all_rows[-1]['time'][:10]}")


if __name__ == "__main__":
    main()
