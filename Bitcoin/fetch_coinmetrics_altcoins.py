"""Fetch daily mcap + price for major altcoins from CoinMetrics community API.

Same approach as fetch_coinmetrics_btc.py but for ETH, LTC, BCH, SOL —
to test whether the BTC fundamental signals (stable_growth, mcap_z200)
generalize across crypto.
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
OUT_PATH = Path(__file__).parent / "altcoins_daily_coinmetrics.csv"
ASSETS = ["eth", "ltc", "bch"]  # SOL not in free tier
METRICS = ["PriceUSD", "CapMrktCurUSD", "SplyCur"]


def fetch_page(params):
    for attempt in range(8):
        r = requests.get(URL, params=params, timeout=60,
                         headers={"User-Agent": "btc-research/1.0"})
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 502, 503):
            time.sleep(min(60, 2 ** attempt))
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")


def main():
    print(f"Fetching CoinMetrics altcoins: {ASSETS}, metrics: {METRICS}")
    all_rows = []
    next_token = None
    while True:
        params = {
            "assets": ",".join(ASSETS),
            "metrics": ",".join(METRICS),
            "frequency": "1d",
            "start_time": "2014-01-01",
            "end_time": datetime.now(timezone.utc).date().isoformat(),
            "page_size": 10000,
        }
        if next_token:
            params["next_page_token"] = next_token
        data = fetch_page(params)
        all_rows.extend(data.get("data", []))
        next_token = data.get("next_page_token")
        if not next_token:
            break
        time.sleep(0.3)

    print(f"  fetched {len(all_rows)} rows")
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "asset"] + METRICS)
        for row in all_rows:
            out = [row["time"][:10], row["asset"]]
            for m in METRICS:
                out.append(row.get(m, ""))
            w.writerow(out)
    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
