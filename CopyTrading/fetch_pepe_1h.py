"""Fetch 1H PEPE-USDT klines from KuCoin (Bybit/Binance blocked from this region)."""
import csv
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

URL = "https://api.kucoin.com/api/v1/market/candles"
SYMBOL = "PEPE-USDT"
OUT_PATH = Path(__file__).parent / "1000pepe_1h_kucoin.csv"

START_SEC = int(datetime(2024, 9, 1, tzinfo=timezone.utc).timestamp())
END_SEC = int(datetime(2025, 10, 1, tzinfo=timezone.utc).timestamp())
CHUNK_HOURS = 1400  # under 1500 limit


def fetch(start_s, end_s):
    for attempt in range(8):
        r = requests.get(URL, timeout=30,
                         params={"symbol": SYMBOL, "type": "1hour",
                                 "startAt": start_s, "endAt": end_s},
                         headers={"User-Agent": "research/1.0"})
        if r.status_code == 200:
            data = r.json()
            if data.get("code") == "200000":
                return data["data"]
        time.sleep(min(30, 2 ** attempt))
    raise RuntimeError(f"Failed {start_s}-{end_s}")


def main():
    print(f"Fetching {SYMBOL} 1H bars from KuCoin...")
    all_bars = {}
    cursor = END_SEC
    while cursor > START_SEC:
        start = max(cursor - CHUNK_HOURS * 3600, START_SEC)
        bars = fetch(start, cursor)
        if not bars:
            break
        new = 0
        for row in bars:
            ts = int(row[0])
            if ts not in all_bars:
                all_bars[ts] = row
                new += 1
        if new == 0:
            break
        earliest = min(int(r[0]) for r in bars)
        cursor = earliest - 1
        time.sleep(0.3)

    rows = sorted(all_bars.values(), key=lambda r: int(r[0]))
    rows = [r for r in rows if START_SEC <= int(r[0]) <= END_SEC]

    # KuCoin order: [time, open, close, high, low, volume, turnover]
    with OUT_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datetime", "open", "high", "low", "close", "volume", "turnover"])
        for r in rows:
            ts, op, cl, hi, lo, vol, turn = r[:7]
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            w.writerow([dt, op, hi, lo, cl, vol, turn])

    if rows:
        first = datetime.fromtimestamp(int(rows[0][0]), tz=timezone.utc)
        last = datetime.fromtimestamp(int(rows[-1][0]), tz=timezone.utc)
        print(f"Saved {len(rows)} bars to {OUT_PATH}")
        print(f"Range: {first} → {last}")


if __name__ == "__main__":
    main()
