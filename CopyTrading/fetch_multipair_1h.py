"""Fetch 1H KuCoin klines for top pairs the trader uses.

Bybit pair → KuCoin spot symbol mapping. Scale differences (1000X) don't
matter for percent-based indicators.
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

PAIRS = {
    # bybit_contract: kucoin_symbol
    "1000PEPEUSDT":   "PEPE-USDT",
    "DOGEUSDT":       "DOGE-USDT",
    "SOLUSDT":        "SOL-USDT",
    "1000FLOKIUSDT":  "FLOKI-USDT",
    "1000BONKUSDT":   "BONK-USDT",
    "SHIB1000USDT":   "SHIB-USDT",
    "WIFUSDT":        "WIF-USDT",
    "WLDUSDT":        "WLD-USDT",
    "XRPUSDT":        "XRP-USDT",
    "POPCATUSDT":     "POPCAT-USDT",
}

OUT_DIR = Path(__file__).parent / "pairs_1h"
OUT_DIR.mkdir(exist_ok=True)
START_SEC = int(datetime(2024, 9, 1, tzinfo=timezone.utc).timestamp())
END_SEC = int(datetime(2025, 10, 1, tzinfo=timezone.utc).timestamp())
CHUNK = 1400 * 3600


def fetch(symbol, start_s, end_s):
    for attempt in range(6):
        r = requests.get("https://api.kucoin.com/api/v1/market/candles", timeout=30,
                         params={"symbol": symbol, "type": "1hour",
                                 "startAt": start_s, "endAt": end_s},
                         headers={"User-Agent": "research/1.0"})
        if r.status_code == 200:
            data = r.json()
            if data.get("code") == "200000":
                return data["data"]
        time.sleep(min(30, 2 ** attempt))
    return []


def fetch_pair(symbol):
    all_bars = {}
    cursor = END_SEC
    while cursor > START_SEC:
        start = max(cursor - CHUNK, START_SEC)
        bars = fetch(symbol, start, cursor)
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
        cursor = min(int(r[0]) for r in bars) - 1
        time.sleep(0.25)
    return sorted(all_bars.values(), key=lambda r: int(r[0]))


def main():
    for bybit, kucoin in PAIRS.items():
        out = OUT_DIR / f"{bybit.lower()}_1h.csv"
        if out.exists():
            print(f"  {bybit}: cached")
            continue
        print(f"  fetching {kucoin} → {out.name} ...")
        bars = fetch_pair(kucoin)
        if not bars:
            print(f"    no data")
            continue
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["datetime", "open", "high", "low", "close", "volume"])
            for r in bars:
                ts, op, cl, hi, lo, vol = r[0], r[1], r[2], r[3], r[4], r[5]
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                w.writerow([dt, op, hi, lo, cl, vol])
        print(f"    saved {len(bars)} bars")


if __name__ == "__main__":
    main()
