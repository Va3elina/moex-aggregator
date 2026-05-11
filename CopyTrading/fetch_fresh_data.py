"""Fetch fresh 1H bars (Oct 2025 - now) for PEPE/BONK/FLOKI/BTC etc."""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OUT_DIR = Path(__file__).parent / "pairs_1h_fresh"
OUT_DIR.mkdir(exist_ok=True)
PAIRS = {
    "1000PEPEUSDT":  "PEPE-USDT",
    "DOGEUSDT":      "DOGE-USDT",
    "1000FLOKIUSDT": "FLOKI-USDT",
    "1000BONKUSDT":  "BONK-USDT",
    "SHIB1000USDT":  "SHIB-USDT",
    "WIFUSDT":       "WIF-USDT",
    "POPCATUSDT":    "POPCAT-USDT",
    "SOLUSDT":       "SOL-USDT",
}
START_SEC = int(datetime(2025, 10, 1, tzinfo=timezone.utc).timestamp())
END_SEC = int(datetime.now(timezone.utc).timestamp())


def fetch(symbol, start_s, end_s):
    for attempt in range(6):
        try:
            r = requests.get("https://api.kucoin.com/api/v1/market/candles",
                             params={"symbol": symbol, "type": "1hour",
                                     "startAt": start_s, "endAt": end_s},
                             timeout=20)
            if r.status_code == 200 and r.json().get("code") == "200000":
                return r.json()["data"]
        except Exception:
            pass
        time.sleep(2 ** attempt)
    return []


def fetch_btc():
    """Fetch BTC fresh 1H. Try Coinbase first (we already use it), fallback KuCoin."""
    out = OUT_DIR / "btc_fresh_1h.csv"
    if out.exists():
        return
    # Coinbase
    all_bars = {}
    cursor_end = END_SEC
    while cursor_end > START_SEC:
        start = max(cursor_end - 290 * 3600, START_SEC)
        try:
            r = requests.get("https://api.exchange.coinbase.com/products/BTC-USD/candles",
                             params={"granularity": 3600,
                                     "start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                                     "end": datetime.fromtimestamp(cursor_end, tz=timezone.utc).isoformat()},
                             timeout=20)
            if r.status_code == 200:
                bars = r.json()
                for row in bars:
                    ts = int(row[0])
                    if ts not in all_bars:
                        all_bars[ts] = row
                if not bars:
                    break
                cursor_end = min(int(r[0]) for r in bars) - 1
            else:
                break
        except Exception:
            break
        time.sleep(0.3)
    rows = sorted(all_bars.values(), key=lambda r: int(r[0]))
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datetime", "open", "high", "low", "close", "volume"])
        for r in rows:
            ts, low, high, op, cl, vol = r[:6]
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            w.writerow([dt, op, high, low, cl, vol])
    print(f"  BTC fresh: {len(rows)} bars saved to {out.name}")


def main():
    print(f"Fetching fresh data from {datetime.fromtimestamp(START_SEC, tz=timezone.utc).date()} "
          f"to {datetime.fromtimestamp(END_SEC, tz=timezone.utc).date()}")
    fetch_btc()
    for bn, kn in PAIRS.items():
        out = OUT_DIR / f"{bn.lower()}_1h.csv"
        if out.exists():
            continue
        all_bars = {}
        cursor = END_SEC
        while cursor > START_SEC:
            start = max(cursor - 1400 * 3600, START_SEC)
            bars = fetch(kn, start, cursor)
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
        rows = sorted(all_bars.values(), key=lambda r: int(r[0]))
        if not rows:
            print(f"  {bn}: no data")
            continue
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["datetime", "open", "high", "low", "close", "volume"])
            for r in rows:
                ts, op, cl, hi, lo, vol = r[0], r[1], r[2], r[3], r[4], r[5]
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                w.writerow([dt, op, hi, lo, cl, vol])
        print(f"  {bn}: {len(rows)} bars saved")


if __name__ == "__main__":
    main()
