"""Fetch MOEX RVI (Russian Volatility Index) — analog of VIX for Russian RTS options.

RVI is published by MOEX based on RTS futures options.
Try multiple endpoints to find historical data.
"""
import requests
import pandas as pd
from pathlib import Path
import time

ROOT = Path(__file__).parent


def try_endpoint(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=30,
                          headers={'User-Agent': 'research/1.0'})
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"    Error: {e}")
    return None


def fetch_rvi_history():
    """Try multiple MOEX endpoints for RVI history."""
    endpoints = [
        # Stock market index
        "https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RVI.json",
        # Futures index
        "https://iss.moex.com/iss/history/engines/futures/markets/index/securities/RVI.json",
        # Currency index
        "https://iss.moex.com/iss/history/engines/currency/markets/index/securities/RVI.json",
        # Direct security
        "https://iss.moex.com/iss/securities/RVI.json",
        # Get all indices
        "https://iss.moex.com/iss/engines/stock/markets/index/boards/SNDX/securities.json",
    ]

    for url in endpoints:
        print(f"  Trying: {url}")
        data = try_endpoint(url)
        if data:
            # Check what we got
            for key in data.keys():
                if isinstance(data[key], dict) and 'data' in data[key]:
                    rows = data[key]['data']
                    cols = data[key]['columns']
                    if len(rows) > 0:
                        print(f"    Got {len(rows)} rows with columns: {cols}")
                        # Return if it looks like price data
                        if 'CLOSE' in cols or 'CLOSEPRICE' in cols or 'VALUE' in cols:
                            df = pd.DataFrame(rows, columns=cols)
                            return df, url
                        elif len(rows) < 10:
                            # Show first row to understand format
                            print(f"    First row: {rows[0]}")
        time.sleep(0.3)
    return None, None


def fetch_rvi_with_pagination():
    """Fetch RVI with pagination from main endpoint."""
    base = "https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RVI.json"
    all_data = []
    start = 0
    while True:
        params = {'start': start, 'limit': 100}
        data = try_endpoint(base, params)
        if not data:
            break
        hist = data.get('history', {})
        rows = hist.get('data', [])
        cols = hist.get('columns', [])
        if not rows:
            break
        all_data.extend(rows)
        if len(rows) < 100:
            break
        start += 100
        time.sleep(0.2)
        if start > 10000:  # safety
            break

    if all_data and cols:
        df = pd.DataFrame(all_data, columns=cols)
        return df
    return None


def main():
    print("=" * 100)
    print("Fetching MOEX RVI (Russian Volatility Index)")
    print("=" * 100)

    # First try to find RVI in any endpoint
    df, found_url = fetch_rvi_history()
    if df is not None:
        print(f"\n✅ Found data at: {found_url}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Rows: {len(df)}")
        print(df.head())

    # Try pagination
    print(f"\nTrying paginated history...")
    df2 = fetch_rvi_with_pagination()
    if df2 is not None and len(df2) > 0:
        print(f"✅ Got {len(df2)} rows with columns: {list(df2.columns)}")
        # Find close column
        close_col = None
        for c in ['CLOSE', 'CLOSEPRICE', 'OPEN', 'VALUE', 'CURRENTVALUE']:
            if c in df2.columns:
                close_col = c
                break
        if close_col:
            date_col = 'TRADEDATE' if 'TRADEDATE' in df2.columns else df2.columns[0]
            df2[date_col] = pd.to_datetime(df2[date_col])
            df2 = df2.set_index(date_col).sort_index()
            df2[close_col] = pd.to_numeric(df2[close_col], errors='coerce')
            df2 = df2.dropna(subset=[close_col])
            df2 = df2[df2[close_col] > 0]
            print(f"\n  Date range: {df2.index[0].date()} → {df2.index[-1].date()}")
            print(f"  RVI range: {df2[close_col].min():.1f} - {df2[close_col].max():.1f}")
            print(f"  Median: {df2[close_col].median():.1f}")
            print(f"  Last 5:")
            print(df2[[close_col]].tail())

            # Save
            out = df2[[close_col]].rename(columns={close_col: 'close'})
            out.to_csv(ROOT / 'rvi_daily.csv')
            print(f"\n  ✅ Saved to rvi_daily.csv")


if __name__ == "__main__":
    main()
