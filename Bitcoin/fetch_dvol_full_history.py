"""Fetch full BTC/ETH DVOL history from Deribit (since 2021-03-24)."""
import requests
import time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent
URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"


def fetch_window(currency, start_ts, end_ts, resolution=86400):
    params = {
        'currency': currency,
        'start_timestamp': start_ts,
        'end_timestamp': end_ts,
        'resolution': resolution,
    }
    r = requests.get(URL, params=params, timeout=30,
                     headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    return r.json()['result']['data']


def fetch_all(currency, start_dt, end_dt):
    """Iterate 90-day windows."""
    all_rows = []
    cur = start_dt
    while cur < end_dt:
        next_dt = min(cur + pd.Timedelta(days=90), end_dt)
        rows = fetch_window(currency, int(cur.timestamp()*1000), int(next_dt.timestamp()*1000))
        all_rows.extend(rows)
        cur = next_dt
        time.sleep(0.1)
    return all_rows


def main():
    for cur in ['BTC', 'ETH']:
        print(f"\nFetching {cur} DVOL full history...")
        rows = fetch_all(cur, pd.Timestamp('2021-03-24'), pd.Timestamp.utcnow().tz_localize(None))
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
        df = df[['date', 'open', 'high', 'low', 'close']].drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
        out = ROOT / f"{cur.lower()}_dvol_daily.csv"
        df.to_csv(out, index=False)
        print(f"  Saved {len(df)} days to {out.name}")
        print(f"  Range: {df['date'].iloc[0].date()} → {df['date'].iloc[-1].date()}")
        print(f"  DVOL: min={df['close'].min():.0f}  max={df['close'].max():.0f}  mean={df['close'].mean():.0f}")


if __name__ == "__main__":
    main()
