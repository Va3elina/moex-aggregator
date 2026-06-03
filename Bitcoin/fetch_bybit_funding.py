"""Fetch BTC/ETH perpetual funding history.

Deribit provides hourly funding (interest_1h) with derived 8h equivalent.
Bybit/Binance use 8h funding directly.
All blocked by GeoIP except Deribit, so we use Deribit as ground truth.
Note: Bybit/Binance rates are typically 1.5-2x higher in FOMO periods due
to retail dominance. Deribit is institutional → more arbitraged.

Sign convention:
  - Positive rate: LONG pays SHORT (typical bull market)
  - Negative rate: SHORT pays LONG (bear / contango unwind)
"""
import requests
import time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent


def fetch_binance(symbol, since_dt):
    """Binance fapi: /fapi/v1/fundingRate. Returns up to 1000 events per call."""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    since_ms = int(since_dt.timestamp() * 1000)
    all_rows = []
    start = since_ms
    while True:
        params = {'symbol': symbol, 'startTime': start, 'limit': 1000}
        r = requests.get(url, params=params, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        all_rows.extend(chunk)
        last_ts = chunk[-1]['fundingTime']
        if len(chunk) < 1000:
            break
        start = last_ts + 1
        time.sleep(0.15)
    return all_rows


def fetch_bybit(symbol, since_dt):
    url = "https://api.bybit.com/v5/market/funding/history"
    since_ms = int(since_dt.timestamp() * 1000)
    end_ms = int(time.time() * 1000)
    all_rows = []
    while True:
        params = {'category': 'linear', 'symbol': symbol, 'limit': 200, 'endTime': end_ms}
        r = requests.get(url, params=params, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        data = r.json()
        if data.get('retCode') != 0:
            raise RuntimeError(f"Bybit error: {data}")
        chunk = data['result']['list']
        if not chunk:
            break
        all_rows.extend(chunk)
        oldest_ts = int(chunk[-1]['fundingRateTimestamp'])
        if oldest_ts <= since_ms or len(chunk) < 200:
            break
        end_ms = oldest_ts - 1
        time.sleep(0.15)
    return all_rows


def fetch_deribit(instrument, start_dt, end_dt):
    """Deribit funding. Limit ~744 entries per call (~31 days hourly).
    Iterate in 30-day windows."""
    url = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
    all_rows = []
    cur = start_dt
    while cur < end_dt:
        next_dt = min(cur + pd.Timedelta(days=30), end_dt)
        params = {
            'instrument_name': instrument,
            'start_timestamp': int(cur.timestamp() * 1000),
            'end_timestamp': int(next_dt.timestamp() * 1000),
        }
        r = requests.get(url, params=params, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        chunk = r.json().get('result', [])
        all_rows.extend(chunk)
        cur = next_dt
        time.sleep(0.1)
    return all_rows


def main():
    for symbol in ['BTC-PERPETUAL', 'ETH-PERPETUAL']:
        print(f"\nFetching {symbol} funding history from Deribit...")
        rows = fetch_deribit(symbol,
                              pd.Timestamp('2023-09-01'),
                              pd.Timestamp.utcnow().tz_localize(None) if pd.Timestamp.utcnow().tz is None else pd.Timestamp.utcnow().tz_localize(None))
        df = pd.DataFrame(rows)
        df['ts'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms', utc=True)
        df['rate'] = df['interest_8h'].astype(float)  # 8h rate equivalent
        df = df[['ts', 'rate']].sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df['symbol'] = symbol

        out_name = symbol.split('-')[0].lower() + '_funding.csv'
        out = ROOT / out_name
        df.to_csv(out, index=False)
        print(f"  Saved {len(df)} funding events to {out.name}")
        print(f"  Range: {df['ts'].iloc[0]} → {df['ts'].iloc[-1]}")
        # Note: this is interest_8h which is essentially the hourly_rate * 8 expression
        # Annualized = mean rate per hour * 24 * 365 (since Deribit funds hourly)
        # but we use interest_8h to compare with 8h venues
        hourly_mean = df['rate'].mean() / 8  # if interest_8h is 8h projection
        annualized = df['rate'].mean() / 8 * 24 * 365 * 100  # hourly is the truth
        print(f"  Mean rate (hourly equivalent): {hourly_mean*100:.5f}%/h → {annualized:+.1f}% annualized")
        print(f"  Percentiles (annualized): "
              f"p10={df['rate'].quantile(0.1)/8*24*365*100:+.0f}%  "
              f"p50={df['rate'].quantile(0.5)/8*24*365*100:+.0f}%  "
              f"p90={df['rate'].quantile(0.9)/8*24*365*100:+.0f}%  "
              f"p99={df['rate'].quantile(0.99)/8*24*365*100:+.0f}%  "
              f"max={df['rate'].max()/8*24*365*100:+.0f}%")


if __name__ == "__main__":
    main()
