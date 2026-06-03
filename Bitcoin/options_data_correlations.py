"""Step 1: Fetch options-derived data + check correlations with BTC returns.

Sources to try:
  - Deribit DVOL (BTC implied volatility index, free API since 2021)
  - VIX (already have)
  - Realized volatility (computed from BTC price)
  - Volatility Risk Premium (IV - Realized)

Then:
  - Compute correlations with BTC future returns
  - Test if these data points predict V1 strategy entry/exit better
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).parent


def fetch_deribit_dvol(currency='BTC', start_ts=None, end_ts=None):
    """Fetch historical DVOL from Deribit public API."""
    url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
    if start_ts is None:
        start_ts = int(pd.Timestamp('2021-03-01').timestamp() * 1000)
    if end_ts is None:
        end_ts = int(pd.Timestamp.now().timestamp() * 1000)

    params = {
        'currency': currency,
        'start_timestamp': start_ts,
        'end_timestamp': end_ts,
        'resolution': '1D',
    }
    print(f"  Fetching Deribit DVOL for {currency}...")
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        result = data.get('result', {})
        rows = result.get('data', [])
        if not rows:
            print(f"  No data returned")
            return None
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
        df = df.set_index('date').sort_index()
        df = df[['open','high','low','close']].astype(float)
        print(f"  Got {len(df)} days of DVOL data: {df.index[0].date()} → {df.index[-1].date()}")
        return df
    except Exception as e:
        print(f"  Error: {e}")
        return None


def load_btc():
    df = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def load_vix():
    vix = pd.read_csv(ROOT / 'vix_daily.csv', skiprows=3,
                       names=['Date','close','high','low','open','volume'])
    vix['Date'] = pd.to_datetime(vix['Date'])
    return vix.set_index('Date')


def compute_realized_vol(close, window=30):
    """Annualized realized volatility from log returns."""
    ret = np.log(close / close.shift(1))
    rv = ret.rolling(window).std() * np.sqrt(365) * 100  # in %
    return rv


def main():
    btc = load_btc()
    vix = load_vix()

    # Fetch DVOL from Deribit
    print("=" * 100)
    print("STEP 1: Fetching options-derived data")
    print("=" * 100)
    dvol = fetch_deribit_dvol('BTC')
    if dvol is None:
        print("  Couldn't fetch DVOL — will use realized vol proxy")
        return

    # Save DVOL
    dvol.to_csv(ROOT / 'btc_dvol_daily.csv')
    print(f"  Saved to btc_dvol_daily.csv")

    # === Build unified daily dataframe ===
    df = btc[['close']].copy()
    df['btc_close'] = df['close']
    df['rv30'] = compute_realized_vol(df['close'], 30)
    df['rv90'] = compute_realized_vol(df['close'], 90)
    df['vix_high'] = vix['high'].reindex(df.index, method='ffill')
    df['dvol_close'] = dvol['close'].reindex(df.index, method='ffill')

    # Variance Risk Premium (IV - Realized)
    df['vrp'] = df['dvol_close'] - df['rv30']

    # Forward returns
    for h in [7, 30, 60, 90]:
        df[f'fwd_{h}d_ret'] = (df['close'].shift(-h) / df['close'] - 1) * 100

    # Filter to where we have all data
    df_full = df.dropna(subset=['dvol_close', 'rv30', 'vix_high'])
    print(f"\n  Aligned dataframe: {len(df_full)} days with all metrics")
    print(f"  Range: {df_full.index[0].date()} → {df_full.index[-1].date()}")

    # === Compute correlations ===
    print("\n" + "=" * 100)
    print("STEP 2: Correlations between options metrics and BTC future returns")
    print("=" * 100)

    metrics = ['dvol_close', 'rv30', 'rv90', 'vix_high', 'vrp']
    forward_periods = ['fwd_7d_ret', 'fwd_30d_ret', 'fwd_60d_ret', 'fwd_90d_ret']

    print(f"\n  {'Metric':>15}  |  " + "  |  ".join([f"{p:>10}" for p in forward_periods]))
    print("  " + "-" * 85)
    for m in metrics:
        line = f"  {m:>15}  |  "
        for p in forward_periods:
            corr = df_full[[m, p]].corr().iloc[0, 1]
            line += f"{corr:>+10.3f}  |  "
        print(line)

    # === Look at extreme values ===
    print("\n" + "=" * 100)
    print("STEP 3: What happens when metric is extremely high/low?")
    print("=" * 100)

    # DVOL extremes
    print("\n  DVOL extremes — average BTC return over next 60 days:")
    dvol_high = df_full['dvol_close'] >= df_full['dvol_close'].quantile(0.9)
    dvol_low = df_full['dvol_close'] <= df_full['dvol_close'].quantile(0.1)
    print(f"    DVOL ≥ 90th percentile ({df_full['dvol_close'].quantile(0.9):.0f}%): "
          f"avg fwd 60d ret = {df_full.loc[dvol_high, 'fwd_60d_ret'].mean():+.1f}%  "
          f"(N={dvol_high.sum()}, P(up)={((df_full.loc[dvol_high, 'fwd_60d_ret'] > 0).mean()*100):.0f}%)")
    print(f"    DVOL ≤ 10th percentile ({df_full['dvol_close'].quantile(0.1):.0f}%): "
          f"avg fwd 60d ret = {df_full.loc[dvol_low, 'fwd_60d_ret'].mean():+.1f}%  "
          f"(N={dvol_low.sum()}, P(up)={((df_full.loc[dvol_low, 'fwd_60d_ret'] > 0).mean()*100):.0f}%)")

    # VRP extremes
    print("\n  VRP (IV-Realized) extremes:")
    vrp_high = df_full['vrp'] >= df_full['vrp'].quantile(0.9)
    vrp_low = df_full['vrp'] <= df_full['vrp'].quantile(0.1)
    print(f"    VRP ≥ 90th percentile ({df_full['vrp'].quantile(0.9):.0f}%): "
          f"avg fwd 60d ret = {df_full.loc[vrp_high, 'fwd_60d_ret'].mean():+.1f}%  "
          f"(N={vrp_high.sum()}, P(up)={((df_full.loc[vrp_high, 'fwd_60d_ret'] > 0).mean()*100):.0f}%)")
    print(f"    VRP ≤ 10th percentile ({df_full['vrp'].quantile(0.1):.0f}%): "
          f"avg fwd 60d ret = {df_full.loc[vrp_low, 'fwd_60d_ret'].mean():+.1f}%  "
          f"(N={vrp_low.sum()}, P(up)={((df_full.loc[vrp_low, 'fwd_60d_ret'] > 0).mean()*100):.0f}%)")

    # VIX vs DVOL correlation
    print("\n  VIX vs DVOL correlation (since 2021):")
    print(f"    Pearson: {df_full[['vix_high', 'dvol_close']].corr().iloc[0,1]:.3f}")
    print(f"    VIX above 30 ↔ DVOL above 70: {((df_full['vix_high'] > 30) & (df_full['dvol_close'] > 70)).sum()} days")
    print(f"    VIX above 30 only: {(df_full['vix_high'] > 30).sum()} days")
    print(f"    DVOL above 70 only: {(df_full['dvol_close'] > 70).sum()} days")

    # === Specific signals: what if we trade ONLY when DVOL high? ===
    print("\n" + "=" * 100)
    print("STEP 4: Simple options-based signals")
    print("=" * 100)

    INITIAL = 100.0
    closes = df_full['btc_close'].values
    dvol_vals = df_full['dvol_close'].values
    vrp_vals = df_full['vrp'].values
    vix_vals = df_full['vix_high'].values
    dates = df_full.index

    # Buy when DVOL > 80, hold 60 days
    signals = [
        ('DVOL ≥ 70 → hold 60d', dvol_vals >= 70, 60),
        ('DVOL ≥ 80 → hold 60d', dvol_vals >= 80, 60),
        ('VRP ≤ -10 → hold 60d', vrp_vals <= -10, 60),
        ('VRP ≤ -20 → hold 30d', vrp_vals <= -20, 30),
        ('VIX ≥ 30 → hold 60d', vix_vals >= 30, 60),
        ('VIX ≥ 40 → hold 60d', vix_vals >= 40, 60),
        ('DVOL ≥ 70 AND VIX ≥ 30 → 60d', (dvol_vals >= 70) & (vix_vals >= 30), 60),
    ]

    for label, mask, hold_d in signals:
        # Get all event days
        event_idx = np.where(mask)[0]
        if len(event_idx) == 0:
            print(f"  {label}: no signals"); continue
        # Compute return over hold period
        returns = []
        for idx in event_idx:
            if idx + hold_d < len(closes):
                ret = (closes[idx + hold_d] / closes[idx] - 1) * 100
                returns.append(ret)
        if returns:
            n = len(returns)
            avg = np.mean(returns)
            med = np.median(returns)
            p_up = (np.array(returns) > 0).sum() / n * 100
            print(f"  {label:35}  N={n:>3}  avg fwd ret={avg:+6.1f}%  median={med:+6.1f}%  P(up)={p_up:.0f}%")


if __name__ == "__main__":
    main()
