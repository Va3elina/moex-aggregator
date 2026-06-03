"""Fetch BTC + ETH options data from Deribit public API.

Endpoints used (all FREE):
  1. /public/get_volatility_index_data — DVOL (historical IV index)
  2. /public/get_book_summary_by_currency — current options chain summary
  3. /public/ticker — per-instrument data (IV, OI, volume)
  4. /public/get_historical_volatility — Deribit's realized vol

Goal: build comprehensive options-derived dataset for BTC and ETH.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import requests
import pandas as pd
import numpy as np
import time

ROOT = Path(__file__).parent
DERIBIT_API = "https://www.deribit.com/api/v2"


def fetch_dvol(currency, start_date='2021-01-01'):
    """Fetch DVOL (volatility index) historical data."""
    url = f"{DERIBIT_API}/public/get_volatility_index_data"
    start_ts = int(pd.Timestamp(start_date).timestamp() * 1000)
    end_ts = int(pd.Timestamp.now().timestamp() * 1000)
    params = {
        'currency': currency,
        'start_timestamp': start_ts,
        'end_timestamp': end_ts,
        'resolution': '1D',
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json().get('result', {}).get('data', [])
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
    return df.set_index('date')[['open','high','low','close']].astype(float)


def fetch_options_chain_snapshot(currency):
    """Get current snapshot of all options for currency."""
    url = f"{DERIBIT_API}/public/get_book_summary_by_currency"
    params = {'currency': currency, 'kind': 'option'}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json().get('result', [])
    if not rows:
        return None
    df = pd.DataFrame(rows)
    # Parse instrument name: e.g., BTC-30MAY26-100000-C
    df['parts'] = df['instrument_name'].str.split('-')
    df['ccy'] = df['parts'].str[0]
    df['expiry_str'] = df['parts'].str[1]
    df['strike'] = df['parts'].str[2].astype(float)
    df['option_type'] = df['parts'].str[3]  # C or P
    df['expiry_date'] = pd.to_datetime(df['expiry_str'], format='%d%b%y')
    df = df.drop(columns=['parts','expiry_str'])
    return df


def fetch_historical_vol(currency):
    """Deribit's realized volatility estimate."""
    url = f"{DERIBIT_API}/public/get_historical_volatility"
    params = {'currency': currency}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json().get('result', [])
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=['timestamp', 'vol'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
    return df.set_index('date')[['vol']]


def analyze_chain(chain, currency):
    """Compute key options metrics from current snapshot."""
    if chain is None or len(chain) == 0:
        return None
    # Get index price (current spot reference)
    idx_url = f"{DERIBIT_API}/public/get_index_price"
    r = requests.get(idx_url, params={'index_name': f'{currency.lower()}_usd'}, timeout=30)
    spot = r.json().get('result', {}).get('index_price', 0)

    # Filter to liquid options (with volume or OI)
    chain = chain[chain['open_interest'] > 0].copy()
    if len(chain) == 0:
        return {'spot': spot, 'n_options': 0}

    # Compute moneyness
    chain['moneyness'] = chain['strike'] / spot - 1

    # Days to expiry
    chain['days_to_expiry'] = (chain['expiry_date'] - pd.Timestamp.now()).dt.days

    # By type
    calls = chain[chain['option_type'] == 'C']
    puts = chain[chain['option_type'] == 'P']

    # Aggregate metrics
    total_call_oi = calls['open_interest'].sum()
    total_put_oi = puts['open_interest'].sum()
    pcr_oi = total_put_oi / total_call_oi if total_call_oi > 0 else 0
    total_call_vol = calls['volume'].sum() if 'volume' in calls.columns else 0
    total_put_vol = puts['volume'].sum() if 'volume' in puts.columns else 0
    pcr_vol = total_put_vol / total_call_vol if total_call_vol > 0 else 0

    # IV at different moneyness levels (skew)
    # Use only near-term (< 60 days)
    near_term = chain[chain['days_to_expiry'].between(20, 60)].copy()
    if 'mark_iv' in near_term.columns and len(near_term) > 0:
        near_term['mark_iv'] = pd.to_numeric(near_term['mark_iv'], errors='coerce')
        # ATM ±2%
        atm = near_term[(near_term['moneyness'] >= -0.02) & (near_term['moneyness'] <= 0.02)]
        # OTM call +10%
        otm_call_10 = near_term[(near_term['option_type']=='C') & (near_term['moneyness'].between(0.08, 0.12))]
        # OTM put -10%
        otm_put_10 = near_term[(near_term['option_type']=='P') & (near_term['moneyness'].between(-0.12, -0.08))]
        # OTM put -25%
        otm_put_25 = near_term[(near_term['option_type']=='P') & (near_term['moneyness'].between(-0.27, -0.23))]

        atm_iv = atm['mark_iv'].mean() if len(atm) > 0 else None
        call_10_iv = otm_call_10['mark_iv'].mean() if len(otm_call_10) > 0 else None
        put_10_iv = otm_put_10['mark_iv'].mean() if len(otm_put_10) > 0 else None
        put_25_iv = otm_put_25['mark_iv'].mean() if len(otm_put_25) > 0 else None

        skew_10 = (put_10_iv - call_10_iv) if (put_10_iv and call_10_iv) else None
        rr_25d = (call_10_iv - put_10_iv) if (put_10_iv and call_10_iv) else None
    else:
        atm_iv = call_10_iv = put_10_iv = put_25_iv = skew_10 = rr_25d = None

    # OI walls — top 5 strikes by OI
    oi_by_strike = chain.groupby(['strike', 'option_type'])['open_interest'].sum().reset_index()
    call_walls = oi_by_strike[oi_by_strike['option_type']=='C'].nlargest(5, 'open_interest')
    put_walls = oi_by_strike[oi_by_strike['option_type']=='P'].nlargest(5, 'open_interest')

    return {
        'spot': spot,
        'n_options': len(chain),
        'total_call_oi': total_call_oi,
        'total_put_oi': total_put_oi,
        'pcr_oi': pcr_oi,
        'total_call_vol': total_call_vol,
        'total_put_vol': total_put_vol,
        'pcr_vol': pcr_vol,
        'atm_iv': atm_iv,
        'otm_call_10_iv': call_10_iv,
        'otm_put_10_iv': put_10_iv,
        'otm_put_25_iv': put_25_iv,
        'skew_10pct': skew_10,
        'risk_reversal_25d': rr_25d,
        'call_walls': call_walls.to_dict('records'),
        'put_walls': put_walls.to_dict('records'),
    }


def main():
    print("=" * 100)
    print("Fetching BTC + ETH options data from Deribit")
    print("=" * 100)

    for currency in ['BTC', 'ETH']:
        print(f"\n  {'='*80}")
        print(f"  {currency} options data")
        print(f"  {'='*80}")

        # DVOL history
        print(f"\n  Fetching {currency} DVOL...")
        dvol = fetch_dvol(currency)
        if dvol is not None:
            dvol.to_csv(ROOT / f'{currency.lower()}_dvol_daily.csv')
            print(f"    Got {len(dvol)} days: {dvol.index[0].date()} → {dvol.index[-1].date()}")
            print(f"    Current DVOL: {dvol['close'].iloc[-1]:.1f}%")
            print(f"    Min/Max: {dvol['close'].min():.0f}% / {dvol['close'].max():.0f}%")
            print(f"    30-day avg: {dvol['close'].tail(30).mean():.1f}%")
        else:
            print(f"    No DVOL data for {currency}")

        time.sleep(0.5)

        # Realized vol
        print(f"\n  Fetching {currency} historical realized volatility...")
        rv = fetch_historical_vol(currency)
        if rv is not None and len(rv) > 0:
            print(f"    Got {len(rv)} data points")
            print(f"    Current realized vol: {rv['vol'].iloc[-1]:.1f}%")

        time.sleep(0.5)

        # Current chain snapshot
        print(f"\n  Fetching {currency} current options chain...")
        chain = fetch_options_chain_snapshot(currency)
        if chain is not None:
            analysis = analyze_chain(chain, currency)
            print(f"    Spot price: ${analysis['spot']:,.0f}")
            print(f"    Active options: {analysis['n_options']}")
            print(f"    Total call OI: {analysis['total_call_oi']:,.0f}")
            print(f"    Total put OI: {analysis['total_put_oi']:,.0f}")
            print(f"    Put/Call ratio (OI): {analysis['pcr_oi']:.2f}")
            print(f"    Put/Call ratio (Volume): {analysis['pcr_vol']:.2f}")
            if analysis['atm_iv']:
                print(f"\n  Volatility surface (30-60 day expiry):")
                print(f"    ATM IV:           {analysis['atm_iv']:.0f}%")
                print(f"    OTM call +10% IV: {analysis['otm_call_10_iv']:.0f}%" if analysis['otm_call_10_iv'] else "    OTM call +10% IV: n/a")
                print(f"    OTM put -10% IV:  {analysis['otm_put_10_iv']:.0f}%" if analysis['otm_put_10_iv'] else "    OTM put -10% IV: n/a")
                print(f"    OTM put -25% IV:  {analysis['otm_put_25_iv']:.0f}%" if analysis['otm_put_25_iv'] else "    OTM put -25% IV: n/a")
                if analysis['skew_10pct']:
                    print(f"    Skew (put-call 10%): {analysis['skew_10pct']:+.1f}%")
                    if analysis['skew_10pct'] > 5:
                        print(f"    → Bearish skew (puts more expensive — fear of crash)")
                    elif analysis['skew_10pct'] < 0:
                        print(f"    → BULLISH SKEW (calls more expensive — FOMO/euphoria signal!)")

            print(f"\n  Top 5 CALL strike walls (resistance levels):")
            for w in analysis['call_walls']:
                print(f"    ${w['strike']:>7,.0f} call: {w['open_interest']:>10,.0f} OI")
            print(f"\n  Top 5 PUT strike walls (support levels):")
            for w in analysis['put_walls']:
                print(f"    ${w['strike']:>7,.0f} put:  {w['open_interest']:>10,.0f} OI")

            # Save chain
            chain.to_csv(ROOT / f'{currency.lower()}_options_chain_snapshot.csv', index=False)
            print(f"\n  Saved chain snapshot to {currency.lower()}_options_chain_snapshot.csv")

        time.sleep(0.5)


if __name__ == "__main__":
    main()
