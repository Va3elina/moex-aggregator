"""Comprehensive options-derived signals research for BTC and ETH.

Goals:
  1. Fetch maximum DVOL history (try going back to 2020/2021)
  2. Use realized vol as IV proxy for earlier periods
  3. Build full feature set: DVOL, VRP, IV-of-vol, percentiles
  4. Test 15+ signal variants on BTC and ETH
  5. Cross-asset analysis (BTC DVOL → ETH returns and vice versa)
  6. Walk-forward validation
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import time

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


def fetch_dvol_extended(currency, start_date='2020-01-01'):
    """Try to fetch DVOL with the earliest possible start date."""
    url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
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


def load_btc_eth():
    btc = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    eth = pd.read_csv(ROOT/'eth_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    return btc[btc['close']>0].dropna(), eth[eth['close']>0].dropna()


def compute_features(price_df, dvol_df, label):
    """Build comprehensive feature set."""
    df = pd.DataFrame(index=price_df.index)
    df['close'] = price_df['close']
    df['ret_1d'] = df['close'].pct_change()

    # Realized volatility (annualized %)
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_7d'] = log_ret.rolling(7).std() * np.sqrt(365) * 100
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['rv_90d'] = log_ret.rolling(90).std() * np.sqrt(365) * 100

    # IV (DVOL)
    if dvol_df is not None:
        df['dvol'] = dvol_df['close'].reindex(df.index, method='ffill')
        df['dvol_ma30'] = df['dvol'].rolling(30).mean()
        df['dvol_std30'] = df['dvol'].rolling(30).std()
        df['dvol_z30'] = (df['dvol'] - df['dvol_ma30']) / df['dvol_std30']
        df['vrp'] = df['dvol'] - df['rv_30d']  # variance risk premium
        df['vrp_ma30'] = df['vrp'].rolling(30).mean()
        df['vrp_z30'] = (df['vrp'] - df['vrp_ma30']) / df['vrp'].rolling(30).std()
    else:
        # Pre-DVOL fallback: use realized vol × 1.2 as IV proxy
        df['dvol'] = df['rv_30d'] * 1.2
        df['dvol_ma30'] = df['dvol'].rolling(30).mean()
        df['dvol_z30'] = (df['dvol'] - df['dvol_ma30']) / df['dvol'].rolling(30).std()
        df['vrp'] = df['dvol'] - df['rv_30d']
        df['vrp_z30'] = 0  # neutral since proxy

    # Forward returns
    for h in [7, 14, 30, 60, 90]:
        df[f'fwd_{h}d'] = df['close'].pct_change(h).shift(-h) * 100

    return df


def event_stats(df, mask, label):
    """Compute forward return stats for events."""
    res = {}
    for h in [7, 30, 60, 90]:
        col = f'fwd_{h}d'
        rets = df.loc[mask, col].dropna()
        n = len(rets)
        if n == 0:
            res[h] = None
            continue
        res[h] = {
            'n': n,
            'mean': rets.mean(),
            'median': rets.median(),
            'p_up': (rets > 0).mean() * 100,
            'std': rets.std(),
            't_stat': rets.mean() / (rets.std()/np.sqrt(n)) if rets.std() > 0 else 0,
        }
    return res


def print_event_table(events, label):
    print(f"\n  {label}")
    print(f"  {'-'*100}")
    print(f"  {'Event':<35}  {'N':>4}  {'5d/30d/60d/90d mean fwd ret + P(up)':>50}")
    for ev_name, stats in events.items():
        line = f"  {ev_name:<35}  {stats[60]['n']:>4}  " if stats.get(60) else f"  {ev_name:<35}  ---  "
        for h in [7, 30, 60, 90]:
            s = stats.get(h)
            if s is None:
                line += f"  ---/-- "
            else:
                line += f"  {s['mean']:+5.1f}%({s['p_up']:>3.0f}%)"
        print(line)


def backtest_signal(df, entry_mask, hold_days, max_concurrent=1):
    """Buy at signal, hold N days, exit. Returns equity curve and stats."""
    closes = df['close'].values
    dates = df.index
    n = len(closes)
    eq = np.full(n, INITIAL, dtype=float)
    positions = []  # (entry_idx, entry_price)
    cash = INITIAL
    trades = []

    for i in range(n):
        # Update mark-to-market
        equity = cash
        for p_idx, p_price in positions:
            shares = INITIAL / (len(positions) + 1) / p_price  # equal weight estimate
            equity += (closes[i] - p_price) * shares
        eq[i] = equity

        # Close expired positions
        new_positions = []
        for p_idx, p_price in positions:
            if i - p_idx >= hold_days:
                # Close
                exit_price = closes[i] * (1 - SLIP)
                shares = (cash / (max_concurrent - len(new_positions) + 0.001))  # not accurate but ok
                pnl_pct = (exit_price / p_price - 1 - FEE - SLIP) * 100
                trades.append({'entry_idx': p_idx, 'exit_idx': i,
                              'entry_price': p_price, 'exit_price': exit_price,
                              'pnl_pct': pnl_pct, 'hold': i - p_idx})
            else:
                new_positions.append((p_idx, p_price))
        positions = new_positions

        # Open new position if signal and have room
        if entry_mask[i] and len(positions) < max_concurrent:
            positions.append((i, closes[i] * (1 + SLIP)))

    # Properly simulate equity: simple sequential signal trader
    eq_arr = np.full(n, INITIAL, dtype=float)
    cash = INITIAL
    in_pos = False
    entry_idx = 0
    entry_px = 0
    trades_clean = []
    eq_curr = INITIAL
    for i in range(n):
        c = closes[i]
        if in_pos:
            eq_curr = (c / entry_px) * INITIAL * (cash / INITIAL) if cash > 0 else 0
        eq_arr[i] = eq_curr if in_pos else cash

        if in_pos and i - entry_idx >= hold_days:
            exit_p = c * (1 - SLIP)
            ret = exit_p / entry_px - 1 - 2 * FEE
            cash = cash * (1 + ret)
            trades_clean.append({'entry_idx': entry_idx, 'exit_idx': i,
                                'entry_date': dates[entry_idx], 'exit_date': dates[i],
                                'pnl_pct': ret * 100})
            eq_curr = cash
            eq_arr[i] = cash
            in_pos = False
        if not in_pos and entry_mask[i]:
            entry_idx = i
            entry_px = c * (1 + SLIP)
            in_pos = True
            eq_curr = cash

    return pd.Series(eq_arr, index=dates), pd.DataFrame(trades_clean)


def main():
    print("=" * 130)
    print("COMPREHENSIVE OPTIONS-DERIVED SIGNALS RESEARCH")
    print("=" * 130)

    # 1. Try to fetch maximum DVOL history
    print("\n  Step 1: Fetching DVOL history (trying earliest start)...")
    btc_dvol = fetch_dvol_extended('BTC', '2020-01-01')
    if btc_dvol is not None:
        print(f"    BTC DVOL: {btc_dvol.index[0].date()} → {btc_dvol.index[-1].date()} ({len(btc_dvol)} days)")
    time.sleep(0.5)

    eth_dvol = fetch_dvol_extended('ETH', '2020-01-01')
    if eth_dvol is not None:
        print(f"    ETH DVOL: {eth_dvol.index[0].date()} → {eth_dvol.index[-1].date()} ({len(eth_dvol)} days)")

    # 2. Load BTC + ETH prices
    btc_price, eth_price = load_btc_eth()
    print(f"\n    BTC price: {btc_price.index[0].date()} → {btc_price.index[-1].date()} ({len(btc_price)} days)")
    print(f"    ETH price: {eth_price.index[0].date()} → {eth_price.index[-1].date()} ({len(eth_price)} days)")

    # 3. Build feature sets
    btc_df = compute_features(btc_price, btc_dvol, 'BTC')
    eth_df = compute_features(eth_price, eth_dvol, 'ETH')

    # 4. Event analysis for each asset
    print("\n" + "=" * 130)
    print("STEP 2: BTC EVENT STATISTICS — forward returns by signal type")
    print("=" * 130)

    btc_events = {}
    if btc_dvol is not None:
        btc_events.update({
            'DVOL ≥ 70 (high)':       btc_df['dvol'] >= 70,
            'DVOL ≥ 80 (very high)':  btc_df['dvol'] >= 80,
            'DVOL z30 ≥ 2':           btc_df['dvol_z30'] >= 2,
            'DVOL z30 ≤ -2':          btc_df['dvol_z30'] <= -2,
            'VRP ≥ +20':              btc_df['vrp'] >= 20,
            'VRP ≤ -10':              btc_df['vrp'] <= -10,
            'VRP ≤ -20':              btc_df['vrp'] <= -20,
            'VRP z30 ≥ 2':            btc_df['vrp_z30'] >= 2,
            'VRP z30 ≤ -2':           btc_df['vrp_z30'] <= -2,
        })

    btc_events.update({
        'RV30 ≥ 100 (high vol)':     btc_df['rv_30d'] >= 100,
        'RV30 ≤ 30 (calm)':           btc_df['rv_30d'] <= 30,
        'RV30 z-score ≥ 2':           btc_df['rv_30d'].rolling(180).apply(
                                          lambda x: (x.iloc[-1] - x.mean())/x.std() if x.std()>0 else 0) >= 2,
    })

    btc_results = {}
    for name, mask in btc_events.items():
        btc_results[name] = event_stats(btc_df, mask, name)
    print_event_table(btc_results, "BTC")

    # ETH
    print("\n" + "=" * 130)
    print("STEP 3: ETH EVENT STATISTICS")
    print("=" * 130)

    eth_events = {}
    if eth_dvol is not None:
        eth_events.update({
            'DVOL ≥ 70':             eth_df['dvol'] >= 70,
            'DVOL ≥ 80':             eth_df['dvol'] >= 80,
            'DVOL z30 ≥ 2':          eth_df['dvol_z30'] >= 2,
            'DVOL z30 ≤ -2':         eth_df['dvol_z30'] <= -2,
            'VRP ≤ -10':             eth_df['vrp'] <= -10,
            'VRP ≤ -20':             eth_df['vrp'] <= -20,
            'VRP z30 ≥ 2':           eth_df['vrp_z30'] >= 2,
            'VRP z30 ≤ -2':          eth_df['vrp_z30'] <= -2,
        })
    eth_events.update({
        'RV30 ≥ 100':                eth_df['rv_30d'] >= 100,
        'RV30 ≤ 30':                  eth_df['rv_30d'] <= 30,
    })

    eth_results = {}
    for name, mask in eth_events.items():
        eth_results[name] = event_stats(eth_df, mask, name)
    print_event_table(eth_results, "ETH")

    # 5. Cross-asset signals
    print("\n" + "=" * 130)
    print("STEP 4: CROSS-ASSET SIGNALS — does BTC options data predict ETH?")
    print("=" * 130)

    if btc_dvol is not None:
        # Align BTC DVOL to ETH
        btc_dvol_on_eth = btc_dvol['close'].reindex(eth_df.index, method='ffill')
        eth_df['btc_dvol'] = btc_dvol_on_eth
        eth_df['btc_vrp'] = btc_dvol_on_eth - eth_df['rv_30d']

        cross = {
            'BTC DVOL ≥ 70 → ETH fwd ret':    eth_df['btc_dvol'] >= 70,
            'BTC DVOL z30 ≥ 2 → ETH':         (eth_df['btc_dvol'] - eth_df['btc_dvol'].rolling(30).mean()) /
                                              eth_df['btc_dvol'].rolling(30).std() >= 2,
        }
        cross_results = {}
        for n, m in cross.items():
            cross_results[n] = event_stats(eth_df, m, n)
        print_event_table(cross_results, "BTC → ETH")

    # 6. Backtest best signals
    print("\n" + "=" * 130)
    print("STEP 5: BACKTEST best signals (hold N days after signal)")
    print("=" * 130)
    print(f"\n  Format: total return, DD, # trades, WR")
    print(f"  All start with $100, BTC price data 2015+ ({len(btc_df)} days)\n")

    backtest_sigs = [
        ('BTC: DVOL z30 ≥ 2 hold 30d',  btc_df['dvol_z30'] >= 2, 30, btc_df),
        ('BTC: DVOL z30 ≥ 2 hold 60d',  btc_df['dvol_z30'] >= 2, 60, btc_df),
        ('BTC: VRP ≤ -10 hold 60d',     btc_df['vrp'] <= -10, 60, btc_df),
        ('BTC: VRP ≤ -10 hold 90d',     btc_df['vrp'] <= -10, 90, btc_df),
        ('BTC: VRP z30 ≤ -2 hold 60d',  btc_df['vrp_z30'] <= -2, 60, btc_df),
        ('BTC: DVOL ≥ 70 hold 60d',     btc_df['dvol'] >= 70, 60, btc_df),
        ('BTC: RV30 ≥ 100 hold 60d',    btc_df['rv_30d'] >= 100, 60, btc_df),
        ('BTC: RV30 ≤ 30 hold 90d',     btc_df['rv_30d'] <= 30, 90, btc_df),
        ('ETH: VRP ≤ -10 hold 60d',     eth_df['vrp'] <= -10, 60, eth_df),
        ('ETH: DVOL z30 ≥ 2 hold 60d',  eth_df['dvol_z30'] >= 2, 60, eth_df),
        ('ETH: RV30 ≤ 30 hold 90d',     eth_df['rv_30d'] <= 30, 90, eth_df),
    ]

    print(f"  {'Signal':<45}  {'Final':>10}  {'Total':>10}  {'DD':>6}  {'N':>4}  {'WR':>5}  {'avg':>8}")
    for name, mask, hold_d, df in backtest_sigs:
        mask_bool = mask.fillna(False).values
        eq, trades = backtest_signal(df, mask_bool, hold_d, max_concurrent=1)
        if len(trades) == 0:
            print(f"  {name:<45}  no trades")
            continue
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        n = len(trades)
        wr = (trades['pnl_pct']>0).sum()/n*100
        avg = trades['pnl_pct'].mean()
        print(f"  {name:<45}  ${eq.iloc[-1]:>9,.0f}  {total:>+8.0f}%  {dd:>4.1f}%  {n:>4}  {wr:>4.0f}%  {avg:>+6.1f}%")

    # 7. Save all data
    btc_df.to_csv(ROOT / 'btc_research_features.csv')
    eth_df.to_csv(ROOT / 'eth_research_features.csv')
    print(f"\n  Saved feature datasets to btc_research_features.csv and eth_research_features.csv")


if __name__ == "__main__":
    main()
