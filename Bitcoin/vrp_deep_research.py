"""Deep research on VRP signal — find optimal parameters and combinations.

Tests:
  1. Hold period sweep (30, 45, 60, 90, 120, 180 days)
  2. VRP threshold sweep (-5, -10, -15, -20, -25)
  3. EMA50/EMA100 trend filter
  4. Combine with V1/V2 VIX signal
  5. ETH version (same logic)
  6. Walk-forward / year-by-year stability
  7. Exit strategies: hold N days vs price-based vs VRP normalization
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


def ema(arr, n):
    if hasattr(arr, 'values'): arr = arr.values
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out


def load_data(asset='BTC'):
    if asset == 'BTC':
        price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    else:
        price = pd.read_csv(ROOT/'eth_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()

    dvol = pd.read_csv(ROOT / f'{asset.lower()}_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

    return price, dvol


def compute_features(price, dvol):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    df['high'] = price['high']
    df['low'] = price['low']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']
    df['ema50'] = ema(df['close'].values, 50)
    df['ema100'] = ema(df['close'].values, 100)
    df['ema200'] = ema(df['close'].values, 200)
    return df


def backtest(df, entry_mask, hold_days, exit_rule='days'):
    """exit_rule:
        'days' = exit after hold_days
        'days_or_vrp_normalize' = exit when VRP > 0 OR hold_days
        'days_or_break_ema50' = exit when price < EMA50 OR hold_days
    """
    closes = df['close'].values
    lows = df['low'].values
    vrp = df['vrp'].values
    ema50 = df['ema50'].values
    dates = df.index
    n = len(closes)

    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    intra_min = 0
    trades = []

    for i in range(n):
        c = closes[i]
        if in_pos:
            if intra_min == 0 or lows[i] < intra_min:
                intra_min = lows[i]
            eq[i] = cash * (c / entry_px)
        else:
            eq[i] = cash

        # Exit check
        if in_pos:
            do_exit = False
            reason = 'hold'
            hold = i - entry_idx
            if hold >= hold_days:
                do_exit = True; reason = 'hold'
            elif exit_rule == 'days_or_vrp_normalize' and not np.isnan(vrp[i]) and vrp[i] > 0:
                do_exit = True; reason = 'vrp_normalize'
            elif exit_rule == 'days_or_break_ema50' and not np.isnan(ema50[i]) and c < ema50[i]:
                do_exit = True; reason = 'ema50_break'

            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = (exit_p / entry_px - 1 - 2*FEE) * 100
                mae = (intra_min / entry_px - 1) * 100
                cash *= (1 + ret/100)
                trades.append({'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': exit_p,
                              'pnl_pct': ret, 'mae_pct': mae,
                              'hold_d': hold, 'reason': reason})
                in_pos = False
                eq[i] = cash

        # Entry check
        if not in_pos and entry_mask[i]:
            entry_idx = i
            entry_px = c * (1 + SLIP)
            in_pos = True
            intra_min = lows[i]

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def metrics(eq, trades):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'total': (eq.iloc[-1]/INITIAL - 1)*100, 'DD': 0, 'MAE': 0, 'avg_hold': 0}
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum()/len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = trades['mae_pct'].min()
    avg_hold = trades['hold_d'].mean()
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'total': total, 'DD': dd,
            'MAE': mae, 'avg_hold': avg_hold}


def fmt(m):
    pf_s = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return (f"N={m['N']:>3}  WR={m['WR']:>4.0f}%  PF={pf_s}  total={m['total']:>+7.0f}%  "
            f"DD={m['DD']:>4.1f}%  MAE={m['MAE']:>+5.1f}%  hold={m['avg_hold']:>3.0f}d")


def main():
    btc_price, btc_dvol = load_data('BTC')
    eth_price, eth_dvol = load_data('ETH')
    btc = compute_features(btc_price, btc_dvol)
    eth = compute_features(eth_price, eth_dvol)

    # Use only period with DVOL data
    btc_v = btc.dropna(subset=['vrp']).copy()
    eth_v = eth.dropna(subset=['vrp']).copy()
    print(f"BTC data: {btc_v.index[0].date()} → {btc_v.index[-1].date()} ({len(btc_v)} days)")
    print(f"ETH data: {eth_v.index[0].date()} → {eth_v.index[-1].date()} ({len(eth_v)} days)")

    # Buy & Hold benchmark
    bh_btc = (btc_v['close'].iloc[-1] / btc_v['close'].iloc[0] - 1) * 100
    bh_eth = (eth_v['close'].iloc[-1] / eth_v['close'].iloc[0] - 1) * 100
    print(f"\n  Buy & Hold BTC (same period): {bh_btc:+.0f}%")
    print(f"  Buy & Hold ETH (same period): {bh_eth:+.0f}%\n")

    # === STUDY 1: Hold period sweep
    print("="*130)
    print(f"STUDY 1: BTC VRP ≤ -10 — Hold period sweep")
    print("="*130)
    for hd in [30, 45, 60, 90, 120, 180, 240]:
        mask = (btc_v['vrp'] <= -10).fillna(False).values
        eq, tr = backtest(btc_v, mask, hd)
        m = metrics(eq, tr)
        print(f"  hold {hd:>3}d:  {fmt(m)}")

    # === STUDY 2: VRP threshold sweep (fixed 90d hold)
    print("\n" + "="*130)
    print(f"STUDY 2: BTC — VRP threshold sweep (hold 90d)")
    print("="*130)
    for th in [-5, -8, -10, -12, -15, -20, -25]:
        mask = (btc_v['vrp'] <= th).fillna(False).values
        eq, tr = backtest(btc_v, mask, 90)
        m = metrics(eq, tr)
        print(f"  VRP ≤ {th:>+3}:  {fmt(m)}")

    # === STUDY 3: Trend filters
    print("\n" + "="*130)
    print(f"STUDY 3: BTC VRP ≤ -10 hold 90d + trend filter")
    print("="*130)
    base_mask = (btc_v['vrp'] <= -10)
    filters = [
        ('No filter (baseline)', base_mask),
        ('+ price > EMA50',      base_mask & (btc_v['close'] > btc_v['ema50'])),
        ('+ price > EMA100',     base_mask & (btc_v['close'] > btc_v['ema100'])),
        ('+ price > EMA200',     base_mask & (btc_v['close'] > btc_v['ema200'])),
        ('+ price < EMA50 (contrarian)', base_mask & (btc_v['close'] < btc_v['ema50'])),
    ]
    for label, mask in filters:
        m_arr = mask.fillna(False).values
        eq, tr = backtest(btc_v, m_arr, 90)
        m = metrics(eq, tr)
        print(f"  {label:35}  {fmt(m)}")

    # === STUDY 4: Exit rules
    print("\n" + "="*130)
    print(f"STUDY 4: BTC VRP ≤ -10 — Different exit rules")
    print("="*130)
    for rule_label, rule in [
        ('Hold 60 days', ('days', 60)),
        ('Hold 90 days', ('days', 90)),
        ('Hold 120 days', ('days', 120)),
        ('Exit when VRP > 0 (max 90d)', ('days_or_vrp_normalize', 90)),
        ('Exit when VRP > 0 (max 180d)', ('days_or_vrp_normalize', 180)),
        ('Exit on break EMA50 (max 90d)', ('days_or_break_ema50', 90)),
        ('Exit on break EMA50 (max 180d)', ('days_or_break_ema50', 180)),
    ]:
        exit_rule, hd = rule
        mask = (btc_v['vrp'] <= -10).fillna(False).values
        eq, tr = backtest(btc_v, mask, hd, exit_rule=exit_rule)
        m = metrics(eq, tr)
        print(f"  {rule_label:35}  {fmt(m)}")

    # === STUDY 5: ETH version
    print("\n" + "="*130)
    print(f"STUDY 5: ETH — Same parameters")
    print("="*130)
    for th in [-10, -15, -20, -25]:
        for hd in [60, 90, 120]:
            mask = (eth_v['vrp'] <= th).fillna(False).values
            eq, tr = backtest(eth_v, mask, hd)
            m = metrics(eq, tr)
            print(f"  ETH VRP ≤ {th:>+3} hold {hd:>3}d:  {fmt(m)}")

    # === STUDY 6: Year-by-year stability (walk-forward)
    print("\n" + "="*130)
    print(f"STUDY 6: BTC VRP ≤ -10 hold 90d — Year-by-year breakdown")
    print("="*130)
    mask = (btc_v['vrp'] <= -10).fillna(False).values
    eq, tr = backtest(btc_v, mask, 90)
    if len(tr) > 0:
        tr['year'] = tr['exit_date'].dt.year
        for y in sorted(tr['year'].unique()):
            y_tr = tr[tr['year'] == y]
            y_pnl = (y_tr['pnl_pct'].apply(lambda x: 1+x/100).prod() - 1) * 100
            print(f"  {y}: {len(y_tr)} trades, total {y_pnl:+.1f}%, WR={(y_tr['pnl_pct']>0).sum()/len(y_tr)*100:.0f}%")

    # === STUDY 7: BTC + ETH portfolio
    print("\n" + "="*130)
    print(f"STUDY 7: 50/50 BTC + ETH VRP portfolio")
    print("="*130)
    btc_mask = (btc_v['vrp'] <= -10).fillna(False).values
    eth_mask = (eth_v['vrp'] <= -10).fillna(False).values
    eq_btc, tr_btc = backtest(btc_v, btc_mask, 90)
    eq_eth, tr_eth = backtest(eth_v, eth_mask, 90)
    common = btc_v.index.intersection(eth_v.index)
    eq_btc_aligned = eq_btc.reindex(common, method='ffill').fillna(method='bfill')
    eq_eth_aligned = eq_eth.reindex(common, method='ffill').fillna(method='bfill')
    port = 0.5 * eq_btc_aligned + 0.5 * eq_eth_aligned
    p_total = (port.iloc[-1]/INITIAL - 1)*100
    p_dd = ((port.cummax() - port)/port.cummax()).max()*100
    print(f"  50/50 BTC+ETH VRP90:  total={p_total:+.0f}%  DD={p_dd:.1f}%")
    print(f"  vs BTC alone:         total={(eq_btc.iloc[-1]/INITIAL-1)*100:+.0f}%  DD={((eq_btc.cummax()-eq_btc)/eq_btc.cummax()).max()*100:.1f}%")
    print(f"  vs ETH alone:         total={(eq_eth.iloc[-1]/INITIAL-1)*100:+.0f}%  DD={((eq_eth.cummax()-eq_eth)/eq_eth.cummax()).max()*100:.1f}%")

    # === STUDY 8: Trade-by-trade detail for best variant
    print("\n" + "="*130)
    print(f"STUDY 8: BEST variant trade detail — BTC VRP ≤ -10 hold 90d")
    print("="*130)
    mask = (btc_v['vrp'] <= -10).fillna(False).values
    eq, tr = backtest(btc_v, mask, 90)
    if len(tr) > 0:
        print(f"  {'#':>3}  {'entry':>11}  {'exit':>11}  {'days':>5}  {'entry_px':>10}  {'exit_px':>10}  {'PnL':>8}  {'MAE':>7}  {'reason':>15}")
        for i, t in enumerate(tr.itertuples(), 1):
            print(f"  {i:>3}  {t.entry_date.date()}  {t.exit_date.date()}  "
                  f"{int(t.hold_d):>4}d  ${t.entry_px:>9,.0f}  ${t.exit_px:>9,.0f}  "
                  f"{t.pnl_pct:>+7.1f}%  {t.mae_pct:>+6.1f}%  {t.reason:>15}")


if __name__ == "__main__":
    main()
