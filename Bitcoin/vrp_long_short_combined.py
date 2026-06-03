"""Test 3 ideas + portfolio combinations:

Strategy A: LONG VRP ≤ -10 + bearish EXIT filter (if DVOL ≤ 35 → close LONG early)
Strategy B: SHORT tactical 14-day on DVOL ≤ 35 (1% / 3% / 5% of capital)
Strategy C: SHORT only in BEAR regime (BTC < weekly EMA200)
Strategy D: Portfolio combinations of A + B + C
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


def load_btc():
    price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    price = price[price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    return price, dvol


def build_features(price, dvol):
    df = pd.DataFrame(index=price.index)
    df['close'] = price['close']
    df['high'] = price['high']
    df['low'] = price['low']
    log_ret = np.log(df['close'] / df['close'].shift(1))
    df['rv_30d'] = log_ret.rolling(30).std() * np.sqrt(365) * 100
    df['dvol'] = dvol['close'].reindex(df.index, method='ffill')
    df['vrp'] = df['dvol'] - df['rv_30d']

    # Weekly EMA200 for bear regime filter
    weekly = price['close'].resample('W-MON', label='left', closed='left').last().dropna()
    w_ema200 = pd.Series(ema(weekly.values, 200), index=weekly.index)
    df['w_ema200'] = w_ema200.reindex(df.index, method='ffill')
    df['bear_regime'] = df['close'] < df['w_ema200']
    return df


def backtest_long(df, entry_mask, hold_days, bearish_exit_mask=None):
    """LONG strategy. Optional: exit early when bearish_exit_mask fires."""
    closes = df['close'].values
    lows = df['low'].values
    dates = df.index
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False; entry_idx = 0; entry_px = 0; intra_min = 0
    trades = []

    bearish_exit = bearish_exit_mask.values if bearish_exit_mask is not None else None

    for i in range(n):
        c = closes[i]
        if in_pos:
            if intra_min == 0 or lows[i] < intra_min: intra_min = lows[i]
            eq[i] = cash * (c / entry_px)
        else:
            eq[i] = cash

        if in_pos:
            hold = i - entry_idx
            do_exit = False
            reason = ''
            if hold >= hold_days:
                do_exit = True; reason = 'maxhold'
            elif bearish_exit is not None and bearish_exit[i]:
                do_exit = True; reason = 'bearish_exit'

            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE) * 100
                mae = (intra_min/entry_px - 1) * 100
                cash *= (1 + ret/100)
                trades.append({'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'pnl_pct': ret, 'mae_pct': mae,
                              'hold_d': hold, 'reason': reason})
                in_pos = False
                eq[i] = cash

        if not in_pos and entry_mask[i]:
            entry_idx = i
            entry_px = c * (1 + SLIP)
            in_pos = True
            intra_min = lows[i]

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def backtest_short(df, entry_mask, hold_days, regime_filter=None):
    """SHORT strategy. Optional regime filter — only enter when filter is True."""
    closes = df['close'].values
    highs = df['high'].values
    dates = df.index
    n = len(closes)
    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False; entry_idx = 0; entry_px = 0; intra_max = 0
    trades = []
    rf = regime_filter.values if regime_filter is not None else None

    for i in range(n):
        c = closes[i]
        if in_pos:
            if highs[i] > intra_max: intra_max = highs[i]
            eq[i] = cash * (1 + (entry_px - c)/entry_px)
        else:
            eq[i] = cash

        if in_pos:
            hold = i - entry_idx
            if hold >= hold_days:
                exit_p = c * (1 + SLIP)
                ret = (entry_px - exit_p)/entry_px - 2*FEE
                mae = (entry_px - intra_max)/entry_px - 2*FEE
                cash *= (1 + ret)
                trades.append({'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'pnl_pct': ret*100, 'mae_pct': mae*100, 'hold_d': hold})
                in_pos = False
                eq[i] = cash

        if not in_pos and entry_mask[i]:
            if rf is not None and not rf[i]:
                continue
            entry_idx = i
            entry_px = c * (1 - SLIP)
            in_pos = True
            intra_max = highs[i]

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def metrics(eq, trades):
    if len(trades) == 0:
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        return {'N': 0, 'WR': 0, 'total': total, 'DD': 0}
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum()/len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'total': total, 'DD': dd}


def fmt(m):
    if m['N'] == 0:
        return f"N=  0  total={m['total']:>+7.0f}%  DD={m['DD']:>4.1f}%"
    pf_s = f"{m.get('PF', 0):>5.2f}" if m.get('PF', 0) != float('inf') else "  inf"
    return f"N={m['N']:>3}  WR={m['WR']:>4.0f}%  PF={pf_s}  total={m['total']:>+7.0f}%  DD={m['DD']:>4.1f}%"


def portfolio_combine(eq_a, weight_a, eq_b, weight_b):
    """Combine two equity curves with fixed weights. Returns total port equity (starting at $100)."""
    common = eq_a.index.intersection(eq_b.index)
    a = eq_a.reindex(common, method='ffill')
    b = eq_b.reindex(common, method='ffill')
    # Each side gets weight_x × INITIAL initial capital, evolves separately
    a_share = (a / INITIAL) * (weight_a * INITIAL)
    b_share = (b / INITIAL) * (weight_b * INITIAL)
    return a_share + b_share


def main():
    price, dvol = load_btc()
    df = build_features(price, dvol)
    df = df.dropna(subset=['dvol']).copy()
    print(f"BTC data: {df.index[0].date()} → {df.index[-1].date()} ({len(df)} days)")

    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"  Buy & Hold BTC (same period): {bh:+.0f}%\n")

    # ============ STRATEGY A: LONG with bearish exit filter ============
    print("="*150)
    print(f"STRATEGY A: LONG VRP ≤ -10 hold 90d — with bearish exit filter")
    print("="*150)

    long_entry = (df['vrp'] <= -10).fillna(False).values

    # Baseline (no bearish exit)
    eq_baseline, tr_baseline = backtest_long(df, long_entry, 90, bearish_exit_mask=None)
    m = metrics(eq_baseline, tr_baseline)
    print(f"\n  Baseline (no early exit):                {fmt(m)}")

    # With various bearish exits
    bearish_options = [
        ('DVOL ≤ 35', df['dvol'] <= 35),
        ('DVOL ≤ 40', df['dvol'] <= 40),
        ('DVOL ≤ 45', df['dvol'] <= 45),
        ('VRP ≥ +20', df['vrp'] >= 20),
        ('VRP ≥ +15', df['vrp'] >= 15),
        ('DVOL ≤ 40 AND VRP ≥ +10', (df['dvol'] <= 40) & (df['vrp'] >= 10)),
    ]
    for label, bearish in bearish_options:
        bearish_filled = bearish.fillna(False)
        eq, tr = backtest_long(df, long_entry, 90, bearish_exit_mask=bearish_filled)
        m = metrics(eq, tr)
        bearish_exits = (tr['reason'] == 'bearish_exit').sum() if len(tr) > 0 else 0
        print(f"  Exit if {label:35}  {fmt(m)}  bear_exits={bearish_exits}")

    # ============ STRATEGY B: Tactical SHORT (small allocation) ============
    print("\n" + "="*150)
    print(f"STRATEGY B: Tactical SHORT DVOL ≤ 35 hold 14d — different capital allocations")
    print("="*150)

    short_entry = (df['dvol'] <= 35).fillna(False).values
    eq_short, tr_short = backtest_short(df, short_entry, 14)
    m_short = metrics(eq_short, tr_short)
    print(f"\n  SHORT standalone (100% capital):         {fmt(m_short)}")

    if len(tr_short) > 0:
        print(f"\n  Trade-by-trade SHORT:")
        for _, t in tr_short.iterrows():
            print(f"    {t['entry_date'].date()} → {t['exit_date'].date()}  "
                  f"PnL {t['pnl_pct']:+5.1f}%  MAE {t['mae_pct']:+5.1f}%")

    # ============ STRATEGY C: SHORT only in bear regime ============
    print("\n" + "="*150)
    print(f"STRATEGY C: SHORT DVOL ≤ 35 hold 14d — only when BTC < weekly EMA200")
    print("="*150)

    bear_regime = df['bear_regime']
    n_bear_days = bear_regime.sum()
    print(f"\n  Days in bear regime (BTC < weekly EMA200): {n_bear_days} of {len(df)} ({n_bear_days/len(df)*100:.0f}%)")

    eq_short_bear, tr_short_bear = backtest_short(df, short_entry, 14, regime_filter=bear_regime)
    m_sb = metrics(eq_short_bear, tr_short_bear)
    print(f"  SHORT in bear regime only:               {fmt(m_sb)}")

    # Also try various short triggers in bear regime
    bear_shorts = [
        ('DVOL ≤ 40 hold 14d', (df['dvol'] <= 40).fillna(False).values, 14),
        ('DVOL ≤ 35 hold 14d', (df['dvol'] <= 35).fillna(False).values, 14),
        ('DVOL ≤ 35 hold 30d', (df['dvol'] <= 35).fillna(False).values, 30),
        ('VRP slope ↑ +10 hold 14d', (df['vrp'].diff(7) >= 10).fillna(False).values, 14),
    ]
    print(f"\n  Various bear-regime shorts:")
    for label, mask, hd in bear_shorts:
        eq, tr = backtest_short(df, mask, hd, regime_filter=bear_regime)
        m = metrics(eq, tr)
        print(f"    {label:30}  {fmt(m)}")

    # ============ STRATEGY D: Combined portfolios ============
    print("\n" + "="*150)
    print(f"STRATEGY D: COMBINED PORTFOLIOS (LONG + tactical SHORT)")
    print("="*150)

    # Best long: VRP ≤ -10 with DVOL ≤ 35 bearish exit
    bearish_filled = (df['dvol'] <= 35).fillna(False)
    eq_long_smart, tr_long_smart = backtest_long(df, long_entry, 90, bearish_exit_mask=bearish_filled)
    m_long = metrics(eq_long_smart, tr_long_smart)
    print(f"\n  LONG with bearish exit (DVOL≤35):        {fmt(m_long)}")

    # Best short: 14d, bear-regime only
    eq_short_best = eq_short_bear
    tr_short_best = tr_short_bear

    portfolios = [
        ('100% LONG only', 1.00, eq_long_smart, 0.00, eq_short_best),
        ('99% LONG + 1% short',  0.99, eq_long_smart, 0.01, eq_short_best),
        ('97% LONG + 3% short',  0.97, eq_long_smart, 0.03, eq_short_best),
        ('95% LONG + 5% short',  0.95, eq_long_smart, 0.05, eq_short_best),
        ('90% LONG + 10% short', 0.90, eq_long_smart, 0.10, eq_short_best),
    ]
    print(f"\n  Portfolio allocations (LONG with smart exit + bear-regime SHORT):")
    for label, w_long, eq_l, w_short, eq_s in portfolios:
        port = portfolio_combine(eq_l, w_long, eq_s, w_short)
        total = (port.iloc[-1]/INITIAL - 1) * 100
        dd = ((port.cummax() - port)/port.cummax()).max() * 100
        # Worst single drawdown month/quarter
        monthly = port.resample('ME').last().pct_change().dropna()
        worst_month = monthly.min() * 100
        print(f"    {label:30}  total={total:>+7.0f}%  DD={dd:>4.1f}%  worst month={worst_month:>+6.1f}%")

    # ============ STRATEGY E: Long w/o bearish exit + various shorts ============
    print("\n" + "="*150)
    print(f"STRATEGY E: Baseline LONG (no bearish exit) + small SHORT allocation")
    print("="*150)
    print(f"  Compare: does bearish exit on LONG or tactical SHORT add more value?")

    portfolios_e = [
        ('100% LONG (baseline)', 1.00, eq_baseline, 0.00, eq_short_best),
        ('99% LONG (baseline) + 1% short', 0.99, eq_baseline, 0.01, eq_short_best),
        ('95% LONG (baseline) + 5% short', 0.95, eq_baseline, 0.05, eq_short_best),
        ('100% LONG smart-exit (with bearish)', 1.00, eq_long_smart, 0.00, eq_short_best),
        ('95% LONG smart-exit + 5% short',     0.95, eq_long_smart, 0.05, eq_short_best),
    ]
    print(f"\n  Comparison:")
    for label, wl, el, ws, es in portfolios_e:
        port = portfolio_combine(el, wl, es, ws) if ws > 0 else el * wl
        total = (port.iloc[-1]/INITIAL - 1) * 100
        dd = ((port.cummax() - port)/port.cummax()).max() * 100
        print(f"    {label:45}  total={total:>+7.0f}%  DD={dd:>4.1f}%")

    # Show timing of short trades
    print("\n" + "="*150)
    print(f"BEAR-REGIME SHORT trades (timing in BTC history):")
    print("="*150)
    if len(tr_short_bear) > 0:
        for _, t in tr_short_bear.iterrows():
            print(f"    {t['entry_date'].date()} → {t['exit_date'].date()}  "
                  f"hold {int(t['hold_d'])}d  PnL {t['pnl_pct']:+5.1f}%  MAE {t['mae_pct']:+5.1f}%")
    else:
        print(f"  No bear-regime short trades — BTC has been mostly above weekly EMA200 since 2023-09!")


if __name__ == "__main__":
    main()
