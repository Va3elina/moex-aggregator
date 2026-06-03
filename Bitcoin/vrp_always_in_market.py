"""Always-in-market: LONG (VRP signal) → SHORT between → LONG (next signal).

Test with realistic crypto costs:
  - Perps funding: ~0.01-0.03% per 8h = 11-33% annual carrying cost
  - Futures roll: ~3-5% annual (contango/backwardation)
  - Spot short via borrow: 5-15% annual borrow fees

Test scenarios:
  - 0% annual short cost (idealized)
  - 10% annual short cost (decent funding)
  - 20% annual short cost (high funding periods)
  - 30% annual short cost (extreme funding)
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0005
SLIP = 0.0003


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
    return df


def backtest_always_in_market(df, long_entry, long_exit_min_hold, long_exit_dvol_level,
                                 long_max_hold, short_annual_cost_pct=10.0):
    """Always in market:
    - When LONG signal fires → go LONG
    - When LONG exits → immediately go SHORT
    - When LONG signal fires again → close SHORT, go LONG
    - SHORT pays daily funding cost = short_annual_cost_pct / 365

    long_exit_min_hold: min days before LONG can exit (60)
    long_exit_dvol_level: DVOL threshold to exit LONG (70)
    long_max_hold: force exit after N days (180)
    """
    closes = df['close'].values
    dates = df.index
    dvol = df['dvol'].values
    n = len(closes)

    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    # Position: 0=flat, 1=long, -1=short
    pos = 0
    entry_idx = 0
    entry_px = 0
    daily_short_cost = short_annual_cost_pct / 365 / 100  # convert to daily decimal
    trades = []

    for i in range(n):
        c = closes[i]

        # Mark to market
        if pos == 1:
            eq[i] = cash * (c / entry_px)
        elif pos == -1:
            # short MTM, also subtract carrying cost
            days_in_short = i - entry_idx
            mtm_ret = (entry_px - c) / entry_px
            cost = daily_short_cost * days_in_short
            eq[i] = cash * (1 + mtm_ret - cost)
        else:
            eq[i] = cash

        # === Check exits ===
        if pos == 1:
            hold = i - entry_idx
            do_exit = False
            if hold >= long_max_hold:
                do_exit = True
            elif hold >= long_exit_min_hold and not np.isnan(dvol[i]) and dvol[i] >= long_exit_dvol_level:
                do_exit = True

            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1 + ret)
                trades.append({'side': 'long', 'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': exit_p,
                              'pnl_pct': ret*100, 'hold_d': hold})
                # Immediately open SHORT
                entry_idx = i
                entry_px = c * (1 - SLIP)  # short open
                pos = -1
                eq[i] = cash
                continue

        # === Check LONG entry (closes short if needed) ===
        if long_entry[i]:
            if pos == -1:
                # Close short first
                exit_p = c * (1 + SLIP)
                days_in_short = i - entry_idx
                gross_ret = (entry_px - exit_p)/entry_px - 2*FEE
                cost = daily_short_cost * days_in_short
                net_ret = gross_ret - cost
                cash *= (1 + net_ret)
                trades.append({'side': 'short', 'entry_date': dates[entry_idx], 'exit_date': dates[i],
                              'entry_px': entry_px, 'exit_px': exit_p,
                              'pnl_pct': net_ret*100, 'gross_pnl_pct': gross_ret*100,
                              'cost_pct': cost*100, 'hold_d': days_in_short})
                pos = 0
                eq[i] = cash

            if pos == 0:
                # Open new LONG
                entry_idx = i
                entry_px = c * (1 + SLIP)
                pos = 1
                eq[i] = cash

    # Close any open position at end
    if pos != 0:
        c = closes[-1]
        if pos == 1:
            exit_p = c * (1 - SLIP)
            ret = (exit_p/entry_px - 1 - 2*FEE)
        else:
            exit_p = c * (1 + SLIP)
            days_in_short = (n-1) - entry_idx
            gross_ret = (entry_px - exit_p)/entry_px - 2*FEE
            cost = daily_short_cost * days_in_short
            ret = gross_ret - cost
        cash *= (1 + ret)
        trades.append({'side': 'long' if pos == 1 else 'short',
                      'entry_date': dates[entry_idx], 'exit_date': dates[-1],
                      'entry_px': entry_px, 'exit_px': exit_p,
                      'pnl_pct': ret*100, 'hold_d': (n-1) - entry_idx})

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def main():
    price, dvol = load_btc()
    df = build_features(price, dvol)
    df = df.dropna(subset=['dvol']).copy()
    print(f"BTC data: {df.index[0].date()} → {df.index[-1].date()}")

    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"  Buy & Hold BTC: {bh:+.0f}%\n")

    long_entry = (df['vrp'] <= -10).fillna(False).values

    print("="*150)
    print("Always-in-market: LONG (VRP≤-10) ↔ SHORT (always between)")
    print("With varying annual short cost (perps funding / borrow fees)")
    print("="*150)

    for short_cost in [0, 5, 10, 15, 20, 30, 50]:
        eq, tr = backtest_always_in_market(df, long_entry,
                                              long_exit_min_hold=60,
                                              long_exit_dvol_level=70,
                                              long_max_hold=180,
                                              short_annual_cost_pct=short_cost)
        total = (eq.iloc[-1]/INITIAL - 1) * 100
        dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
        n_long = (tr['side']=='long').sum() if len(tr) > 0 else 0
        n_short = (tr['side']=='short').sum() if len(tr) > 0 else 0
        avg_long_pnl = tr[tr['side']=='long']['pnl_pct'].mean() if n_long > 0 else 0
        avg_short_pnl = tr[tr['side']=='short']['pnl_pct'].mean() if n_short > 0 else 0
        total_short_cost = 0
        if 'cost_pct' in tr.columns:
            total_short_cost = tr['cost_pct'].sum()
        print(f"  Short carry {short_cost:>3.0f}%/yr:  total={total:>+7.0f}%  DD={dd:>4.1f}%  "
              f"long N={n_long}(avg{avg_long_pnl:+.1f}%)  short N={n_short}(avg{avg_short_pnl:+.1f}%)  "
              f"short_cost_total={total_short_cost:.1f}%")

    print("\n  Compare to LONG-only strategy (no short between):")
    # Run pure LONG version (no short)
    cash = INITIAL
    closes = df['close'].values
    eq2 = np.full(len(df), INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    long_only_trades = []
    for i in range(len(df)):
        c = closes[i]
        if in_pos:
            eq2[i] = cash * (c / entry_px)
        else:
            eq2[i] = cash
        if in_pos:
            hold = i - entry_idx
            do_exit = False
            if hold >= 180: do_exit = True
            elif hold >= 60 and not np.isnan(df['dvol'].values[i]) and df['dvol'].values[i] >= 70: do_exit = True
            if do_exit:
                exit_p = c * (1 - SLIP)
                ret = (exit_p/entry_px - 1 - 2*FEE)
                cash *= (1 + ret)
                long_only_trades.append({'pnl_pct': ret*100, 'hold_d': hold})
                in_pos = False
                eq2[i] = cash
        if not in_pos and long_entry[i]:
            entry_idx = i
            entry_px = c * (1 + SLIP)
            in_pos = True

    lt = pd.DataFrame(long_only_trades)
    lo_total = (eq2[-1]/INITIAL - 1) * 100
    lo_dd = ((pd.Series(eq2).cummax() - eq2)/pd.Series(eq2).cummax()).max() * 100
    print(f"  LONG only (no short between):  total={lo_total:+.0f}%  DD={lo_dd:.1f}%  N={len(lt)}")

    # Show what BTC did during "between" periods
    print("\n" + "="*150)
    print("WHAT BTC DID between LONG trades (the 'short periods')")
    print("="*150)
    # Compute periods between LONG trades
    if len(lt) > 0:
        eq2_series = pd.Series(eq2, index=df.index)
        eq, tr_full = backtest_always_in_market(df, long_entry, 60, 70, 180, 10)
        print(f"\n  Between-trade periods:")
        prev_exit = None
        prev_exit_idx = 0
        for _, t in tr_full.iterrows():
            if t['side'] == 'short':
                btc_move = (t['exit_px']/t['entry_px'] - 1) * 100
                direction = "📉 BTC down (short wins)" if btc_move < 0 else "📈 BTC UP (short loses)"
                print(f"    {t['entry_date'].date()} → {t['exit_date'].date()}  ({int(t['hold_d'])}d)  "
                      f"BTC moved {btc_move:+.1f}%  {direction}")


if __name__ == "__main__":
    main()
