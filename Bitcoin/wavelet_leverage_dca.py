"""Study: leverage feasibility + DCA averaging + hard MAE stop.

For BTC champion (8a k=1.5 std30) and ETH champion (8a k=1.5 + EMA100, 4H):
  1) baseline (no stop, no DCA)
  2) + hard MAE stop at -15%, -20%, -25%
  3) + DCA ladder: 33%/33%/33% at signal, -5%, -10%
  4) + simulate 2x and 3x leverage on best safe variant

Constraint: MAE per trade ≤ 20% (so leverage 3x has buffer to liquidation at 33%).
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


# ============ Wavelet stack ============
def ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def atr_vec(high, low, close, n=14):
    pc = np.empty_like(close); pc[0] = close[0]; pc[1:] = close[:-1]
    tr = np.maximum(high-low, np.maximum(np.abs(high-pc), np.abs(low-pc)))
    out = np.full(len(close), np.nan)
    out[0] = tr[0]; a = 1.0/n
    for i in range(1, len(close)):
        out[i] = a*tr[i] + (1-a)*out[i-1]
    return out

def atrous_haar(x, J=6):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm

def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'price': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        'index': df.index,
        's5': sm[5], 's6': sm[6],
        'std30': pd.Series(log_c - sm[5]).rolling(30).std().values,
        'std50': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'ema100': ema(df['close'].values, 100),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
    }


# ============ Signal generators ============
def signal_btc_champion(p, k=1.5):
    """8a (k=1.5, std30) signals: entry & exit bool arrays."""
    upper = p['s5'] + k * p['std30']
    entry = p['log_c'] > upper
    exit_ = p['log_c'] < p['s5']
    mask = np.isnan(p['std30'])
    return np.where(mask, False, entry), np.where(mask, False, exit_)

def signal_eth_champion(p, k=1.5):
    """8a (k=1.5) + EMA100 signals."""
    upper = p['s5'] + k * p['std50']
    entry = (p['log_c'] > upper) & (p['price'] > p['ema100'])
    exit_ = (p['log_c'] < p['s5']) | (p['price'] < p['ema100'])
    mask = np.isnan(p['std50']) | np.isnan(p['ema100'])
    return np.where(mask, False, entry), np.where(mask, False, exit_)


# ============ Backtest with optional hard MAE stop & DCA ============
def backtest(p, entry_cond, exit_cond,
             leverage=1.0, hard_mae_stop=None,
             dca_steps=None):
    """
    leverage: position multiplier (1=cash, 2=2x, 3=3x).
    hard_mae_stop: e.g. -0.20 → close if (low/entry - 1) <= -0.20
    dca_steps: list of (fraction, drop_pct_from_initial_entry) — e.g. [(0.34, 0), (0.33, -0.05), (0.33, -0.10)]
               If None → single all-in entry.

    With leverage: pnl per unit move is multiplied. Cash collateral is INITIAL.
    Liquidation if (price/entry - 1) <= -1/leverage (account wiped).
    """
    closes = p['price']; highs = p['high']; lows = p['low']; dates = p['index']
    n = len(closes)
    cash = INITIAL
    # Track position as "notional value" / entry_avg_px
    units = 0.0  # in BTC
    entry_avg = 0.0  # weighted avg entry
    total_invested_notional = 0.0  # nominal value committed via leverage
    dca_idx = 0  # which DCA step we're on
    initial_signal_px = 0.0  # price at first signal in current sequence
    intra_min_px = 0; intra_max_px = 0
    eq = np.zeros(n)
    trades = []
    entry_date = None

    # Default DCA: all-in
    if dca_steps is None:
        dca_steps = [(1.0, 0.0)]
    dca_steps = list(dca_steps)

    for i in range(n):
        c = closes[i]; h = highs[i]; l = lows[i]
        # Update equity (mark-to-market)
        if units > 0:
            mtm_pnl = (c - entry_avg) * units  # units already has leverage built in
            equity = cash + mtm_pnl
            # Update intra trade tracking
            if l < intra_min_px: intra_min_px = l
            if h > intra_max_px: intra_max_px = h
        else:
            equity = cash
        eq[i] = max(equity, 0.001)  # protect log

        # Liquidation check (if leveraged and position open)
        if units > 0 and leverage > 1:
            liq_px = entry_avg * (1 - 1.0/leverage + 0.01)  # 1% buffer before liquidation
            if l <= liq_px:
                # Forced close at liquidation price
                cash = 0.001  # account wiped
                trades.append({
                    'entry_date': entry_date, 'exit_date': dates[i],
                    'entry_px': entry_avg, 'exit_px': liq_px,
                    'pnl_pct': -100.0, 'raw_pnl_pct': -100.0,
                    'mae_pct': -100.0, 'mfe_pct': (intra_max_px/entry_avg - 1)*100,
                    'reason': 'LIQUIDATION',
                    'hold_d': (dates[i] - entry_date).total_seconds()/86400,
                })
                units = 0; entry_avg = 0; total_invested_notional = 0
                dca_idx = 0
                continue

        # Hard MAE stop
        if units > 0 and hard_mae_stop is not None:
            stop_px = entry_avg * (1 + hard_mae_stop)
            if l <= stop_px:
                # Exit at stop_px — units already has leverage built in
                eff = stop_px * (1 - SLIP)
                pnl_unit_pct = (eff - entry_avg) / entry_avg
                pnl_dollars = (eff - entry_avg) * units  # units already includes leverage
                fee = units * eff * FEE
                cash += pnl_dollars - fee
                trades.append({
                    'entry_date': entry_date, 'exit_date': dates[i],
                    'entry_px': entry_avg, 'exit_px': eff,
                    'pnl_pct': pnl_unit_pct * 100 * leverage,
                    'raw_pnl_pct': pnl_unit_pct * 100,
                    'mae_pct': (intra_min_px/entry_avg - 1) * 100,
                    'mfe_pct': (intra_max_px/entry_avg - 1) * 100,
                    'reason': 'HARD_STOP',
                    'hold_d': (dates[i] - entry_date).total_seconds()/86400,
                })
                units = 0; entry_avg = 0; total_invested_notional = 0
                dca_idx = 0
                continue

        # Exit signal
        if units > 0 and exit_cond[i]:
            eff = c * (1 - SLIP)
            pnl_unit_pct = (eff - entry_avg) / entry_avg
            pnl_dollars = (eff - entry_avg) * units  # units already includes leverage
            fee = units * eff * FEE
            cash += pnl_dollars - fee
            trades.append({
                'entry_date': entry_date, 'exit_date': dates[i],
                'entry_px': entry_avg, 'exit_px': eff,
                'pnl_pct': pnl_unit_pct * 100 * leverage,
                'raw_pnl_pct': pnl_unit_pct * 100,
                'mae_pct': (intra_min_px/entry_avg - 1) * 100,
                'mfe_pct': (intra_max_px/entry_avg - 1) * 100,
                'reason': 'SIGNAL',
                'hold_d': (dates[i] - entry_date).total_seconds()/86400,
            })
            units = 0; entry_avg = 0; total_invested_notional = 0
            dca_idx = 0
            continue

        # Entry / DCA add
        if units == 0 and entry_cond[i]:
            # First entry
            eff = c * (1 + SLIP)
            frac, drop_pct = dca_steps[0]
            position_size_dollars = cash * 0.99 * frac  # use fraction of cash
            buy_units = (position_size_dollars * leverage) / eff
            fee = buy_units * eff * FEE
            cash -= fee
            units = buy_units
            entry_avg = eff
            total_invested_notional = buy_units * eff
            initial_signal_px = eff
            intra_min_px = l; intra_max_px = h
            entry_date = dates[i]
            dca_idx = 1
        elif units > 0 and dca_idx < len(dca_steps):
            # Check next DCA trigger
            next_frac, next_drop_pct = dca_steps[dca_idx]
            trigger_px = initial_signal_px * (1 + next_drop_pct)
            if l <= trigger_px:
                # Execute add at trigger price
                eff = trigger_px * (1 + SLIP)
                position_size_dollars = INITIAL * next_frac  # fraction of INITIAL capital
                add_units = (position_size_dollars * leverage) / eff
                fee = add_units * eff * FEE
                cash -= fee
                # Update weighted avg entry
                total_invested_notional += add_units * eff
                units += add_units
                entry_avg = total_invested_notional / units
                dca_idx += 1

    # Force close at end
    if units > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        pnl_unit_pct = (eff - entry_avg) / entry_avg
        pnl_dollars = (eff - entry_avg) * units
        fee = units * eff * FEE
        cash += pnl_dollars - fee
        trades.append({
            'entry_date': entry_date, 'exit_date': dates[-1],
            'entry_px': entry_avg, 'exit_px': eff,
            'pnl_pct': pnl_unit_pct * 100 * leverage,
            'raw_pnl_pct': pnl_unit_pct * 100,
            'mae_pct': (intra_min_px/entry_avg - 1) * 100,
            'mfe_pct': (intra_max_px/entry_avg - 1) * 100,
            'reason': 'EOH',
            'hold_d': (dates[-1] - entry_date).total_seconds()/86400,
        })

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(trades, final_eq, eq):
    if len(trades) == 0: return {}
    total_ret = (final_eq/INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae_worst = trades['mae_pct'].min()
    n_unsafe = (trades['mae_pct'] < -20).sum()
    n_liq = (trades.get('reason', pd.Series()) == 'LIQUIDATION').sum() if 'reason' in trades.columns else 0
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'max_loss': trades['pnl_pct'].min(),
        'mae_worst': mae_worst,
        'n_mae_over_20': n_unsafe,
        'n_liquidations': n_liq,
        'avg_pnl': trades['pnl_pct'].mean(),
    }


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def load_hourly(asset):
    name = "btc_usd_hourly_coinbase.csv" if asset == 'BTC' else "eth_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close']>0].dropna(subset=['close'])

def resample_4h(hourly):
    return hourly.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def fmt_row(label, m):
    if not m: return f"  {label:50}  (no trades)"
    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return (f"  {label:50}  N={m['N']:>3}  WR={m['WR']:>4.1f}%  PF={pf_str}  "
            f"ret={m['total_ret']:>+7.0f}%  DD={m['total_dd']:>4.1f}%  "
            f"worstMAE={m['mae_worst']:>+5.1f}%  unsafe={m['n_mae_over_20']:>2}  liq={m['n_liquidations']}")


def main():
    btc = load_daily('BTC')
    eth_4h = resample_4h(load_hourly('ETH'))

    print("="*150)
    print("BTC 1D — 8a (k=1.5, std30) — leverage / hard-stop / DCA study")
    print("="*150)
    p = precompute(btc)
    entry, exit_ = signal_btc_champion(p)

    print("\n  STAGE 1: baseline vs hard MAE stop")
    print(f"  {'='*100}")
    for stop in [None, -0.25, -0.20, -0.15, -0.12]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=1.0, hard_mae_stop=stop)
        m = metrics(tr, fe, eq)
        label = f"baseline (no stop)" if stop is None else f"hard MAE stop at {stop*100:.0f}%"
        print(fmt_row(label, m))

    print("\n  STAGE 2: DCA ladder (33% / 33% / 34% at signal, -5%, -10%)")
    print(f"  {'='*100}")
    # No DCA
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0)
    print(fmt_row("all-in (baseline)", metrics(tr, fe, eq)))
    # 2-step DCA: 50/50
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0,
                          dca_steps=[(0.5, 0.0), (0.5, -0.05)])
    print(fmt_row("DCA 50/50 at signal & -5%", metrics(tr, fe, eq)))
    # 3-step DCA: 34/33/33
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0,
                          dca_steps=[(0.34, 0.0), (0.33, -0.05), (0.33, -0.10)])
    print(fmt_row("DCA 34/33/33 at signal/-5/-10%", metrics(tr, fe, eq)))
    # 3-step deeper drops
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0,
                          dca_steps=[(0.34, 0.0), (0.33, -0.08), (0.33, -0.15)])
    print(fmt_row("DCA 34/33/33 at signal/-8/-15%", metrics(tr, fe, eq)))

    print("\n  STAGE 3: leverage 2x and 3x (no DCA, no stop)")
    print(f"  {'='*100}")
    for lev in [1.0, 2.0, 3.0]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=lev)
        print(fmt_row(f"leverage {lev:.0f}x (no stop, all-in)", metrics(tr, fe, eq)))

    print("\n  STAGE 4: leverage 2x/3x + hard MAE stop at -20%")
    print(f"  {'='*100}")
    for lev in [1.0, 2.0, 3.0]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=lev, hard_mae_stop=-0.20)
        print(fmt_row(f"leverage {lev:.0f}x + stop -20%", metrics(tr, fe, eq)))

    print("\n  STAGE 5: leverage 2x/3x + DCA 50/50")
    print(f"  {'='*100}")
    for lev in [1.0, 2.0, 3.0]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=lev,
                              dca_steps=[(0.5, 0.0), (0.5, -0.05)])
        print(fmt_row(f"leverage {lev:.0f}x + DCA 50/50", metrics(tr, fe, eq)))

    # ===== ETH =====
    print("\n\n" + "="*150)
    print("ETH 4H — 8a (k=1.5) + EMA100 — leverage / hard-stop / DCA study")
    print("="*150)
    p = precompute(eth_4h)
    entry, exit_ = signal_eth_champion(p)

    print("\n  STAGE 1: baseline vs hard MAE stop")
    print(f"  {'='*100}")
    for stop in [None, -0.20, -0.15, -0.12, -0.10]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=1.0, hard_mae_stop=stop)
        m = metrics(tr, fe, eq)
        label = f"baseline (no stop)" if stop is None else f"hard MAE stop at {stop*100:.0f}%"
        print(fmt_row(label, m))

    print("\n  STAGE 2: DCA")
    print(f"  {'='*100}")
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0)
    print(fmt_row("all-in (baseline)", metrics(tr, fe, eq)))
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0,
                          dca_steps=[(0.5, 0.0), (0.5, -0.03)])
    print(fmt_row("DCA 50/50 at signal/-3%", metrics(tr, fe, eq)))
    tr, fe, eq = backtest(p, entry, exit_, leverage=1.0,
                          dca_steps=[(0.34, 0.0), (0.33, -0.04), (0.33, -0.08)])
    print(fmt_row("DCA 34/33/33 at signal/-4/-8%", metrics(tr, fe, eq)))

    print("\n  STAGE 3: leverage 2x and 3x")
    print(f"  {'='*100}")
    for lev in [1.0, 2.0, 3.0]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=lev)
        print(fmt_row(f"leverage {lev:.0f}x", metrics(tr, fe, eq)))

    print("\n  STAGE 4: leverage 2x/3x + hard MAE stop at -15%")
    print(f"  {'='*100}")
    for lev in [1.0, 2.0, 3.0]:
        tr, fe, eq = backtest(p, entry, exit_, leverage=lev, hard_mae_stop=-0.15)
        print(fmt_row(f"leverage {lev:.0f}x + stop -15%", metrics(tr, fe, eq)))


if __name__ == "__main__":
    main()
