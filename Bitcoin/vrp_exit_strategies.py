"""Test smarter exit rules for VRP strategy.

Beyond simple "hold N days" — try:
  A. Take profit at +N% above entry
  B. Trailing stop on ATR
  C. Exit when DVOL re-spikes above 70
  D. Exit on wavelet slope flip negative
  E. RSI overbought (like V1)
  F. Min N days + then RSI exit (hybrid)
  G. Profit ladder: take 50% at +20%, rest at +50% or 120d
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

def rsi_a(close, n=14):
    s = pd.Series(close)
    delta = s.diff(); gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values

def atr(high, low, close, n=14):
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
    df['rsi'] = rsi_a(df['close'].values, 14)
    df['rsi_sma'] = pd.Series(df['rsi']).rolling(14).mean().values
    df['atr14'] = atr(df['high'].values, df['low'].values, df['close'].values, 14)
    # Wavelet
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    df['s6'] = sm[6]
    df['slope_s6'] = np.r_[np.full(5, np.nan), sm[6][5:] - sm[6][:-5]]
    return df


def backtest_with_exit(df, entry_mask, exit_func, max_hold_days=180):
    """Generic backtest with custom exit function.
    exit_func(df, i, entry_idx, entry_px, intra_max) returns (do_exit, reason)
    """
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    dates = df.index
    n = len(closes)

    cash = INITIAL
    eq = np.full(n, INITIAL, dtype=float)
    in_pos = False
    entry_idx = 0
    entry_px = 0
    intra_max = 0
    intra_min = 0
    trades = []

    for i in range(n):
        c = closes[i]
        if in_pos:
            if highs[i] > intra_max: intra_max = highs[i]
            if intra_min == 0 or lows[i] < intra_min: intra_min = lows[i]
            eq[i] = cash * (c / entry_px)
        else:
            eq[i] = cash

        if in_pos:
            hold = i - entry_idx
            do_exit, reason = exit_func(df, i, entry_idx, entry_px, intra_max, intra_min)
            if hold >= max_hold_days:
                do_exit = True; reason = 'maxhold'

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

        if not in_pos and entry_mask[i]:
            entry_idx = i
            entry_px = c * (1 + SLIP)
            in_pos = True
            intra_max = highs[i]
            intra_min = lows[i]

    return pd.Series(eq, index=dates), pd.DataFrame(trades)


def metrics(eq, trades):
    if len(trades) == 0:
        return None
    total = (eq.iloc[-1]/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum()/len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae = trades['mae_pct'].min()
    avg_hold = trades['hold_d'].mean()
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'total': total, 'DD': dd, 'MAE': mae, 'avg_hold': avg_hold}


def fmt(m):
    if m is None: return "no trades"
    pf_s = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    return f"N={m['N']:>3}  WR={m['WR']:>4.0f}%  PF={pf_s}  total={m['total']:>+7.0f}%  DD={m['DD']:>4.1f}%  MAE={m['MAE']:>+5.1f}%  hold={m['avg_hold']:>4.0f}d"


# Exit rule generators
def make_exit_take_profit(tp_pct):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        c = df['close'].values[i]
        if (c / entry_px - 1) * 100 >= tp_pct:
            return True, 'TP'
        return False, ''
    return fn

def make_exit_trailing_atr(atr_mult):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        atr_v = df['atr14'].values[i]
        if np.isnan(atr_v): return False, ''
        trail_level = intra_max - atr_mult * atr_v
        if df['low'].values[i] <= trail_level:
            return True, 'trail'
        return False, ''
    return fn

def make_exit_rsi_overbought(rsi_threshold=70):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        if i < 1: return False, ''
        rsi_sma = df['rsi_sma'].values
        if np.isnan(rsi_sma[i]) or np.isnan(rsi_sma[i-1]): return False, ''
        # Cross under threshold (was above, now below)
        if rsi_sma[i-1] >= rsi_threshold and rsi_sma[i] < rsi_threshold:
            return True, 'rsi'
        return False, ''
    return fn

def make_exit_min_hold_then_rsi(min_days=30, rsi_thr=70):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        if i - entry_idx < min_days: return False, ''
        if i < 1: return False, ''
        rsi_sma = df['rsi_sma'].values
        if np.isnan(rsi_sma[i]) or np.isnan(rsi_sma[i-1]): return False, ''
        if rsi_sma[i-1] >= rsi_thr and rsi_sma[i] < rsi_thr:
            return True, 'min+rsi'
        return False, ''
    return fn

def make_exit_slope6_neg(min_days=30):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        if i - entry_idx < min_days: return False, ''
        slope = df['slope_s6'].values
        if np.isnan(slope[i]) or np.isnan(slope[i-1]): return False, ''
        if slope[i-1] >= 0 and slope[i] < 0:
            return True, 'slope6_neg'
        return False, ''
    return fn

def make_exit_dvol_spike(threshold=70, min_days=30):
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        if i - entry_idx < min_days: return False, ''
        dvol = df['dvol'].values[i]
        if np.isnan(dvol): return False, ''
        if dvol >= threshold:
            return True, 'dvol_spike'
        return False, ''
    return fn

def make_exit_combo(min_days=60, tp_pct=50, rsi_thr=70):
    """Hybrid: take profit if hit OR RSI cross after min_days."""
    def fn(df, i, entry_idx, entry_px, intra_max, intra_min):
        c = df['close'].values[i]
        # Take profit
        if (c / entry_px - 1) * 100 >= tp_pct:
            return True, 'TP'
        if i - entry_idx < min_days: return False, ''
        if i < 1: return False, ''
        rsi_sma = df['rsi_sma'].values
        if np.isnan(rsi_sma[i]) or np.isnan(rsi_sma[i-1]): return False, ''
        if rsi_sma[i-1] >= rsi_thr and rsi_sma[i] < rsi_thr:
            return True, 'min+rsi'
        return False, ''
    return fn


def main():
    btc_price = pd.read_csv(ROOT/'btc_usd_daily_coinbase.csv', parse_dates=['date']).set_index('date').sort_index()
    btc_price = btc_price[btc_price['close']>0].dropna()
    dvol = pd.read_csv(ROOT / 'btc_dvol_daily.csv', parse_dates=['date']).set_index('date').sort_index()

    df = compute_features(btc_price, dvol)
    df_v = df.dropna(subset=['vrp']).copy()

    entry_mask = (df_v['vrp'] <= -10).fillna(False).values
    print(f"BTC VRP signal — entry mask: {entry_mask.sum()} signal bars")
    print(f"Period: {df_v.index[0].date()} → {df_v.index[-1].date()}\n")
    print("="*130)
    print("VRP exit rules comparison — looking for SMARTER than fixed hold")
    print("="*130)

    # Baseline: fixed holds
    print("\n  Baselines (fixed hold):")
    for hd in [60, 90, 120, 180]:
        def exit_fn(df, i, entry_idx, entry_px, im, imn, hd=hd):
            return (i - entry_idx >= hd, 'hold')
        eq, tr = backtest_with_exit(df_v, entry_mask, exit_fn, max_hold_days=hd)
        m = metrics(eq, tr)
        print(f"    Fixed hold {hd:>3}d:        {fmt(m)}")

    # Take profit variants
    print("\n  Take profit (max 180d hold):")
    for tp in [20, 30, 50, 70, 100]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_take_profit(tp), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    TP at +{tp:>3}%:           {fmt(m)}")

    # Trailing ATR
    print("\n  Trailing ATR stop (max 180d):")
    for am in [2.0, 3.0, 4.0, 5.0, 7.0]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_trailing_atr(am), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    ATR({am})x trail:        {fmt(m)}")

    # RSI overbought
    print("\n  RSI exit:")
    for rt in [60, 65, 70, 75, 80]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_rsi_overbought(rt), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    RSI crossunder {rt}:      {fmt(m)}")

    # Min hold + RSI
    print("\n  Min hold + then RSI:")
    for mh, rt in [(30, 70), (60, 70), (90, 70), (60, 75), (60, 65)]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_min_hold_then_rsi(mh, rt), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    Min {mh}d + RSI<{rt}:        {fmt(m)}")

    # Slope6 neg
    print("\n  Wavelet slope6 negative (min N days):")
    for mh in [30, 60, 90]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_slope6_neg(mh), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    Min {mh}d + slope6<0:    {fmt(m)}")

    # DVOL spike exit
    print("\n  Exit when DVOL spikes back:")
    for mh, thr in [(30, 60), (30, 70), (60, 60), (60, 70)]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_dvol_spike(thr, mh), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    Min {mh}d + DVOL>{thr}:      {fmt(m)}")

    # Combo: TP or min hold + RSI
    print("\n  Combo (TP or min hold + RSI):")
    for mh, tp in [(60, 30), (60, 50), (90, 50), (90, 100)]:
        eq, tr = backtest_with_exit(df_v, entry_mask, make_exit_combo(mh, tp, 70), max_hold_days=180)
        m = metrics(eq, tr)
        print(f"    Min {mh}d + TP{tp}% + RSI:  {fmt(m)}")


if __name__ == "__main__":
    main()
