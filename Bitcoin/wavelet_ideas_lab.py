"""Test 8 different wavelet-based strategies on BTC/ETH.

Ideas:
  1) s5 × s6 cross: long when s5 > s6
  2) Distance |s5-s6| widening: long when spread growing in positive direction
  3a) Full alignment: price > s5 > s6
  3b) Bull regime (s5 > s6 alone), regardless of price
  4)  s4 as timing layer: enter on s4 slope flip up while s5∧s6 up
  5)  Hierarchical scale-out: 33% per scale that flips down
  6)  2nd derivative: slope5 up AND slope of slope5 up (accelerating)
  7a) Pullback in uptrend: s6 up, price below s5, slope5 flips up
  7b) Pullback wider: s6 up, price below s5, exit when price > s5
  8a) Wavelet bands breakout: long on break above s5 + k*std
  8b) Wavelet bands mean-revert: long at s5 - k*std, exit at s5

Plus existing winners as benchmarks: W1 (s5∧s6), BTC-I1, ETH-C6.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
ASSETS = {
    'BTC': 'btc_usd_daily_coinbase.csv',
    'ETH': 'eth_usd_daily_coinbase.csv',
}
FEE = 0.0005
SLIP = 0.0003
INITIAL = 100.0


def ema(series, n):
    s = series.values
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=series.index)
    out[n-1] = s[:n].mean()
    k = 2.0 / (n + 1.0)
    for i in range(n, len(s)):
        out[i] = s[i] * k + out[i-1] * (1 - k)
    return pd.Series(out, index=series.index)


def atrous_haar(x, J=6):
    n = len(x)
    smooths = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J + 1):
        gap = 2 ** (j - 1)
        prev = smooths[-1]
        s = np.full(n, np.nan)
        for i in range(n):
            if i - gap < 0:
                s[i] = prev[i]
            else:
                s[i] = 0.5 * (prev[i] + prev[i - gap])
        smooths.append(s)
    return smooths


def slope(arr, lookback=5):
    out = np.full(len(arr), np.nan)
    for i in range(lookback, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i - lookback]):
            out[i] = arr[i] - arr[i - lookback]
    return out


# ----------------- Backtest engine -----------------
def backtest(df, target):
    """target ∈ [0,1] per bar. Rebalances on change."""
    closes = df['close'].values
    dates = df.index
    cash = INITIAL
    qty = 0.0
    eq_curve = np.zeros(len(closes))
    trades = []
    entry_value = 0.0
    entry_date = None
    prev_tgt = 0.0
    for i in range(len(closes)):
        c = closes[i]; date = dates[i]
        equity = cash + qty * c
        eq_curve[i] = equity
        tgt = target[i]
        if np.isnan(tgt): continue

        # Compute desired qty
        desired_value = equity * tgt
        desired_qty = desired_value / c if c > 0 else 0
        diff = desired_qty - qty
        if abs(diff * c) < equity * 0.01:
            prev_tgt = tgt; continue

        if diff > 0:
            eff = c * (1 + SLIP)
            cost = diff * eff * (1 + FEE)
            if cost > cash:
                diff = (cash * 0.99) / (eff * (1 + FEE))
                cost = diff * eff * (1 + FEE)
            cash -= cost
            if qty == 0:
                entry_value = cost; entry_date = date
            else:
                entry_value += cost
            qty += diff
        else:
            sell_qty = -diff
            eff = c * (1 - SLIP)
            proceeds = sell_qty * eff * (1 - FEE)
            cash += proceeds
            if qty > 0:
                cost_portion = entry_value * (sell_qty / qty)
                pnl_pct = (proceeds / cost_portion - 1) * 100 if cost_portion > 0 else 0
                trades.append({'entry_date': entry_date, 'exit_date': date,
                              'pnl_pct': pnl_pct, 'hold_days': (date - entry_date).days,
                              'qty_frac': sell_qty / qty})
                entry_value -= cost_portion
            qty -= sell_qty
            if qty < 1e-9:
                qty = 0; entry_value = 0; entry_date = None
        prev_tgt = tgt

    if qty > 0:
        eff = closes[-1] * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl_pct = (proceeds / entry_value - 1) * 100 if entry_value > 0 else 0
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'hold_days': (dates[-1] - entry_date).days,
                      'qty_frac': 1.0})
    return pd.DataFrame(trades), cash, pd.Series(eq_curve, index=dates)


def metrics(trades, final_eq, eq_curve):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'avg': 0,
                'ret': (final_eq/INITIAL-1)*100, 'DD': 0}
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    ret = (final_eq / INITIAL - 1) * 100
    dd = ((eq_curve.cummax() - eq_curve) / eq_curve.cummax()).max() * 100
    return {'N': len(trades), 'WR': wr, 'PF': pf, 'avg': trades['pnl_pct'].mean(),
            'ret': ret, 'DD': dd}


# ----------------- Idea generators -----------------
def precompute(df):
    """Returns dict of arrays we'll use."""
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        's3': sm[3], 's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl3': slope(sm[3], 5),
        'sl4': slope(sm[4], 5),
        'sl5': slope(sm[5], 5),
        'sl6': slope(sm[6], 5),
        'log_c': log_c,
        'price': df['close'].values,
        'ema50': ema(df['close'], 50).values,
    }


def idea_baseline_w1(p):
    """W1 baseline: s5∧s6 slope up."""
    n = len(p['sl5'])
    t = np.zeros(n)
    for i in range(n):
        if np.isnan(p['sl5'][i]) or np.isnan(p['sl6'][i]):
            t[i] = np.nan; continue
        t[i] = 1.0 if (p['sl5'][i] > 0 and p['sl6'][i] > 0) else 0.0
    return t


def idea1_cross(p):
    """s5 > s6 → long."""
    n = len(p['s5'])
    t = np.zeros(n)
    for i in range(n):
        if np.isnan(p['s5'][i]) or np.isnan(p['s6'][i]):
            t[i] = np.nan; continue
        t[i] = 1.0 if p['s5'][i] > p['s6'][i] else 0.0
    return t


def idea2_widening(p):
    """Long when spread (s5-s6) is positive AND growing."""
    n = len(p['s5'])
    spread = p['s5'] - p['s6']
    spread_slope = slope(spread, 5)
    t = np.zeros(n)
    for i in range(n):
        if np.isnan(spread[i]) or np.isnan(spread_slope[i]):
            t[i] = np.nan; continue
        if spread[i] > 0 and spread_slope[i] > 0:
            t[i] = 1.0
        else:
            t[i] = 0.0
    return t


def idea3a_alignment(p):
    """price > s5 > s6 (full bull alignment, in log space)."""
    n = len(p['s5'])
    t = np.zeros(n)
    for i in range(n):
        if np.isnan(p['s5'][i]) or np.isnan(p['s6'][i]):
            t[i] = np.nan; continue
        if p['log_c'][i] > p['s5'][i] and p['s5'][i] > p['s6'][i]:
            t[i] = 1.0
        else:
            t[i] = 0.0
    return t


def idea3b_bull_regime(p):
    """s5 > s6 (bull regime), regardless of price position."""
    return idea1_cross(p)  # same as Idea 1, kept for clarity in output


def idea4_s4_timing(p):
    """Regime s5∧s6 up. Enter on s4 slope flipping up. Exit when regime breaks."""
    n = len(p['sl5'])
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl4','sl5','sl6')):
            t[i] = state; continue
        regime_ok = p['sl5'][i] > 0 and p['sl6'][i] > 0
        if state == 0:
            # need s4 just flipped up while regime ok
            if regime_ok and p['sl4'][i] > 0 and i > 0 and not np.isnan(p['sl4'][i-1]) and p['sl4'][i-1] <= 0:
                state = 1
        elif state == 1:
            if not regime_ok:
                state = 0
        t[i] = float(state)
    return t


def idea5_hierarchical(p):
    """Position = (count of (sl4,sl5,sl6) > 0) / 3 → 0, 0.33, 0.67, 1.0."""
    n = len(p['sl5'])
    t = np.zeros(n)
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl4','sl5','sl6')):
            t[i] = np.nan; continue
        cnt = (p['sl4'][i] > 0) + (p['sl5'][i] > 0) + (p['sl6'][i] > 0)
        t[i] = cnt / 3.0
    return t


def idea6_acceleration(p):
    """Slope5 > 0 AND slope of slope5 > 0 (uptrend accelerating)."""
    n = len(p['sl5'])
    sl5_slope = slope(p['sl5'], 5)
    t = np.zeros(n)
    for i in range(n):
        if np.isnan(p['sl5'][i]) or np.isnan(sl5_slope[i]):
            t[i] = np.nan; continue
        if p['sl5'][i] > 0 and sl5_slope[i] > 0:
            t[i] = 1.0
        else:
            t[i] = 0.0
    return t


def idea7a_pullback_strict(p):
    """Bull regime (s6 up), price below s5, slope5 flips from neg to pos → enter.
    Exit when price > s5."""
    n = len(p['sl5'])
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl5','sl6','s5')):
            t[i] = state; continue
        regime_ok = p['sl6'][i] > 0
        below = p['log_c'][i] < p['s5'][i]
        flip_up = i > 0 and not np.isnan(p['sl5'][i-1]) and p['sl5'][i-1] <= 0 and p['sl5'][i] > 0
        if state == 0:
            if regime_ok and below and flip_up:
                state = 1
        elif state == 1:
            if p['log_c'][i] > p['s5'][i] or p['sl6'][i] < 0:
                state = 0
        t[i] = float(state)
    return t


def idea7b_pullback_wide(p):
    """Bull regime (s6 up). Enter whenever price drops below s5 (the medium).
    Exit when price climbs back above s5."""
    n = len(p['sl5'])
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl6','s5')):
            t[i] = state; continue
        regime_ok = p['sl6'][i] > 0
        below = p['log_c'][i] < p['s5'][i]
        above = p['log_c'][i] >= p['s5'][i]
        if state == 0:
            if regime_ok and below:
                state = 1
        elif state == 1:
            if above or not regime_ok:
                state = 0
        t[i] = float(state)
    return t


def idea8a_band_breakout(p, k=1.5, lookback=50):
    """Long when price breaks above s5 + k*std. Exit when price returns to s5."""
    n = len(p['s5'])
    dev = p['log_c'] - p['s5']
    band_std = pd.Series(dev).rolling(lookback).std().values
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if np.isnan(band_std[i]):
            t[i] = state; continue
        upper = p['s5'][i] + k * band_std[i]
        if state == 0:
            if p['log_c'][i] > upper:
                state = 1
        elif state == 1:
            if p['log_c'][i] < p['s5'][i]:
                state = 0
        t[i] = float(state)
    return t


def idea8b_band_meanrevert(p, k=1.5, lookback=50):
    """Long at s5 - k*std (oversold to mean). Exit at s5 (mean reached)."""
    n = len(p['s5'])
    dev = p['log_c'] - p['s5']
    band_std = pd.Series(dev).rolling(lookback).std().values
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if np.isnan(band_std[i]):
            t[i] = state; continue
        lower = p['s5'][i] - k * band_std[i]
        if state == 0:
            if p['log_c'][i] < lower:
                state = 1
        elif state == 1:
            if p['log_c'][i] >= p['s5'][i]:
                state = 0
        t[i] = float(state)
    return t


# Winner benchmarks
def btc_i1(p):
    """BTC winner: slope5 > 0.005 AND slope6 > 0.005."""
    n = len(p['sl5'])
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if np.isnan(p['sl5'][i]) or np.isnan(p['sl6'][i]):
            t[i] = state; continue
        if state == 0:
            if p['sl5'][i] > 0.005 and p['sl6'][i] > 0.005:
                state = 1
        elif state == 1:
            if p['sl5'][i] <= 0 or p['sl6'][i] <= 0:
                state = 0
        t[i] = float(state)
    return t


def eth_c6(p, df):
    """ETH winner: triple slope > 0 AND close > EMA50; exit on any slope < -0.005 or close < EMA50."""
    n = len(p['sl5'])
    t = np.zeros(n)
    state = 0
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl4','sl5','sl6')) or np.isnan(p['ema50'][i]):
            t[i] = state; continue
        all_up = p['sl4'][i] > 0 and p['sl5'][i] > 0 and p['sl6'][i] > 0
        mom = p['price'][i] > p['ema50'][i]
        soft_down = p['sl4'][i] < -0.005 or p['sl5'][i] < -0.005 or p['sl6'][i] < -0.005
        if state == 0:
            if all_up and mom:
                state = 1
        elif state == 1:
            if soft_down or not mom:
                state = 0
        t[i] = float(state)
    return t


# ----------------- Run -----------------
def load(path):
    df = pd.read_csv(ROOT / path, parse_dates=['date']).set_index('date').sort_index()
    df = df[df['close'] > 0].dropna(subset=['close'])
    return df


def main():
    dfs = {a: load(p) for a, p in ASSETS.items()}

    # Buy & hold
    print("=" * 100)
    print("Buy & Hold")
    print("=" * 100)
    for a, df in dfs.items():
        bh_qty = INITIAL / df['close'].iloc[0]
        bh_eq = bh_qty * df['close']
        bh_ret = (bh_eq.iloc[-1] / INITIAL - 1) * 100
        bh_dd = ((bh_eq.cummax() - bh_eq) / bh_eq.cummax()).max() * 100
        print(f"  {a}: ret={bh_ret:+.0f}%  DD={bh_dd:.1f}%")

    # Run all ideas
    IDEAS = [
        ('W1 baseline (s5∧s6 up)',   idea_baseline_w1),
        ('I1 BTC winner (slope≥0.005)', btc_i1),
        ('1) s5 × s6 cross',          idea1_cross),
        ('2) spread widening',        idea2_widening),
        ('3a) price > s5 > s6',       idea3a_alignment),
        ('3b) bull regime s5>s6',     idea3b_bull_regime),
        ('4) s4 timing on regime',    idea4_s4_timing),
        ('5) hierarchical scale-out', idea5_hierarchical),
        ('6) accelerating uptrend',   idea6_acceleration),
        ('7a) pullback strict',       idea7a_pullback_strict),
        ('7b) pullback wide',         idea7b_pullback_wide),
        ('8a) bands breakout',        idea8a_band_breakout),
        ('8b) bands mean-revert',     idea8b_band_meanrevert),
    ]

    results = {}  # asset → list of (label, metrics)
    for a, df in dfs.items():
        p = precompute(df)
        rows = []
        for label, fn in IDEAS:
            target = fn(p)
            tr, eq, eqc = backtest(df, target)
            m = metrics(tr, eq, eqc)
            rows.append((label, m))
        # ETH C6 only meaningful for ETH but compute for both
        target = eth_c6(p, df)
        tr, eq, eqc = backtest(df, target)
        rows.append(('C6 ETH winner (triple+EMA+soft)', metrics(tr, eq, eqc)))
        results[a] = rows

    for a in dfs:
        print("\n" + "=" * 100)
        print(f"{a} — all wavelet ideas")
        print("=" * 100)
        print(f"  {'idea':40}  {'N':>4}  {'WR%':>5}  {'PF':>6}  {'avg%':>7}  {'ret%':>11}  {'DD%':>6}")
        print("  " + "-" * 88)
        for label, m in results[a]:
            pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
            print(f"  {label:40}  {m['N']:>4}  {m['WR']:>4.1f}  {pf_str}  "
                  f"{m['avg']:>+6.1f}  {m['ret']:>+10.0f}%  {m['DD']:>5.1f}")

    # Best by return/DD per asset
    print("\n" + "=" * 100)
    print("Best by return/DD per asset")
    print("=" * 100)
    print(f"  {'asset':6}  {'best idea':40}  {'ret':>11}  {'DD':>7}  {'ret/DD':>8}")
    for a in dfs:
        best = None; best_score = -1e9
        for label, m in results[a]:
            if m['DD'] <= 0 or m['N'] == 0: continue
            score = m['ret'] / m['DD']
            if score > best_score:
                best_score = score; best = (label, m)
        if best:
            l, m = best
            print(f"  {a:6}  {l:40}  {m['ret']:>+10.0f}%  {m['DD']:>6.1f}%  {m['ret']/m['DD']:>7.1f}")


if __name__ == "__main__":
    main()
