"""Hybrid variants of 8a Bands Breakout — testing improvements.

Baseline: 8a (price break above s5+1.5σ, exit at s5)

Hybrids:
  H1: 8a + EMA50 filter (only enter if close > EMA50)
  H2: 8a + slope6 regime filter (only enter if slope6 > 0)
  H3: 8a + ATR trail stop (instead of mean-revert exit)
  H4: 8a + EMA50 + slope6 + soft exit on slope6<0 (all-in hybrid)
  H5: 8a + EMA50 + ATR trail (breakout in uptrend, trail exit)

Tested on BTC and ETH.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
ASSETS = {'BTC': 'btc_usd_daily_coinbase.csv', 'ETH': 'eth_usd_daily_coinbase.csv'}
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def ema(series, n):
    s = series.values
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=series.index)
    out[n-1] = s[:n].mean()
    k = 2.0/(n+1.0)
    for i in range(n, len(s)):
        out[i] = s[i]*k + out[i-1]*(1-k)
    return pd.Series(out, index=series.index)


def atr(df, n=14):
    h, l, c = df['high'], df['low'], df['close']
    pc = c.shift(1)
    tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()


def atrous_haar(x, J=6):
    n = len(x); smooths = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = smooths[-1]; s = np.full(n, np.nan)
        for i in range(n):
            s[i] = prev[i] if i-gap < 0 else 0.5*(prev[i] + prev[i-gap])
        smooths.append(s)
    return smooths


def slope(arr, lookback=5):
    out = np.full(len(arr), np.nan)
    for i in range(lookback, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i-lookback]):
            out[i] = arr[i] - arr[i-lookback]
    return out


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    dev = log_c - sm[5]
    std50 = pd.Series(dev).rolling(50).std().values
    return {
        'log_c': log_c, 'price': df['close'].values,
        's5': sm[5], 's6': sm[6],
        'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
        'std50': std50,
        'ema50': ema(df['close'], 50).values,
        'atr14': atr(df, 14).values,
    }


# ===== Variants =====
def baseline_8a(p, k=1.5):
    """Price > s5+k*std → long; price < s5 → exit."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    for i in range(n):
        if np.isnan(p['std50'][i]):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0 and p['log_c'][i] > upper:
            state = 1
        elif state == 1 and p['log_c'][i] < p['s5'][i]:
            state = 0
        target[i] = float(state)
    return target


def h1_ema_filter(p, k=1.5):
    """8a + close > EMA50 at entry."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    for i in range(n):
        if np.isnan(p['std50'][i]) or np.isnan(p['ema50'][i]):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0 and p['log_c'][i] > upper and p['price'][i] > p['ema50'][i]:
            state = 1
        elif state == 1 and p['log_c'][i] < p['s5'][i]:
            state = 0
        target[i] = float(state)
    return target


def h2_regime_filter(p, k=1.5):
    """8a + slope6 > 0 at entry."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    for i in range(n):
        if np.isnan(p['std50'][i]) or np.isnan(p['sl6'][i]):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0 and p['log_c'][i] > upper and p['sl6'][i] > 0:
            state = 1
        elif state == 1 and p['log_c'][i] < p['s5'][i]:
            state = 0
        target[i] = float(state)
    return target


def h3_atr_trail(p, k=1.5, atr_mult=3.0):
    """8a entry, ATR trailing stop exit."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    trail_stop = 0.0
    highest_since_entry = 0.0
    for i in range(n):
        if np.isnan(p['std50'][i]) or np.isnan(p['atr14'][i]):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0 and p['log_c'][i] > upper:
            state = 1
            highest_since_entry = p['price'][i]
            trail_stop = p['price'][i] - atr_mult * p['atr14'][i]
        elif state == 1:
            if p['price'][i] > highest_since_entry:
                highest_since_entry = p['price'][i]
                trail_stop = max(trail_stop, p['price'][i] - atr_mult * p['atr14'][i])
            if p['price'][i] < trail_stop:
                state = 0
        target[i] = float(state)
    return target


def h4_all_in(p, k=1.5):
    """8a + EMA50 + slope6 + soft exit on slope6 < 0."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    for i in range(n):
        if any(np.isnan(p[key][i]) for key in ('std50','ema50','sl6')):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0:
            if p['log_c'][i] > upper and p['price'][i] > p['ema50'][i] and p['sl6'][i] > 0:
                state = 1
        elif state == 1:
            # exit if back to s5 OR slope6 turned negative OR price below EMA50
            if p['log_c'][i] < p['s5'][i] or p['sl6'][i] < 0 or p['price'][i] < p['ema50'][i]:
                state = 0
        target[i] = float(state)
    return target


def h5_ema_atr_trail(p, k=1.5, atr_mult=3.0):
    """8a + EMA50 filter + ATR trail."""
    n = len(p['s5']); target = np.zeros(n); state = 0
    trail_stop = 0.0
    highest = 0.0
    for i in range(n):
        if any(np.isnan(p[key][i]) for key in ('std50','ema50','atr14')):
            target[i] = state; continue
        upper = p['s5'][i] + k*p['std50'][i]
        if state == 0:
            if p['log_c'][i] > upper and p['price'][i] > p['ema50'][i]:
                state = 1
                highest = p['price'][i]
                trail_stop = p['price'][i] - atr_mult * p['atr14'][i]
        elif state == 1:
            if p['price'][i] > highest:
                highest = p['price'][i]
                trail_stop = max(trail_stop, p['price'][i] - atr_mult * p['atr14'][i])
            if p['price'][i] < trail_stop:
                state = 0
        target[i] = float(state)
    return target


def h6_ema_atr_trail_4x(p, k=1.5):
    """8a + EMA50 + ATR(4x) wider trail (let winners run more)."""
    return h5_ema_atr_trail(p, k, atr_mult=4.0)


def h7_ema_atr_trail_2x(p, k=1.5):
    """8a + EMA50 + ATR(2x) tighter trail."""
    return h5_ema_atr_trail(p, k, atr_mult=2.0)


# ===== Backtest =====
def backtest(df, target):
    closes = df['close'].values; dates = df.index
    cash = INITIAL; qty = 0.0
    eq_curve = np.zeros(len(closes))
    trades = []; entry_value = 0; entry_date = None
    for i in range(len(closes)):
        c = closes[i]; date = dates[i]
        eq_curve[i] = cash + qty*c
        tgt = target[i]
        if np.isnan(tgt): continue
        if tgt == 1 and qty == 0:
            eff = c * (1+SLIP); spend = cash * 0.99
            bq = spend / eff; cost = bq * eff * (1+FEE)
            cash -= cost; qty = bq
            entry_value = cost; entry_date = date
        elif tgt == 0 and qty > 0:
            eff = c * (1-SLIP); proceeds = qty * eff * (1-FEE)
            cash += proceeds
            pnl_pct = (proceeds/entry_value - 1) * 100
            trades.append({'entry_date': entry_date, 'exit_date': date,
                          'pnl_pct': pnl_pct, 'hold_days': (date - entry_date).days})
            qty = 0; entry_value = 0; entry_date = None
    if qty > 0:
        eff = closes[-1] * (1-SLIP); proceeds = qty * eff * (1-FEE)
        cash += proceeds
        pnl_pct = (proceeds/entry_value - 1) * 100
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl_pct, 'hold_days': (dates[-1] - entry_date).days})
    return pd.DataFrame(trades), cash, pd.Series(eq_curve, index=dates)


def metrics(trades, final_eq, eq):
    if len(trades) == 0:
        return {'N':0,'WR':0,'PF':0,'avg':0,'ret':(final_eq/INITIAL-1)*100,'DD':0,'maxL':0}
    wr = (trades['pnl_pct']>0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    ret = (final_eq/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    return {'N':len(trades), 'WR':wr, 'PF':pf, 'avg':trades['pnl_pct'].mean(),
            'ret':ret, 'DD':dd, 'maxL':trades['pnl_pct'].min()}


def load(path):
    df = pd.read_csv(ROOT/path, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def main():
    dfs = {a: load(p) for a,p in ASSETS.items()}
    VARIANTS = [
        ('8a baseline (price>s5+1.5σ; exit at s5)', baseline_8a),
        ('H1 + EMA50 entry filter',                  h1_ema_filter),
        ('H2 + slope6>0 regime filter',              h2_regime_filter),
        ('H3 + ATR(3x) trail stop',                  h3_atr_trail),
        ('H4 all-in (EMA+s6+softexit)',              h4_all_in),
        ('H5 EMA+ATR(3x) trail',                     h5_ema_atr_trail),
        ('H6 EMA+ATR(4x) wider trail',               h6_ema_atr_trail_4x),
        ('H7 EMA+ATR(2x) tighter trail',             h7_ema_atr_trail_2x),
    ]

    for a, df in dfs.items():
        p = precompute(df)
        print(f"\n{'='*110}")
        print(f"{a} — hybrid variants of 8a")
        print(f"{'='*110}")
        print(f"  {'variant':46}  {'N':>4}  {'WR%':>5}  {'PF':>6}  {'avg%':>7}  "
              f"{'ret%':>11}  {'DD%':>6}  {'maxL%':>7}")
        print("  " + "-"*94)
        bh = INITIAL / df['close'].iloc[0] * df['close']
        bh_ret = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
        print(f"  {'Buy & Hold':46}  {'  1':>4}  {'100.0':>5}  {' inf':>6}  "
              f"{bh_ret:>+6.0f}  {bh_ret:>+10.0f}%  {bh_dd:>5.1f}  {'0.0':>7}")
        for label, fn in VARIANTS:
            target = fn(p)
            tr, eq, eqc = backtest(df, target)
            m = metrics(tr, eq, eqc)
            pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
            print(f"  {label:46}  {m['N']:>4}  {m['WR']:>4.1f}  {pf_str}  "
                  f"{m['avg']:>+6.1f}  {m['ret']:>+10.0f}%  {m['DD']:>5.1f}  {m['maxL']:>+6.1f}")


if __name__ == "__main__":
    main()
