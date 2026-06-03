"""Replace EMAs with wavelets in user's original strategy.

Original logic (weekly):
  - Entry 1 (33%): crossunder(close, EMA200_w) — price below long EMA
  - Entry 2 (+67%): crossover(EMA20_w, EMA5_w) — EMA20 catches up to EMA5
  - Exit: crossunder(SMA(RSI,14), 70)

Wavelet replacement (also on weekly):
  Each EMA gets nearest wavelet smooth by time-scale.
  - Long EMA200 (~4 years) → s7 (~2.5y) or s8 (~5y)
  - Mid EMA20 (~5 months) → s4 (~4mo) or s5 (~8mo)
  - Short EMA5 (~5 weeks) → s2 (~4w) or s3 (~2mo)

Tests on BTC + ETH weekly:
  V0 = baseline EMA200/20/5 + RSI exit (user's strategy)
  V1 = s7/s4/s2 + RSI exit
  V2 = s6/s4/s2 + RSI exit (shorter long)
  V3 = s7/s5/s3 + RSI exit (slower mid/short)
  V4 = s8/s4/s2 + RSI exit (longest long if data allows)
  V5 = s7/s4/s2 + wavelet exit (slope5 < 0 instead of RSI)
  V6 = s7/s4/s2 + price < s5 exit
  V7 = s6/s4 + RSI exit, no pyramid (simpler)
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


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


def atrous_haar(x, J=8):
    """Causal à trous Haar — extended to 8 levels for longer scales."""
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = prev.copy()
        if len(x) > gap: s[gap:] = 0.5*(prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm

def slope_a(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb: out[lb:] = arr[lb:] - arr[:-lb]
    return out


def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def to_weekly(daily):
    return daily.resample('W-MON', label='left', closed='left').agg(
        {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def backtest_signals(daily, weekly_signals, pyramid=True):
    """Engine = daily. Signals are weekly bool series (entry/add/exit), forward-filled to daily."""
    closes = daily['close'].values
    dates = daily.index
    n = len(closes)

    # Forward-fill weekly bool signals to daily (with lag)
    sig_d = {}
    for name, s_weekly in weekly_signals.items():
        s = s_weekly.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)
        prev = s.shift(1).fillna(False)
        sig_d[name] = (s & (~prev)).values

    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    entry_value = 0.0; entry_date = None
    entries_taken = 0
    intra_min = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            low = daily['low'].values[i]
            if intra_min == 0 or low < intra_min: intra_min = low

        if qty > 0 and sig_d['exit'][i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/entry_value - 1) * 100
            mae = (intra_min/(entry_value/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': entry_date, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - entry_date).total_seconds()/86400})
            qty = 0; entry_value = 0; entry_date = None
            entries_taken = 0; intra_min = 0
            continue

        if qty == 0 and sig_d['entry1'][i]:
            eff = c * (1 + SLIP)
            spend = cash * (0.33 if pyramid else 0.99)
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty += bq
            entry_value = cost; entry_date = dates[i]
            entries_taken = 1
            intra_min = daily['low'].values[i]
            continue

        if pyramid and qty > 0 and entries_taken == 1 and sig_d['entry2'][i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                entry_value += cost
                entries_taken = 2

    if qty > 0:
        c = closes[-1]
        eff = c * (1 - SLIP); proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/entry_value - 1) * 100
        mae = (intra_min/(entry_value/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - entry_date).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


# === Strategy variants ===
def make_signals_baseline_ema(weekly):
    """Original: EMA200/20/5 + RSI exit."""
    wc = weekly['close']
    e200 = pd.Series(ema(wc.values, 200), index=wc.index)
    e20  = pd.Series(ema(wc.values, 20),  index=wc.index)
    e5   = pd.Series(ema(wc.values, 5),   index=wc.index)
    rsi  = pd.Series(rsi_a(wc.values, 14), index=wc.index)
    rsi_sma = rsi.rolling(14).mean()
    return {
        'entry1': (wc < e200) & (wc.shift(1) >= e200.shift(1)),
        'entry2': (e20 > e5) & (e20.shift(1) <= e5.shift(1)),
        'exit':   (rsi_sma < 70) & (rsi_sma.shift(1) >= 70),
    }

def make_signals_wavelet(weekly, long_lvl, mid_lvl, short_lvl, exit_rule='rsi'):
    """Wavelet replacement: configurable levels."""
    wc = weekly['close']
    log_wc = np.log(wc.values)
    sm = atrous_haar(log_wc, J=8)
    s_long  = pd.Series(sm[long_lvl],  index=wc.index)
    s_mid   = pd.Series(sm[mid_lvl],   index=wc.index)
    s_short = pd.Series(sm[short_lvl], index=wc.index)
    log_wc_s = pd.Series(log_wc, index=wc.index)

    # Entry 1: log(close) crosses below s_long (mimic crossunder(close, EMA200))
    entry1 = (log_wc_s < s_long) & (log_wc_s.shift(1) >= s_long.shift(1))
    # Entry 2: s_mid crosses above s_short (mimic crossover(EMA20, EMA5))
    entry2 = (s_mid > s_short) & (s_mid.shift(1) <= s_short.shift(1))

    if exit_rule == 'rsi':
        rsi  = pd.Series(rsi_a(wc.values, 14), index=wc.index)
        rsi_sma = rsi.rolling(14).mean()
        exit_ = (rsi_sma < 70) & (rsi_sma.shift(1) >= 70)
    elif exit_rule == 'slope_neg':
        # exit when slope of s_mid turns negative
        slope_m = slope_a(sm[mid_lvl], 5)
        slope_m_s = pd.Series(slope_m, index=wc.index)
        exit_ = (slope_m_s < 0) & (slope_m_s.shift(1) >= 0)
    elif exit_rule == 'price_below_mid':
        exit_ = (log_wc_s < s_mid) & (log_wc_s.shift(1) >= s_mid.shift(1))
    else:
        rsi  = pd.Series(rsi_a(wc.values, 14), index=wc.index)
        rsi_sma = rsi.rolling(14).mean()
        exit_ = (rsi_sma < 70) & (rsi_sma.shift(1) >= 70)

    return {'entry1': entry1, 'entry2': entry2, 'exit': exit_}


def metrics_period(trades, eq, period_start, period_end):
    eq_p = eq[(eq.index >= period_start) & (eq.index < period_end)]
    if len(eq_p) < 2:
        return {'ret': 0, 'dd': 0, 'N': 0, 'wr': 0, 'mae': 0}
    ret = (eq_p.iloc[-1]/eq_p.iloc[0] - 1) * 100
    dd = ((eq_p.cummax() - eq_p)/eq_p.cummax()).max() * 100
    tr_p = trades[(trades['exit_date'] >= period_start) & (trades['exit_date'] < period_end)] if len(trades) else pd.DataFrame()
    n = len(tr_p)
    wr = (tr_p['pnl_pct']>0).sum()/n*100 if n else 0
    mae = tr_p['mae_pct'].min() if n else 0
    return {'ret': ret, 'dd': dd, 'N': n, 'wr': wr, 'mae': mae}


def report(name, tr, fe, eq, dates, periods):
    if len(tr) == 0:
        print(f"  {name}: no trades")
        return
    total = (fe/INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr)*100
    mae = tr['mae_pct'].min()
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    pf_str = f"{pf:>5.2f}" if pf != float('inf') else "  inf"
    print(f"\n  {name}")
    print(f"    OVERALL: N={len(tr)}  WR={wr:.0f}%  PF={pf_str}  total={total:>+8.0f}%  DD={total_dd:>4.1f}%  worstMAE={mae:>+5.1f}%")
    print(f"    {'Период':>12}  {'ret':>9}  {'DD':>5}  {'N':>3}  {'WR':>4}  {'MAE':>6}")
    for label, ps, pe in periods:
        m = metrics_period(tr, eq, ps, pe)
        print(f"    {label:>12}  {m['ret']:>+8.0f}%  {m['dd']:>4.1f}%  {m['N']:>3}  {m['wr']:>3.0f}%  {m['mae']:>+5.1f}%")


def main():
    btc_daily = load_daily('BTC')
    eth_daily = load_daily('ETH')
    btc_weekly = to_weekly(btc_daily)
    eth_weekly = to_weekly(eth_daily)

    print(f"BTC weekly bars: {len(btc_weekly)} ({btc_weekly.index[0].date()} → {btc_weekly.index[-1].date()})")
    print(f"ETH weekly bars: {len(eth_weekly)} ({eth_weekly.index[0].date()} → {eth_weekly.index[-1].date()})")
    print(f"\nWavelet scales on weekly TF:")
    for j in range(1, 9):
        weeks = 2**j
        print(f"  s{j} = {weeks} weeks ≈ {weeks/52:.1f} years")

    periods = [
        ('2015-2017', pd.Timestamp(2015,1,1), pd.Timestamp(2018,1,1)),
        ('2018-2020', pd.Timestamp(2018,1,1), pd.Timestamp(2021,1,1)),
        ('2021-2023', pd.Timestamp(2021,1,1), pd.Timestamp(2024,1,1)),
        ('2024-2026', pd.Timestamp(2024,1,1), pd.Timestamp(2027,1,1)),
    ]

    # === Variants
    variants = [
        ('V0: BASELINE EMA200/20/5 + RSI exit', 'ema',  None, None, None, 'rsi', True),
        ('V1: s7 (2.5y) / s4 / s2 + RSI exit',  'wav',  7, 4, 2, 'rsi', True),
        ('V2: s6 (1.3y) / s4 / s2 + RSI exit',  'wav',  6, 4, 2, 'rsi', True),
        ('V3: s7 / s5 / s3 + RSI exit',         'wav',  7, 5, 3, 'rsi', True),
        ('V4: s8 (5y) / s4 / s2 + RSI exit',    'wav',  8, 4, 2, 'rsi', True),
        ('V5: s7/s4/s2 + slope-neg exit',       'wav',  7, 4, 2, 'slope_neg', True),
        ('V6: s7/s4/s2 + price<s_mid exit',     'wav',  7, 4, 2, 'price_below_mid', True),
        ('V7: s6/s4 no pyramid (all-in)',       'wav',  6, 4, 2, 'rsi', False),
        ('V8: s5 (8mo) / s3 / s1 + RSI exit',   'wav',  5, 3, 1, 'rsi', True),
    ]

    for asset, daily, weekly in [('BTC', btc_daily, btc_weekly),
                                  ('ETH', eth_daily, eth_weekly)]:
        print(f"\n\n{'='*120}")
        print(f"{asset} weekly logic — wavelet replacement variants")
        print(f"{'='*120}")
        for label, kind, l, m, sh, exit_r, pyr in variants:
            if kind == 'ema':
                sigs = make_signals_baseline_ema(weekly)
            else:
                sigs = make_signals_wavelet(weekly, l, m, sh, exit_rule=exit_r)
            tr, fe, eq = backtest_signals(daily, sigs, pyramid=pyr)
            report(label, tr, fe, eq, daily.index, periods)


if __name__ == "__main__":
    main()
