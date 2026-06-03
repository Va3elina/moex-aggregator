"""Fix ETH 4H champion's 2026 problem — test ~25 variants targeting chop avoidance.

Current ETH champion: 8a (k=1.5) + EMA100, 4H → 2026 = -22.9% over 11 trades.
Problem: keeps buying bounces in a downtrend.

Variants tested:
  A) Slower / different trend filters (EMA200, SMA, multi-EMA)
  B) Wavelet regime filters (slope6, s5>s6)
  C) Daily-TF confirmation filter (multi-timeframe)
  D) Cooldown after exit
  E) Tighter entry criteria (higher k, longer std window)
  F) Move to daily TF
  G) ATR trail exit
  H) Combinations

For each variant report:
  - Overall: total return, total DD, MAE worst
  - 2026 return (the key fix target)
  - Recent 3y return
  - Worst calendar year
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0
KEY_YEAR = 2026


def ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean()
    k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def sma(arr, n):
    return pd.Series(arr).rolling(n).mean().values

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

def slope_a(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb: out[lb:] = arr[lb:] - arr[:-lb]
    return out


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'price': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        'index': df.index,
        's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl4': slope_a(sm[4], 5), 'sl5': slope_a(sm[5], 5), 'sl6': slope_a(sm[6], 5),
        'std30': pd.Series(log_c - sm[5]).rolling(30).std().values,
        'std50': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'std100': pd.Series(log_c - sm[5]).rolling(100).std().values,
        'ema50': ema(df['close'].values, 50),
        'ema100': ema(df['close'].values, 100),
        'ema200': ema(df['close'].values, 200),
        'sma100': sma(df['close'].values, 100),
        'sma200': sma(df['close'].values, 200),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
    }


# Add daily-TF wavelet info to 4H precompute for multi-TF variants
def add_daily_context(p_4h, df_4h, df_daily):
    """Add daily slope6 and EMA values aligned to 4H bars (forward-filled)."""
    log_c_d = np.log(df_daily['close'].values)
    sm_d = atrous_haar(log_c_d, J=6)
    s6_d = pd.Series(sm_d[6], index=df_daily.index)
    sl6_d = pd.Series(slope_a(sm_d[6], 5), index=df_daily.index)
    ema50_d = pd.Series(ema(df_daily['close'].values, 50), index=df_daily.index)
    # Align to 4H bars: each 4H bar gets last completed daily value
    sl6_d_4h = sl6_d.shift(1).reindex(df_4h.index, method='ffill').values
    ema50_d_4h = ema50_d.shift(1).reindex(df_4h.index, method='ffill').values
    close_d_4h = df_daily['close'].shift(1).reindex(df_4h.index, method='ffill').values
    p_4h['daily_sl6'] = sl6_d_4h
    p_4h['daily_ema50'] = ema50_d_4h
    p_4h['daily_close'] = close_d_4h


def state_machine(entry_cond, exit_cond):
    n = len(entry_cond); state = np.zeros(n); s = 0
    for i in range(n):
        if s == 1:
            if exit_cond[i]: s = 0
        else:
            if entry_cond[i]: s = 1
        state[i] = float(s)
    return state


def state_machine_cooldown(entry_cond, exit_cond, cooldown_bars):
    n = len(entry_cond); state = np.zeros(n); s = 0
    cooldown_remaining = 0
    for i in range(n):
        if s == 1:
            if exit_cond[i]:
                s = 0
                cooldown_remaining = cooldown_bars
        else:
            if cooldown_remaining > 0:
                cooldown_remaining -= 1
            elif entry_cond[i]:
                s = 1
        state[i] = float(s)
    return state


def state_machine_atr_trail(entry_cond, exit_cond_aux, price, atr14, atr_mult):
    """Long on entry_cond, exit on ATR trail OR exit_cond_aux (whichever first)."""
    n = len(price); state = np.zeros(n); s = 0
    trail = 0.0; high = 0.0
    for i in range(n):
        if s == 1:
            if not np.isnan(atr14[i]):
                if price[i] > high: high = price[i]
                cand = high - atr_mult * atr14[i]
                if cand > trail: trail = cand
                if price[i] < trail or (exit_cond_aux is not None and exit_cond_aux[i]):
                    s = 0
        else:
            if entry_cond[i]:
                s = 1; high = price[i]
                trail = price[i] - atr_mult * atr14[i] if not np.isnan(atr14[i]) else 0
        state[i] = float(s)
    return state


# ============ Variant generators ============
def make_8a_signals(p, k, std_key):
    std = p[std_key]
    upper = p['s5'] + k*std
    entry = p['log_c'] > upper
    exit_to_mean = p['log_c'] < p['s5']
    mask_std = np.isnan(std)
    return entry, exit_to_mean, mask_std

# Baseline: current champion
def v_baseline(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

# A) Trend filter variants
def v_ema200(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema200'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema200'])
    mask = m | np.isnan(p['ema200'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_ema50(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema50'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema50'])
    mask = m | np.isnan(p['ema50'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_sma200(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['sma200'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['sma200'])
    mask = m | np.isnan(p['sma200'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_ema50_ema200(p):
    """Entry needs both close > EMA50 AND close > EMA200."""
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema50']) & (p['price'] > p['ema200'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema50']) | (p['price'] < p['ema200'])
    mask = m | np.isnan(p['ema50']) | np.isnan(p['ema200'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

# B) Wavelet regime
def v_slope6(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['sl6'] > 0)
    entry = entry & cond
    exit_ = exit_ | (p['sl6'] < 0)
    mask = m | np.isnan(p['sl6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_slope5_slope6(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['sl5'] > 0) & (p['sl6'] > 0)
    entry = entry & cond
    exit_ = exit_ | (p['sl5'] < 0) | (p['sl6'] < 0)
    mask = m | np.isnan(p['sl5']) | np.isnan(p['sl6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_s5_gt_s6(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['s5'] > p['s6'])
    entry = entry & cond
    exit_ = exit_ | (p['s5'] < p['s6'])
    mask = m | np.isnan(p['s5']) | np.isnan(p['s6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

# C) Daily-TF filter
def v_daily_slope6(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['daily_sl6'] > 0)
    entry = entry & cond
    exit_ = exit_ | (p['daily_sl6'] < 0)
    mask = m | np.isnan(p['daily_sl6'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_daily_ema50(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['daily_close'] > p['daily_ema50'])
    entry = entry & cond
    exit_ = exit_ | (p['daily_close'] < p['daily_ema50'])
    mask = m | np.isnan(p['daily_close']) | np.isnan(p['daily_ema50'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

# D) Cooldown variants
def v_cooldown_6(p):
    """24h cooldown (6 bars × 4H)."""
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine_cooldown(np.where(mask, False, entry), np.where(mask, False, exit_), 6)

def v_cooldown_12(p):
    """48h cooldown."""
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine_cooldown(np.where(mask, False, entry), np.where(mask, False, exit_), 12)

def v_cooldown_24(p):
    """96h cooldown."""
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine_cooldown(np.where(mask, False, entry), np.where(mask, False, exit_), 24)

# E) Tighter entry
def v_k2(p):
    entry, exit_, m = make_8a_signals(p, 2.0, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_k25(p):
    entry, exit_, m = make_8a_signals(p, 2.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_std100(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std100')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['ema100'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

# G) ATR trail exit
def v_atr3(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    mask = m | np.isnan(p['ema100'])
    return state_machine_atr_trail(np.where(mask, False, entry), None, p['price'], p['atr14'], 3.0)

def v_atr2(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['price'] > p['ema100'])
    entry = entry & cond
    mask = m | np.isnan(p['ema100'])
    return state_machine_atr_trail(np.where(mask, False, entry), None, p['price'], p['atr14'], 2.0)

# H) Combinations
def v_slope6_ema200(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['sl6'] > 0) & (p['price'] > p['ema200'])
    entry = entry & cond
    exit_ = exit_ | (p['sl6'] < 0) | (p['price'] < p['ema200'])
    mask = m | np.isnan(p['sl6']) | np.isnan(p['ema200'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))

def v_slope6_cooldown(p):
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['sl6'] > 0) & (p['price'] > p['ema100'])
    entry = entry & cond
    exit_ = exit_ | (p['sl6'] < 0) | (p['price'] < p['ema100'])
    mask = m | np.isnan(p['sl6']) | np.isnan(p['ema100'])
    return state_machine_cooldown(np.where(mask, False, entry), np.where(mask, False, exit_), 12)

def v_all_filters(p):
    """slope6 + EMA200 + cooldown 12 bars + k=2.0."""
    entry, exit_, m = make_8a_signals(p, 2.0, 'std50')
    cond = (p['sl6'] > 0) & (p['price'] > p['ema200'])
    entry = entry & cond
    exit_ = exit_ | (p['sl6'] < 0) | (p['price'] < p['ema200'])
    mask = m | np.isnan(p['sl6']) | np.isnan(p['ema200'])
    return state_machine_cooldown(np.where(mask, False, entry), np.where(mask, False, exit_), 12)

def v_daily_slope6_ema200(p):
    """Daily slope6 + 4H EMA200."""
    entry, exit_, m = make_8a_signals(p, 1.5, 'std50')
    cond = (p['daily_sl6'] > 0) & (p['price'] > p['ema200'])
    entry = entry & cond
    exit_ = exit_ | (p['daily_sl6'] < 0) | (p['price'] < p['ema200'])
    mask = m | np.isnan(p['daily_sl6']) | np.isnan(p['ema200'])
    return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))


# ============ Backtest ============
def backtest(p, target):
    closes = p['price']; highs = p['high']; lows = p['low']; dates = p['index']
    n = len(closes); cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0; intra_max = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            if lows[i] < intra_min: intra_min = lows[i]
            if highs[i] > intra_max: intra_max = highs[i]
        tgt = target[i]
        if tgt == 1.0 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = dates[i]
            intra_min = lows[i]; intra_max = highs[i]
        elif tgt == 0.0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100
            hold = (dates[i] - ed).total_seconds()/86400
            trades.append({'entry_date': ed, 'exit_date': dates[i], 'pnl_pct': pnl, 'mae_pct': mae, 'hold_d': hold})
            qty = 0; ev = 0; ed = None
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100
        hold = (dates[-1] - ed).total_seconds()/86400
        trades.append({'entry_date': ed, 'exit_date': dates[-1], 'pnl_pct': pnl, 'mae_pct': mae, 'hold_d': hold})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(trades, final_eq, eq, dates):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'total_ret': (final_eq/INITIAL-1)*100,
                'total_dd': 0, 'mae_worst': 0, 'year_2026_ret': 0, 'recent_3y_ret': 0, 'worst_year': 0}
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae_worst = trades['mae_pct'].min()
    # 2026 specific
    eq_2026 = eq[eq.index >= pd.Timestamp(KEY_YEAR, 1, 1)]
    year_2026_ret = (eq_2026.iloc[-1]/eq_2026.iloc[0] - 1) * 100 if len(eq_2026) > 1 else 0
    n_2026 = len(trades[trades['exit_date'].dt.year == KEY_YEAR])
    # recent 3y
    recent_start = pd.Timestamp("2023-05-11")
    eq_recent = eq[eq.index >= recent_start]
    recent_3y_ret = (eq_recent.iloc[-1]/eq_recent.iloc[0] - 1) * 100 if len(eq_recent) > 1 else 0
    # worst year
    worst_year = 0
    for year in dates.year.unique():
        ym = (dates.year == year)
        sub = eq[ym]
        if len(sub) < 2: continue
        ret = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
        if ret < worst_year: worst_year = ret
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'mae_worst': mae_worst,
        'year_2026_ret': year_2026_ret,
        'n_2026': n_2026,
        'recent_3y_ret': recent_3y_ret,
        'worst_year': worst_year,
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


VARIANTS = [
    ('BASELINE (8a k=1.5 + EMA100)',     v_baseline,           False),
    # A) Trend filters
    ('A1: + EMA200 (instead of EMA100)', v_ema200,             False),
    ('A2: + EMA50',                      v_ema50,              False),
    ('A3: + SMA200',                     v_sma200,             False),
    ('A4: + EMA50 AND EMA200',           v_ema50_ema200,       False),
    # B) Wavelet regime
    ('B1: + slope6 > 0',                 v_slope6,             False),
    ('B2: + slope5 AND slope6 > 0',      v_slope5_slope6,      False),
    ('B3: + s5 > s6',                    v_s5_gt_s6,           False),
    # C) Daily-TF filter
    ('C1: + daily slope6 > 0',           v_daily_slope6,       True),
    ('C2: + daily close > daily EMA50',  v_daily_ema50,        True),
    # D) Cooldowns
    ('D1: + cooldown 6 bars (24h)',      v_cooldown_6,         False),
    ('D2: + cooldown 12 bars (48h)',     v_cooldown_12,        False),
    ('D3: + cooldown 24 bars (96h)',     v_cooldown_24,        False),
    # E) Tighter
    ('E1: k=2.0 (instead of 1.5)',       v_k2,                 False),
    ('E2: k=2.5',                        v_k25,                False),
    ('E3: std_lookback=100',             v_std100,             False),
    # G) ATR trail
    ('G1: ATR(3x) trail exit',           v_atr3,               False),
    ('G2: ATR(2x) trail exit',           v_atr2,               False),
    # H) Combinations
    ('H1: slope6 + EMA200',              v_slope6_ema200,      False),
    ('H2: slope6 + cooldown 48h',        v_slope6_cooldown,    False),
    ('H3: all (slope6+EMA200+cool+k2)',  v_all_filters,        False),
    ('H4: daily_slope6 + 4H EMA200',     v_daily_slope6_ema200,True),
]


def main():
    eth_hourly = load_hourly('ETH')
    eth_4h = resample_4h(eth_hourly)
    eth_daily = load_daily('ETH')

    p = precompute(eth_4h); p['index'] = eth_4h.index
    add_daily_context(p, eth_4h, eth_daily)
    dates = eth_4h.index

    # B&H benchmark
    bh = INITIAL / eth_4h['close'].iloc[0] * eth_4h['close']
    bh_2026 = bh[bh.index >= pd.Timestamp(KEY_YEAR, 1, 1)]
    bh_2026_ret = (bh_2026.iloc[-1]/bh_2026.iloc[0] - 1) * 100 if len(bh_2026) > 1 else 0
    print(f"  ETH 4H Buy & Hold 2026: {bh_2026_ret:+.1f}%\n")

    print("="*150)
    print("ETH 4H champion fixes — 2026 return target")
    print("="*150)
    print(f"  {'variant':38}  {'N':>4}  {'WR%':>5}  {'PF':>6}  {'total%':>9}  {'DD%':>5}  {'mMAE%':>6}  {'2026%':>7}  {'N26':>3}  {'recent3y%':>10}  {'worstY%':>8}")
    print("  " + "-" * 140)

    results = []
    for label, fn, needs_daily in VARIANTS:
        target = fn(p)
        tr, fe, eq = backtest(p, target)
        m = metrics(tr, fe, eq, dates)
        results.append({'label': label, **m})
        pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
        n_str = f"{m['N']:>4}" if isinstance(m['N'], int) else f"{int(m['N']):>4}"
        print(f"  {label:38}  {n_str}  {m['WR']:>4.1f}  {pf_str}  "
              f"{m['total_ret']:>+8.0f}%  {m['total_dd']:>4.1f}%  "
              f"{m['mae_worst']:>+5.1f}%  {m['year_2026_ret']:>+6.1f}%  {m['n_2026']:>3}  "
              f"{m['recent_3y_ret']:>+9.0f}%  {m['worst_year']:>+7.0f}%")

    # Top fixes for 2026
    print("\n" + "="*150)
    print("TOP 5 fixes for 2026 (positive 2026 return + maintains historical edge)")
    print("="*150)
    sorted_r = sorted(results, key=lambda r: (r['year_2026_ret'] + r['total_ret']/1000), reverse=True)[:5]
    for r in sorted_r:
        print(f"  {r['label']:42}  2026={r['year_2026_ret']:>+6.1f}%  total={r['total_ret']:>+7.0f}%  DD={r['total_dd']:>4.1f}%  recent3y={r['recent_3y_ret']:>+5.0f}%  worstY={r['worst_year']:>+5.0f}%")

    # Best risk-adjusted (recent_3y - DD - max(0, -worst_year))
    print("\n" + "="*150)
    print("TOP 5 by recent risk-adjusted (recent3y - DD)")
    print("="*150)
    sorted_r = sorted(results, key=lambda r: r['recent_3y_ret'] - r['total_dd'], reverse=True)[:5]
    for r in sorted_r:
        print(f"  {r['label']:42}  2026={r['year_2026_ret']:>+6.1f}%  total={r['total_ret']:>+7.0f}%  DD={r['total_dd']:>4.1f}%  recent3y={r['recent_3y_ret']:>+5.0f}%  worstY={r['worst_year']:>+5.0f}%")


if __name__ == "__main__":
    main()
