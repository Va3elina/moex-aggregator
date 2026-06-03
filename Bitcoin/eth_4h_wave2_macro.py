"""Wave 2: combinations of best filters + RSI exit + BTC macro regime filter.

After Wave 1 found EMA200 and s5>s6 as best single filters, Wave 2 tests:
  - BTC daily EMA200 macro filter (KEY discovery — blocks ALL 11 bad 2026 trades)
  - RSI-based entry filter (RSI < 70 to avoid overbought)
  - RSI-based exit (exit when RSI < 40, regardless of price)
  - Combinations of macro + local filters
  - "Stay out" when conditions are clearly bad

Reports: total return, DD, MAE worst, 2026 return, recent 3y, worst year, # trades in 2026.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


def ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean()
    k = 2.0/(n+1.0)
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

def rsi_arr(close, n=14):
    s = pd.Series(close)
    delta = s.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/n, adjust=False).mean()
    return (100 - 100/(1 + avg_g/avg_l)).values

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
        'sl5': slope_a(sm[5], 5), 'sl6': slope_a(sm[6], 5),
        'std30': pd.Series(log_c - sm[5]).rolling(30).std().values,
        'std50': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'std100': pd.Series(log_c - sm[5]).rolling(100).std().values,
        'ema100': ema(df['close'].values, 100),
        'ema200': ema(df['close'].values, 200),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
        'rsi14': rsi_arr(df['close'].values, 14),
    }


def add_btc_macro(p, df_target, df_btc_daily):
    """Add BTC daily macro filter: BTC > EMA200 daily, slope6 daily > 0."""
    btc_close = df_btc_daily['close']
    btc_ema200 = pd.Series(ema(btc_close.values, 200), index=df_btc_daily.index)
    log_c_btc = np.log(btc_close.values)
    sm_btc = atrous_haar(log_c_btc, J=6)
    btc_sl6 = pd.Series(slope_a(sm_btc[6], 5), index=df_btc_daily.index)
    btc_s5 = pd.Series(sm_btc[5], index=df_btc_daily.index)
    btc_s6 = pd.Series(sm_btc[6], index=df_btc_daily.index)
    # Forward-fill into target's index (with 1-bar lag to avoid look-ahead)
    btc_close_shift = btc_close.shift(1)
    btc_ema200_shift = btc_ema200.shift(1)
    btc_sl6_shift = btc_sl6.shift(1)
    btc_s5_shift = btc_s5.shift(1)
    btc_s6_shift = btc_s6.shift(1)
    p['btc_close'] = btc_close_shift.reindex(df_target.index, method='ffill').values
    p['btc_ema200'] = btc_ema200_shift.reindex(df_target.index, method='ffill').values
    p['btc_sl6'] = btc_sl6_shift.reindex(df_target.index, method='ffill').values
    p['btc_s5'] = btc_s5_shift.reindex(df_target.index, method='ffill').values
    p['btc_s6'] = btc_s6_shift.reindex(df_target.index, method='ffill').values


def state_machine(entry_cond, exit_cond):
    n = len(entry_cond); state = np.zeros(n); s = 0
    for i in range(n):
        if s == 1:
            if exit_cond[i]: s = 0
        else:
            if entry_cond[i]: s = 1
        state[i] = float(s)
    return state


# ============ Variants ============
def make_8a(p, k=1.5, std_key='std50'):
    std = p[std_key]
    upper = p['s5'] + k*std
    entry = p['log_c'] > upper
    exit_to_mean = p['log_c'] < p['s5']
    mask_std = np.isnan(std)
    return entry, exit_to_mean, mask_std


# Baseline (Wave 1 best simple)
def v_baseline_ema100(p):
    e, x, m = make_8a(p)
    cond = (p['price'] > p['ema100'])
    return state_machine(np.where(m | np.isnan(p['ema100']), False, e & cond),
                          np.where(m | np.isnan(p['ema100']), False, x | (p['price'] < p['ema100'])))

# Wave 1 winners
def v_ema200(p):
    e, x, m = make_8a(p)
    cond = (p['price'] > p['ema200'])
    return state_machine(np.where(m | np.isnan(p['ema200']), False, e & cond),
                          np.where(m | np.isnan(p['ema200']), False, x | (p['price'] < p['ema200'])))

def v_s5_gt_s6(p):
    e, x, m = make_8a(p)
    cond = (p['s5'] > p['s6'])
    return state_machine(np.where(m, False, e & cond),
                          np.where(m, False, x | (p['s5'] < p['s6'])))

# Wave 2: macro BTC filter
def v_macro_only(p):
    """8a + EMA100 ETH + BTC daily > EMA200."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    mask = m | np.isnan(p['ema100']) | np.isnan(p['btc_ema200'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | (p['btc_close'] < p['btc_ema200'])))

def v_macro_ema200(p):
    """8a + EMA200 ETH + BTC daily > EMA200."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema200'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    mask = m | np.isnan(p['ema200']) | np.isnan(p['btc_ema200'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema200']) | (p['btc_close'] < p['btc_ema200'])))

def v_macro_s5_s6(p):
    """8a + s5>s6 + BTC > EMA200."""
    e, x, m = make_8a(p)
    eth_ok = (p['s5'] > p['s6'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    mask = m | np.isnan(p['btc_ema200']) | np.isnan(p['s5']) | np.isnan(p['s6'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['s5'] < p['s6']) | (p['btc_close'] < p['btc_ema200'])))

def v_btc_sl6_only(p):
    """8a + EMA100 + BTC daily slope6 > 0."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    btc_ok = (p['btc_sl6'] > 0)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['btc_sl6'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | (p['btc_sl6'] < 0)))

def v_macro_btc_s5s6(p):
    """8a + EMA100 + BTC s5>s6 daily."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    btc_ok = (p['btc_s5'] > p['btc_s6'])
    mask = m | np.isnan(p['ema100']) | np.isnan(p['btc_s5']) | np.isnan(p['btc_s6'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | (p['btc_s5'] < p['btc_s6'])))

# Wave 2: RSI variants
def v_rsi_entry_70(p):
    """Baseline + RSI<70 at entry (no overbought)."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    rsi_ok = (p['rsi14'] < 70)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok & rsi_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100'])))

def v_rsi_exit_40(p):
    """Baseline + exit when RSI < 40."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    rsi_exit = (p['rsi14'] < 40)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | rsi_exit))

def v_rsi_exit_45(p):
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    rsi_exit = (p['rsi14'] < 45)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | rsi_exit))

def v_rsi_exit_50(p):
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    rsi_exit = (p['rsi14'] < 50)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | rsi_exit))

# Best combinations
def v_macro_rsi_exit(p):
    """BTC>EMA200 + EMA100 + RSI<45 exit."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema100'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    rsi_exit = (p['rsi14'] < 45)
    mask = m | np.isnan(p['ema100']) | np.isnan(p['btc_ema200']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema100']) | (p['btc_close'] < p['btc_ema200']) | rsi_exit))

def v_macro_s5s6_rsi(p):
    """BTC>EMA200 + s5>s6 + RSI<45 exit."""
    e, x, m = make_8a(p)
    eth_ok = (p['s5'] > p['s6'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    rsi_exit = (p['rsi14'] < 45)
    mask = m | np.isnan(p['btc_ema200']) | np.isnan(p['rsi14']) | np.isnan(p['s5'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['s5'] < p['s6']) | (p['btc_close'] < p['btc_ema200']) | rsi_exit))

def v_macro_ema200_rsi(p):
    """BTC>EMA200 + ETH>EMA200 + RSI<45 exit."""
    e, x, m = make_8a(p)
    eth_ok = (p['price'] > p['ema200'])
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    rsi_exit = (p['rsi14'] < 45)
    mask = m | np.isnan(p['ema200']) | np.isnan(p['btc_ema200']) | np.isnan(p['rsi14'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema200']) | (p['btc_close'] < p['btc_ema200']) | rsi_exit))

def v_kitchen_sink(p):
    """ALL filters: BTC>EMA200 + ETH>EMA200 + s5>s6 + slope6>0 + k=2 + RSI<45 exit."""
    e, x, m = make_8a(p, k=2.0)
    eth_ok = (p['price'] > p['ema200']) & (p['s5'] > p['s6']) & (p['sl6'] > 0)
    btc_ok = (p['btc_close'] > p['btc_ema200'])
    rsi_exit = (p['rsi14'] < 45)
    mask = m | np.isnan(p['ema200']) | np.isnan(p['btc_ema200']) | np.isnan(p['rsi14']) | np.isnan(p['sl6']) | np.isnan(p['s5'])
    return state_machine(np.where(mask, False, e & eth_ok & btc_ok),
                          np.where(mask, False, x | (p['price'] < p['ema200']) | (p['btc_close'] < p['btc_ema200']) | rsi_exit | (p['s5'] < p['s6']) | (p['sl6'] < 0)))


# ============ Backtest ============
def backtest(p, target):
    closes = p['price']; highs = p['high']; lows = p['low']; dates = p['index']
    n = len(closes); cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None; intra_min = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0 and lows[i] < intra_min: intra_min = lows[i]
        tgt = target[i]
        if tgt == 1.0 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = dates[i]; intra_min = lows[i]
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
        return {'N': 0, 'WR': 0, 'PF': 0, 'total_ret': 0,
                'total_dd': 0, 'mae_worst': 0, 'year_2026_ret': 0, 'n_2026': 0,
                'recent_3y_ret': 0, 'worst_year': 0, 'best_year': 0}
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae_worst = trades['mae_pct'].min()
    eq_2026 = eq[eq.index >= pd.Timestamp(2026, 1, 1)]
    year_2026_ret = (eq_2026.iloc[-1]/eq_2026.iloc[0] - 1) * 100 if len(eq_2026) > 1 else 0
    n_2026 = len(trades[trades['exit_date'].dt.year == 2026])
    recent_start = pd.Timestamp("2023-05-11")
    eq_recent = eq[eq.index >= recent_start]
    recent_3y_ret = (eq_recent.iloc[-1]/eq_recent.iloc[0] - 1) * 100 if len(eq_recent) > 1 else 0
    worst_year = 0; best_year = 0
    for year in dates.year.unique():
        ym = (dates.year == year)
        sub = eq[ym]
        if len(sub) < 2: continue
        ret = (sub.iloc[-1]/sub.iloc[0] - 1) * 100
        if ret < worst_year: worst_year = ret
        if ret > best_year: best_year = ret
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'mae_worst': mae_worst,
        'year_2026_ret': year_2026_ret,
        'n_2026': n_2026,
        'recent_3y_ret': recent_3y_ret,
        'worst_year': worst_year,
        'best_year': best_year,
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
    ('BASELINE (8a+EMA100, no fix)',          v_baseline_ema100),
    # Wave 1 winners reminder
    ('W1: + EMA200 (Wave 1 winner)',          v_ema200),
    ('W1: + s5>s6 (Wave 1 winner)',           v_s5_gt_s6),
    # New: BTC macro filter
    ('M1: + BTC daily > EMA200',              v_macro_only),
    ('M2: ETH>EMA200 + BTC>EMA200',           v_macro_ema200),
    ('M3: s5>s6 + BTC>EMA200',                v_macro_s5_s6),
    ('M4: + BTC daily slope6 > 0',            v_btc_sl6_only),
    ('M5: + BTC daily s5>s6',                 v_macro_btc_s5s6),
    # RSI variants
    ('R1: + RSI<70 at entry',                 v_rsi_entry_70),
    ('R2: + RSI<40 exit',                     v_rsi_exit_40),
    ('R3: + RSI<45 exit',                     v_rsi_exit_45),
    ('R4: + RSI<50 exit',                     v_rsi_exit_50),
    # Combos
    ('C1: macro+RSI<45 exit',                 v_macro_rsi_exit),
    ('C2: macro+s5>s6+RSI<45 exit',           v_macro_s5s6_rsi),
    ('C3: macro+ETH EMA200+RSI<45 exit',      v_macro_ema200_rsi),
    ('C4: KITCHEN SINK (all filters)',        v_kitchen_sink),
]


def main():
    eth_4h = resample_4h(load_hourly('ETH'))
    btc_daily = load_daily('BTC')

    p = precompute(eth_4h); p['index'] = eth_4h.index
    add_btc_macro(p, eth_4h, btc_daily)
    dates = eth_4h.index

    bh = INITIAL / eth_4h['close'].iloc[0] * eth_4h['close']
    bh_2026 = bh[bh.index >= pd.Timestamp(2026, 1, 1)]
    bh_2026_ret = (bh_2026.iloc[-1]/bh_2026.iloc[0] - 1) * 100 if len(bh_2026) > 1 else 0
    bh_total = (bh.iloc[-1]/INITIAL - 1) * 100

    print(f"Buy & Hold: total={bh_total:+.0f}%, 2026={bh_2026_ret:+.1f}%\n")

    print("="*160)
    print("ETH 4H Wave 2 — combinations + RSI + BTC macro filter")
    print("="*160)
    print(f"  {'variant':38}  {'N':>4}  {'WR%':>5}  {'PF':>6}  {'total%':>9}  {'DD%':>5}  {'mMAE%':>6}  {'2026%':>7}  {'N26':>3}  {'recent3y%':>10}  {'worstY%':>8}  {'bestY%':>8}")
    print("  " + "-" * 150)

    results = []
    for label, fn in VARIANTS:
        target = fn(p)
        tr, fe, eq = backtest(p, target)
        m = metrics(tr, fe, eq, dates)
        results.append({'label': label, **m})
        pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
        print(f"  {label:38}  {m['N']:>4}  {m['WR']:>4.1f}  {pf_str}  "
              f"{m['total_ret']:>+8.0f}%  {m['total_dd']:>4.1f}%  "
              f"{m['mae_worst']:>+5.1f}%  {m['year_2026_ret']:>+6.1f}%  {m['n_2026']:>3}  "
              f"{m['recent_3y_ret']:>+9.0f}%  {m['worst_year']:>+7.0f}%  {m['best_year']:>+7.0f}%")

    print("\n" + "="*160)
    print("RANKED BY 2026 RETURN")
    print("="*160)
    sorted_by_2026 = sorted(results, key=lambda r: r['year_2026_ret'], reverse=True)
    for r in sorted_by_2026[:8]:
        print(f"  {r['label']:42}  2026={r['year_2026_ret']:>+6.1f}% (N={r['n_2026']:>2})  total={r['total_ret']:>+7.0f}%  DD={r['total_dd']:>4.1f}%  worstY={r['worst_year']:>+5.0f}%  recent3y={r['recent_3y_ret']:>+5.0f}%")

    print("\n" + "="*160)
    print("RANKED BY RECENT 3Y RETURN")
    print("="*160)
    sorted_by_recent = sorted(results, key=lambda r: r['recent_3y_ret'], reverse=True)
    for r in sorted_by_recent[:8]:
        print(f"  {r['label']:42}  recent3y={r['recent_3y_ret']:>+5.0f}%  total={r['total_ret']:>+7.0f}%  DD={r['total_dd']:>4.1f}%  2026={r['year_2026_ret']:>+6.1f}%  worstY={r['worst_year']:>+5.0f}%")

    print("\n" + "="*160)
    print("RANKED BY 'NO BAD YEAR' (highest worst_year)")
    print("="*160)
    sorted_by_worst = sorted(results, key=lambda r: r['worst_year'], reverse=True)
    for r in sorted_by_worst[:8]:
        print(f"  {r['label']:42}  worstY={r['worst_year']:>+5.0f}%  bestY={r['best_year']:>+5.0f}%  total={r['total_ret']:>+7.0f}%  2026={r['year_2026_ret']:>+6.1f}%  recent3y={r['recent_3y_ret']:>+5.0f}%")


if __name__ == "__main__":
    main()
