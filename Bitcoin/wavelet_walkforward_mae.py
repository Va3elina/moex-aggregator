"""Walk-forward validation + MAE analysis for wavelet champions.

For each strategy:
1. Track MAE (max adverse excursion) per trade — critical for leverage sizing
2. Walk-forward: roll through history in yearly chunks, train on past, test on future
3. Report intra-trade MAE distribution → tells us if leverage 2x/3x is feasible

Leverage safety rules (with 5pp buffer):
  - 2x leverage safe if MAE ≤ 45%
  - 3x leverage safe if MAE ≤ 28%
  - 5x leverage safe if MAE ≤ 15%
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0


# ============ Wavelet stack (vectorized) ============
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
    tr = np.maximum(high - low, np.maximum(np.abs(high - pc), np.abs(low - pc)))
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


def slope(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb: out[lb:] = arr[lb:] - arr[:-lb]
    return out


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'price': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl4': slope(sm[4], 5), 'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
        'std50': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'std30': pd.Series(log_c - sm[5]).rolling(30).std().values,
        'ema50': ema(df['close'].values, 50),
        'ema100': ema(df['close'].values, 100),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
    }


# ============ State machines ============
def state_machine(entry_cond, exit_cond):
    n = len(entry_cond); state = np.zeros(n); s = 0
    for i in range(n):
        if s == 1:
            if exit_cond[i]: s = 0
        else:
            if entry_cond[i]: s = 1
        state[i] = float(s)
    return state


def state_machine_atr(entry, price, atr14, atr_mult):
    n = len(price); state = np.zeros(n); s = 0
    trail = 0.0; high = 0.0
    for i in range(n):
        if s == 1:
            if not np.isnan(atr14[i]):
                if price[i] > high: high = price[i]
                cand = price[i] - atr_mult * atr14[i]
                if cand > trail: trail = cand
                if price[i] < trail: s = 0
        else:
            if entry[i]:
                s = 1; high = price[i]
                trail = price[i] - atr_mult * atr14[i] if not np.isnan(atr14[i]) else 0
        state[i] = float(s)
    return state


# ============ Variants ============
def v_w1_thr(thr):
    def fn(p):
        entry = (p['sl5'] > thr) & (p['sl6'] > thr)
        exit_ = (p['sl5'] <= 0) | (p['sl6'] <= 0)
        mask = np.isnan(p['sl5']) | np.isnan(p['sl6'])
        return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))
    return fn

def v_8a(k=1.5, std_key='std50'):
    def fn(p):
        std = p[std_key]; upper = p['s5'] + k*std
        entry = p['log_c'] > upper
        exit_ = p['log_c'] < p['s5']
        mask = np.isnan(std)
        return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))
    return fn

def v_8a_ema(k=1.5, ema_key='ema50', std_key='std50'):
    def fn(p):
        std = p[std_key]; upper = p['s5'] + k*std
        entry = (p['log_c'] > upper) & (p['price'] > p[ema_key])
        exit_ = (p['log_c'] < p['s5']) | (p['price'] < p[ema_key])
        mask = np.isnan(std) | np.isnan(p[ema_key])
        return state_machine(np.where(mask, False, entry), np.where(mask, False, exit_))
    return fn

def v_8a_atr(k=1.5, atr_mult=3.0, std_key='std50'):
    def fn(p):
        std = p[std_key]; upper = p['s5'] + k*std
        entry = p['log_c'] > upper
        return state_machine_atr(np.where(np.isnan(std), False, entry),
                                  p['price'], p['atr14'], atr_mult)
    return fn


# ============ Backtest with MAE tracking ============
def backtest(p, target):
    """Returns trades with MAE (max adverse excursion) and MFE per trade."""
    closes = p['price']; highs = p['high']; lows = p['low']
    dates = p['index']
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    entry_px = 0; entry_date = None; entry_idx = 0
    intra_min_px = 0; intra_max_px = 0
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            if lows[i] < intra_min_px: intra_min_px = lows[i]
            if highs[i] > intra_max_px: intra_max_px = highs[i]
        tgt = target[i]
        if tgt == 1.0 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            qty = spend/eff; cash -= qty*eff*(1+FEE)
            entry_px = eff; entry_date = dates[i]; entry_idx = i
            intra_min_px = lows[i]; intra_max_px = highs[i]
        elif tgt == 0.0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/(qty*entry_px) - 1) * 100  # net pct after fees
            raw_pnl = (eff/entry_px - 1) * 100  # gross pct
            mae = (intra_min_px/entry_px - 1) * 100  # negative
            mfe = (intra_max_px/entry_px - 1) * 100  # positive
            hold = (dates[i] - entry_date).total_seconds() / 86400
            trades.append({
                'entry_date': entry_date, 'exit_date': dates[i],
                'entry_px': entry_px, 'exit_px': eff,
                'pnl_pct': pnl, 'raw_pnl_pct': raw_pnl,
                'mae_pct': mae, 'mfe_pct': mfe,
                'hold_d': hold, 'entry_idx': entry_idx, 'exit_idx': i,
            })
            qty = 0
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/(qty*entry_px) - 1) * 100
        mae = (intra_min_px/entry_px - 1) * 100
        mfe = (intra_max_px/entry_px - 1) * 100
        hold = (dates[-1] - entry_date).total_seconds() / 86400
        trades.append({'entry_date': entry_date, 'exit_date': dates[-1],
                       'entry_px': entry_px, 'exit_px': eff,
                       'pnl_pct': pnl, 'raw_pnl_pct': pnl,
                       'mae_pct': mae, 'mfe_pct': mfe,
                       'hold_d': hold, 'entry_idx': entry_idx, 'exit_idx': n-1})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(trades, final_eq, eq):
    if len(trades) == 0:
        return {}
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    # MAE statistics
    mae_med = trades['mae_pct'].median()
    mae_p95 = trades['mae_pct'].quantile(0.05)  # 5th percentile = worst-case MAE
    mae_max = trades['mae_pct'].min()
    n_unsafe_3x = (trades['mae_pct'] < -28).sum()  # MAE worse than -28%
    n_unsafe_2x = (trades['mae_pct'] < -45).sum()
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'avg_pnl': trades['pnl_pct'].mean(),
        'max_loss': trades['pnl_pct'].min(),
        'mae_median': mae_med, 'mae_p5': mae_p95, 'mae_worst': mae_max,
        'n_unsafe_3x': n_unsafe_3x, 'n_unsafe_2x': n_unsafe_2x,
        'pct_unsafe_3x': n_unsafe_3x / len(trades) * 100,
        'pct_unsafe_2x': n_unsafe_2x / len(trades) * 100,
    }


# ============ Walk-forward ============
def walk_forward_yearly(p, target, dates, eq_curve, trades):
    """Split equity curve by calendar year, report annual stats."""
    if len(trades) == 0:
        return pd.DataFrame()
    years_present = sorted(dates.year.unique())
    rows = []
    for year in years_present:
        year_mask = (dates.year == year)
        if year_mask.sum() < 5: continue
        sub_eq = eq_curve[year_mask]
        if len(sub_eq) < 2: continue
        year_ret = (sub_eq.iloc[-1] / sub_eq.iloc[0] - 1) * 100
        year_dd = ((sub_eq.cummax() - sub_eq) / sub_eq.cummax()).max() * 100
        year_start = pd.Timestamp(year, 1, 1)
        year_end = pd.Timestamp(year, 12, 31, 23, 59, 59)
        year_trades = trades[(trades['exit_date'] >= year_start) & (trades['exit_date'] <= year_end)]
        n_t = len(year_trades)
        worst_mae = year_trades['mae_pct'].min() if n_t > 0 else 0
        worst_trade = year_trades['pnl_pct'].min() if n_t > 0 else 0
        wr = (year_trades['pnl_pct'] > 0).sum() / n_t * 100 if n_t > 0 else 0
        rows.append({
            'year': year, 'n_trades': n_t,
            'year_ret_pct': year_ret, 'year_dd_pct': year_dd,
            'wr_pct': wr, 'worst_trade_pct': worst_trade, 'worst_mae_pct': worst_mae,
        })
    return pd.DataFrame(rows)


# ============ Loaders ============
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


def report_champion(asset, tf, variant_label, variant_fn, df):
    print(f"\n{'='*120}")
    print(f"{asset} {tf} — {variant_label}")
    print(f"{'='*120}")
    p = precompute(df); p['index'] = df.index
    target = variant_fn(p)
    trades, final_eq, eq = backtest(p, target)
    m = metrics(trades, final_eq, eq)

    pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
    print(f"  Overall: N={m['N']}  WR={m['WR']:.1f}%  PF={pf_str}  total={m['total_ret']:+.0f}%  DD={m['total_dd']:.1f}%")
    print(f"  Per-trade PnL: avg={m['avg_pnl']:+.2f}%  max_loss={m['max_loss']:+.1f}%")
    print(f"  MAE (intra-trade adverse excursion):")
    print(f"    median: {m['mae_median']:+.1f}%   5th-percentile (worst 5%): {m['mae_p5']:+.1f}%   absolute worst: {m['mae_worst']:+.1f}%")
    print(f"  LEVERAGE SAFETY:")
    print(f"    Trades with MAE > -28% (unsafe for 3x): {m['n_unsafe_3x']}/{m['N']} ({m['pct_unsafe_3x']:.1f}%)")
    print(f"    Trades with MAE > -45% (unsafe for 2x): {m['n_unsafe_2x']}/{m['N']} ({m['pct_unsafe_2x']:.1f}%)")
    # Year-by-year walk-forward
    yearly = walk_forward_yearly(p, target, df.index, eq, trades)
    print(f"\n  Year-by-year walk-forward:")
    print(f"    {'year':>4}  {'N':>3}  {'ret%':>7}  {'DD%':>5}  {'WR%':>5}  {'worstT%':>8}  {'worstMAE%':>9}")
    for _, r in yearly.iterrows():
        print(f"    {int(r['year'])}  {int(r['n_trades']):>3}  {r['year_ret_pct']:>+6.1f}%  {r['year_dd_pct']:>4.1f}%  {r['wr_pct']:>4.1f}%  {r['worst_trade_pct']:>+7.1f}%  {r['worst_mae_pct']:>+8.1f}%")
    # Worst 5 trades by MAE
    print(f"\n  Worst 5 trades by MAE (= biggest in-trade drawdowns):")
    worst = trades.nsmallest(5, 'mae_pct')
    for _, t in worst.iterrows():
        print(f"    {t['entry_date'].date()} → {t['exit_date'].date()}  "
              f"hold={t['hold_d']:.0f}d  MAE={t['mae_pct']:+.1f}%  "
              f"final PnL={t['pnl_pct']:+.1f}%  MFE={t['mfe_pct']:+.1f}%")
    return m, yearly, trades


def main():
    # Champions to validate
    btc_daily = load_daily('BTC')
    eth_hourly = load_hourly('ETH')
    eth_4h = resample_4h(eth_hourly)

    print("=" * 120)
    print("WALK-FORWARD VALIDATION + MAE ANALYSIS — current champions")
    print("=" * 120)

    report_champion('BTC', '1D', "8a (k=1.5, std30) [BTC champion]",
                    v_8a(k=1.5, std_key='std30'), btc_daily)

    report_champion('ETH', '4H', "8a (k=1.5) + EMA100 [ETH champion]",
                    v_8a_ema(k=1.5, ema_key='ema100'), eth_4h)

    # Bonus: also check old champions to see how MAE looks
    print("\n" + "=" * 120)
    print("BONUS: previous champions (for comparison)")
    print("=" * 120)
    report_champion('BTC', '1D', "W1 slope ≥ 0.005 [old BTC champ]",
                    v_w1_thr(0.005), btc_daily)
    report_champion('ETH', '1D', "8a (k=1.5) + ATR 3.0 [old ETH champ H3]",
                    v_8a_atr(1.5, 3.0), load_daily('ETH'))


if __name__ == "__main__":
    main()
