"""Grand search: ~25 wavelet variants × 3 timeframes (1D, 4H, 1H) × 2 assets (BTC, ETH).

For each (asset, timeframe, variant) compute:
- Full-period return + DD
- Recent (last 3y) return + DD ← critical for "current relevance"
- Worst 12-month rolling return ← stress test
- WR, PF, N, avg hold

Goal: find which variants stay robust in 2023-2026, not just historical winners.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0
RECENT_START = pd.Timestamp("2023-05-11")  # 3 years before end


def ema(s, n):
    arr = s.values; out = np.full(len(arr), np.nan)
    if len(arr) < n: return pd.Series(out, index=s.index)
    out[n-1] = arr[:n].mean(); k = 2.0/(n+1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return pd.Series(out, index=s.index)


def atr(df, n=14):
    pc = df['close'].shift(1)
    tr = pd.concat([df['high']-df['low'], (df['high']-pc).abs(), (df['low']-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()


def atrous_haar(x, J=6):
    n = len(x); sm = [np.asarray(x, dtype=float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]; s = np.full(n, np.nan)
        for i in range(n):
            s[i] = prev[i] if i-gap < 0 else 0.5*(prev[i] + prev[i-gap])
        sm.append(s)
    return sm


def slope(arr, lb=5):
    out = np.full(len(arr), np.nan)
    for i in range(lb, len(arr)):
        if not np.isnan(arr[i]) and not np.isnan(arr[i-lb]):
            out[i] = arr[i] - arr[i-lb]
    return out


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'price': df['close'].values, 'high': df['high'].values,
        'low': df['low'].values, 'index': df.index,
        's3': sm[3], 's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl3': slope(sm[3], 5), 'sl4': slope(sm[4], 5),
        'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
        'std50_dev_s5': pd.Series(log_c - sm[5]).rolling(50).std().values,
        'std30_dev_s5': pd.Series(log_c - sm[5]).rolling(30).std().values,
        'ema50': ema(df['close'], 50).values,
        'ema100': ema(df['close'], 100).values,
        'atr14': atr(df, 14).values,
    }


# ============== Variant generators ==============
def v_w1(p):
    """W1: s5 up AND s6 up."""
    n = len(p['sl5']); t = np.zeros(n); st = 0
    for i in range(n):
        if np.isnan(p['sl5'][i]) or np.isnan(p['sl6'][i]):
            t[i] = st; continue
        if st == 0 and p['sl5'][i] > 0 and p['sl6'][i] > 0: st = 1
        elif st == 1 and (p['sl5'][i] <= 0 or p['sl6'][i] <= 0): st = 0
        t[i] = float(st)
    return t


def v_w1_thr(thr):
    def fn(p):
        n = len(p['sl5']); t = np.zeros(n); st = 0
        for i in range(n):
            if np.isnan(p['sl5'][i]) or np.isnan(p['sl6'][i]):
                t[i] = st; continue
            if st == 0 and p['sl5'][i] > thr and p['sl6'][i] > thr: st = 1
            elif st == 1 and (p['sl5'][i] <= 0 or p['sl6'][i] <= 0): st = 0
            t[i] = float(st)
        return t
    return fn


def v_triple(p):
    """s4 up AND s5 up AND s6 up."""
    n = len(p['sl5']); t = np.zeros(n); st = 0
    for i in range(n):
        if any(np.isnan(p[k][i]) for k in ('sl4','sl5','sl6')):
            t[i] = st; continue
        ok = p['sl4'][i] > 0 and p['sl5'][i] > 0 and p['sl6'][i] > 0
        if st == 0 and ok: st = 1
        elif st == 1 and not ok: st = 0
        t[i] = float(st)
    return t


def v_cross(p):
    """s5 > s6."""
    n = len(p['s5']); t = np.zeros(n); st = 0
    for i in range(n):
        if np.isnan(p['s5'][i]) or np.isnan(p['s6'][i]):
            t[i] = st; continue
        if st == 0 and p['s5'][i] > p['s6'][i]: st = 1
        elif st == 1 and p['s5'][i] < p['s6'][i]: st = 0
        t[i] = float(st)
    return t


def v_8a(k=1.5, std_key='std50_dev_s5'):
    def fn(p):
        n = len(p['s5']); t = np.zeros(n); st = 0
        for i in range(n):
            if np.isnan(p[std_key][i]):
                t[i] = st; continue
            upper = p['s5'][i] + k * p[std_key][i]
            if st == 0 and p['log_c'][i] > upper: st = 1
            elif st == 1 and p['log_c'][i] < p['s5'][i]: st = 0
            t[i] = float(st)
        return t
    return fn


def v_8a_atr_trail(k=1.5, atr_mult=3.0, std_key='std50_dev_s5'):
    def fn(p):
        n = len(p['s5']); t = np.zeros(n); st = 0
        trail = 0.0; high = 0.0
        for i in range(n):
            if np.isnan(p[std_key][i]) or np.isnan(p['atr14'][i]):
                t[i] = st; continue
            upper = p['s5'][i] + k * p[std_key][i]
            if st == 0 and p['log_c'][i] > upper:
                st = 1
                high = p['price'][i]
                trail = p['price'][i] - atr_mult * p['atr14'][i]
            elif st == 1:
                if p['price'][i] > high:
                    high = p['price'][i]
                    trail = max(trail, p['price'][i] - atr_mult * p['atr14'][i])
                if p['price'][i] < trail: st = 0
            t[i] = float(st)
        return t
    return fn


def v_8a_ema(k=1.5, ema_key='ema50', std_key='std50_dev_s5'):
    """8a + EMA filter."""
    def fn(p):
        n = len(p['s5']); t = np.zeros(n); st = 0
        for i in range(n):
            if np.isnan(p[std_key][i]) or np.isnan(p[ema_key][i]):
                t[i] = st; continue
            upper = p['s5'][i] + k * p[std_key][i]
            if st == 0 and p['log_c'][i] > upper and p['price'][i] > p[ema_key][i]: st = 1
            elif st == 1 and (p['log_c'][i] < p['s5'][i] or p['price'][i] < p[ema_key][i]): st = 0
            t[i] = float(st)
        return t
    return fn


def v_w1_ema(ema_key='ema50'):
    def fn(p):
        n = len(p['sl5']); t = np.zeros(n); st = 0
        for i in range(n):
            if np.isnan(p['sl5'][i]) or np.isnan(p['sl6'][i]) or np.isnan(p[ema_key][i]):
                t[i] = st; continue
            ok = p['sl5'][i] > 0 and p['sl6'][i] > 0 and p['price'][i] > p[ema_key][i]
            if st == 0 and ok: st = 1
            elif st == 1 and (p['sl5'][i] <= 0 or p['sl6'][i] <= 0 or p['price'][i] < p[ema_key][i]):
                st = 0
            t[i] = float(st)
        return t
    return fn


# ============== Backtest ==============
def backtest(df, target):
    closes = df['close'].values; dates = df.index
    cash = INITIAL; qty = 0.0
    eq = np.zeros(len(closes)); trades = []
    ev = 0; ed = None
    for i in range(len(closes)):
        c = closes[i]; date = dates[i]
        eq[i] = cash + qty*c
        tgt = target[i]
        if np.isnan(tgt): continue
        if tgt == 1 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = date
        elif tgt == 0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            hold = (date - ed).total_seconds() / 86400 if ed else 0
            trades.append({'entry': ed, 'exit': date, 'pnl_pct': pnl, 'hold_d': hold})
            qty = 0; ev = 0; ed = None
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        hold = (dates[-1] - ed).total_seconds() / 86400 if ed else 0
        trades.append({'entry': ed, 'exit': dates[-1], 'pnl_pct': pnl, 'hold_d': hold})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def compute_period_return(eq_curve, start_date):
    sub = eq_curve[eq_curve.index >= start_date]
    if len(sub) < 2: return 0.0
    return (sub.iloc[-1] / sub.iloc[0] - 1) * 100


def compute_period_dd(eq_curve, start_date):
    sub = eq_curve[eq_curve.index >= start_date]
    if len(sub) < 2: return 0.0
    return ((sub.cummax() - sub) / sub.cummax()).max() * 100


def worst_year_return(eq_curve):
    """Worst 1-year rolling return based on equity ratio."""
    s = eq_curve
    if isinstance(s.index, pd.DatetimeIndex):
        # daily equivalent
        days_per_year = 365
        idx = s.index
        ts_dt = pd.to_datetime(idx)
        worst = 0.0
        for i in range(len(s) - 1, 0, -1):
            year_ago = ts_dt[i] - pd.Timedelta(days=days_per_year)
            past = s[s.index <= year_ago]
            if len(past) == 0: break
            ret = (s.iloc[i] / past.iloc[-1] - 1) * 100
            if ret < worst: worst = ret
        return worst
    return 0.0


def metrics(trades, final_eq, eq_curve, recent_start):
    if len(trades) == 0:
        return None
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq_curve.cummax() - eq_curve) / eq_curve.cummax()).max() * 100
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    recent_ret = compute_period_return(eq_curve, recent_start)
    recent_dd = compute_period_dd(eq_curve, recent_start)
    worst_year = worst_year_return(eq_curve)
    return {
        'N': len(trades), 'WR': wr, 'PF': pf, 'total_ret': total_ret, 'total_dd': total_dd,
        'recent_ret': recent_ret, 'recent_dd': recent_dd, 'worst_y': worst_year,
        'avg_hold_d': trades['hold_d'].mean(),
        'max_loss': trades['pnl_pct'].min(),
    }


# ============== Data loaders ==============
def load_daily(asset):
    if asset == 'BTC':
        df = pd.read_csv(ROOT / "btc_usd_daily_coinbase.csv", parse_dates=['date']).set_index('date').sort_index()
    else:
        df = pd.read_csv(ROOT / "eth_usd_daily_coinbase.csv", parse_dates=['date']).set_index('date').sort_index()
    return df[df['close'] > 0].dropna(subset=['close'])


def load_hourly(asset):
    name = "btc_usd_hourly_coinbase.csv" if asset == 'BTC' else "eth_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT / name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close'] > 0].dropna(subset=['close'])


def resample_4h(hourly):
    return hourly.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


# ============== Main ==============
VARIANTS = [
    # name, fn
    ('W1 baseline (s5∧s6)',           v_w1),
    ('W1 slope≥0.003',                v_w1_thr(0.003)),
    ('W1 slope≥0.005',                v_w1_thr(0.005)),
    ('W1 slope≥0.008',                v_w1_thr(0.008)),
    ('W1 + EMA50',                    v_w1_ema('ema50')),
    ('W1 + EMA100',                   v_w1_ema('ema100')),
    ('Triple s4∧s5∧s6',               v_triple),
    ('s5 × s6 cross',                 v_cross),
    ('8a (k=1.0)',                    v_8a(k=1.0)),
    ('8a (k=1.5)',                    v_8a(k=1.5)),
    ('8a (k=2.0)',                    v_8a(k=2.0)),
    ('8a (k=1.5) + ATR2.5',           v_8a_atr_trail(1.5, 2.5)),
    ('8a (k=1.5) + ATR3.0',           v_8a_atr_trail(1.5, 3.0)),
    ('8a (k=1.5) + ATR4.0',           v_8a_atr_trail(1.5, 4.0)),
    ('8a (k=2.0) + ATR3.0',           v_8a_atr_trail(2.0, 3.0)),
    ('8a (k=1.0) + ATR3.0',           v_8a_atr_trail(1.0, 3.0)),
    ('8a (k=1.5, std30)',             v_8a(k=1.5, std_key='std30_dev_s5')),
    ('8a (k=2.0, std30) + ATR3',      v_8a_atr_trail(2.0, 3.0, 'std30_dev_s5')),
    ('8a (k=1.5) + EMA50',            v_8a_ema(1.5, 'ema50')),
    ('8a (k=1.5) + EMA100',           v_8a_ema(1.5, 'ema100')),
]


def run_combo(asset, tf_name, df, recent_start):
    p = precompute(df)
    results = []
    for label, fn in VARIANTS:
        target = fn(p)
        tr, eq, eqc = backtest(df, target)
        m = metrics(tr, eq, eqc, recent_start)
        if m is None:
            results.append({'variant': label, **{k: None for k in ('N','WR','PF','total_ret','total_dd','recent_ret','recent_dd','worst_y','avg_hold_d','max_loss')}})
        else:
            results.append({'variant': label, **m})
    return results


def fmt_pf(pf):
    if pf is None: return '   —'
    return f"{pf:>5.2f}" if pf != float('inf') else "  inf"


def fmt_num(v, w=6):
    if v is None: return f"{'—':>{w}}"
    return f"{v:>+{w-1}.0f}"


def main():
    # Prepare datasets
    datasets = {}
    for asset in ('BTC', 'ETH'):
        d_daily = load_daily(asset)
        d_hourly = load_hourly(asset)
        d_4h = resample_4h(d_hourly)
        datasets[(asset, '1D')] = d_daily
        datasets[(asset, '4H')] = d_4h
        datasets[(asset, '1H')] = d_hourly

    # For each (asset, tf): run variants
    print(f"\nGrand search — RECENT_START = {RECENT_START.date()}\n")
    all_results = {}
    for (asset, tf), df in datasets.items():
        # Hourly compute is slow, give status
        n_bars = len(df)
        print(f"=== {asset} {tf} ({n_bars} bars, {df.index[0]} → {df.index[-1]})")
        all_results[(asset, tf)] = run_combo(asset, tf, df, RECENT_START)
        # Also B&H
        bh_eq = INITIAL / df['close'].iloc[0] * df['close']
        bh_total = (bh_eq.iloc[-1] / INITIAL - 1) * 100
        bh_dd = ((bh_eq.cummax() - bh_eq) / bh_eq.cummax()).max() * 100
        bh_recent = compute_period_return(bh_eq, RECENT_START)
        bh_recent_dd = compute_period_dd(bh_eq, RECENT_START)
        bh_worst_y = worst_year_return(bh_eq)
        print(f"   B&H: total ret={bh_total:+.0f}%  DD={bh_dd:.1f}%  recent_ret={bh_recent:+.0f}%  recent_DD={bh_recent_dd:.1f}%  worst_y={bh_worst_y:+.0f}%")
        # print top 5 by recent_ret
        sorted_by_recent = sorted(all_results[(asset, tf)], key=lambda r: r['recent_ret'] if r['recent_ret'] is not None else -1e9, reverse=True)[:5]
        for r in sorted_by_recent:
            if r['N'] is None: continue
            print(f"     {r['variant']:30}  N={r['N']:>4}  WR={r['WR']:>4.1f}%  PF={fmt_pf(r['PF'])}  total={r['total_ret']:>+7.0f}%  DD={r['total_dd']:>4.1f}%  recent={r['recent_ret']:>+5.0f}%/{r['recent_dd']:>4.1f}%  worst_y={r['worst_y']:>+5.0f}%")

    # ====== Cross-section summary ======
    for asset in ('BTC', 'ETH'):
        print(f"\n{'='*150}")
        print(f"{asset} — RECENT RETURN (2023-05 to 2026-05) per variant per TF")
        print(f"{'='*150}")
        print(f"  {'variant':32} | {'1D total':>10} {'1D recent':>11} {'1D DD-r':>9} {'4H total':>10} {'4H recent':>11} {'4H DD-r':>9} {'1H total':>10} {'1H recent':>11} {'1H DD-r':>9}")
        print("  " + "-" * 145)
        # baseline B&H first
        bh_line = f"  {'(Buy & Hold)':32} |"
        for tf in ('1D','4H','1H'):
            df = datasets[(asset, tf)]
            bh = INITIAL / df['close'].iloc[0] * df['close']
            total = (bh.iloc[-1]/INITIAL - 1)*100
            recent = compute_period_return(bh, RECENT_START)
            rdd = compute_period_dd(bh, RECENT_START)
            bh_line += f" {total:>+9.0f}% {recent:>+10.0f}% {rdd:>+8.1f}%"
        print(bh_line)
        # variants
        for v_idx, (label, _) in enumerate(VARIANTS):
            line = f"  {label:32} |"
            for tf in ('1D','4H','1H'):
                r = all_results[(asset, tf)][v_idx]
                if r['N'] is None:
                    line += f" {'—':>10} {'—':>11} {'—':>9}"
                else:
                    line += f" {r['total_ret']:>+9.0f}% {r['recent_ret']:>+10.0f}% {r['recent_dd']:>+8.1f}%"
            print(line)

    # Robust champion: best by min(recent_ret across TFs) — must be good on ALL TFs
    print(f"\n{'='*120}")
    print("ROBUST CHAMPIONS — best variants by (recent_ret + 0.2 × total_ret − recent_dd)")
    print(f"{'='*120}")
    for asset in ('BTC', 'ETH'):
        for tf in ('1D','4H','1H'):
            results = all_results[(asset, tf)]
            # composite score
            def score(r):
                if r['N'] is None: return -1e9
                return r['recent_ret'] + 0.2*r['total_ret'] - r['recent_dd']
            best = max(results, key=score)
            print(f"  {asset} {tf}:  {best['variant']:30}  N={best['N']:>4}  total={best['total_ret']:>+6.0f}%  recent={best['recent_ret']:>+5.0f}%/{best['recent_dd']:>4.1f}%  worst_y={best['worst_y']:>+5.0f}%")


if __name__ == "__main__":
    main()
