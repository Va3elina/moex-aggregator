"""Grand search: 20 wavelet variants × 3 timeframes (1D, 4H, 1H) × 2 assets (BTC, ETH).

Vectorized for speed. Reports total return + recent 3y return + worst rolling 1Y return,
so we can identify variants robust in the current regime, not just historical winners.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
FEE, SLIP, INITIAL = 0.0005, 0.0003, 100.0
RECENT_START = pd.Timestamp("2023-05-11")


# ===== Vectorized helpers =====
def ema(arr, n):
    out = np.full(len(arr), np.nan)
    if len(arr) < n: return out
    out[n-1] = arr[:n].mean()
    k = 2.0 / (n + 1.0)
    for i in range(n, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out


def atr_vec(high, low, close, n=14):
    pc = np.empty_like(close); pc[0] = close[0]; pc[1:] = close[:-1]
    tr = np.maximum(high - low, np.maximum(np.abs(high - pc), np.abs(low - pc)))
    out = np.full(len(close), np.nan)
    out[0] = tr[0]
    a = 1.0 / n
    for i in range(1, len(close)):
        out[i] = a * tr[i] + (1-a) * out[i-1]
    return out


def atrous_haar(x, J=6):
    sm = [x.astype(float).copy()]
    for j in range(1, J+1):
        gap = 2**(j-1); prev = sm[-1]
        s = prev.copy()
        if len(x) > gap:
            s[gap:] = 0.5 * (prev[gap:] + prev[:-gap])
        sm.append(s)
    return sm


def slope(arr, lb=5):
    out = np.full(len(arr), np.nan)
    if len(arr) > lb:
        out[lb:] = arr[lb:] - arr[:-lb]
    return out


def rolling_std(arr, n):
    """Vectorized rolling std via pandas."""
    return pd.Series(arr).rolling(n).std().values


def precompute(df):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    return {
        'log_c': log_c, 'price': df['close'].values,
        's3': sm[3], 's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl3': slope(sm[3], 5), 'sl4': slope(sm[4], 5),
        'sl5': slope(sm[5], 5), 'sl6': slope(sm[6], 5),
        'std50': rolling_std(log_c - sm[5], 50),
        'std30': rolling_std(log_c - sm[5], 30),
        'ema50': ema(df['close'].values, 50),
        'ema100': ema(df['close'].values, 100),
        'atr14': atr_vec(df['high'].values, df['low'].values, df['close'].values, 14),
    }


# ===== State machine =====
def state_machine(entry_cond, exit_cond):
    """Tight state machine: enter on entry_cond, exit on exit_cond.
    Both arrays should be bool with NaN treated as False."""
    n = len(entry_cond)
    state = np.zeros(n, dtype=np.float64)
    s = 0
    for i in range(n):
        if s == 1:
            if exit_cond[i]: s = 0
        else:
            if entry_cond[i]: s = 1
        state[i] = float(s)
    return state


def state_machine_atr(entry_cond, price, atr14, atr_mult):
    """Entry on entry_cond. Exit when price < trailing_stop (max(price)-atr_mult*atr_at_entry)."""
    n = len(price)
    state = np.zeros(n, dtype=np.float64)
    s = 0
    trail = 0.0
    high = 0.0
    for i in range(n):
        if s == 1:
            if not np.isnan(atr14[i]):
                if price[i] > high: high = price[i]
                cand = price[i] - atr_mult * atr14[i]
                if cand > trail: trail = cand
                if price[i] < trail:
                    s = 0
        else:
            if entry_cond[i]:
                s = 1
                high = price[i]
                if not np.isnan(atr14[i]):
                    trail = price[i] - atr_mult * atr14[i]
                else:
                    trail = 0
        state[i] = float(s)
    return state


def to_bool(arr):
    """Convert arr (float with NaN) to bool, NaN -> False."""
    return np.where(np.isnan(arr), False, arr.astype(bool))


# ===== Variants (all return target array) =====
def v_w1(p):
    entry = (p['sl5'] > 0) & (p['sl6'] > 0)
    exit_ = (p['sl5'] <= 0) | (p['sl6'] <= 0)
    entry = np.where(np.isnan(p['sl5']) | np.isnan(p['sl6']), False, entry)
    exit_ = np.where(np.isnan(p['sl5']) | np.isnan(p['sl6']), False, exit_)
    return state_machine(entry, exit_)


def v_w1_thr(thr):
    def fn(p):
        entry = (p['sl5'] > thr) & (p['sl6'] > thr)
        exit_ = (p['sl5'] <= 0) | (p['sl6'] <= 0)
        mask = np.isnan(p['sl5']) | np.isnan(p['sl6'])
        entry = np.where(mask, False, entry)
        exit_ = np.where(mask, False, exit_)
        return state_machine(entry, exit_)
    return fn


def v_triple(p):
    entry = (p['sl4'] > 0) & (p['sl5'] > 0) & (p['sl6'] > 0)
    exit_ = (p['sl4'] <= 0) | (p['sl5'] <= 0) | (p['sl6'] <= 0)
    mask = np.isnan(p['sl4']) | np.isnan(p['sl5']) | np.isnan(p['sl6'])
    entry = np.where(mask, False, entry)
    exit_ = np.where(mask, False, exit_)
    return state_machine(entry, exit_)


def v_cross(p):
    entry = p['s5'] > p['s6']
    exit_ = p['s5'] < p['s6']
    mask = np.isnan(p['s5']) | np.isnan(p['s6'])
    entry = np.where(mask, False, entry)
    exit_ = np.where(mask, False, exit_)
    return state_machine(entry, exit_)


def v_8a(k=1.5, std_key='std50'):
    def fn(p):
        std = p[std_key]
        upper = p['s5'] + k * std
        entry = p['log_c'] > upper
        exit_ = p['log_c'] < p['s5']
        mask = np.isnan(std)
        entry = np.where(mask, False, entry)
        exit_ = np.where(mask, False, exit_)
        return state_machine(entry, exit_)
    return fn


def v_8a_atr(k=1.5, atr_mult=3.0, std_key='std50'):
    def fn(p):
        std = p[std_key]
        upper = p['s5'] + k * std
        entry = p['log_c'] > upper
        mask = np.isnan(std)
        entry = np.where(mask, False, entry)
        return state_machine_atr(entry, p['price'], p['atr14'], atr_mult)
    return fn


def v_8a_ema(k=1.5, ema_key='ema50', std_key='std50'):
    def fn(p):
        std = p[std_key]
        upper = p['s5'] + k * std
        entry = (p['log_c'] > upper) & (p['price'] > p[ema_key])
        exit_ = (p['log_c'] < p['s5']) | (p['price'] < p[ema_key])
        mask = np.isnan(std) | np.isnan(p[ema_key])
        entry = np.where(mask, False, entry)
        exit_ = np.where(mask, False, exit_)
        return state_machine(entry, exit_)
    return fn


def v_w1_ema(ema_key='ema50'):
    def fn(p):
        entry = (p['sl5'] > 0) & (p['sl6'] > 0) & (p['price'] > p[ema_key])
        exit_ = (p['sl5'] <= 0) | (p['sl6'] <= 0) | (p['price'] < p[ema_key])
        mask = np.isnan(p['sl5']) | np.isnan(p['sl6']) | np.isnan(p[ema_key])
        entry = np.where(mask, False, entry)
        exit_ = np.where(mask, False, exit_)
        return state_machine(entry, exit_)
    return fn


# ===== Backtest =====
def backtest(closes, dates, target):
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None
    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        tgt = target[i]
        if tgt == 1.0 and qty == 0:
            eff = c*(1+SLIP); spend = cash*0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq; ev = cost; ed = dates[i]
        elif tgt == 0.0 and qty > 0:
            eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            hold = (dates[i] - ed).total_seconds() / 86400 if ed is not None else 0
            trades.append({'entry': ed, 'exit': dates[i], 'pnl_pct': pnl, 'hold_d': hold})
            qty = 0; ev = 0; ed = None
    if qty > 0:
        c = closes[-1]; eff = c*(1-SLIP); proceeds = qty*eff*(1-FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        hold = (dates[-1] - ed).total_seconds() / 86400 if ed is not None else 0
        trades.append({'entry': ed, 'exit': dates[-1], 'pnl_pct': pnl, 'hold_d': hold})
    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def period_ret(eq, start):
    sub = eq[eq.index >= start]
    if len(sub) < 2: return 0.0
    return (sub.iloc[-1] / sub.iloc[0] - 1) * 100


def period_dd(eq, start):
    sub = eq[eq.index >= start]
    if len(sub) < 2: return 0.0
    return ((sub.cummax() - sub) / sub.cummax()).max() * 100


def worst_year(eq):
    if len(eq) < 2: return 0
    days = pd.Series(eq.index).diff().dt.total_seconds().median() / 86400
    bars_per_year = max(1, int(365 / days))
    if len(eq) < bars_per_year: return 0
    ratios = eq.values[bars_per_year:] / eq.values[:-bars_per_year]
    return (ratios.min() - 1) * 100


def metrics(trades, final_eq, eq):
    if len(trades) == 0:
        return None
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct'] > 0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct'] > 0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct'] < 0]['pnl_pct'].sum())
    pf = pos / neg if neg > 0 else float('inf')
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'recent_ret': period_ret(eq, RECENT_START),
        'recent_dd': period_dd(eq, RECENT_START),
        'worst_y': worst_year(eq),
        'avg_hold_d': trades['hold_d'].mean(),
        'max_loss': trades['pnl_pct'].min(),
    }


# ===== Data loaders =====
def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT / name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close'] > 0].dropna(subset=['close'])


def load_hourly(asset):
    name = "btc_usd_hourly_coinbase.csv" if asset == 'BTC' else "eth_usd_hourly_coinbase.csv"
    df = pd.read_csv(ROOT / name, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    return df[df['close'] > 0].dropna(subset=['close'])


def resample_4h(hourly):
    return hourly.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


# ===== Main =====
VARIANTS = [
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
    ('8a (k=1.5) + ATR2.5',           v_8a_atr(1.5, 2.5)),
    ('8a (k=1.5) + ATR3.0',           v_8a_atr(1.5, 3.0)),
    ('8a (k=1.5) + ATR4.0',           v_8a_atr(1.5, 4.0)),
    ('8a (k=2.0) + ATR3.0',           v_8a_atr(2.0, 3.0)),
    ('8a (k=1.0) + ATR3.0',           v_8a_atr(1.0, 3.0)),
    ('8a (k=1.5, std30)',             v_8a(k=1.5, std_key='std30')),
    ('8a (k=2.0, std30) + ATR3',      v_8a_atr(2.0, 3.0, 'std30')),
    ('8a (k=1.5) + EMA50',            v_8a_ema(1.5, 'ema50')),
    ('8a (k=1.5) + EMA100',           v_8a_ema(1.5, 'ema100')),
]


def main():
    import time
    datasets = {}
    for asset in ('BTC', 'ETH'):
        d_daily = load_daily(asset)
        d_hourly = load_hourly(asset)
        d_4h = resample_4h(d_hourly)
        datasets[(asset, '1D')] = d_daily
        datasets[(asset, '4H')] = d_4h
        datasets[(asset, '1H')] = d_hourly

    print(f"\nGrand search vectorized — RECENT_START = {RECENT_START.date()}\n", flush=True)

    all_results = {}
    for (asset, tf), df in datasets.items():
        t0 = time.time()
        p = precompute(df)
        closes = df['close'].values
        dates = df.index
        results = []
        for label, fn in VARIANTS:
            target = fn(p)
            tr, eq, eqc = backtest(closes, dates, target)
            m = metrics(tr, eq, eqc)
            results.append({'variant': label, **(m or {})})
        all_results[(asset, tf)] = results

        # B&H baseline
        bh = INITIAL / df['close'].iloc[0] * df['close']
        bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100
        bh_recent = period_ret(bh, RECENT_START)
        bh_recent_dd = period_dd(bh, RECENT_START)
        bh_worst_y = worst_year(bh)

        elapsed = time.time() - t0
        print(f"=== {asset} {tf} ({len(df)} bars)  [{elapsed:.1f}s]", flush=True)
        print(f"   B&H: total={bh_total:+.0f}%/DD={bh_dd:.0f}%  recent={bh_recent:+.0f}%/DD={bh_recent_dd:.0f}%  worst_y={bh_worst_y:+.0f}%", flush=True)
        # top 5 by recent_ret
        sorted_r = sorted([r for r in results if r.get('N')], key=lambda r: r['recent_ret'], reverse=True)[:5]
        for r in sorted_r:
            pf_str = f"{r['PF']:>5.2f}" if r['PF'] != float('inf') else "  inf"
            print(f"     {r['variant']:30}  N={r['N']:>4}  WR={r['WR']:>4.1f}%  PF={pf_str}  total={r['total_ret']:>+6.0f}%/DD={r['total_dd']:>4.1f}%  recent={r['recent_ret']:>+5.0f}%/{r['recent_dd']:>4.1f}%  worstY={r['worst_y']:>+4.0f}%", flush=True)

    # Cross-section
    for asset in ('BTC', 'ETH'):
        print(f"\n{'='*150}", flush=True)
        print(f"{asset} — ALL variants per TF (RECENT 3-year focus)", flush=True)
        print(f"{'='*150}", flush=True)
        print(f"  {'variant':30} |{'1D total':>9}{'1D recnt':>9}{'1D DDr':>7} |{'4H total':>9}{'4H recnt':>9}{'4H DDr':>7} |{'1H total':>9}{'1H recnt':>9}{'1H DDr':>7}", flush=True)
        print("  " + "-" * 130, flush=True)
        # BH first
        line = f"  {'(Buy & Hold)':30} |"
        for tf in ('1D','4H','1H'):
            df = datasets[(asset, tf)]
            bh = INITIAL / df['close'].iloc[0] * df['close']
            t = (bh.iloc[-1]/INITIAL - 1)*100
            r = period_ret(bh, RECENT_START); dd = period_dd(bh, RECENT_START)
            line += f"{t:>+8.0f}%{r:>+8.0f}%{dd:>+6.0f}% |"
        print(line, flush=True)
        for idx, (label, _) in enumerate(VARIANTS):
            line = f"  {label:30} |"
            for tf in ('1D','4H','1H'):
                r = all_results[(asset, tf)][idx]
                if not r.get('N'):
                    line += f"{'—':>8}{'—':>9}{'—':>7} |"
                else:
                    line += f"{r['total_ret']:>+8.0f}%{r['recent_ret']:>+8.0f}%{r['recent_dd']:>+6.0f}% |"
            print(line, flush=True)

    # Robust champions
    print(f"\n{'='*120}", flush=True)
    print("ROBUST CHAMPIONS — best by (recent_ret − recent_dd) — i.e. recent risk-adjusted", flush=True)
    print(f"{'='*120}", flush=True)
    for asset in ('BTC', 'ETH'):
        for tf in ('1D','4H','1H'):
            rs = all_results[(asset, tf)]
            best = max([r for r in rs if r.get('N')], key=lambda r: r['recent_ret'] - r['recent_dd'])
            print(f"  {asset} {tf}:  {best['variant']:30}  N={best['N']:>4}  total={best['total_ret']:>+6.0f}%/{best['total_dd']:>4.1f}%  recent={best['recent_ret']:>+5.0f}%/{best['recent_dd']:>4.1f}%  worstY={best['worst_y']:>+4.0f}%", flush=True)


if __name__ == "__main__":
    main()
