"""SUPER GRID SEARCH — beat user's strategy.

Goal: DD ≤ 20%, WR ≥ 90%, total > +1601%, no shorts, no trading in bearish regime.

Tests:
  - 15 entry conditions (EMA crosses, wavelet crosses, RSI events, price crosses)
  - 5 exit conditions (RSI, price-below-MA, slope flips)
  - 3 macro regime filters (none, price>EMA200_W, s7 slope up)
  - 2 assets: BTC, ETH (weekly logic on daily engine)
  - With and without pyramiding 33/67

Total: ~450 variants per asset.
Top 20 reported by composite score.
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
    files = {'BTC': 'btc_usd_daily_coinbase.csv', 'ETH': 'eth_usd_daily_coinbase.csv',
             'LTC': 'ltc_usd_daily_coinbase.csv', 'SOL': 'sol_usd_daily_coinbase.csv'}
    df = pd.read_csv(ROOT / files[asset], parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])

def to_weekly(daily):
    return daily.resample('W-MON', label='left', closed='left').agg(
        {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()


def compute_indicators(weekly):
    """Compute all indicators on weekly TF, return as dict of pd.Series."""
    wc = weekly['close']
    log_wc = np.log(wc.values)
    sm = atrous_haar(log_wc, J=8)
    rsi = pd.Series(rsi_a(wc.values, 14), index=wc.index)
    ind = {
        'close': wc,
        'log_close': pd.Series(log_wc, index=wc.index),
        'ema5':   pd.Series(ema(wc.values, 5),   index=wc.index),
        'ema10':  pd.Series(ema(wc.values, 10),  index=wc.index),
        'ema20':  pd.Series(ema(wc.values, 20),  index=wc.index),
        'ema50':  pd.Series(ema(wc.values, 50),  index=wc.index),
        'ema100': pd.Series(ema(wc.values, 100), index=wc.index),
        'ema200': pd.Series(ema(wc.values, 200), index=wc.index),
        's2': pd.Series(sm[2], index=wc.index),
        's3': pd.Series(sm[3], index=wc.index),
        's4': pd.Series(sm[4], index=wc.index),
        's5': pd.Series(sm[5], index=wc.index),
        's6': pd.Series(sm[6], index=wc.index),
        's7': pd.Series(sm[7], index=wc.index),
        'rsi14': rsi,
        'rsi_sma14': rsi.rolling(14).mean(),
        'slope_s5': pd.Series(slope_a(sm[5], 5), index=wc.index),
        'slope_s6': pd.Series(slope_a(sm[6], 5), index=wc.index),
        'slope_s7': pd.Series(slope_a(sm[7], 5), index=wc.index),
    }
    return ind


def cross_over(a, b):
    """a crosses above b. Both pd.Series."""
    return (a > b) & (a.shift(1) <= b.shift(1))

def cross_under(a, b):
    return (a < b) & (a.shift(1) >= b.shift(1))

def cross_level(a, level, direction='above'):
    if direction == 'above':
        return (a > level) & (a.shift(1) <= level)
    else:
        return (a < level) & (a.shift(1) >= level)


def build_signals(ind):
    """Build ALL combinations of entry/exit/macro signals."""
    # Macro filters: bullish regime (must be TRUE to allow entry)
    macros = {
        'none': pd.Series(True, index=ind['close'].index),
        'price>EMA200': ind['log_close'] > np.log(ind['ema200']),
        'slope_s7>0':  ind['slope_s7'] > 0,
        'price>s7':    ind['log_close'] > ind['s7'],
        'price>EMA50': ind['log_close'] > np.log(ind['ema50']),
    }

    # Entries
    entries = {
        # User's original logic
        'crossunder_EMA200':  cross_under(ind['close'], ind['ema200']),
        # Trend-following variants
        'crossover_EMA200':   cross_over(ind['close'], ind['ema200']),
        'crossover_EMA50':    cross_over(ind['close'], ind['ema50']),
        'crossover_EMA20':    cross_over(ind['close'], ind['ema20']),
        # Wavelet long entries
        'crossover_s5':       cross_over(ind['log_close'], ind['s5']),
        'crossover_s6':       cross_over(ind['log_close'], ind['s6']),
        'crossover_s7':       cross_over(ind['log_close'], ind['s7']),
        'crossunder_s7':      cross_under(ind['log_close'], ind['s7']),  # buy oversold to long-term wavelet
        'crossunder_s6':      cross_under(ind['log_close'], ind['s6']),
        # Wavelet golden cross
        'cross_s5_above_s6':  cross_over(ind['s5'], ind['s6']),
        'cross_s4_above_s5':  cross_over(ind['s4'], ind['s5']),
        # Slope flips
        'slope_s5_pos':       cross_level(ind['slope_s5'], 0, 'above'),
        'slope_s6_pos':       cross_level(ind['slope_s6'], 0, 'above'),
        # RSI
        'rsi_cross_30':       cross_level(ind['rsi14'], 30, 'above'),
        'rsi_cross_50':       cross_level(ind['rsi14'], 50, 'above'),
    }

    # Exits
    exits = {
        'rsi_sma_under70':    cross_under(ind['rsi_sma14'], pd.Series(70, index=ind['rsi_sma14'].index)),
        'rsi_sma_under65':    cross_under(ind['rsi_sma14'], pd.Series(65, index=ind['rsi_sma14'].index)),
        'rsi_sma_under75':    cross_under(ind['rsi_sma14'], pd.Series(75, index=ind['rsi_sma14'].index)),
        'price_below_EMA20':  cross_under(ind['close'], ind['ema20']),
        'price_below_EMA50':  cross_under(ind['close'], ind['ema50']),
        'price_below_s5':     cross_under(ind['log_close'], ind['s5']),
        'slope_s6_negative':  cross_level(ind['slope_s6'], 0, 'below'),
    }

    # Add signals (for pyramid)
    adds = {
        'crossover_EMA20_5':  cross_over(ind['ema20'], ind['ema5']),  # user's logic
        'crossover_EMA5_20':  cross_over(ind['ema5'], ind['ema20']),  # opposite
        'crossover_s4_s2':    cross_over(ind['s4'], ind['s2']),
        'crossover_s2_s4':    cross_over(ind['s2'], ind['s4']),
        'rsi_cross_50':       cross_level(ind['rsi14'], 50, 'above'),
        'none':               pd.Series(False, index=ind['close'].index),  # no add
    }

    return entries, exits, adds, macros


def backtest_weekly_signals(daily, weekly_idx, entry_w, exit_w, add_w, macro_w, pyramid=True):
    """Backtest with weekly signals applied to daily engine."""
    dates = daily.index

    # Forward-fill weekly bool series to daily (with 1-bar lag)
    def to_daily(s):
        return s.shift(1).reindex(dates, method='ffill').fillna(False).astype(bool)

    entry_d = to_daily(entry_w)
    exit_d = to_daily(exit_w)
    add_d = to_daily(add_w)
    macro_d = to_daily(macro_w)

    # Detect transitions (Pine fires once)
    def first_true(s):
        prev = s.shift(1).fillna(False)
        return (s & ~prev).values

    e_strict = first_true(entry_d)
    a_strict = first_true(add_d)
    x_strict = first_true(exit_d)
    macro_arr = macro_d.values

    closes = daily['close'].values
    lows = daily['low'].values
    n = len(closes)
    cash = INITIAL; qty = 0.0
    eq = np.zeros(n); trades = []
    ev = 0; ed = None
    entries_taken = 0
    intra_min = 0

    for i in range(n):
        c = closes[i]
        eq[i] = cash + qty*c
        if qty > 0:
            if intra_min == 0 or lows[i] < intra_min: intra_min = lows[i]

        # Exit
        if qty > 0 and x_strict[i]:
            eff = c * (1 - SLIP)
            proceeds = qty * eff * (1 - FEE)
            cash += proceeds
            pnl = (proceeds/ev - 1) * 100
            mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
            trades.append({'entry_date': ed, 'exit_date': dates[i],
                          'pnl_pct': pnl, 'mae_pct': mae,
                          'hold_d': (dates[i] - ed).total_seconds()/86400})
            qty = 0; ev = 0; ed = None; entries_taken = 0; intra_min = 0
            continue

        # Entry (only if macro filter allows)
        if qty == 0 and e_strict[i] and macro_arr[i]:
            eff = c * (1 + SLIP)
            spend = cash * (0.33 if pyramid else 0.99)
            bq = spend/eff; cost = bq*eff*(1+FEE)
            cash -= cost; qty = bq
            ev = cost; ed = dates[i]; entries_taken = 1
            intra_min = lows[i]
            continue

        # Pyramid add
        if pyramid and qty > 0 and entries_taken == 1 and a_strict[i] and macro_arr[i]:
            eff = c * (1 + SLIP)
            spend = cash * 0.99
            bq = spend/eff; cost = bq*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; qty += bq
                ev += cost; entries_taken = 2

    if qty > 0:
        c = closes[-1]; eff = c * (1 - SLIP)
        proceeds = qty * eff * (1 - FEE)
        cash += proceeds
        pnl = (proceeds/ev - 1) * 100
        mae = (intra_min/(ev/qty) - 1) * 100 if qty > 0 else 0
        trades.append({'entry_date': ed, 'exit_date': dates[-1],
                      'pnl_pct': pnl, 'mae_pct': mae,
                      'hold_d': (dates[-1] - ed).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def quick_metrics(tr, fe, eq):
    if len(tr) == 0:
        return None
    total = (fe/INITIAL - 1) * 100
    dd = ((eq.cummax() - eq)/eq.cummax()).max() * 100
    wr = (tr['pnl_pct']>0).sum()/len(tr) * 100
    mae = tr['mae_pct'].min()
    pos = tr[tr['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(tr[tr['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    eq_26 = eq[eq.index >= pd.Timestamp(2026,1,1)]
    y26 = (eq_26.iloc[-1]/eq_26.iloc[0] - 1)*100 if len(eq_26)>1 else 0
    eq_rec = eq[eq.index >= pd.Timestamp(2022,1,1)]
    rec = (eq_rec.iloc[-1]/eq_rec.iloc[0] - 1)*100 if len(eq_rec)>1 else 0
    return {'N': len(tr), 'WR': wr, 'PF': pf, 'total': total, 'dd': dd,
            'mae': mae, 'rec4y': rec, 'y26': y26}


def main():
    targets = {'total': 1601, 'dd': 20.57, 'WR': 100, 'N': 4}  # user's strategy

    for asset in ['BTC', 'ETH']:
        daily = load_daily(asset)
        weekly = to_weekly(daily)
        ind = compute_indicators(weekly)
        entries, exits, adds, macros = build_signals(ind)

        print(f"\n\n{'='*180}")
        print(f"{asset} — SUPER GRID SEARCH")
        print(f"  User's target: total>{targets['total']}%, DD<{targets['dd']}%, WR={targets['WR']}%, N>{targets['N']}")
        print(f"{'='*180}")

        results = []
        for em_name, em_sig in entries.items():
            for ex_name, ex_sig in exits.items():
                for mac_name, mac_sig in macros.items():
                    # No pyramid first (faster)
                    for ad_name, ad_sig in [('none', adds['none'])]:
                        tr, fe, eq = backtest_weekly_signals(daily, weekly.index,
                                                              em_sig, ex_sig, ad_sig, mac_sig, pyramid=False)
                        m = quick_metrics(tr, fe, eq)
                        if m is None: continue
                        label = f"{em_name} → {ex_name} | macro={mac_name} | no-pyr"
                        results.append({'label': label, **m})
                    # Try pyramid with key adds
                    for ad_name, ad_sig in [('EMA5over20', adds['crossover_EMA5_20']),
                                              ('s2_over_s4', adds['crossover_s2_s4'])]:
                        tr, fe, eq = backtest_weekly_signals(daily, weekly.index,
                                                              em_sig, ex_sig, ad_sig, mac_sig, pyramid=True)
                        m = quick_metrics(tr, fe, eq)
                        if m is None: continue
                        label = f"{em_name} → {ex_name} | macro={mac_name} | pyr+{ad_name}"
                        results.append({'label': label, **m})

        print(f"\n  Total variants tested: {len(results)}")

        # Filter by user's criteria first
        df_res = pd.DataFrame(results)

        # === FILTER 1: Beat user's strategy ===
        filt1 = df_res[(df_res['total'] > 1601) & (df_res['dd'] < 20.57) & (df_res['WR'] >= 95) & (df_res['N'] > 4)]
        print(f"\n  📊 Strict filter (beat user's strategy on ALL metrics): {len(filt1)} variants")
        if len(filt1) > 0:
            top = filt1.sort_values('total', ascending=False).head(15)
            print(f"  {'#':>3}  {'label':80}  {'N':>3}  {'WR':>4}  {'PF':>6}  {'total':>8}  {'DD':>5}  {'rec4y':>6}  {'2026':>7}")
            for i, (_, r) in enumerate(top.iterrows(), 1):
                pf_s = f"{r['PF']:>5.2f}" if r['PF'] != float('inf') else "  inf"
                print(f"  {i:>3}  {r['label']:80}  {int(r['N']):>3}  {r['WR']:>3.0f}%  {pf_s}  {r['total']:>+7.0f}%  {r['dd']:>4.1f}%  {r['rec4y']:>+5.0f}%  {r['y26']:>+6.1f}%")

        # === FILTER 2: Just DD < 20% AND WR > 80% (loose) ===
        filt2 = df_res[(df_res['dd'] < 20) & (df_res['WR'] >= 80) & (df_res['N'] >= 3)]
        print(f"\n  📊 DD<20% + WR>=80%: {len(filt2)} variants")
        if len(filt2) > 0:
            top = filt2.sort_values('total', ascending=False).head(10)
            print(f"  {'#':>3}  {'label':80}  {'N':>3}  {'WR':>4}  {'PF':>6}  {'total':>8}  {'DD':>5}")
            for i, (_, r) in enumerate(top.iterrows(), 1):
                pf_s = f"{r['PF']:>5.2f}" if r['PF'] != float('inf') else "  inf"
                print(f"  {i:>3}  {r['label']:80}  {int(r['N']):>3}  {r['WR']:>3.0f}%  {pf_s}  {r['total']:>+7.0f}%  {r['dd']:>4.1f}%")

        # === FILTER 3: TOP by composite score (return / DD with WR bonus) ===
        df_res['score'] = df_res['total'] / df_res['dd'].replace(0, 0.1) + df_res['WR'] * 5
        top_score = df_res.sort_values('score', ascending=False).head(15)
        print(f"\n  🏆 TOP 15 by score (return/DD + 5*WR):")
        print(f"  {'#':>3}  {'label':80}  {'N':>3}  {'WR':>4}  {'total':>8}  {'DD':>5}  {'rec4y':>6}  {'2026':>7}")
        for i, (_, r) in enumerate(top_score.iterrows(), 1):
            print(f"  {i:>3}  {r['label']:80}  {int(r['N']):>3}  {r['WR']:>3.0f}%  {r['total']:>+7.0f}%  {r['dd']:>4.1f}%  {r['rec4y']:>+5.0f}%  {r['y26']:>+6.1f}%")


if __name__ == "__main__":
    main()
