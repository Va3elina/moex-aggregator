"""Pure-signal strategy based on patterns still working in 2022-2026.

LONG entries (proven bullish):
  E1: Price crosses above s5 (still works 2024-2026)
  E2: Extreme overextension up — price > s5 + 2×(s5-s6), STRONGEST recent BTC signal
  E3: Pullback within uptrend (slope6>0, slope5 turns from neg to pos)

LONG exits:
  X1: Price falls back below s5
  X2: slope6 turns negative

LONG blocked when:
  🪤 false_dawn: slope6 turns up while price still under s5 (P(up)=24%)

SHORT entries (proven bearish, ETH-focused):
  ES1: Bearish alignment starts (s5<s6 AND slope6<0 → entering this state)
  ES2: false_dawn detected — slope6 up below s5 (60d mean -16% for BTC, -19% ETH)

SHORT exits:
  XS1: Price rises above s5
  XS2: slope6 turns positive while price > s5

Variants tested:
  V1: pure E1+E2 (long only)
  V2: pure E1+E2+E3 (long only)
  V3: V2 + SHORT on ES1+ES2 (long+short)
  V4: V3 + BTC macro filter (BTC>EMA200_daily for long, < for short)
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

def load_daily(asset):
    name = "btc_usd_daily_coinbase.csv" if asset == 'BTC' else "eth_usd_daily_coinbase.csv"
    df = pd.read_csv(ROOT/name, parse_dates=['date']).set_index('date').sort_index()
    return df[df['close']>0].dropna(subset=['close'])


def precompute(df, btc_daily=None):
    log_c = np.log(df['close'].values)
    sm = atrous_haar(log_c, J=6)
    p = {
        'log_c': log_c, 'close': df['close'].values,
        'high': df['high'].values, 'low': df['low'].values,
        'index': df.index,
        's4': sm[4], 's5': sm[5], 's6': sm[6],
        'sl4': slope_a(sm[4], 5), 'sl5': slope_a(sm[5], 5), 'sl6': slope_a(sm[6], 5),
        'ema200': ema(df['close'].values, 200),
    }
    if btc_daily is not None:
        btc_close = btc_daily['close']
        btc_ema200 = pd.Series(ema(btc_close.values, 200), index=btc_daily.index).shift(1)
        btc_close_shift = btc_close.shift(1)
        p['btc_close'] = btc_close_shift.reindex(df.index, method='ffill').values
        p['btc_ema200'] = btc_ema200.reindex(df.index, method='ffill').values
    return p


def build_signals(p):
    """Compute all entry/exit/block signals as bool arrays."""
    log_c, s4, s5, s6 = p['log_c'], p['s4'], p['s5'], p['s6']
    sl5, sl6 = p['sl5'], p['sl6']
    n = len(log_c)

    # E1: price crosses above s5
    e1 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(log_c[i-1]) or np.isnan(s5[i-1])):
            if log_c[i-1] <= s5[i-1] and log_c[i] > s5[i]:
                e1[i] = True

    # E2: extreme overextension up — price crosses above s5 + 2*(s5-s6)
    upper = s5 + 2 * (s5 - s6)
    e2 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(upper[i-1]) or np.isnan(upper[i])):
            if log_c[i-1] <= upper[i-1] and log_c[i] > upper[i]:
                e2[i] = True

    # E3: pullback within uptrend (slope6>0, slope5 turns from neg to pos)
    e3 = np.zeros(n, dtype=bool)
    prev_sl5 = np.r_[np.nan, sl5[:-1]]
    for i in range(1, n):
        if not (np.isnan(sl5[i]) or np.isnan(sl6[i]) or np.isnan(prev_sl5[i])):
            if sl6[i] > 0 and prev_sl5[i] <= 0 and sl5[i] > 0:
                e3[i] = True

    # X1: price falls below s5
    x1 = log_c < s5
    # X2: slope6 turns negative
    x2 = sl6 < 0

    # F: false dawn — slope6 just turned up while price < s5 (BLOCK long, optional SHORT)
    fdawn = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if not (np.isnan(sl6[i]) or np.isnan(log_c[i]) or np.isnan(s5[i])):
            if sl6[i-1] <= 0 and sl6[i] > 0 and log_c[i] < s5[i]:
                fdawn[i] = True

    # ES1: bearish alignment starts (entering state where s5<s6 AND slope6<0)
    bearish = (s5 < s6) & (sl6 < 0)
    es1 = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if bearish[i] and not bearish[i-1]:
            es1[i] = True

    return {
        'E1_price_above_s5': e1,
        'E2_overextension': e2,
        'E3_pullback': e3,
        'X1_price_below_s5': x1,
        'X2_slope6_neg': x2,
        'F_false_dawn': fdawn,
        'ES1_bearish_align': es1,
        'ES2_false_dawn_short': fdawn,  # same event, used as short trigger
    }


def state_machine_long_short(entry_long, exit_long, entry_short, exit_short, block_long=None, allow_short=True):
    """State: 0=cash, 1=long, -1=short. Returns target array."""
    n = len(entry_long)
    state = np.zeros(n)
    s = 0
    for i in range(n):
        if s == 1:
            if exit_long[i]: s = 0
        elif s == -1:
            if exit_short[i]: s = 0
        if s == 0:
            blocked = block_long[i] if block_long is not None else False
            if entry_long[i] and not blocked:
                s = 1
            elif allow_short and entry_short[i]:
                s = -1
        state[i] = float(s)
    return state


# Strategy variants
def v1_e1_long_only(sigs, p):
    """E1 (price>s5 cross) only, exit on price<s5 or slope6<0."""
    entry = sigs['E1_price_above_s5']
    exit_ = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    no_short = np.zeros(len(entry), dtype=bool)
    return state_machine_long_short(entry, exit_, no_short, no_short, allow_short=False)


def v2_e1_e2_long(sigs, p):
    """E1 + E2 long, normal exits."""
    entry = sigs['E1_price_above_s5'] | sigs['E2_overextension']
    exit_ = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    no_short = np.zeros(len(entry), dtype=bool)
    return state_machine_long_short(entry, exit_, no_short, no_short, allow_short=False)


def v3_e1_e2_e3_long(sigs, p):
    """E1 + E2 + E3 long, normal exits."""
    entry = sigs['E1_price_above_s5'] | sigs['E2_overextension'] | sigs['E3_pullback']
    exit_ = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    no_short = np.zeros(len(entry), dtype=bool)
    return state_machine_long_short(entry, exit_, no_short, no_short, allow_short=False)


def v4_e2_only(sigs, p):
    """Only E2 (overextension) — strongest BTC recent signal."""
    entry = sigs['E2_overextension']
    exit_ = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    no_short = np.zeros(len(entry), dtype=bool)
    return state_machine_long_short(entry, exit_, no_short, no_short, allow_short=False)


def v5_long_short(sigs, p):
    """V3 + SHORT on ES1 (bearish alignment) + ES2 (false dawn)."""
    entry_l = sigs['E1_price_above_s5'] | sigs['E2_overextension'] | sigs['E3_pullback']
    exit_l = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    entry_s = sigs['ES1_bearish_align'] | sigs['ES2_false_dawn_short']
    exit_s = sigs['E1_price_above_s5'] | sigs['E2_overextension']
    block_l = sigs['F_false_dawn']
    return state_machine_long_short(entry_l, exit_l, entry_s, exit_s, block_long=block_l, allow_short=True)


def v6_long_short_macro(sigs, p):
    """V5 + BTC macro filter: long requires BTC>EMA200, short requires BTC<EMA200."""
    entry_l = sigs['E1_price_above_s5'] | sigs['E2_overextension'] | sigs['E3_pullback']
    exit_l = sigs['X1_price_below_s5'] | sigs['X2_slope6_neg']
    entry_s = sigs['ES1_bearish_align'] | sigs['ES2_false_dawn_short']
    exit_s = sigs['E1_price_above_s5'] | sigs['E2_overextension']
    block_l = sigs['F_false_dawn']
    btc_bull = (p['btc_close'] > p['btc_ema200'])
    btc_bear = ~btc_bull
    entry_l = entry_l & btc_bull
    entry_s = entry_s & btc_bear
    return state_machine_long_short(entry_l, exit_l, entry_s, exit_s, block_long=block_l, allow_short=True)


def v7_short_only(sigs, p):
    """SHORT only on ES1+ES2."""
    no_long = np.zeros(len(sigs['E1_price_above_s5']), dtype=bool)
    entry_s = sigs['ES1_bearish_align'] | sigs['ES2_false_dawn_short']
    exit_s = sigs['E1_price_above_s5'] | sigs['E2_overextension']
    return state_machine_long_short(no_long, no_long, entry_s, exit_s, allow_short=True)


# Backtest with leverage L
def backtest(p, target, leverage=1.0):
    closes = p['close']; highs = p['high']; lows = p['low']; dates = p['index']
    n = len(closes)
    cash = INITIAL; units = 0.0; side = 0  # 1 long, -1 short
    eq = np.zeros(n); trades = []
    entry_px = 0.0; entry_date = None
    intra_min = 0; intra_max = 0
    for i in range(n):
        c = closes[i]; h = highs[i]; l = lows[i]
        # Mark to market
        if side == 1:
            mtm = (c - entry_px) * units
            equity = cash + mtm
            if l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        elif side == -1:
            mtm = (entry_px - c) * units
            equity = cash + mtm
            if l < intra_min: intra_min = l
            if h > intra_max: intra_max = h
        else:
            equity = cash
        eq[i] = max(equity, 0.001)

        tgt = target[i]
        # Close if target changed to 0 or opposite
        if side == 1 and tgt != 1:
            eff = c * (1 - SLIP)
            pnl = (eff - entry_px) * units - units * eff * FEE
            cash += pnl
            pnl_pct = (eff/entry_px - 1) * 100 * leverage
            mae = (intra_min/entry_px - 1) * 100 * leverage
            hold = (dates[i] - entry_date).total_seconds()/86400
            trades.append({'side':'long', 'entry_date': entry_date, 'exit_date': dates[i],
                          'pnl_pct': pnl_pct, 'mae_pct': mae, 'hold_d': hold})
            side = 0; units = 0; entry_px = 0
        elif side == -1 and tgt != -1:
            eff = c * (1 + SLIP)
            pnl = (entry_px - eff) * units - units * eff * FEE
            cash += pnl
            pnl_pct = (entry_px/eff - 1) * 100 * leverage
            mae_short = (entry_px/intra_max - 1) * 100 * leverage  # MAE for short is when price rises
            hold = (dates[i] - entry_date).total_seconds()/86400
            trades.append({'side':'short', 'entry_date': entry_date, 'exit_date': dates[i],
                          'pnl_pct': pnl_pct, 'mae_pct': mae_short, 'hold_d': hold})
            side = 0; units = 0; entry_px = 0

        # Open new
        if tgt == 1 and side == 0:
            eff = c * (1 + SLIP)
            units = (cash * 0.99 * leverage) / eff
            fee = units * eff * FEE
            cash -= fee
            entry_px = eff; entry_date = dates[i]; side = 1
            intra_min = l; intra_max = h
        elif tgt == -1 and side == 0:
            eff = c * (1 - SLIP)
            units = (cash * 0.99 * leverage) / eff
            fee = units * eff * FEE
            cash -= fee
            entry_px = eff; entry_date = dates[i]; side = -1
            intra_min = l; intra_max = h

    if side != 0:
        c = closes[-1]
        if side == 1:
            eff = c * (1 - SLIP)
            pnl = (eff - entry_px) * units - units * eff * FEE
        else:
            eff = c * (1 + SLIP)
            pnl = (entry_px - eff) * units - units * eff * FEE
        cash += pnl
        pnl_pct = ((eff/entry_px - 1) if side == 1 else (entry_px/eff - 1)) * 100 * leverage
        trades.append({'side': 'long' if side == 1 else 'short',
                       'entry_date': entry_date, 'exit_date': dates[-1],
                       'pnl_pct': pnl_pct, 'mae_pct': 0, 'hold_d': (dates[-1] - entry_date).total_seconds()/86400})

    return pd.DataFrame(trades), cash, pd.Series(eq, index=dates)


def metrics(trades, final_eq, eq, dates):
    if len(trades) == 0:
        return {'N': 0, 'WR': 0, 'PF': 0, 'total_ret': 0,
                'total_dd': 0, 'mae_worst': 0,
                'year_2026_ret': 0, 'recent_3y_ret': 0,
                'worst_year': 0, 'best_year': 0, 'n_short': 0, 'n_long': 0}
    total_ret = (final_eq / INITIAL - 1) * 100
    total_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100
    wr = (trades['pnl_pct']>0).sum() / len(trades) * 100
    pos = trades[trades['pnl_pct']>0]['pnl_pct'].sum()
    neg = abs(trades[trades['pnl_pct']<0]['pnl_pct'].sum())
    pf = pos/neg if neg > 0 else float('inf')
    mae_worst = trades['mae_pct'].min()
    eq_2026 = eq[eq.index >= pd.Timestamp(2026, 1, 1)]
    year_2026_ret = (eq_2026.iloc[-1]/eq_2026.iloc[0] - 1) * 100 if len(eq_2026) > 1 else 0
    recent_start = pd.Timestamp("2022-01-01")
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
    n_long = (trades['side'] == 'long').sum()
    n_short = (trades['side'] == 'short').sum()
    return {
        'N': len(trades), 'WR': wr, 'PF': pf,
        'total_ret': total_ret, 'total_dd': total_dd,
        'mae_worst': mae_worst,
        'year_2026_ret': year_2026_ret,
        'recent_3y_ret': recent_3y_ret,
        'worst_year': worst_year, 'best_year': best_year,
        'n_short': n_short, 'n_long': n_long,
    }


VARIANTS = [
    ('V1: E1 (price>s5 cross) long only',         v1_e1_long_only),
    ('V2: E1+E2 long',                            v2_e1_e2_long),
    ('V3: E1+E2+E3 long (all bullish)',           v3_e1_e2_e3_long),
    ('V4: E2 only (extreme overextension)',       v4_e2_only),
    ('V5: V3 + SHORT (ES1+ES2)',                  v5_long_short),
    ('V6: V5 + BTC macro filter',                 v6_long_short_macro),
    ('V7: SHORT only (ES1+ES2)',                  v7_short_only),
]


def main():
    btc = load_daily('BTC')
    eth = load_daily('ETH')

    for asset, df in [('BTC', btc), ('ETH', eth)]:
        # Need BTC daily for macro filter (for ETH)
        p = precompute(df, btc_daily=btc if asset == 'ETH' else df)
        sigs = build_signals(p)
        dates = df.index

        # B&H benchmark
        bh = INITIAL / df['close'].iloc[0] * df['close']
        bh_total = (bh.iloc[-1]/INITIAL - 1) * 100
        bh_2026 = bh[bh.index >= pd.Timestamp(2026, 1, 1)]
        bh_2026_ret = (bh_2026.iloc[-1]/bh_2026.iloc[0] - 1) * 100 if len(bh_2026) > 1 else 0
        bh_recent_4y = bh[bh.index >= pd.Timestamp(2022, 1, 1)]
        bh_recent_4y_ret = (bh_recent_4y.iloc[-1]/bh_recent_4y.iloc[0] - 1) * 100 if len(bh_recent_4y) > 1 else 0
        bh_dd = ((bh.cummax() - bh)/bh.cummax()).max() * 100

        print(f"\n{'='*160}")
        print(f"{asset} — pure signal strategy (daily)")
        print(f"  B&H: total={bh_total:+.0f}%, DD={bh_dd:.0f}%, recent4y={bh_recent_4y_ret:+.0f}%, 2026={bh_2026_ret:+.1f}%")
        print(f"{'='*160}")
        print(f"  {'variant':40}  {'N(L/S)':>8}  {'WR%':>5}  {'PF':>6}  {'total%':>8}  {'DD%':>5}  {'mMAE%':>6}  {'recent4y%':>10}  {'2026%':>7}  {'worstY%':>8}  {'bestY%':>8}")
        print("  " + "-" * 155)

        results = []
        for label, fn in VARIANTS:
            target = fn(sigs, p)
            tr, fe, eq = backtest(p, target, leverage=1.0)
            m = metrics(tr, fe, eq, dates)
            results.append({'label': label, 'm': m})
            pf_str = f"{m['PF']:>5.2f}" if m['PF'] != float('inf') else "  inf"
            ns_str = f"{m['n_long']}/{m['n_short']}"
            print(f"  {label:40}  {ns_str:>8}  {m['WR']:>4.1f}  {pf_str}  "
                  f"{m['total_ret']:>+7.0f}%  {m['total_dd']:>4.1f}%  "
                  f"{m['mae_worst']:>+5.1f}%  {m['recent_3y_ret']:>+9.0f}%  "
                  f"{m['year_2026_ret']:>+6.1f}%  {m['worst_year']:>+7.0f}%  {m['best_year']:>+7.0f}%")

        # Per-year breakdown for best variant
        # Pick V5 or V6 (the rich one)
        for vlabel, fn in [('V5 long+short', v5_long_short), ('V6 long+short + BTC macro', v6_long_short_macro)]:
            target = fn(sigs, p)
            tr, fe, eq = backtest(p, target, leverage=1.0)
            print(f"\n  {asset} — per-year for {vlabel}:")
            print(f"    {'year':>4}  {'ret%':>7}  {'DD%':>5}  {'N':>3}  {'L/S':>5}  {'worstT%':>7}  {'worstMAE%':>9}")
            for year in dates.year.unique():
                year_mask = (dates.year == year)
                if year_mask.sum() < 5: continue
                sub_eq = eq[year_mask]
                if len(sub_eq) < 2: continue
                ret = (sub_eq.iloc[-1]/sub_eq.iloc[0] - 1) * 100
                dd = ((sub_eq.cummax() - sub_eq)/sub_eq.cummax()).max() * 100
                year_start = pd.Timestamp(year, 1, 1); year_end = pd.Timestamp(year, 12, 31)
                year_tr = tr[(tr['exit_date'] >= year_start) & (tr['exit_date'] <= year_end)] if len(tr) > 0 else pd.DataFrame()
                n_t = len(year_tr); n_l = (year_tr['side'] == 'long').sum() if n_t else 0; n_s = (year_tr['side'] == 'short').sum() if n_t else 0
                worst_t = year_tr['pnl_pct'].min() if n_t else 0
                worst_mae = year_tr['mae_pct'].min() if n_t else 0
                print(f"    {year}  {ret:>+6.1f}%  {dd:>4.1f}%  {n_t:>3}  {n_l}/{n_s:<3}  {worst_t:>+6.1f}%  {worst_mae:>+8.1f}%")


if __name__ == "__main__":
    main()
