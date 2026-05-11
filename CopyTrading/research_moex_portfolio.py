"""Portfolio backtest — all 8 RU pairs trading simultaneously.

Capital allocation: equal weight across 8 stocks ($100/8 = $12.5 each).
When signal fires on pair X, capital from X's slot is used (with leverage).
This gives realistic dynamic equity curve, not per-pair-then-average.

Also: same with risk-parity (allocate inverse to ATR — less to volatile stocks).
"""
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_1h"
INITIAL = 100.0
FEE = 0.0005
SLIPPAGE = 0.0003
PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    rng = (h-l).replace(0, np.nan)
    d['lower_wick'] = (pd.concat([c,o], axis=1).min(axis=1) - l) / rng
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14); d['atr_pct'] = atr14 / c * 100; d['atr'] = atr14
    e50 = c.ewm(span=50, adjust=False).mean(); e200 = c.ewm(span=200, adjust=False).mean()
    d['ema_50_200'] = (e50 - e200) / c * 100
    d['ret_4h'] = c.pct_change(4); d['ret_72h'] = c.pct_change(72)
    return d


def signal(d, R):
    j = d.join(R, how='left', rsuffix='_R')
    return ((j['ema_50_200_R']>2.0) & (j['ret_72h_R']>0.02) &
            (j['ema_50_200']>1.0) & (j['ret_4h']<-0.01) &
            (j['atr_pct']>0.5) & (j['lower_wick']>0.25)).fillna(False)


def load(name):
    p = DIR / f'{name.lower()}_1h.csv'
    df = pd.read_csv(p, parse_dates=['datetime']).set_index('datetime').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def portfolio_backtest(leverage=1.0, hold=48, allocation='equal'):
    imoex_f = features(load('imoex'))[['ema_50_200','ret_4h','ret_72h','atr_pct']]
    # Load all pairs
    pair_data = {}
    for p in PAIRS:
        bars = features(load(p))
        sig = signal(bars, imoex_f)
        pair_data[p] = {'bars': bars, 'sig': sig}

    # Master timeline: union of all bar timestamps
    all_idx = sorted(set().union(*[d['bars'].index for d in pair_data.values()]))
    master = pd.DatetimeIndex(all_idx)

    # Capital allocation: each pair gets a slot
    if allocation == 'equal':
        slot_capital = {p: INITIAL / len(PAIRS) for p in PAIRS}
    elif allocation == 'risk_parity':
        # Allocate inverse to median ATR — less to volatile
        median_atr = {p: pair_data[p]['bars']['atr_pct'].median() for p in PAIRS}
        inv = {p: 1/v if v > 0 else 0 for p, v in median_atr.items()}
        s = sum(inv.values())
        slot_capital = {p: INITIAL * inv[p] / s for p in PAIRS}

    # Per-pair state
    state = {p: {'cash': slot_capital[p], 'pos_qty': 0, 'entry': 0, 'held': 0,
                  'notional': 0, 'liquidated': False, 'trades': []} for p in PAIRS}

    # Iterate over master timeline
    eq_curve = []
    eq_dates = []
    for t in master:
        for p in PAIRS:
            s = state[p]
            bars = pair_data[p]['bars']
            if t not in bars.index or s['liquidated']:
                continue
            c = bars['close'].loc[t]
            h = bars['high'].loc[t]
            l = bars['low'].loc[t]
            sig_t = pair_data[p]['sig'].loc[t]

            # Manage open position
            if s['pos_qty'] > 0:
                s['held'] += 1
                # Liquidation check (5% maint margin of notional)
                if s['notional'] > 0:
                    worst_eq = s['cash'] + s['pos_qty'] * l - s['pos_qty'] * s['entry']
                    if worst_eq < s['notional'] * 0.005:
                        # Liquidated
                        s['cash'] = 0
                        s['pos_qty'] = 0
                        s['liquidated'] = True
                        s['trades'].append({'ret_pct': -100, 'exit': 'LIQ'})
                        continue
                if s['held'] >= hold:
                    eff = c * (1 - SLIPPAGE)
                    proceeds = s['pos_qty'] * eff
                    pnl = proceeds - s['pos_qty'] * s['entry']
                    fee = proceeds * FEE
                    s['cash'] += pnl - fee
                    s['trades'].append({'ret_pct': (eff/s['entry']-1)*100*leverage,
                                          'exit': 'TIME', 'time': t})
                    s['pos_qty'] = 0; s['held'] = 0

            # Entry
            if s['pos_qty'] == 0 and sig_t and not s['liquidated'] and s['cash'] > 0:
                eff = c * (1 + SLIPPAGE)
                s['notional'] = s['cash'] * leverage * 0.95
                s['pos_qty'] = s['notional'] / eff
                s['entry'] = eff
                s['cash'] -= s['notional'] * FEE  # entry fee
                s['held'] = 0

        # Mark to market portfolio equity
        total_eq = 0
        for p in PAIRS:
            s = state[p]
            if s['liquidated']:
                continue
            if t in pair_data[p]['bars'].index:
                c_p = pair_data[p]['bars']['close'].loc[t]
            else:
                # use last available
                idx = pair_data[p]['bars'].index
                prev = idx[idx <= t]
                if len(prev) == 0:
                    total_eq += s['cash']
                    continue
                c_p = pair_data[p]['bars']['close'].loc[prev[-1]]
            if s['pos_qty'] > 0:
                pnl_now = s['pos_qty'] * (c_p - s['entry'])
                total_eq += s['cash'] + pnl_now
            else:
                total_eq += s['cash']
        eq_curve.append(total_eq)
        eq_dates.append(t)

    return eq_curve, eq_dates, state


def metrics(eq, dates):
    arr = np.array(eq)
    final = arr[-1]
    total_ret = (final / INITIAL - 1) * 100
    n_years = (dates[-1] - dates[0]).days / 365.25
    cagr = ((final / INITIAL) ** (1/n_years) - 1) * 100 if n_years > 0 and final > 0 else -100
    peak = np.maximum.accumulate(arr)
    dd = (arr / peak - 1) * 100
    return total_ret, cagr, dd.min(), n_years


def main():
    print("=" * 100)
    print("PORTFOLIO backtest — all 8 RU pairs trading simultaneously")
    print("=" * 100)
    print(f"\n{'mode':12}  {'leverage':>8}  {'CAGR%':>7}  {'TotalRet%':>10}  "
          f"{'MaxDD%':>8}  {'Years':>5}  {'final$':>9}  {'liq':>4}")
    print("-" * 80)
    for alloc in ['equal', 'risk_parity']:
        for lev in [1.0, 2.0, 3.0, 5.0]:
            eq, dates, state = portfolio_backtest(leverage=lev, hold=48, allocation=alloc)
            tr, cagr, mdd, ny = metrics(eq, dates)
            n_liq = sum(1 for p in state if state[p]['liquidated'])
            print(f"  {alloc:12}  {lev:>6.1f}x  {cagr:>+6.1f}%  {tr:>+9.1f}%  "
                  f"{mdd:>+7.1f}%  {ny:>5.1f}  ${eq[-1]:>7.2f}  {n_liq:>4}/8")

    # Detailed dump for the most interesting config
    print("\n" + "=" * 100)
    print("DETAILED: equal-weight portfolio @ 3x leverage")
    print("=" * 100)
    eq, dates, state = portfolio_backtest(leverage=3.0, hold=48, allocation='equal')
    df = pd.DataFrame({'eq': eq}, index=dates)
    df['peak'] = df['eq'].cummax()
    df['dd'] = (df['eq'] / df['peak'] - 1) * 100

    # Yearly breakdown
    df['year'] = df.index.year
    print(f"\n  Year-by-year performance:")
    print(f"  {'year':>5}  {'start':>8}  {'end':>8}  {'return%':>8}  {'maxDD%':>7}  {'trades':>6}")
    for yr, grp in df.groupby('year'):
        start = grp['eq'].iloc[0]
        end = grp['eq'].iloc[-1]
        ret = (end / start - 1) * 100
        max_dd_yr = (grp['eq'] / grp['eq'].cummax() - 1).min() * 100
        # count trades in this year
        n_tr = 0
        for p in PAIRS:
            for t in state[p]['trades']:
                if 'time' in t and t['time'].year == yr:
                    n_tr += 1
        print(f"  {yr:>5}  ${start:>6.2f}  ${end:>6.2f}  {ret:>+7.1f}%  {max_dd_yr:>+6.1f}%  {n_tr:>6}")

    # Trade stats overall
    all_tr = []
    for p in PAIRS:
        all_tr.extend(state[p]['trades'])
    tr_df = pd.DataFrame(all_tr)
    if len(tr_df):
        time_exits = tr_df[tr_df['exit']=='TIME']
        print(f"\n  Total trades: {len(tr_df)} ({(tr_df['exit']=='LIQ').sum()} liquidations)")
        print(f"  Win rate (time exits): {(time_exits['ret_pct']>0).mean()*100:.1f}%")
        print(f"  Avg / trade: {time_exits['ret_pct'].mean():+.2f}%")


if __name__ == "__main__":
    main()
