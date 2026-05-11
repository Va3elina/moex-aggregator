"""Parameter sweep for MOEX Cluster 0 setup.

Tests:
1. Hold time: 4, 8, 12, 24, 36, 48, 72, 96, 120 bars
2. Per-condition threshold sensitivity
3. Take-profit / stop-loss combinations
4. Higher timeframe (4H) version
5. Best combined config + walk-forward stability
"""
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from itertools import product
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_1h"
INITIAL = 100.0
FEE = 0.0005
SLIPPAGE = 0.0003

PAIRS = ["AFKS", "SMLT", "POSI", "ASTR", "VKCO", "SOFL", "WUSH", "DELI"]


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
    atr14 = wilder_ma(tr, 14); d['atr_pct'] = atr14 / c * 100; d['atr14'] = atr14
    e50 = c.ewm(span=50, adjust=False).mean(); e200 = c.ewm(span=200, adjust=False).mean()
    d['ema_50_200'] = (e50 - e200) / c * 100
    d['ret_4h'] = c.pct_change(4); d['ret_72h'] = c.pct_change(72)
    return d


def signal(d, R, reg_ema=3.0, reg_ret=0.02, pair_ema=3.0,
           pullback=-0.01, atr_min=0.5, wick=0.25):
    j = d.join(R, how='left', rsuffix='_R')
    return ((j['ema_50_200_R']>reg_ema) & (j['ret_72h_R']>reg_ret) &
            (j['ema_50_200']>pair_ema) & (j['ret_4h']<pullback) &
            (j['atr_pct']>atr_min) & (j['lower_wick']>wick)).fillna(False)


def backtest(bars, sig, hold=24, sl_atr=None, tp_atr=None):
    closes = bars['close'].values; highs = bars['high'].values; lows = bars['low'].values
    atrs = bars['atr14'].values
    cash, pos, ep, eatr, held = INITIAL, 0.0, 0, 0, 0
    trades = []
    for i in range(len(bars)):
        if pos > 0:
            held += 1
            exit_p, exit_t = None, None
            if sl_atr and lows[i] <= ep - sl_atr * eatr:
                exit_p, exit_t = ep - sl_atr * eatr, 'SL'
            elif tp_atr and highs[i] >= ep + tp_atr * eatr:
                exit_p, exit_t = ep + tp_atr * eatr, 'TP'
            elif held >= hold:
                exit_p, exit_t = closes[i], 'TIME'
            if exit_p is not None:
                eff = exit_p * (1 - SLIPPAGE)
                cash += pos * eff * (1 - FEE)
                trades.append({'ret_pct': (eff/ep - 1)*100, 'exit': exit_t})
                pos = 0; held = 0
        if pos == 0 and sig.iloc[i]:
            eff = closes[i] * (1 + SLIPPAGE)
            qty = (cash*0.95)/eff; cost = qty*eff*(1+FEE)
            if cost <= cash:
                cash -= cost; pos = qty; ep = eff; eatr = atrs[i]
    eq = cash + pos * closes[-1]
    return pd.DataFrame(trades), eq


def load(name):
    p = DIR / f'{name.lower()}_1h.csv'
    df = pd.read_csv(p, parse_dates=['datetime']).set_index('datetime').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def aggregate_score(pairs, R_f, hold=24, sl=None, tp=None, **sig_kw):
    """Run across all pairs, return aggregated metrics."""
    total_eq = 0; total_bh = 0; all_t = []; n_pairs = 0
    for p in pairs:
        try:
            bars = features(load(p))
        except FileNotFoundError:
            continue
        sig = signal(bars, R_f, **sig_kw)
        if sig.sum() == 0:
            continue
        tr, eq = backtest(bars, sig, hold, sl, tp)
        if len(tr) == 0:
            continue
        bh = bars['close'].iloc[-1] / bars['close'].iloc[0] * INITIAL
        total_eq += eq; total_bh += bh; all_t.append(tr); n_pairs += 1
    if not all_t or n_pairs == 0:
        return None
    df = pd.concat(all_t)
    n = len(df); wins = (df['ret_pct']>0).sum()
    wr = wins/n*100 if n else 0
    w_pnl = df[df['ret_pct']>0]['ret_pct'].sum()
    l_pnl = abs(df[df['ret_pct']<0]['ret_pct'].sum())
    pf = w_pnl/l_pnl if l_pnl > 0 else float('inf')
    return {
        'n_pairs': n_pairs, 'n_trades': n, 'wr': wr, 'pf': pf,
        'avg': df['ret_pct'].mean(),
        'eq_avg': total_eq / n_pairs,
        'bh_avg': total_bh / n_pairs,
        'edge_vs_bh': (total_eq / n_pairs - total_bh / n_pairs) / INITIAL * 100,
    }


def main():
    imoex_f = features(load('imoex'))[['ema_50_200','ret_4h','ret_72h','atr_pct']]

    # ===== TEST 1: Hold time sweep =====
    print("=" * 100)
    print("TEST 1: Hold time sweep (1H bars)")
    print("=" * 100)
    print(f"  {'hold':>5}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  {'avg%':>6}  "
          f"{'strat$':>8}  {'B&H$':>8}  {'edge_vs_BH':>10}")
    print("  " + "-" * 80)
    for hold in [4, 8, 12, 18, 24, 36, 48, 72, 96, 120, 168]:
        m = aggregate_score(PAIRS, imoex_f, hold=hold)
        if m:
            print(f"  {hold:>5}  {m['n_trades']:>6}  {m['wr']:>4.1f}%  {m['pf']:>4.2f}  "
                  f"{m['avg']:>+5.2f}%  ${m['eq_avg']:>6.2f}  ${m['bh_avg']:>6.2f}  "
                  f"{m['edge_vs_bh']:>+9.1f}pp")

    # ===== TEST 2: Threshold sensitivity (per condition) =====
    print("\n" + "=" * 100)
    print("TEST 2: Threshold sensitivity (hold=24)")
    print("=" * 100)
    sens_grid = {
        'regime_ema': [1.0, 2.0, 3.0, 4.0, 5.0],
        'regime_ret': [0.0, 0.01, 0.02, 0.03, 0.05],
        'pair_ema':   [0.0, 1.0, 3.0, 5.0, 8.0],
        'pullback':   [-0.005, -0.01, -0.015, -0.02, -0.03],
        'atr_min':    [0.3, 0.5, 0.8, 1.0, 1.5],
        'wick':       [0.10, 0.20, 0.25, 0.35, 0.50],
    }
    base_kw = {'reg_ema': 3.0, 'reg_ret': 0.02, 'pair_ema': 3.0,
               'pullback': -0.01, 'atr_min': 0.5, 'wick': 0.25}
    print(f"  {'param':14}  {'value':>6}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  "
          f"{'avg%':>6}  {'edge_vs_BH':>10}")
    print("  " + "-" * 75)
    for param, values in sens_grid.items():
        key_in_signal = {'regime_ema': 'reg_ema', 'regime_ret': 'reg_ret',
                         'pair_ema': 'pair_ema', 'pullback': 'pullback',
                         'atr_min': 'atr_min', 'wick': 'wick'}[param]
        for v in values:
            kw = dict(base_kw); kw[key_in_signal] = v
            m = aggregate_score(PAIRS, imoex_f, hold=24, **kw)
            if m:
                print(f"  {param:14}  {v:>+6.3f}  {m['n_trades']:>6}  {m['wr']:>4.1f}%  "
                      f"{m['pf']:>4.2f}  {m['avg']:>+5.2f}%  {m['edge_vs_bh']:>+9.1f}pp")
        print()

    # ===== TEST 3: SL/TP combinations =====
    print("\n" + "=" * 100)
    print("TEST 3: Stop-Loss / Take-Profit combos (hold=24)")
    print("=" * 100)
    print(f"  {'SL ATR':>6}  {'TP ATR':>6}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  "
          f"{'avg%':>6}  {'edge_vs_BH':>10}  exits")
    print("  " + "-" * 90)
    for sl in [None, 1.0, 1.5, 2.0, 3.0]:
        for tp in [None, 2.0, 3.0, 4.0, 6.0]:
            if sl is None and tp is None and False:
                continue
            m = aggregate_score(PAIRS, imoex_f, hold=24, sl=sl, tp=tp)
            if m:
                # Just show edge
                sl_s = f"{sl}" if sl else "—"
                tp_s = f"{tp}" if tp else "—"
                print(f"  {sl_s:>6}  {tp_s:>6}  {m['n_trades']:>6}  {m['wr']:>4.1f}%  "
                      f"{m['pf']:>4.2f}  {m['avg']:>+5.2f}%  {m['edge_vs_bh']:>+9.1f}pp")

    # ===== TEST 4: Best config — find combination =====
    print("\n" + "=" * 100)
    print("TEST 4: GRID SEARCH best config (looking at top 10)")
    print("=" * 100)
    results = []
    for hold in [12, 18, 24, 36, 48, 72]:
        for r_ema in [2.0, 3.0, 4.0]:
            for p_ema in [1.0, 3.0, 5.0]:
                for pb in [-0.008, -0.012, -0.018, -0.025]:
                    for atr in [0.3, 0.5, 0.8]:
                        m = aggregate_score(PAIRS, imoex_f, hold=hold,
                                              reg_ema=r_ema, pair_ema=p_ema,
                                              pullback=pb, atr_min=atr)
                        if m and m['n_trades'] >= 30:
                            results.append({
                                'hold': hold, 'r_ema': r_ema, 'p_ema': p_ema,
                                'pb': pb, 'atr': atr,
                                'n': m['n_trades'], 'wr': m['wr'],
                                'pf': m['pf'], 'avg': m['avg'],
                                'edge': m['edge_vs_bh']
                            })
    df = pd.DataFrame(results)
    print(f"\n  Top 15 by edge_vs_BH:")
    print(df.nlargest(15, 'edge').to_string(index=False,
        float_format=lambda x: f"{x:+.2f}" if abs(x) < 1000 else f"{x:.0f}"))
    print(f"\n  Top 10 by PF (with min 50 trades):")
    high_n = df[df['n'] >= 50]
    if len(high_n):
        print(high_n.nlargest(10, 'pf').to_string(index=False,
            float_format=lambda x: f"{x:+.2f}" if abs(x) < 1000 else f"{x:.0f}"))

    # ===== TEST 5: Walk-forward on optimal config =====
    print("\n" + "=" * 100)
    print("TEST 5: Walk-forward — check best config by year")
    print("=" * 100)
    if len(df):
        best = df.nlargest(1, 'edge').iloc[0]
        print(f"  Best config: hold={best['hold']}, regime_ema>{best['r_ema']}%, "
              f"pair_ema>{best['p_ema']}%, pullback<{best['pb']*100}%, atr>{best['atr']}%")
        for year in [2022, 2023, 2024, 2025]:
            start = f"{year}-01-01"; end = f"{year+1}-01-01"
            total_eq = 0; total_bh = 0; all_t = []; n_p = 0
            for p in PAIRS:
                try: bars = features(load(p))
                except FileNotFoundError: continue
                m = (bars.index >= start) & (bars.index < end)
                w = bars[m]
                if len(w) < 200: continue
                imo_w = imoex_f
                sig = signal(w, imo_w,
                              reg_ema=best['r_ema'], reg_ret=0.02,
                              pair_ema=best['p_ema'], pullback=best['pb'],
                              atr_min=best['atr'], wick=0.25)
                if sig.sum() == 0: continue
                tr, eq = backtest(w, sig, hold=int(best['hold']))
                if len(tr) == 0: continue
                bh = w['close'].iloc[-1] / w['close'].iloc[0] * INITIAL
                total_eq += eq; total_bh += bh; all_t.append(tr); n_p += 1
            if all_t:
                tdf = pd.concat(all_t)
                wr = (tdf['ret_pct']>0).mean()*100
                print(f"  {year}: {n_p} pairs, {len(tdf)} trades, "
                      f"WR {wr:.1f}%, strat ${total_eq/n_p:.2f}, B&H ${total_bh/n_p:.2f}, "
                      f"edge {(total_eq-total_bh)/n_p:+.1f}")


if __name__ == "__main__":
    main()
