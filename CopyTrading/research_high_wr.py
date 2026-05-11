"""Grid search for HIGH WIN RATE configurations on MOEX daily momentum.

Goal: WR 70-80%, medium-to-low frequency (10-50 trades/year per pair).
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd

DIR = Path(__file__).parent / "moex_daily"
INITIAL = 100.0; FEE = 0.0005; SLIPPAGE = 0.0003
PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14)
    d['atr_pct'] = atr14 / c * 100
    d['atr_z_50'] = (atr14 - atr14.rolling(50).mean()) / atr14.rolling(50).std()
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2*sd20; lo = ma20 - 2*sd20
    d['bb_pct_b'] = (c - lo) / (up - lo)
    d['ret_20'] = c.pct_change(20)
    d['ret_60'] = c.pct_change(60)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    # EMA alignment
    e20 = c.ewm(span=20, adjust=False).mean()
    e50 = c.ewm(span=50, adjust=False).mean()
    e200 = c.ewm(span=200, adjust=False).mean()
    d['ema_20_above_50'] = (e20 > e50).astype(int)
    d['ema_50_above_200'] = (e50 > e200).astype(int)
    d['all_emas_aligned'] = (d['ema_20_above_50'] & d['ema_50_above_200']).astype(int)
    return d


def signal(d, rsi_min=70, pos_min=0.85, ret20_min=0.05, ret60_min=0.10,
           bb_min=0.7, atr_z_min=0.5, require_aligned=True):
    sig = ((d['rsi_14'] > rsi_min) & (d['pos_60'] > pos_min) &
            (d['ret_20'] > ret20_min) & (d['ret_60'] > ret60_min) &
            (d['bb_pct_b'] > bb_min) & (d['atr_z_50'] > atr_z_min))
    if require_aligned:
        sig = sig & (d['all_emas_aligned'] == 1)
    return sig.fillna(False)


def backtest(bars, sig, hold=15):
    """Leverage-style accounting (cash represents equity, position off-balance)."""
    closes = bars['close'].values; times = bars.index
    cash, pos, ep, held, notional = INITIAL, 0.0, 0, 0, 0
    trades = []
    for i in range(len(bars)):
        if pos > 0:
            held += 1
            if held >= hold:
                eff = closes[i] * (1 - SLIPPAGE)
                proceeds = pos * eff
                pnl = proceeds - pos * ep
                cash += pnl - proceeds * FEE
                trades.append({'entry': times[entry_i], 'exit': times[i],
                                'price_pct': (eff/ep - 1)*100})
                pos = 0; held = 0
        if pos == 0 and sig.iloc[i]:
            eff = closes[i] * (1 + SLIPPAGE)
            notional = cash * 0.95
            pos = notional / eff
            ep = eff; entry_i = i
            cash -= notional * FEE
    eq = cash + (pos * (closes[-1] - ep) if pos > 0 else 0)
    return pd.DataFrame(trades), eq


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def aggregate(pairs, hold=15, **sig_kw):
    total_eq = 0; total_bh = 0; all_t = []; n_p = 0
    for p in pairs:
        bars = features(load(p))
        sig = signal(bars, **sig_kw)
        tr, eq = backtest(bars, sig, hold=hold)
        if len(tr) == 0: continue
        bh = bars['close'].iloc[-1] / bars['close'].iloc[0] * INITIAL
        total_eq += eq; total_bh += bh; all_t.append(tr); n_p += 1
    if not all_t: return None
    df = pd.concat(all_t); n = len(df)
    wins = (df['price_pct']>0).sum()
    pos_pnl = df[df['price_pct']>0]['price_pct'].sum()
    neg_pnl = abs(df[df['price_pct']<0]['price_pct'].sum())
    return {'n_pairs': n_p, 'n_trades': n,
            'wr': wins/n*100 if n else 0,
            'pf': pos_pnl/neg_pnl if neg_pnl>0 else float('inf'),
            'avg': df['price_pct'].mean(),
            'eq_avg': total_eq/n_p, 'bh_avg': total_bh/n_p,
            'edge': (total_eq - total_bh)/n_p,
            'trades': df}


def main():
    print("=" * 100)
    print("HIGH-WR GRID SEARCH — momentum + ALL filters strict")
    print("=" * 100)

    results = []
    for hold in [10, 15, 20]:
        for rsi in [60, 65, 70, 75]:
            for pos in [0.7, 0.8, 0.9, 0.95]:
                for ret20 in [0.02, 0.05, 0.10]:
                    for ret60 in [0.0, 0.05, 0.10, 0.20]:
                        for aligned in [True, False]:
                            m = aggregate(PAIRS, hold=hold, rsi_min=rsi,
                                           pos_min=pos, ret20_min=ret20,
                                           ret60_min=ret60, bb_min=0.5,
                                           atr_z_min=-1.0,
                                           require_aligned=aligned)
                            if m and m['n_trades'] >= 20:
                                results.append({
                                    'hold': hold, 'rsi': rsi, 'pos': pos,
                                    'r20': ret20, 'r60': ret60, 'aligned': aligned,
                                    'n': m['n_trades'], 'wr': m['wr'],
                                    'pf': m['pf'], 'avg': m['avg'],
                                    'edge': m['edge']
                                })

    df = pd.DataFrame(results)
    print(f"\n  Total configs tested: {len(df)}\n")

    # Filter for WR >= 70%
    high_wr = df[df['wr'] >= 70]
    print(f"  Configs with WR >= 70%: {len(high_wr)}")
    print(f"  Configs with WR >= 75%: {(df['wr']>=75).sum()}")
    print(f"  Configs with WR >= 80%: {(df['wr']>=80).sum()}")

    # Top by WR among high-WR
    print(f"\nTop 15 by WR (with min 20 trades):")
    top_wr = df.nlargest(15, 'wr')
    print(top_wr.to_string(index=False, float_format=lambda x: f"{x:+.2f}"))

    # Top 15 by edge for WR >= 65%
    print(f"\nTop 15 by EDGE with WR >= 65%:")
    if len(df[df['wr']>=65]):
        print(df[df['wr']>=65].nlargest(15, 'edge').to_string(
            index=False, float_format=lambda x: f"{x:+.2f}"))

    # Best WR-edge balance: WR >= 70 AND highest edge
    if len(high_wr):
        best = high_wr.nlargest(1, 'edge').iloc[0]
        print(f"\nBEST high-WR config:")
        print(f"  hold={best['hold']}, rsi>{best['rsi']}, pos>{best['pos']}, "
              f"ret20>{best['r20']*100:.0f}%, ret60>{best['r60']*100:.0f}%, "
              f"aligned={best['aligned']}")
        print(f"  → trades={int(best['n'])}, WR={best['wr']:.1f}%, "
              f"PF={best['pf']:.2f}, avg={best['avg']:+.2f}%/trade, edge={best['edge']:+.1f}pp")

        # Per-pair detail
        print(f"\nPer-pair detail with this config:")
        for p in PAIRS:
            bars = features(load(p))
            sig = signal(bars, rsi_min=best['rsi'], pos_min=best['pos'],
                          ret20_min=best['r20'], ret60_min=best['r60'],
                          bb_min=0.5, atr_z_min=-1.0,
                          require_aligned=best['aligned'])
            tr, eq = backtest(bars, sig, hold=int(best['hold']))
            if len(tr) == 0:
                print(f"  {p:6}: 0 trades"); continue
            wr = (tr['price_pct']>0).sum()/len(tr)*100
            bh = bars['close'].iloc[-1]/bars['close'].iloc[0]*INITIAL
            yrs = (bars.index[-1] - bars.index[0]).days / 365.25
            per_yr = len(tr) / yrs
            print(f"  {p:6}: {len(tr):>3} trades ({per_yr:.1f}/yr), WR={wr:>4.1f}%, "
                  f"avg={tr['price_pct'].mean():+5.2f}%, eq=${eq:>6.2f}, B&H=${bh:>6.2f}")


if __name__ == "__main__":
    main()
