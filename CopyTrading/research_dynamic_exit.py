"""Find OPTIMAL DYNAMIC EXIT for MOEX daily momentum.

For each entry signal, test 8 different exit rules:
  1. Fixed time exits: 5/10/15/20/30 days
  2. EMA20 trail (exit when close < EMA20)
  3. ATR chandelier (exit when low < highest_high - N*ATR)
  4. RSI exit (exit when RSI < 50)
  5. BB exit (exit when bb_pct_b < 0.3)
  6. Multi-condition (first weakness signal)
  7. Adaptive: tight stop early, loose later

Compare: WR, avg ret, avg hold time, max DD per trade.
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
    d['atr_14'] = wilder_ma(tr, 14)
    d['atr_pct'] = d['atr_14'] / c * 100
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2*sd20; lo = ma20 - 2*sd20
    d['bb_pct_b'] = (c - lo) / (up - lo)
    d['ema_20'] = c.ewm(span=20, adjust=False).mean()
    d['ret_20'] = c.pct_change(20)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    return d


def basic_signal(d):
    return ((d['rsi_14'] > 60) & (d['pos_60'] > 0.8) &
            (d['ret_20'] > 0.03) & (d['bb_pct_b'] > 0.5)).fillna(False)


def find_exit_idx(bars, entry_idx, exit_rule, max_bars=60, **kw):
    """For a given entry, find when to exit by rule. Returns (exit_idx, exit_price, reason)."""
    closes = bars['close'].values
    highs = bars['high'].values
    lows = bars['low'].values
    ema20 = bars['ema_20'].values
    rsi = bars['rsi_14'].values
    bb = bars['bb_pct_b'].values
    atr = bars['atr_14'].values
    entry_price = closes[entry_idx]
    max_high = entry_price

    end = min(entry_idx + max_bars + 1, len(bars))
    for j in range(entry_idx + 1, end):
        h, l, c = highs[j], lows[j], closes[j]
        max_high = max(max_high, h)

        if exit_rule == 'fixed_5'  and (j - entry_idx) >= 5:   return j, c, 'time'
        if exit_rule == 'fixed_10' and (j - entry_idx) >= 10:  return j, c, 'time'
        if exit_rule == 'fixed_15' and (j - entry_idx) >= 15:  return j, c, 'time'
        if exit_rule == 'fixed_20' and (j - entry_idx) >= 20:  return j, c, 'time'
        if exit_rule == 'fixed_30' and (j - entry_idx) >= 30:  return j, c, 'time'

        # EMA20 trail: exit when close < EMA20
        if exit_rule == 'ema20_trail' and c < ema20[j]:
            return j, c, 'EMA20'

        # ATR chandelier
        if exit_rule.startswith('atr_'):
            mult = float(exit_rule.split('_')[1])
            stop_level = max_high - mult * atr[j]
            if l <= stop_level:
                return j, stop_level, f'ATR{mult}'

        # RSI < 50
        if exit_rule == 'rsi50' and rsi[j] < 50:
            return j, c, 'RSI<50'

        # BB %B < 0.3
        if exit_rule == 'bb30' and bb[j] < 0.3:
            return j, c, 'BB<0.3'

        # Multi: any weakness signal
        if exit_rule == 'multi':
            if c < ema20[j] or rsi[j] < 50 or bb[j] < 0.3:
                return j, c, 'multi'

        # Adaptive: very tight stop first 5 days (locking out crashes early)
        # then chandelier ATR2.5
        if exit_rule == 'adaptive':
            bars_in = j - entry_idx
            if bars_in <= 5:
                # First 5 days: hard stop at -1 ATR from entry
                if l <= entry_price - 1.0 * atr[j]:
                    return j, entry_price - 1.0 * atr[j], 'early_SL'
            else:
                # After: chandelier 2.5 ATR
                stop_level = max_high - 2.5 * atr[j]
                if l <= stop_level:
                    return j, stop_level, 'chandelier'
                # Or break of EMA20
                if c < ema20[j]:
                    return j, c, 'EMA20_late'

    # Max bars reached
    return end - 1, closes[end - 1], 'maxtime'


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def run_with_exit(pairs, exit_rule, max_bars=60):
    all_trades = []
    for p in pairs:
        bars = features(load(p))
        sig = basic_signal(bars)
        in_pos = False
        skip_until = -1
        for i in range(len(bars)):
            if i < skip_until: continue
            if sig.iloc[i] and not in_pos:
                exit_idx, exit_price, reason = find_exit_idx(bars, i, exit_rule, max_bars)
                eff_in = bars['close'].iloc[i] * (1 + SLIPPAGE)
                eff_out = exit_price * (1 - SLIPPAGE)
                pnl_pct = (eff_out / eff_in - 1) * 100
                all_trades.append({
                    'pair': p,
                    'entry_idx': i, 'exit_idx': exit_idx,
                    'entry_date': bars.index[i], 'exit_date': bars.index[exit_idx],
                    'entry_price': eff_in, 'exit_price': eff_out,
                    'hold': exit_idx - i, 'pnl_pct': pnl_pct, 'reason': reason
                })
                skip_until = exit_idx + 1
    return pd.DataFrame(all_trades)


def main():
    print("=" * 100)
    print("DYNAMIC EXIT comparison — same entry rules, different exits")
    print("=" * 100)
    print(f"\n  {'exit rule':14}  {'trades':>6}  {'WR%':>5}  {'PF':>5}  "
          f"{'avg%':>6}  {'avg_hold':>8}  {'med_hold':>8}  {'max_loss':>8}  "
          f"{'max_win':>8}")
    print("  " + "-" * 95)

    rules = [
        'fixed_5', 'fixed_10', 'fixed_15', 'fixed_20', 'fixed_30',
        'ema20_trail', 'atr_2.0', 'atr_2.5', 'atr_3.0',
        'rsi50', 'bb30', 'multi', 'adaptive'
    ]
    results = []
    for rule in rules:
        df = run_with_exit(PAIRS, rule)
        if len(df) == 0: continue
        wr = (df['pnl_pct']>0).sum() / len(df) * 100
        pos = df[df['pnl_pct']>0]['pnl_pct'].sum()
        neg = abs(df[df['pnl_pct']<0]['pnl_pct'].sum())
        pf = pos/neg if neg>0 else float('inf')
        print(f"  {rule:14}  {len(df):>6}  {wr:>4.1f}%  {pf:>4.2f}  "
              f"{df['pnl_pct'].mean():>+5.2f}%  {df['hold'].mean():>7.1f}d  "
              f"{df['hold'].median():>7.0f}d  {df['pnl_pct'].min():>+7.2f}%  "
              f"{df['pnl_pct'].max():>+7.2f}%")
        results.append({'rule': rule, 'n': len(df), 'wr': wr, 'pf': pf,
                          'avg': df['pnl_pct'].mean(),
                          'avg_hold': df['hold'].mean(),
                          'reasons': df['reason'].value_counts().to_dict()})

    # Find best by various metrics
    res = pd.DataFrame(results)
    print(f"\nBest WR: {res.nlargest(1, 'wr').iloc[0]['rule']}")
    print(f"Best PF: {res.nlargest(1, 'pf').iloc[0]['rule']}")
    print(f"Best avg: {res.nlargest(1, 'avg').iloc[0]['rule']}")

    # Detail for top 3 rules
    print("\n" + "=" * 100)
    print("Top 3 by PF — exit reason breakdown:")
    print("=" * 100)
    for _, r in res.nlargest(3, 'pf').iterrows():
        print(f"\n  {r['rule']}:")
        for reason, n in r['reasons'].items():
            print(f"    {reason}: {n}")

    # Compare best rule per-pair
    best_rule = res.nlargest(1, 'pf').iloc[0]['rule']
    print(f"\n{'='*100}")
    print(f"Per-pair detail with best rule '{best_rule}':")
    print(f"{'='*100}")
    df = run_with_exit(PAIRS, best_rule)
    for p in PAIRS:
        sub = df[df['pair'] == p]
        if len(sub) == 0:
            print(f"  {p:6}: 0 trades"); continue
        wr = (sub['pnl_pct']>0).sum() / len(sub) * 100
        print(f"  {p:6}: {len(sub):>3} trades, WR={wr:>4.1f}%, "
              f"avg={sub['pnl_pct'].mean():+5.2f}%, "
              f"avg_hold={sub['hold'].mean():.1f}d, "
              f"max_loss={sub['pnl_pct'].min():+5.2f}%")


if __name__ == "__main__":
    main()
