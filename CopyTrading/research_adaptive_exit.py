"""Deep dive on adaptive exits for MOEX momentum.

Hypotheses to test:
1. Volume confirmation: exit on EMA20 break ONLY if volume > avg (filter noise)
2. Adaptive ATR multiplier: tight in choppy, wide in trending
3. Two-stage: wide stop early, tight after profit
4. Profit ceiling: if return > X%, lock in via tight trail
5. Peak detection: exit on bearish divergence (price up, RSI down)
6. Volume anomaly tier: bigger position on tier-3 volume entry
7. "Give back" analysis: how much do losers vs winners give back from peak?
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
    d = bars.copy(); c,h,l,o,v = d['close'],d['high'],d['low'],d['open'],d['volume']
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    d['atr_14'] = wilder_ma(tr, 14)
    d['atr_pct'] = d['atr_14'] / c * 100
    # ADX (trend strength)
    up_move = h - h.shift(1); dn_move = l.shift(1) - l
    plus_dm = pd.Series(np.where((up_move > dn_move) & (up_move > 0), up_move, 0), index=d.index)
    minus_dm = pd.Series(np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0), index=d.index)
    plus_di = 100 * wilder_ma(plus_dm, 14) / d['atr_14']
    minus_di = 100 * wilder_ma(minus_dm, 14) / d['atr_14']
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    d['adx_14'] = wilder_ma(dx, 14)
    # BB
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2*sd20; lo = ma20 - 2*sd20
    d['bb_pct_b'] = (c - lo) / (up - lo)
    # EMAs
    d['ema_20'] = c.ewm(span=20, adjust=False).mean()
    d['ema_50'] = c.ewm(span=50, adjust=False).mean()
    # Volume
    d['vol_ma_20'] = v.rolling(20).mean()
    d['vol_ratio'] = v / d['vol_ma_20']
    # Volume tier (from anomalous volume research)
    d['vol_tier'] = 0
    d.loc[d['vol_ratio'] >= 1.5, 'vol_tier'] = 1
    d.loc[d['vol_ratio'] >= 3.25, 'vol_tier'] = 2
    d.loc[d['vol_ratio'] >= 6.0, 'vol_tier'] = 3
    # Returns
    d['ret_20'] = c.pct_change(20)
    d['pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    return d


def basic_signal(d):
    return ((d['rsi_14'] > 60) & (d['pos_60'] > 0.8) &
            (d['ret_20'] > 0.03) & (d['bb_pct_b'] > 0.5)).fillna(False)


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def track_trade(bars, entry_idx, max_bars=60):
    """Return time-series of trade: per-bar profit, max profit so far, indicators."""
    closes = bars['close'].values; highs = bars['high'].values; lows = bars['low'].values
    ema20 = bars['ema_20'].values; ema50 = bars['ema_50'].values
    rsi = bars['rsi_14'].values; bb = bars['bb_pct_b'].values
    atr = bars['atr_14'].values; vol_ratio = bars['vol_ratio'].values
    adx = bars['adx_14'].values
    entry_price = closes[entry_idx]; entry_atr = atr[entry_idx]; entry_adx = adx[entry_idx]
    rows = []
    max_high = entry_price
    end = min(entry_idx + max_bars + 1, len(bars))
    for j in range(entry_idx + 1, end):
        h, l, c = highs[j], lows[j], closes[j]
        max_high = max(max_high, h)
        rows.append({
            'bars_in': j - entry_idx,
            'close': c, 'high': h, 'low': l,
            'pnl_pct': (c / entry_price - 1) * 100,
            'max_pnl_pct': (max_high / entry_price - 1) * 100,
            'below_ema20': c < ema20[j],
            'below_ema50': c < ema50[j],
            'rsi': rsi[j], 'bb': bb[j],
            'atr_pct': atr[j] / c * 100,
            'vol_ratio': vol_ratio[j],
            'adx': adx[j],
            'dist_to_stop': (l - (max_high - 3 * entry_atr)) / entry_price * 100,
        })
    return pd.DataFrame(rows), entry_atr, entry_adx


def analyze_give_back():
    """For each trade, compute max profit vs final profit.
    What signals would have caught the peak BEFORE giveback?"""
    all_trades = []
    for p in PAIRS:
        bars = features(load(p))
        sig = basic_signal(bars)
        i = 0
        while i < len(bars):
            if sig.iloc[i]:
                trade, e_atr, e_adx = track_trade(bars, i, max_bars=60)
                if len(trade) == 0:
                    i += 1; continue
                # End trade with EMA20 break (current rule)
                exit_bars = trade[trade['below_ema20']]
                if len(exit_bars) > 0:
                    exit_idx_in_trade = exit_bars.index[0]
                else:
                    exit_idx_in_trade = len(trade) - 1
                final_pnl = trade.iloc[exit_idx_in_trade]['pnl_pct']
                max_pnl = trade['max_pnl_pct'].max()
                max_pnl_bar = trade['max_pnl_pct'].idxmax()
                all_trades.append({
                    'pair': p,
                    'entry_idx': i,
                    'final_pnl': final_pnl,
                    'max_pnl': max_pnl,
                    'give_back': max_pnl - final_pnl,
                    'max_pnl_bar': max_pnl_bar,
                    'exit_bar': exit_idx_in_trade,
                    'entry_atr_pct': e_atr / bars['close'].iloc[i] * 100,
                    'entry_adx': e_adx,
                    'entry_vol_ratio': bars['vol_ratio'].iloc[i],
                    'entry_vol_tier': bars['vol_tier'].iloc[i],
                })
                i = i + exit_idx_in_trade + 1
            else:
                i += 1
    return pd.DataFrame(all_trades)


def main():
    print("=" * 100)
    print("PART 1: GIVE-BACK ANALYSIS — how much profit do trades give up from peak?")
    print("=" * 100)
    df = analyze_give_back()
    print(f"\nTotal trades: {len(df)}")
    print(f"  Winners (final>0): {(df['final_pnl']>0).sum()} ({(df['final_pnl']>0).mean()*100:.1f}%)")
    print(f"  Losers (final<0):  {(df['final_pnl']<=0).sum()}")

    print(f"\n  Average max profit reached: {df['max_pnl'].mean():+.2f}%")
    print(f"  Average final profit:       {df['final_pnl'].mean():+.2f}%")
    print(f"  Average give-back from peak: {df['give_back'].mean():+.2f}%")

    winners = df[df['final_pnl'] > 0]
    losers = df[df['final_pnl'] <= 0]
    print(f"\n  WINNERS: avg max_pnl={winners['max_pnl'].mean():+.2f}%, "
          f"final={winners['final_pnl'].mean():+.2f}%, give_back={winners['give_back'].mean():.2f}%")
    print(f"  LOSERS:  avg max_pnl={losers['max_pnl'].mean():+.2f}%, "
          f"final={losers['final_pnl'].mean():+.2f}%, give_back={losers['give_back'].mean():.2f}%")

    # Many losers were once winners
    once_winners = losers[losers['max_pnl'] > 2]
    print(f"\n  Losers that WERE in profit >2% then turned: {len(once_winners)} "
          f"({len(once_winners)/len(losers)*100:.0f}% of losers)")
    print(f"    Average max profit they reached: {once_winners['max_pnl'].mean():+.2f}%")
    print(f"    Average final loss:              {once_winners['final_pnl'].mean():+.2f}%")
    print(f"    → If we exited at max_pnl, we'd save: {once_winners['max_pnl'].mean() - once_winners['final_pnl'].mean():.2f}% per trade")

    # ===== PART 2: entry conditions predict trade quality? =====
    print(f"\n{'='*100}")
    print("PART 2: Does ENTRY context predict trade quality?")
    print(f"{'='*100}")

    # By entry ADX
    print("\n  By ENTRY ADX (trend strength at entry):")
    print(f"  {'ADX range':>15}  {'N':>4}  {'WR%':>5}  {'avg_final':>10}  {'avg_max':>9}  {'avg_give_back':>13}")
    for lo, hi in [(0, 15), (15, 25), (25, 35), (35, 100)]:
        sub = df[(df['entry_adx'] >= lo) & (df['entry_adx'] < hi)]
        if len(sub) < 5: continue
        wr = (sub['final_pnl']>0).mean()*100
        print(f"  {f'{lo}-{hi}':>15}  {len(sub):>4}  {wr:>4.1f}%  "
              f"{sub['final_pnl'].mean():>+9.2f}%  {sub['max_pnl'].mean():>+8.2f}%  "
              f"{sub['give_back'].mean():>+12.2f}%")

    # By entry volume tier
    print("\n  By ENTRY volume tier (анти-объёмы):")
    print(f"  {'tier':>5}  {'N':>4}  {'WR%':>5}  {'avg_final':>10}  {'avg_max':>9}  {'avg_give_back':>13}")
    for t in [0, 1, 2, 3]:
        sub = df[df['entry_vol_tier'] == t]
        if len(sub) < 5: continue
        wr = (sub['final_pnl']>0).mean()*100
        print(f"  {t:>5}  {len(sub):>4}  {wr:>4.1f}%  "
              f"{sub['final_pnl'].mean():>+9.2f}%  {sub['max_pnl'].mean():>+8.2f}%  "
              f"{sub['give_back'].mean():>+12.2f}%")

    # By ATR regime
    print("\n  By entry ATR%:")
    print(f"  {'ATR':>8}  {'N':>4}  {'WR%':>5}  {'avg_final':>10}  {'avg_max':>9}")
    for lo, hi in [(0, 2), (2, 3), (3, 4), (4, 10)]:
        sub = df[(df['entry_atr_pct'] >= lo) & (df['entry_atr_pct'] < hi)]
        if len(sub) < 5: continue
        wr = (sub['final_pnl']>0).mean()*100
        print(f"  {f'{lo}-{hi}%':>8}  {len(sub):>4}  {wr:>4.1f}%  "
              f"{sub['final_pnl'].mean():>+9.2f}%  {sub['max_pnl'].mean():>+8.2f}%")

    # ===== PART 3: profit-ceiling exit =====
    print(f"\n{'='*100}")
    print("PART 3: PROFIT CEILING — if we exited at first time profit reached X%, what's avg?")
    print(f"{'='*100}")
    print(f"\n  Assume: we set TP at X% profit. If reached, exit. If not, fall back to EMA20.")
    for tp in [3, 5, 7, 10, 15, 20]:
        # if max_pnl >= tp, we'd exit at tp. otherwise final_pnl (EMA20 exit)
        new_pnl = df.apply(lambda r: tp if r['max_pnl'] >= tp else r['final_pnl'], axis=1)
        wr = (new_pnl > 0).sum() / len(new_pnl) * 100
        avg = new_pnl.mean()
        print(f"  TP={tp}%: WR={wr:>4.1f}%, avg={avg:+5.2f}%, "
              f"hit_TP={(df['max_pnl']>=tp).sum()} ({(df['max_pnl']>=tp).mean()*100:.0f}%)")

    # ===== PART 4: combined adaptive exit =====
    print(f"\n{'='*100}")
    print("PART 4: ADAPTIVE EXIT — combine: TP ceiling + give-back trailing")
    print(f"{'='*100}")
    print("Rule: lock in TP at +X%. If not hit yet, exit on EMA20 break.")
    print("ALSO: 'give-back' rule — if peak > Y%, exit on 'give back Z%' from peak.\n")

    # Best: combo
    for tp_threshold, giveback_pct in [(5, 50), (5, 30), (8, 40), (10, 30), (15, 30)]:
        gains = []
        for _, r in df.iterrows():
            if r['max_pnl'] >= tp_threshold:
                # We had a profitable trade — apply give-back rule
                # If peak was X%, we exit when price drops giveback_pct% of X (or to break-even)
                lock_in = r['max_pnl'] * (1 - giveback_pct / 100)
                # If final_pnl >= lock_in, then we held to EMA20 exit and got final
                # If final_pnl < lock_in, then give-back rule would have kicked in earlier
                # Conservative estimate: assume we hit at least lock_in (or final, whichever higher)
                gains.append(max(r['final_pnl'], lock_in))
            else:
                gains.append(r['final_pnl'])
        gains = pd.Series(gains)
        wr = (gains > 0).sum() / len(gains) * 100
        print(f"  TP≥{tp_threshold}% + give-back {giveback_pct}% of peak: "
              f"WR={wr:>4.1f}%, avg={gains.mean():+5.2f}%, "
              f"max_loss={gains.min():+5.2f}%, max_win={gains.max():+5.2f}%")


if __name__ == "__main__":
    main()
