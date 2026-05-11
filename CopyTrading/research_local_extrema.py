"""Find local minima (pullback bottoms) in BTC 1H during Nov 2023 - now uptrend.
For each local low, capture conditions: ATR, volume, RSI, drawdown depth, candle shape.
Then distinguish "good dips" (followed by rally) from "bad dips" (continued down).
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp

H1_PATH = Path(__file__).parent.parent / "Bitcoin" / "btc_usd_hourly_coinbase.csv"


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars):
    d = bars.copy(); c,h,l,o,v = d['close'],d['high'],d['low'],d['open'],d['volume']
    rng = (h - l).replace(0, np.nan)
    d['lower_wick'] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    d['body_pct'] = (c - o).abs() / rng
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    d['atr_14'] = wilder_ma(tr, 14)
    d['atr_pct'] = d['atr_14'] / c * 100
    d['atr_z_50'] = (d['atr_14'] - d['atr_14'].rolling(50).mean()) / d['atr_14'].rolling(50).std()
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    d['rsi_14'] = 100 - 100/(1 + avg_g/avg_l)
    d['vol_ma_20'] = v.rolling(20).mean()
    d['vol_ratio'] = v / d['vol_ma_20']
    d['ema_50'] = c.ewm(span=50, adjust=False).mean()
    d['ema_200'] = c.ewm(span=200, adjust=False).mean()
    # Drawdown from rolling 24h, 72h, 168h high
    d['dd_from_24h_high'] = (c / h.rolling(24).max() - 1) * 100
    d['dd_from_72h_high'] = (c / h.rolling(72).max() - 1) * 100
    d['dd_from_168h_high'] = (c / h.rolling(168).max() - 1) * 100
    # bars since 72h high
    d['bars_since_72h_high'] = h.rolling(72).apply(lambda x: len(x) - 1 - np.argmax(x.values), raw=False)
    # Price returns
    d['ret_1h'] = c.pct_change()
    d['ret_4h'] = c.pct_change(4)
    d['ret_24h'] = c.pct_change(24)
    return d


def find_local_lows(bars, window=10, min_drawdown_pct=2.0):
    """Local low: bar i such that low[i] is lowest in [i-window, i+window]
    AND drawdown from 72-bar high before i is >= min_drawdown_pct."""
    lows = bars['low'].values
    highs = bars['high'].values
    local_low = np.zeros(len(bars), dtype=bool)
    for i in range(window, len(bars) - window):
        if lows[i] == lows[i-window:i+window+1].min():
            # check drawdown
            prior_high = highs[max(0, i-72):i].max() if i > 0 else lows[i]
            dd = (lows[i] / prior_high - 1) * 100
            if dd <= -min_drawdown_pct:
                local_low[i] = True
    return local_low


def evaluate_local_low(bars, low_idx, forward_window=72):
    """After a local low, did price recover meaningfully?"""
    if low_idx + forward_window >= len(bars):
        return None
    entry_price = bars['close'].iloc[low_idx]
    future = bars.iloc[low_idx+1:low_idx+forward_window+1]
    max_high = future['high'].max()
    final_close = future['close'].iloc[-1]
    return {
        'max_fwd_gain_pct': (max_high / entry_price - 1) * 100,
        'fwd_72h_pct': (final_close / entry_price - 1) * 100,
    }


def load():
    df = pd.read_csv(H1_PATH, parse_dates=['datetime']).set_index('datetime').sort_index()
    df.index = df.index.tz_localize(None)
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def main():
    print("Loading BTC 1H...")
    bars = features(load())
    # Filter Nov 2023 onwards
    bars = bars[bars.index >= '2023-11-01']
    print(f"  {len(bars)} bars from {bars.index[0]} to {bars.index[-1]}")

    # Find local lows
    local_low_mask = find_local_lows(bars, window=10, min_drawdown_pct=2.0)
    n_lows = local_low_mask.sum()
    print(f"\nFound {n_lows} local lows (window=10h, min drawdown 2%)")

    # For each local low, capture features + evaluate forward
    rows = []
    for i in range(len(bars)):
        if not local_low_mask[i]: continue
        ev = evaluate_local_low(bars, i, forward_window=72)
        if ev is None: continue
        row = {
            'datetime': bars.index[i],
            'close': bars['close'].iloc[i],
            'rsi_14': bars['rsi_14'].iloc[i],
            'atr_pct': bars['atr_pct'].iloc[i],
            'atr_z_50': bars['atr_z_50'].iloc[i],
            'vol_ratio': bars['vol_ratio'].iloc[i],
            'lower_wick': bars['lower_wick'].iloc[i],
            'body_pct': bars['body_pct'].iloc[i],
            'dd_24h': bars['dd_from_24h_high'].iloc[i],
            'dd_72h': bars['dd_from_72h_high'].iloc[i],
            'dd_168h': bars['dd_from_168h_high'].iloc[i],
            'bars_since_72h_high': bars['bars_since_72h_high'].iloc[i],
            'ret_4h': bars['ret_4h'].iloc[i] * 100,
            'ret_24h': bars['ret_24h'].iloc[i] * 100,
            'ema_50_pct': (bars['close'].iloc[i] / bars['ema_50'].iloc[i] - 1) * 100,
            'ema_200_pct': (bars['close'].iloc[i] / bars['ema_200'].iloc[i] - 1) * 100,
            **ev
        }
        rows.append(row)

    df_lows = pd.DataFrame(rows)
    df_lows['is_good'] = df_lows['max_fwd_gain_pct'] >= 3.0  # at least +3% rally afterwards
    print(f"\nLocal lows analysed: {len(df_lows)}")
    print(f"  Good (≥3% rally within 72h): {df_lows['is_good'].sum()} ({df_lows['is_good'].mean()*100:.1f}%)")
    print(f"  Bad: {(~df_lows['is_good']).sum()}")

    # Compare good vs bad
    print("\n" + "=" * 100)
    print("Feature distribution at local low: GOOD vs BAD")
    print("=" * 100)
    feats = ['rsi_14','atr_pct','atr_z_50','vol_ratio','lower_wick','body_pct',
             'dd_24h','dd_72h','dd_168h','bars_since_72h_high','ret_4h','ret_24h',
             'ema_50_pct','ema_200_pct']
    print(f"  {'feature':22}  {'GOOD':>8}  {'BAD':>8}  {'diff':>7}  {'t':>6}  {'p':>7}")
    print("  " + "-" * 72)
    good = df_lows[df_lows['is_good']]
    bad = df_lows[~df_lows['is_good']]
    for f in feats:
        gw = good[f].dropna(); bw = bad[f].dropna()
        if len(gw) < 5 or len(bw) < 5: continue
        t, p = sp.ttest_ind(gw, bw, equal_var=False)
        sig = "***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.1 else ""))
        print(f"  {f:22}  {gw.mean():>+7.2f}  {bw.mean():>+7.2f}  "
              f"{gw.mean() - bw.mean():>+6.2f}  {t:>+5.2f}  {p:>6.4f} {sig}")

    # Quartile analysis: what conditions had highest "good" rate
    print("\n" + "=" * 100)
    print("Best signals to predict GOOD local low")
    print("=" * 100)
    # Try filtering combos
    rules = [
        ("RSI < 30",                lambda d: d['rsi_14'] < 30),
        ("RSI < 35",                lambda d: d['rsi_14'] < 35),
        ("vol_ratio > 1.5",         lambda d: d['vol_ratio'] > 1.5),
        ("vol_ratio > 2",           lambda d: d['vol_ratio'] > 2),
        ("atr_z > 0",               lambda d: d['atr_z_50'] > 0),
        ("lower_wick > 0.3",        lambda d: d['lower_wick'] > 0.3),
        ("dd_72h < -5%",            lambda d: d['dd_72h'] < -5),
        ("dd_72h < -8%",            lambda d: d['dd_72h'] < -8),
        ("dd_168h < -10%",          lambda d: d['dd_168h'] < -10),
        ("price > EMA200",          lambda d: d['ema_200_pct'] > 0),
        ("RSI<30 AND vol>1.5",      lambda d: (d['rsi_14']<30) & (d['vol_ratio']>1.5)),
        ("RSI<35 AND vol>2 AND wick>0.3", lambda d: (d['rsi_14']<35) & (d['vol_ratio']>2) & (d['lower_wick']>0.3)),
        ("dd_72h<-5 AND RSI<35 AND price>EMA200", lambda d: (d['dd_72h']<-5) & (d['rsi_14']<35) & (d['ema_200_pct']>0)),
        ("dd_72h<-5 AND RSI<35 AND vol>1.5 AND price>EMA200", lambda d: (d['dd_72h']<-5) & (d['rsi_14']<35) & (d['vol_ratio']>1.5) & (d['ema_200_pct']>0)),
    ]
    print(f"  {'rule':50}  {'N':>4}  {'good%':>6}  {'avg_fwd':>8}  {'med_fwd':>8}")
    base_rate = df_lows['is_good'].mean() * 100
    print(f"  {'(baseline — all local lows)':50}  {len(df_lows):>4}  {base_rate:>5.1f}%  "
          f"{df_lows['max_fwd_gain_pct'].mean():>+7.2f}%  "
          f"{df_lows['max_fwd_gain_pct'].median():>+7.2f}%")
    print("  " + "-" * 90)
    for label, fn in rules:
        sub = df_lows[fn(df_lows)]
        if len(sub) < 5: continue
        gp = sub['is_good'].mean() * 100
        print(f"  {label:50}  {len(sub):>4}  {gp:>5.1f}%  "
              f"{sub['max_fwd_gain_pct'].mean():>+7.2f}%  "
              f"{sub['max_fwd_gain_pct'].median():>+7.2f}%")

    # Save trades for visualization
    out_csv = Path(__file__).parent / "btc_local_lows.csv"
    df_lows.to_csv(out_csv, index=False)
    print(f"\nSaved to {out_csv.name}")


if __name__ == "__main__":
    main()
