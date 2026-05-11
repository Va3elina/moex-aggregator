"""Daily-timeframe research on MOEX 8 stocks.

For each indicator, bin into quantiles, measure forward returns (fwd_5d, fwd_20d).
Find which indicators predict returns on DAILY timeframe.
"""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats as sp

DIR = Path(__file__).parent / "moex_daily"
PAIRS = ['AFKS','SMLT','POSI','ASTR','VKCO','SOFL','WUSH','DELI']
N_BINS = 5
FWD = [1, 5, 10, 20]


def wilder_ma(s, n):
    out = np.full(len(s), np.nan)
    if len(s) < n: return pd.Series(out, index=s.index)
    v = s.values; out[n-1] = np.nanmean(v[:n])
    for i in range(n, len(s)): out[i] = (out[i-1]*(n-1) + v[i]) / n
    return pd.Series(out, index=s.index)


def features(bars, prefix=""):
    d = bars.copy(); c,h,l,o = d['close'],d['high'],d['low'],d['open']
    rng = (h-l).replace(0, np.nan)
    f = pd.DataFrame(index=d.index)
    # Returns
    f[f'{prefix}ret_1'] = c.pct_change()
    f[f'{prefix}ret_3'] = c.pct_change(3)
    f[f'{prefix}ret_5'] = c.pct_change(5)
    f[f'{prefix}ret_10'] = c.pct_change(10)
    f[f'{prefix}ret_20'] = c.pct_change(20)
    f[f'{prefix}ret_60'] = c.pct_change(60)
    # EMAs
    e20 = c.ewm(span=20, adjust=False).mean()
    e50 = c.ewm(span=50, adjust=False).mean()
    e200 = c.ewm(span=200, adjust=False).mean()
    f[f'{prefix}ema_20_50'] = (e20 - e50) / c * 100
    f[f'{prefix}ema_50_200'] = (e50 - e200) / c * 100
    f[f'{prefix}ema_dist_50'] = (c - e50) / c * 100
    f[f'{prefix}ema_dist_200'] = (c - e200) / c * 100
    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_g = wilder_ma(gain, 14); avg_l = wilder_ma(loss, 14)
    f[f'{prefix}rsi_14'] = 100 - 100 / (1 + avg_g/avg_l)
    # ATR
    pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    atr14 = wilder_ma(tr, 14)
    f[f'{prefix}atr_pct'] = atr14 / c * 100
    f[f'{prefix}atr_z_50'] = (atr14 - atr14.rolling(50).mean()) / atr14.rolling(50).std()
    # Bollinger
    ma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
    up = ma20 + 2*sd20; lo = ma20 - 2*sd20
    f[f'{prefix}bb_pct_b'] = (c - lo) / (up - lo)
    f[f'{prefix}bb_width'] = (up - lo) / ma20
    # Candle shape
    f[f'{prefix}lower_wick'] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    f[f'{prefix}body_pct'] = (c - o).abs() / rng
    # Drawdown / position in range
    f[f'{prefix}dd_20'] = c / h.rolling(20).max() - 1
    f[f'{prefix}pos_20'] = (c - l.rolling(20).min()) / (h.rolling(20).max() - l.rolling(20).min())
    f[f'{prefix}pos_60'] = (c - l.rolling(60).min()) / (h.rolling(60).max() - l.rolling(60).min())
    return f


def load(name):
    p = DIR / f'{name.lower()}_d.csv'
    df = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    for c in ['open','high','low','close','volume']: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def build_combined():
    """Combine all 8 pairs into one big dataset with IMOEX regime context."""
    imoex = load('imoex')
    imoex_f = features(imoex, prefix="reg_")
    all_data = []
    for p in PAIRS:
        bars = load(p)
        f = features(bars, prefix="")
        f['close'] = bars['close']
        f['pair'] = p
        # Forward returns
        for k in FWD:
            f[f'fwd_{k}'] = bars['close'].shift(-k) / bars['close'] - 1
        # Join IMOEX regime
        f = f.join(imoex_f, how='left')
        all_data.append(f)
    return pd.concat(all_data, ignore_index=False).dropna(subset=['fwd_5'])


def quantile_analysis(df, feat, fwd_col='fwd_5'):
    valid = df.dropna(subset=[feat, fwd_col])
    if len(valid) < 100: return None
    try:
        valid['bin'] = pd.qcut(valid[feat], N_BINS, labels=False, duplicates='drop')
    except ValueError:
        return None
    out = []
    for b in range(N_BINS):
        cell = valid[valid['bin']==b]
        if len(cell) < 20: continue
        t, p = sp.ttest_ind(cell[fwd_col].dropna(), valid[fwd_col].dropna(), equal_var=False)
        out.append({'bin': b, 'N': len(cell),
                    'lo': valid.loc[valid['bin']==b, feat].min(),
                    'hi': valid.loc[valid['bin']==b, feat].max(),
                    'mean%': cell[fwd_col].mean()*100,
                    'p_up%': (cell['fwd_1']>0).mean()*100,
                    't': t, 'p': p})
    return pd.DataFrame(out)


def main():
    df = build_combined()
    print(f"Combined dataset: {len(df):,} bar-days across 8 pairs")
    print(f"Baseline fwd_5: mean={df['fwd_5'].mean()*100:+.3f}%, "
          f"P(up_1)={(df['fwd_1']>0).mean()*100:.1f}%\n")

    feats_pair = ['ret_1','ret_3','ret_5','ret_10','ret_20','ret_60',
                  'ema_20_50','ema_50_200','ema_dist_50','ema_dist_200',
                  'rsi_14','atr_pct','atr_z_50','bb_pct_b','bb_width',
                  'lower_wick','body_pct','dd_20','pos_20','pos_60']
    feats_reg = ['reg_ret_5','reg_ret_20','reg_ema_50_200','reg_rsi_14',
                 'reg_atr_pct','reg_pos_20']

    print("=" * 110)
    print("PAIR-LEVEL features — Q1 (low) vs Q5 (high) fwd_5 returns")
    print("=" * 110)
    print(f"  {'feature':18}  {'Q1 mean%':>9}  {'Q5 mean%':>9}  {'spread':>7}  "
          f"{'Q1 t':>6}  {'Q5 t':>6}  {'best p':>9}")
    print("  " + "-" * 90)
    for f in feats_pair:
        res = quantile_analysis(df, f)
        if res is None or len(res) < 5: continue
        q1 = res.iloc[0]; q5 = res.iloc[-1]
        spread = q5['mean%'] - q1['mean%']
        best_p = res['p'].min()
        print(f"  {f:18}  {q1['mean%']:>+8.2f}%  {q5['mean%']:>+8.2f}%  "
              f"{spread:>+6.2f}  {q1['t']:>+5.2f}  {q5['t']:>+5.2f}  {best_p:>9.4f}")

    print("\n" + "=" * 110)
    print("IMOEX-REGIME features (context filter for trading individual stocks)")
    print("=" * 110)
    print(f"  {'feature':18}  {'Q1 mean%':>9}  {'Q5 mean%':>9}  {'spread':>7}  "
          f"{'Q1 t':>6}  {'Q5 t':>6}  {'best p':>9}")
    print("  " + "-" * 90)
    for f in feats_reg:
        res = quantile_analysis(df, f)
        if res is None or len(res) < 5: continue
        q1 = res.iloc[0]; q5 = res.iloc[-1]
        spread = q5['mean%'] - q1['mean%']
        best_p = res['p'].min()
        print(f"  {f:18}  {q1['mean%']:>+8.2f}%  {q5['mean%']:>+8.2f}%  "
              f"{spread:>+6.2f}  {q1['t']:>+5.2f}  {q5['t']:>+5.2f}  {best_p:>9.4f}")

    # Top 5 most predictive features (by absolute spread)
    print("\n" + "=" * 110)
    print("TOP-10 most predictive features (largest fwd_5 spread Q5−Q1)")
    print("=" * 110)
    results = []
    for f in feats_pair + feats_reg:
        res = quantile_analysis(df, f)
        if res is None or len(res) < 5: continue
        spread = res.iloc[-1]['mean%'] - res.iloc[0]['mean%']
        results.append({'feat': f, 'spread': spread,
                        'Q1_mean': res.iloc[0]['mean%'],
                        'Q5_mean': res.iloc[-1]['mean%'],
                        'best_p': res['p'].min()})
    results = pd.DataFrame(results).sort_values('spread', key=abs, ascending=False)
    print(results.head(10).to_string(index=False,
            float_format=lambda x: f"{x:+.3f}" if abs(x) < 100 else f"{x:.0f}"))

    # Pearson correlations of top features with forward returns
    print("\n" + "=" * 110)
    print("Pearson correlations of top features with fwd_5 and fwd_20")
    print("=" * 110)
    top_feats = results.head(10)['feat'].tolist()
    for f in top_feats:
        valid = df.dropna(subset=[f, 'fwd_5', 'fwd_20'])
        if len(valid) < 100: continue
        c5 = valid[f].corr(valid['fwd_5'])
        c20 = valid[f].corr(valid['fwd_20'])
        print(f"  {f:18}  fwd_5={c5:+.3f}  fwd_20={c20:+.3f}  N={len(valid):,}")


if __name__ == "__main__":
    main()
