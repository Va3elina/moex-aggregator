"""Forward-return quintile analysis for ALL GAZP option indicators.

For each indicator, bucket days into quintiles and measure forward returns
(20d, 60d) + P(up). Identifies which indicators carry directional edge.
Also computes monotonicity score (does Q1→Q5 trend cleanly?).

Combines option indicators + futoi FIZ + basis into one master frame.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent


def load_master():
    ind = pd.read_csv(ROOT/'gazp_option_indicators_daily.csv', parse_dates=['date']).set_index('date')
    gazp = pd.read_csv(ROOT/'gazp_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    futoi = pd.read_csv(ROOT/'gazp_futoi_signals.csv', parse_dates=['date']).set_index('date').sort_index()

    df = gazp[['close']].copy()
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std()*np.sqrt(252)*100
    df = df.join(ind, how='left')
    df['fiz_lsr'] = (futoi['fiz_long']/futoi['fiz_short'].replace(0,1)).reindex(df.index)
    df['yur_lsr'] = (futoi['yur_long']/futoi['yur_short'].replace(0,1)).reindex(df.index)
    # derived
    df['vrp'] = df['atm_iv'] - df['rv30']
    if 'varswap_iv' in df.columns:
        df['vrp_vs'] = df['varswap_iv'] - df['rv30']
    df = df[df['atm_iv'].notna()].copy()
    for h in [20, 60]:
        df[f'fwd{h}'] = df['close'].pct_change(h).shift(-h)*100
    return df


def quintile_table(df, col, n=5):
    s = df[[col, 'fwd20', 'fwd60']].dropna(subset=[col])
    if len(s) < 50 or s[col].nunique() < n:
        return None
    try:
        s['q'] = pd.qcut(s[col].rank(method='first'), n, labels=range(1, n+1))
    except Exception:
        return None
    rows = []
    for q in range(1, n+1):
        sub = s[s['q'] == q]
        r20 = sub['fwd20'].dropna(); r60 = sub['fwd60'].dropna()
        rows.append({
            'q': q, 'lo': sub[col].min(), 'hi': sub[col].max(), 'n': len(sub),
            'r20': r20.mean(), 'p20': (r20 > 0).mean()*100,
            'r60': r60.mean(), 'p60': (r60 > 0).mean()*100,
        })
    return pd.DataFrame(rows)


def edge_score(qt):
    """Spread between Q5 and Q1 60d return + monotonicity."""
    if qt is None:
        return 0, 0
    spread = qt.iloc[-1]['r60'] - qt.iloc[0]['r60']
    # monotonicity: correlation of q with r60
    mono = np.corrcoef(qt['q'], qt['r60'])[0, 1]
    return spread, mono


def main():
    df = load_master()
    print(f"Master frame: {df.index[0].date()} → {df.index[-1].date()}  ({len(df)} days)")
    bh = (df['close'].iloc[-1]/df['close'].iloc[0]-1)*100
    print(f"GAZP B&H: {bh:+.0f}%\n")

    indicators = [
        'atm_iv', 'vrp', 'rr25', 'bf25', 'skew_slope', 'term_ratio_30_90',
        'gex_norm', 'dollar_gamma', 'max_pain_dist_pct', 'straddle_move_pct',
        'pc_oi_ratio', 'pc_vol_ratio', 'fiz_lsr', 'yur_lsr',
    ]
    for opt in ['varswap_iv', 'vrp_vs', 'rn_p_down10', 'rn_p_up10', 'rn_skew']:
        if opt in df.columns and df[opt].notna().sum() > 50:
            indicators.append(opt)

    # Rank indicators by edge
    scored = []
    for col in indicators:
        if col not in df.columns:
            continue
        qt = quintile_table(df, col)
        spread, mono = edge_score(qt)
        scored.append((col, spread, mono, qt))
    scored.sort(key=lambda x: abs(x[1]), reverse=True)

    print("="*100)
    print("INDICATOR RANKING by |Q5-Q1 60d return spread| (directional edge)")
    print("="*100)
    print(f"  {'indicator':<20} {'Q5-Q1 60d':>10} {'monotonic':>10}  {'direction':>20}")
    for col, spread, mono, qt in scored:
        direction = "high→bullish" if spread > 0 else "low→bullish"
        print(f"  {col:<20} {spread:>+9.1f}% {mono:>+9.2f}  {direction:>20}")

    # Detailed tables for top 6
    print("\n" + "="*100)
    print("DETAILED QUINTILE TABLES (top 6 by edge)")
    print("="*100)
    for col, spread, mono, qt in scored[:6]:
        if qt is None:
            continue
        print(f"\n{col} (Q5-Q1={spread:+.1f}%, mono={mono:+.2f}):")
        print(f"  {'Q':<3} {'range':<20} {'n':>4} {'r20':>8} {'p20':>6} {'r60':>8} {'p60':>6}")
        for _, r in qt.iterrows():
            rng = f"[{r['lo']:.1f},{r['hi']:.1f}]"
            print(f"  {int(r['q']):<3} {rng:<20} {int(r['n']):>4} "
                  f"{r['r20']:>+7.1f}% {r['p20']:>5.0f}% {r['r60']:>+7.1f}% {r['p60']:>5.0f}%")

    df.to_csv(ROOT/'gazp_master_features.csv')
    print(f"\nSaved master features → gazp_master_features.csv")


if __name__ == "__main__":
    main()
