"""Cross-asset robustness: same signals walk-forward on SBER, IMOEX, GAZP.

For each asset, measure train(2020-2022) vs test(2023-2026) 60d tercile edge
for each robust-candidate signal. A signal that is robust ACROSS all 3 assets
AND both periods is a genuine edge.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
SPLIT = pd.Timestamp('2023-01-01')

# (indicator, direction: high=high-bullish, low=low-bullish)
SIGS = [
    ('fiz_lsr', 'low'), ('vrp', 'high'), ('vrp_vs', 'high'), ('rr25', 'high'),
    ('pc_vol_ratio', 'high'), ('pc_oi_ratio', 'high'), ('term_ratio_30_90', 'high'),
    ('gex_norm', 'high'), ('atm_iv', 'high'), ('dollar_gamma', 'high'),
    ('max_pain_dist_pct', 'low'),
]

ASSETS = {
    'SBER': ('sber_option_indicators_daily.csv', 'sber_daily.csv', 'sber_futoi_signals.csv'),
    'IMOEX': ('imoex_option_indicators_daily.csv', None, 'imoex_futoi_history.csv'),
    'GAZP': ('gazp_option_indicators_daily.csv', 'gazp_daily.csv', 'gazp_futoi_signals.csv'),
}


def load_asset(ind_csv, px_csv, futoi_csv):
    p = ROOT/ind_csv
    if not p.exists():
        return None
    ind = pd.read_csv(p, parse_dates=['date']).set_index('date').sort_index()
    # price: prefer indicator's own F if no px file; else px close
    if px_csv and (ROOT/px_csv).exists():
        px = pd.read_csv(ROOT/px_csv, parse_dates=['date']).set_index('date').sort_index()
        close = px['close']
    else:
        # IMOEX: try imoex_d in CopyTrading, else use F from indicators
        alt = ROOT.parent/'CopyTrading'/'moex_daily'/'imoex_d.csv'
        if alt.exists():
            px = pd.read_csv(alt, parse_dates=['date']).set_index('date').sort_index()
            close = px['close']
        else:
            close = ind['F']
    df = pd.DataFrame(index=ind.index)
    df['close'] = close.reindex(ind.index).ffill()
    df = df.join(ind, how='left')
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp'] = df['atm_iv'] - df['rv30']
    if 'varswap_iv' in df.columns:
        df['vrp_vs'] = df['varswap_iv'] - df['rv30']
    # futoi
    if futoi_csv and (ROOT/futoi_csv).exists():
        ft = pd.read_csv(ROOT/futoi_csv, parse_dates=['date']).set_index('date').sort_index()
        if 'fiz_long' in ft.columns:
            df['fiz_lsr'] = (ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    df['fwd60'] = df['close'].pct_change(60).shift(-60)*100
    return df[df['atm_iv'].notna()].copy()


def tercile_edge(seg, col, d):
    if col not in seg.columns:
        return np.nan
    s = seg[[col,'fwd60']].dropna()
    if len(s) < 40 or s[col].nunique() < 3:
        return np.nan
    s['t'] = pd.qcut(s[col].rank(method='first'), 3, labels=['lo','mid','hi'])
    hi=s[s['t']=='hi']['fwd60'].mean(); lo=s[s['t']=='lo']['fwd60'].mean()
    return (hi-lo) if d=='high' else (lo-hi)


def main():
    data = {}
    for name, (i,p,f) in ASSETS.items():
        d = load_asset(i,p,f)
        if d is not None:
            data[name] = d
            print(f"{name}: {d.index[0].date()}→{d.index[-1].date()} ({len(d)}d)")
    print()

    print("="*100)
    print("CROSS-ASSET ROBUSTNESS — 60d tercile edge, TRAIN(20-22) / TEST(23-26)")
    print("Genuine edge = positive in BOTH periods across MULTIPLE assets")
    print("="*100)
    hdr = f"  {'signal':<18}"
    for a in data: hdr += f" {a+' tr':>8} {a+' te':>8}"
    print(hdr)
    for col, d in SIGS:
        line = f"  {col:<18}"
        robust_count = 0
        for a, df in data.items():
            tr = tercile_edge(df[df.index<SPLIT], col, d)
            te = tercile_edge(df[df.index>=SPLIT], col, d)
            line += f" {tr:>+7.1f}{'%' if not np.isnan(tr) else ' '} {te:>+7.1f}{'%' if not np.isnan(te) else ' '}"
            if not np.isnan(tr) and not np.isnan(te) and tr>0 and te>0:
                robust_count += 1
        tag = " ✓ROBUST-ALL" if robust_count==len(data) else (f" ({robust_count}/{len(data)})" if robust_count else " ✗")
        print(line + tag)

    print("\nLegend: ✓ROBUST-ALL = positive train AND test on every asset = genuine cross-asset edge")


if __name__ == "__main__":
    main()
