"""Deep options-only analysis for Si and RTS (active options markets).

User hypothesis: liquid options markets (Si 9.4 bln/day futures, RTS 3.8 bln)
should show DIFFERENT option signal behavior vs single-stock SBER.

For each asset, test all ~15 option indicators in walk-forward (train 2020-22
/ test 2023-26). Identify per-asset robust signals (different from SBER).
Then build asset-specific multi-signal strategies.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')

# All option signals to test + direction hypothesis (high or low = bullish)
OPTION_SIGS = [
    ('vrp', 'high'),              # ATM IV - RV
    ('varswap_iv', 'high'),       # variance swap rate
    ('atm_iv', 'high'),           # raw IV level
    ('rr25', 'high'),             # 25-delta risk reversal (skew)
    ('bf25', 'high'),             # butterfly
    ('skew_slope', 'high'),       # smile slope
    ('term_ratio_30_90', 'high'), # IV30/IV90
    ('gex_norm', 'high'),         # GEX
    ('dollar_gamma', 'high'),
    ('pc_oi_ratio', 'high'),
    ('pc_vol_ratio', 'high'),
    ('max_pain_dist_pct', 'low'),
    ('straddle_move_pct', 'low'),
    ('rn_p_down10', 'high'),
    ('rn_p_up10', 'low'),
    ('rn_skew', 'high'),
]


def load(name, ind_csv, px_csv, futoi_csv):
    ind = pd.read_csv(ROOT/ind_csv, parse_dates=['date']).set_index('date').sort_index()
    px = pd.read_csv(ROOT/px_csv, parse_dates=['date']).set_index('date')['close']
    df = pd.DataFrame(index=ind.index)
    df['close'] = px.reindex(ind.index).ffill()
    df = df.join(ind, how='left')
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp'] = df['atm_iv'] - df['rv30']
    if 'varswap_iv' in df.columns:
        df['vrp_vs'] = df['varswap_iv'] - df['rv30']
    ft = pd.read_csv(ROOT/futoi_csv, parse_dates=['date']).set_index('date')
    df['fiz_lsr'] = (ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    return df[df['atm_iv'].notna()].copy()


def tercile_edge(seg, col, direction):
    if col not in seg.columns: return np.nan
    s = seg[[col, 'fwd60']].dropna()
    if len(s) < 40 or s[col].nunique() < 3: return np.nan
    s['t'] = pd.qcut(s[col].rank(method='first'), 3, labels=['lo','mid','hi'])
    hi = s[s['t']=='hi']['fwd60'].mean()
    lo = s[s['t']=='lo']['fwd60'].mean()
    return (hi-lo) if direction=='high' else (lo-hi)


def bt(df, em, xm, minh=10, maxh=90, stop=None, lev=1.0):
    c = df['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(df)
    cash=INIT; eq=np.full(n,INIT,float); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            chg=c[i]/ep-1; eq[i]=cash*(1+chg*lev); h=i-ei; do=False
            if stop is not None and chg<=-stop: do=True
            elif h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=(c[i]*(1-SLIP)/ep-1)*lev-2*FEE; cash*=(1+r)
                tr.append((df.index[i], r*100)); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP); inp=True
    s=pd.Series(eq, index=df.index)
    tot=(s.iloc[-1]/INIT-1)*100
    dd=((s.cummax()-s)/s.cummax()).max()*100
    nt=len(tr); wr=(np.array([x[1] for x in tr])>0).mean()*100 if tr else 0
    pos=sum(x[1] for x in tr if x[1]>0); neg=-sum(x[1] for x in tr if x[1]<0)
    pf=pos/neg if neg>0 else 9.99
    yr={}
    for y in range(2020,2027):
        seg=s[s.index.year==y]
        if len(seg)>1: yr[y]=(seg.iloc[-1]/seg.iloc[0]-1)*100
    return tot, dd, nt, wr, pf, yr, s


def threshold_train(train_df, col, direction, quantile=0.7):
    """Compute threshold from train: top-30% if high-bullish, bottom-30% if low."""
    q = quantile if direction == 'high' else 1-quantile
    return train_df[col].quantile(q)


def analyze_asset(name, df):
    print(f"\n{'='*100}")
    print(f"  {name}: {df.index[0].date()}→{df.index[-1].date()} ({len(df)}d) B&H {(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100:+.0f}%")
    print('='*100)

    df['fwd60'] = df['close'].pct_change(60).shift(-60)*100
    train = df[df.index < SPLIT]
    test  = df[df.index >= SPLIT]

    # Walk-forward edge per signal
    print(f"\n  60d tercile edge (train / test), ROBUST = positive both:")
    print(f"  {'signal':<22} {'train':>8} {'test':>8}  verdict")
    robust = []
    for col, d in OPTION_SIGS:
        if col not in df.columns: continue
        tr_e = tercile_edge(train, col, d)
        te_e = tercile_edge(test, col, d)
        if np.isnan(tr_e) or np.isnan(te_e):
            v = 'n/a'
        elif tr_e > 0 and te_e > 0:
            v = 'ROBUST'; robust.append((col, d, tr_e, te_e))
        elif tr_e > 0 and te_e <= 0:
            v = 'fails-OOS'
        else:
            v = 'inconsist'
        print(f"  {col:<22} {tr_e:>+7.1f}% {te_e:>+7.1f}%  {v}")

    if not robust:
        print(f"\n  No robust pure-options signals for {name}")
        return None

    print(f"\n  ROBUST signals on {name}: {[r[0] for r in robust]}")

    # Backtest each robust signal standalone on TEST (true OOS, threshold from TRAIN)
    print(f"\n  Standalone backtests on TEST period (threshold from TRAIN):")
    print(f"  {'signal':<22} {'tot':>7} {'DD':>5} {'N':>4} {'WR':>5} {'PF':>6}  per-year (test)")
    for col, d, _, _ in robust:
        thr = threshold_train(train, col, d, 0.7)
        em = (df[col] >= thr) if d == 'high' else (df[col] <= thr)
        xm = ~em
        # Use full df with stop, evaluate test
        tot, dd, nt, wr, pf, yr, eq = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 0.08, 1.0)
        # split equity at SPLIT to get test-only
        test_eq = eq[eq.index >= SPLIT]
        if len(test_eq) > 1:
            test_tot = (test_eq.iloc[-1]/test_eq.iloc[0]-1)*100
            test_dd = ((test_eq.cummax()-test_eq)/test_eq.cummax()).max()*100
        else:
            test_tot = test_dd = 0
        ys = ' '.join(f"{y%100}:{v:+.0f}" for y,v in yr.items() if y>=2023)
        print(f"  {col:<22} {test_tot:>+6.0f}% {test_dd:>4.0f}% {nt:>4} {wr:>4.0f}% {pf:>5.2f}  {ys}")

    # Build multi-signal ensemble (use TRAIN thresholds, vote on TEST)
    if len(robust) >= 2:
        print(f"\n  ENSEMBLE: LONG when N+ robust signals fire (TRAIN thresholds applied to FULL period):")
        votes = pd.DataFrame(index=df.index)
        for col, d, _, _ in robust:
            thr = threshold_train(train, col, d, 0.7)
            v = (df[col] >= thr) if d == 'high' else (df[col] <= thr)
            votes[col] = v.fillna(False)
        vote_count = votes.sum(axis=1)
        for vmin in [1, 2, 3, len(robust)//2+1]:
            em = (vote_count >= vmin)
            xm = (vote_count < vmin)
            tot, dd, nt, wr, pf, yr, _ = bt(df, em, xm, 10, 90, 0.08, 1.0)
            ys = ' '.join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
            print(f"  votes>={vmin:<2}  tot={tot:>+5.0f}% DD={dd:>3.0f}% N={nt:>3} WR={wr:>3.0f}% PF={pf:.2f}  {ys}")

    return robust


def main():
    assets = {
        'Si':  ('si_option_indicators_daily.csv',  'si_daily.csv',  'si_futoi_history.csv'),
        'RTS': ('rts_option_indicators_daily.csv', 'rts_daily.csv', 'rts_futoi_history.csv'),
        'SBER':('sber_option_indicators_daily.csv','sber_daily.csv','sber_futoi_history.csv'),  # reference
    }
    results = {}
    for name, (i,p,f) in assets.items():
        df = load(name, i, p, f)
        if df is not None and len(df) > 100:
            results[name] = analyze_asset(name, df)

    # Cross-asset summary
    print(f"\n{'='*100}\n  CROSS-ASSET ROBUST OPTION SIGNALS\n{'='*100}")
    all_sigs = set()
    for name, r in results.items():
        if r:
            for s, *_ in r: all_sigs.add(s)
    for s in sorted(all_sigs):
        in_assets = [n for n,r in results.items() if r and any(x[0]==s for x in r)]
        print(f"  {s:<22} {'/'.join(in_assets)}")


if __name__ == "__main__":
    main()
