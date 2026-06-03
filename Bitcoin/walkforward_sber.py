"""Walk-forward / out-of-sample validation for SBER signals.

Split: TRAIN 2020-01 → 2022-12  (includes COVID + war crash)
       TEST  2023-01 → 2026-05  (recovery + new regime)

For each signal, compute forward-return edge separately on train and test.
A robust signal shows the SAME directional edge in BOTH periods.
Also runs the actual strategies on test-only to check live-like performance.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INITIAL = 100.0
FEE = 0.0003
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')


def load():
    df = pd.read_csv(ROOT/'sber_master_features.csv', parse_dates=['date']).set_index('date')
    df = df[df['atm_iv'].notna()].copy()
    for h in [20, 60]:
        df[f'fwd{h}'] = df['close'].pct_change(h).shift(-h)*100
    return df


def edge_split(df, col, direction):
    """direction: 'high' = high values bullish, 'low' = low bullish.
    Returns (train_60d_edge, test_60d_edge) measured as top-tercile minus bottom-tercile."""
    out = {}
    for name, seg in [('train', df[df.index < SPLIT]), ('test', df[df.index >= SPLIT])]:
        s = seg[[col, 'fwd60']].dropna()
        if len(s) < 60:
            out[name] = np.nan; continue
        try:
            s['t'] = pd.qcut(s[col].rank(method='first'), 3, labels=['lo','mid','hi'])
        except Exception:
            out[name] = np.nan; continue
        hi = s[s['t']=='hi']['fwd60'].mean()
        lo = s[s['t']=='lo']['fwd60'].mean()
        edge = (hi - lo) if direction == 'high' else (lo - hi)
        out[name] = edge
    return out.get('train', np.nan), out.get('test', np.nan)


def backtest(df, em, xm, min_hold=10, max_hold=60):
    closes=df['close'].values; dates=df.index; n=len(df)
    em=em.values; xm=xm.values
    cash=INITIAL; eq=np.full(n,INITIAL); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        c=closes[i]
        if inp:
            eq[i]=cash*(c/ep); h=i-ei; do=False
            if h>=max_hold: do=True
            elif h>=min_hold and xm[i]: do=True
            if do:
                xp=c*(1-SLIP); r=xp/ep-1-2*FEE; cash*=(1+r)
                tr.append({'pnl':r*100,'entry':dates[ei],'exit':dates[i]}); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c*(1+SLIP); inp=True
    return pd.Series(eq,index=dates), pd.DataFrame(tr)


def perf(eq, tr):
    total=(eq.iloc[-1]/INITIAL-1)*100; dd=((eq.cummax()-eq)/eq.cummax()).max()*100
    n=len(tr); wr=(tr['pnl']>0).sum()/max(n,1)*100
    return total, dd, n, wr


def main():
    df = load()
    print(f"Full: {df.index[0].date()} → {df.index[-1].date()}")
    print(f"TRAIN: 2020-01 → 2022-12  ({(df.index<SPLIT).sum()} days)")
    print(f"TEST:  2023-01 → 2026-05  ({(df.index>=SPLIT).sum()} days)")
    tr_bh = df[df.index<SPLIT]['close']; te_bh = df[df.index>=SPLIT]['close']
    print(f"  B&H train: {(tr_bh.iloc[-1]/tr_bh.iloc[0]-1)*100:+.0f}%  "
          f"test: {(te_bh.iloc[-1]/te_bh.iloc[0]-1)*100:+.0f}%\n")

    # Edge stability check
    sigs = [
        ('fiz_lsr', 'low'), ('term_ratio_30_90', 'high'), ('vrp_vs', 'high'),
        ('vrp', 'high'), ('rr25', 'high'), ('pc_vol_ratio', 'high'),
        ('gex_norm', 'high'), ('max_pain_dist_pct', 'low'), ('atm_iv', 'high'),
    ]
    print("="*80)
    print("EDGE STABILITY (top-tercile minus bottom-tercile 60d return)")
    print("Robust = same SIGN and similar magnitude in train AND test")
    print("="*80)
    print(f"  {'signal':<20} {'train':>8} {'test':>8}  {'verdict':>14}")
    for col, d in sigs:
        if col not in df.columns: continue
        tr_e, te_e = edge_split(df, col, d)
        if np.isnan(tr_e) or np.isnan(te_e):
            verdict = "n/a"
        elif tr_e > 0 and te_e > 0:
            verdict = "ROBUST" if te_e > tr_e*0.4 else "weak-OOS"
        elif tr_e > 0 and te_e <= 0:
            verdict = "FAILS-OOS"
        else:
            verdict = "inconsistent"
        print(f"  {col:<20} {tr_e:>+7.1f}% {te_e:>+7.1f}%  {verdict:>14}")

    # Strategy on TEST only
    print("\n" + "="*80)
    print("STRATEGY PERFORMANCE — TEST PERIOD ONLY (2023-2026, true OOS)")
    print("="*80)
    test = df[df.index >= SPLIT].copy()
    bh_test = (test['close'].iloc[-1]/test['close'].iloc[0]-1)*100
    print(f"  SBER B&H (test): {bh_test:+.0f}%\n")

    strategies = [
        ("FIZ<=1.4 + ATM_IV>=30", (test['fiz_lsr']<=1.4)&(test['atm_iv']>=30), (test['fiz_lsr']>=3.5)),
        ("FIZ<=1.6 alone",        (test['fiz_lsr']<=1.6), (test['fiz_lsr']>=3.0)),
        ("term_ratio>=1.05",      (test['term_ratio_30_90']>=1.05), (test['term_ratio_30_90']<1.0)),
        ("vrp_vs>=8",             (test['vrp_vs']>=8) if 'vrp_vs' in test else (test['vrp']>=8),
                                   (test['vrp_vs']<3) if 'vrp_vs' in test else (test['vrp']<3)),
        ("pos+term",              (test['fiz_lsr']<=1.6)&(test['term_ratio_30_90']>=1.05),
                                   ~((test['fiz_lsr']<=1.6)&(test['term_ratio_30_90']>=1.05))),
    ]
    print(f"  {'strategy':<28} {'total':>8} {'DD':>6} {'N':>4} {'WR':>5}  {'vs B&H':>8}")
    for name, em, xm in strategies:
        em = em.fillna(False); xm = xm.fillna(False)
        eq, tr = backtest(test, em, xm)
        total, dd, n, wr = perf(eq, tr)
        print(f"  {name:<28} {total:>+7.0f}% {dd:>5.0f}% {n:>4} {wr:>4.0f}%  {total-bh_test:>+7.0f}pp")

    # Same on TRAIN for comparison
    print("\n  --- same strategies on TRAIN (2020-2022) for reference ---")
    train = df[df.index < SPLIT].copy()
    bh_train = (train['close'].iloc[-1]/train['close'].iloc[0]-1)*100
    print(f"  SBER B&H (train): {bh_train:+.0f}%")
    strategies_tr = [
        ("FIZ<=1.4 + ATM_IV>=30", (train['fiz_lsr']<=1.4)&(train['atm_iv']>=30), (train['fiz_lsr']>=3.5)),
        ("FIZ<=1.6 alone",        (train['fiz_lsr']<=1.6), (train['fiz_lsr']>=3.0)),
        ("term_ratio>=1.05",      (train['term_ratio_30_90']>=1.05), (train['term_ratio_30_90']<1.0)),
        ("pos+term",              (train['fiz_lsr']<=1.6)&(train['term_ratio_30_90']>=1.05),
                                   ~((train['fiz_lsr']<=1.6)&(train['term_ratio_30_90']>=1.05))),
    ]
    for name, em, xm in strategies_tr:
        em = em.fillna(False); xm = xm.fillna(False)
        eq, tr = backtest(train, em, xm)
        total, dd, n, wr = perf(eq, tr)
        print(f"  {name:<28} {total:>+7.0f}% {dd:>5.0f}% {n:>4} {wr:>4.0f}%  {total-bh_train:>+7.0f}pp")


if __name__ == "__main__":
    main()
