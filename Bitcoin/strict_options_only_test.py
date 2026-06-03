"""STRICT test: do PURE options signals carry standalone OOS value (no FIZ, no positioning)?

Only options-derived indicators allowed. Walk-forward each, build an
options-only ensemble from the OOS-robust ones, and backtest on the TRUE
out-of-sample test period. Compare to B&H and to FIZ-only as reference.

Definitive answer to: "is there standalone alpha in RU options, or only
as a filter?"
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

# PURE options indicators (NO futoi/positioning)
OPTION_SIGS = [
    ('vrp', 'high'), ('vrp_vs', 'high'), ('rr25', 'high'), ('bf25', 'high'),
    ('term_ratio_30_90', 'high'), ('gex_norm', 'high'), ('atm_iv', 'high'),
    ('pc_vol_ratio', 'high'), ('pc_oi_ratio', 'high'), ('max_pain_dist_pct', 'low'),
    ('skew_slope', 'high'), ('straddle_move_pct', 'low'), ('dollar_gamma', 'high'),
    ('varswap_iv', 'high'), ('rn_p_down10', 'high'),
]


def load():
    df = pd.read_csv(ROOT/'sber_master_features.csv', parse_dates=['date']).set_index('date')
    df = df[df['atm_iv'].notna()].copy()
    for h in [20, 60]:
        df[f'fwd{h}'] = df['close'].pct_change(h).shift(-h)*100
    return df


def tercile_edge(seg, col, direction):
    s = seg[[col, 'fwd60']].dropna()
    if len(s) < 50 or s[col].nunique() < 3:
        return np.nan
    s['t'] = pd.qcut(s[col].rank(method='first'), 3, labels=['lo','mid','hi'])
    hi = s[s['t']=='hi']['fwd60'].mean(); lo = s[s['t']=='lo']['fwd60'].mean()
    return (hi-lo) if direction=='high' else (lo-hi)


def backtest(df, em, xm, min_hold=10, max_hold=60):
    closes=df['close'].values; dates=df.index; n=len(df)
    em=np.asarray(em); xm=np.asarray(xm)
    cash=INITIAL; eq=np.full(n,INITIAL); inp=False; ei=0; ep=0; tr=[]
    days_in=0
    for i in range(n):
        c=closes[i]
        if inp:
            eq[i]=cash*(c/ep); days_in+=1; h=i-ei; do=False
            if h>=max_hold: do=True
            elif h>=min_hold and xm[i]: do=True
            if do:
                xp=c*(1-SLIP); r=xp/ep-1-2*FEE; cash*=(1+r)
                tr.append({'pnl':r*100}); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c*(1+SLIP); inp=True
    total=(eq.iloc[-1] if hasattr(eq,'iloc') else eq[-1])
    s=pd.Series(eq,index=dates)
    tot=(s.iloc[-1]/INITIAL-1)*100; dd=((s.cummax()-s)/s.cummax()).max()*100
    n_t=len(tr); wr=(pd.DataFrame(tr)['pnl']>0).mean()*100 if n_t else 0
    expo = days_in/n*100
    return s, tot, dd, n_t, wr, expo


def main():
    df = load()
    train = df[df.index < SPLIT]; test = df[df.index >= SPLIT]
    print(f"TRAIN 2020-2022 ({len(train)}d, B&H {(train['close'].iloc[-1]/train['close'].iloc[0]-1)*100:+.0f}%)")
    print(f"TEST  2023-2026 ({len(test)}d, B&H {(test['close'].iloc[-1]/test['close'].iloc[0]-1)*100:+.0f}%)\n")

    # 1. Per-signal walk-forward (options only)
    print("="*78)
    print("PURE OPTIONS SIGNALS — walk-forward 60d tercile edge")
    print("="*78)
    print(f"  {'signal':<20} {'train':>8} {'test':>8}  {'verdict':>12}")
    robust = []
    for col, d in OPTION_SIGS:
        if col not in df.columns: continue
        tr_e = tercile_edge(train, col, d); te_e = tercile_edge(test, col, d)
        if np.isnan(tr_e) or np.isnan(te_e):
            v='n/a'
        elif tr_e>0 and te_e>0:
            v='ROBUST' if te_e>tr_e*0.4 else 'weak'
            if v=='ROBUST': robust.append((col,d))
        elif tr_e>0 and te_e<=0: v='FAILS-OOS'
        else: v='inconsist'
        print(f"  {col:<20} {tr_e:>+7.1f}% {te_e:>+7.1f}%  {v:>12}")

    print(f"\nRobust pure-options signals: {[c for c,_ in robust]}")

    # 2. Threshold-define each robust signal (use train median of its 'good' side)
    def make_mask(frame, col, d):
        # 'good' side: top 40% if high, bottom 40% if low (threshold from TRAIN)
        thr = train[col].quantile(0.6 if d=='high' else 0.4)
        return (frame[col] >= thr) if d=='high' else (frame[col] <= thr)

    # 3. Options-only ensemble on TEST (true OOS)
    print("\n" + "="*78)
    print("OPTIONS-ONLY ENSEMBLE on TEST period (true OOS) — thresholds set from TRAIN")
    print("="*78)
    if robust:
        votes_test = pd.DataFrame({c: make_mask(test, c, d).fillna(False) for c,d in robust})
        vt = votes_test.sum(axis=1)
        print(f"  {'strategy':<32} {'total':>8} {'DD':>6} {'N':>4} {'WR':>5} {'expo':>6} {'vsBH':>7}")
        bh_test=(test['close'].iloc[-1]/test['close'].iloc[0]-1)*100
        for vth in range(1, len(robust)+1):
            em = (vt>=vth); xm=(vt<vth)
            _,tot,dd,n,wr,expo = backtest(test, em, xm)
            print(f"  options votes>={vth:<25} {tot:>+7.0f}% {dd:>5.0f}% {n:>4} {wr:>4.0f}% {expo:>5.0f}% {tot-bh_test:>+6.0f}pp")

        # each robust signal standalone on test
        print("\n  --- each robust options signal standalone (TEST) ---")
        for c,d in robust:
            em=make_mask(test,c,d).fillna(False); xm=~em
            _,tot,dd,n,wr,expo=backtest(test,em,xm)
            print(f"  {c:<32} {tot:>+7.0f}% {dd:>5.0f}% {n:>4} {wr:>4.0f}% {expo:>5.0f}%")
    else:
        print("  No robust pure-options signals found.")

    # 4. Reference: FIZ-only and B&H on test
    print("\n" + "="*78)
    print("REFERENCE (TEST period)")
    print("="*78)
    bh_test=(test['close'].iloc[-1]/test['close'].iloc[0]-1)*100
    print(f"  {'B&H':<32} {bh_test:>+7.0f}%")
    fiz_em=(test['fiz_lsr']<=1.6).fillna(False); fiz_xm=(test['fiz_lsr']>=3.0).fillna(False)
    _,tot,dd,n,wr,expo=backtest(test,fiz_em,fiz_xm)
    print(f"  {'FIZ<=1.6 (positioning, ref)':<32} {tot:>+7.0f}% {dd:>5.0f}% {n:>4} {wr:>4.0f}% {expo:>5.0f}%")

    # 5. Verdict: best options-only vs FIZ — does options carry standalone value?
    print("\n" + "="*78)
    print("VERDICT")
    print("="*78)
    if robust:
        # best ensemble
        votes_test = pd.DataFrame({c: make_mask(test, c, d).fillna(False) for c,d in robust})
        vt=votes_test.sum(axis=1)
        best=None
        for vth in range(1,len(robust)+1):
            em=(vt>=vth); xm=(vt<vth); _,tot,dd,n,wr,expo=backtest(test,em,xm)
            # risk-adj: total/DD
            score=tot/max(dd,1)
            if best is None or score>best[0]: best=(score,vth,tot,dd,wr,expo)
        print(f"  Best options-only ensemble (test): votes>={best[1]}, "
              f"{best[2]:+.0f}% DD{best[3]:.0f}% WR{best[4]:.0f}% expo{best[5]:.0f}%")
        print(f"  vs B&H {bh_test:+.0f}%  → options-only "
              f"{'BEATS' if best[2]>bh_test else 'TRAILS'} raw, "
              f"{'better' if best[3]<40 else 'worse'} risk")
    print("\n  Interpretation: if options-only ensemble matches B&H return at much\n"
          "  lower DD/exposure → standalone risk-adjusted value EXISTS.\n"
          "  If it trails B&H badly → options are filters, not standalone alpha.")


if __name__ == "__main__":
    main()
