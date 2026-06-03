"""Thorough test of FIZ + VRP combinations on SBER, with train/test split.

FIZ (retail positioning) was strongest standalone. VRP is robust cross-asset.
Test how to best combine: AND, OR, FIZ-primary+VRP-exit, etc.
Report TRAIN(2020-22) and TEST(2023-26) separately to avoid overfit.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')


def load():
    df=pd.read_csv(ROOT/'sber_master_features.csv', parse_dates=['date']).set_index('date')
    df=df[df['atm_iv'].notna()].copy()
    df['ret']=df['close'].pct_change()
    df['rv30']=df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp']=df['atm_iv']-df['rv30']
    return df


def bt(df, em, xm, minh, maxh):
    c=df['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(df)
    cash=INIT; eq=np.full(n,INIT); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            eq[i]=cash*(c[i]/ep); h=i-ei; do=False
            if h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=c[i]*(1-SLIP)/ep-1-2*FEE; cash*=(1+r); tr.append(r*100); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP); inp=True
    s=pd.Series(eq,index=df.index); tot=(s.iloc[-1]/INIT-1)*100
    dd=((s.cummax()-s)/s.cummax()).max()*100; nt=len(tr)
    wr=(np.array(tr)>0).mean()*100 if tr else 0
    pf=(sum(x for x in tr if x>0)/-sum(x for x in tr if x<0)) if any(x<0 for x in tr) else 9
    return tot,dd,nt,wr,pf


def run(df, name, em_f, xm_f, minh, maxh):
    """Run on train and test separately."""
    res=[]
    for seg_name, seg in [('TRAIN',df[df.index<SPLIT]),('TEST',df[df.index>=SPLIT])]:
        em=em_f(seg).fillna(False); xm=xm_f(seg).fillna(False)
        res.append(bt(seg,em,xm,minh,maxh))
    return name, res


def main():
    df=load()
    bh_tr=(df[df.index<SPLIT]['close'].iloc[-1]/df[df.index<SPLIT]['close'].iloc[0]-1)*100
    bh_te=(df[df.index>=SPLIT]['close'].iloc[-1]/df[df.index>=SPLIT]['close'].iloc[0]-1)*100
    print(f"TRAIN B&H {bh_tr:+.0f}%   TEST B&H {bh_te:+.0f}%\n")

    configs = [
        # name, entry_fn, exit_fn, minhold, maxhold
        ("FIZ<=1.4 alone (champion)",
            lambda d:(d['fiz_lsr']<=1.4), lambda d:(d['fiz_lsr']>=3.5), 10,60),
        ("FIZ<=1.6 alone",
            lambda d:(d['fiz_lsr']<=1.6), lambda d:(d['fiz_lsr']>=3.0), 10,90),
        ("VRP>=10 alone",
            lambda d:(d['vrp']>=10), lambda d:(d['vrp']<5), 15,90),
        ("FIZ<=1.6 OR VRP>=12 (union)",
            lambda d:((d['fiz_lsr']<=1.6)|(d['vrp']>=12)), lambda d:((d['fiz_lsr']>=3.0)&(d['vrp']<5)), 15,90),
        ("FIZ<=1.6 AND VRP>=5 (loose AND)",
            lambda d:((d['fiz_lsr']<=1.6)&(d['vrp']>=5)), lambda d:(d['fiz_lsr']>=3.0), 10,90),
        ("FIZ-primary, VRP exit (FIZ<=1.6 in, exit FIZ>=3 or VRP<0)",
            lambda d:(d['fiz_lsr']<=1.6), lambda d:((d['fiz_lsr']>=3.0)|(d['vrp']<0)), 10,90),
        ("FIZ<=1.4 + ATM_IV>=30 (old champ)",
            lambda d:((d['fiz_lsr']<=1.4)&(d['atm_iv']>=30)), lambda d:(d['fiz_lsr']>=3.5), 10,60),
        ("FIZ<=1.8 (wider), hold 20-120",
            lambda d:(d['fiz_lsr']<=1.8), lambda d:(d['fiz_lsr']>=3.0), 20,120),
    ]

    print(f"{'config':<52} {'TRAIN tot/DD/N/WR/PF':<28} {'TEST tot/DD/N/WR/PF'}")
    print("="*108)
    for name, em_f, xm_f, mh, xh in configs:
        _, res = run(df, name, em_f, xm_f, mh, xh)
        tr, te = res
        tr_s=f"{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}"
        te_s=f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}"
        print(f"{name:<52} {tr_s:<28} {te_s}")

    print("\n(format: total / maxDD / #trades / winrate / profit-factor)")
    print("Robust = good in BOTH train and test")


if __name__ == "__main__":
    main()
