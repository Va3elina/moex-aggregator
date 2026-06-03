"""Comprehensive FIZ strategy optimization with train/test + cross-asset.

1. Entry/exit threshold grid on SBER (train/test robustness)
2. Hold period sweep
3. VRP-exit variants
4. Cross-asset confirmation (SBER, GAZP, IMOEX)
5. Leverage scaling
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')


def load_asset(ind_csv, px_csv, futoi_csv):
    if not (ROOT/ind_csv).exists():
        return None
    ind=pd.read_csv(ROOT/ind_csv, parse_dates=['date']).set_index('date').sort_index()
    if px_csv and (ROOT/px_csv).exists():
        close=pd.read_csv(ROOT/px_csv, parse_dates=['date']).set_index('date')['close']
    else:
        alt=ROOT.parent/'CopyTrading'/'moex_daily'/'imoex_d.csv'
        close=pd.read_csv(alt, parse_dates=['date']).set_index('date')['close'] if alt.exists() else ind['F']
    df=pd.DataFrame(index=ind.index); df['close']=close.reindex(ind.index).ffill()
    df=df.join(ind, how='left')
    df['ret']=df['close'].pct_change(); df['rv30']=df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp']=df['atm_iv']-df['rv30']
    ft=pd.read_csv(ROOT/futoi_csv, parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr']=(ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    return df[df['atm_iv'].notna()].copy()


def bt(df, em, xm, minh, maxh, lev=1.0):
    c=df['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(df)
    cash=INIT; eq=np.full(n,INIT,float); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            base=c[i]/ep-1; eq[i]=cash*(1+base*lev); h=i-ei; do=False
            if h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=(c[i]*(1-SLIP)/ep-1)*lev-2*FEE; cash*=(1+r); tr.append(r*100); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP); inp=True
    s=pd.Series(eq,index=df.index); tot=(s.iloc[-1]/INIT-1)*100
    dd=((s.cummax()-s)/s.cummax()).max()*100; nt=len(tr)
    wr=(np.array(tr)>0).mean()*100 if tr else 0
    pf=(sum(x for x in tr if x>0)/-sum(x for x in tr if x<0)) if any(x<0 for x in tr) else 9.99
    return tot,dd,nt,wr,pf


def split_bt(df, ef, xf, mh, xh, lev=1.0):
    out=[]
    for seg in [df[df.index<SPLIT], df[df.index>=SPLIT]]:
        out.append(bt(seg, ef(seg).fillna(False), xf(seg).fillna(False), mh, xh, lev))
    return out


def main():
    sber=load_asset('sber_option_indicators_daily.csv','sber_daily.csv','sber_futoi_signals.csv')
    print("="*104)
    print("1) ENTRY threshold sweep (SBER) — exit FIZ>=3.0, hold 10-90")
    print("="*104)
    print(f"{'entry FIZ<=':<14}{'TRAIN t/DD/N/WR/PF':<30}{'TEST t/DD/N/WR/PF'}")
    for thr in [1.0,1.2,1.4,1.6,1.8,2.0]:
        tr,te=split_bt(sber, lambda d,t=thr:(d['fiz_lsr']<=t), lambda d:(d['fiz_lsr']>=3.0),10,90)
        print(f"{thr:<14}{f'{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}':<30}"
              f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}")

    print("\n"+"="*104)
    print("2) EXIT threshold sweep (SBER) — entry FIZ<=1.5, hold 10-90")
    print("="*104)
    print(f"{'exit FIZ>=':<14}{'TRAIN':<30}{'TEST'}")
    for xthr in [2.5,3.0,3.5,4.0,5.0]:
        tr,te=split_bt(sber, lambda d:(d['fiz_lsr']<=1.5), lambda d,x=xthr:(d['fiz_lsr']>=x),10,90)
        print(f"{xthr:<14}{f'{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}':<30}"
              f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}")

    print("\n"+"="*104)
    print("3) HOLD period sweep (SBER) — entry FIZ<=1.5, exit FIZ>=3.5")
    print("="*104)
    print(f"{'minh-maxh':<14}{'TRAIN':<30}{'TEST'}")
    for mh,xh in [(5,60),(10,60),(10,90),(20,120),(20,180),(30,180)]:
        tr,te=split_bt(sber, lambda d:(d['fiz_lsr']<=1.5), lambda d:(d['fiz_lsr']>=3.5),mh,xh)
        print(f"{f'{mh}-{xh}':<14}{f'{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}':<30}"
              f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}")

    print("\n"+"="*104)
    print("4) VRP-exit variants (SBER) — entry FIZ<=1.5")
    print("="*104)
    variants=[
        ("exit FIZ>=3.5", lambda d:(d['fiz_lsr']>=3.5)),
        ("exit FIZ>=3.5 OR VRP<0", lambda d:((d['fiz_lsr']>=3.5)|(d['vrp']<0))),
        ("exit FIZ>=3.5 OR VRP<-5", lambda d:((d['fiz_lsr']>=3.5)|(d['vrp']<-5))),
        ("exit VRP<5 only", lambda d:(d['vrp']<5)),
    ]
    print(f"{'exit rule':<26}{'TRAIN':<30}{'TEST'}")
    for nm,xf in variants:
        tr,te=split_bt(sber, lambda d:(d['fiz_lsr']<=1.5), xf,10,90)
        print(f"{nm:<26}{f'{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}':<30}"
              f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}")

    print("\n"+"="*104)
    print("5) CROSS-ASSET: FIZ<=thr (per-asset tuned), exit FIZ>=3.0, hold 10-90")
    print("="*104)
    assets={'SBER':sber,
            'GAZP':load_asset('gazp_option_indicators_daily.csv','gazp_daily.csv','gazp_futoi_signals.csv'),
            'IMOEX':load_asset('imoex_option_indicators_daily.csv',None,'imoex_futoi_history.csv')}
    print(f"{'asset':<8}{'FIZ thr':<9}{'TRAIN':<30}{'TEST'}")
    for nm,d in assets.items():
        if d is None: print(f"{nm}: n/a"); continue
        # per-asset threshold = train 20th pct of fiz_lsr (extreme low)
        thr=round(d[d.index<SPLIT]['fiz_lsr'].quantile(0.20),2)
        tr,te=split_bt(d, lambda x,t=thr:(x['fiz_lsr']<=t), lambda x:(x['fiz_lsr']>=3.0),10,90)
        print(f"{nm:<8}{thr:<9}{f'{tr[0]:+.0f}% {tr[1]:.0f}% {tr[2]} {tr[3]:.0f}% {tr[4]:.2f}':<30}"
              f"{te[0]:+.0f}% {te[1]:.0f}% {te[2]} {te[3]:.0f}% {te[4]:.2f}")

    print("\n"+"="*104)
    print("6) LEVERAGE on best SBER config (FIZ<=1.5, exit FIZ>=3.5, 10-90)")
    print("="*104)
    print(f"{'lev':<6}{'TRAIN t/DD':<20}{'TEST t/DD'}")
    for lev in [1.0,1.5,2.0,3.0]:
        tr,te=split_bt(sber, lambda d:(d['fiz_lsr']<=1.5), lambda d:(d['fiz_lsr']>=3.5),10,90,lev)
        print(f"{lev:<6}{f'{tr[0]:+.0f}% DD{tr[1]:.0f}%':<20}{te[0]:+.0f}% DD{te[1]:.0f}%")

    print("\n(format: total / maxDD / #trades / winrate / profit-factor)")


if __name__ == "__main__":
    main()
