"""Adaptive FIZ strategy — rolling-percentile threshold to handle regime drift.

Problem: FIZ LSR drifted up over years (retail less net-short), so a FIXED
entry threshold fires less and less (0 in 2026). Fix: enter when FIZ is in
the LOW percentile of its own trailing window -> adapts to regime, trades
every year.

Targets: positive EVERY year, DD<=20%, high WR, high PF.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
import optimize_fiz as o

INIT=100; FEE=0.0003; SLIP=0.0001


def prep(d):
    d=d.copy()
    d['ret']=d['close'].pct_change()
    d['rv30']=d['ret'].rolling(30).std()*np.sqrt(252)*100
    d['vrp']=d['atm_iv']-d['rv30']
    # rolling percentile rank of fiz_lsr over trailing window
    win=252
    d['fiz_pct']=d['fiz_lsr'].rolling(win, min_periods=60).apply(
        lambda x: (x.iloc[-1] <= x).mean()*100, raw=False)  # % of window >= current (low=panic)
    # Actually we want: how low is current vs window. pct of days current is below.
    d['fiz_rank']=d['fiz_lsr'].rolling(win, min_periods=60).apply(
        lambda x: (x < x.iloc[-1]).mean()*100, raw=False)  # 0=lowest in window
    return d


def bt(d, em, xm, minh, maxh, lev=1.0, stop=None):
    c=d['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(d)
    cash=INIT; eq=np.full(n,INIT,float); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            chg=c[i]/ep-1; eq[i]=cash*(1+chg*lev); h=i-ei; do=False
            if stop is not None and chg*lev<=-stop: do=True
            elif h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=(c[i]*(1-SLIP)/ep-1)*lev-2*FEE; cash*=(1+r)
                tr.append((d.index[i],r*100)); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP); inp=True
    s=pd.Series(eq,index=d.index); tot=(s.iloc[-1]/INIT-1)*100
    dd=((s.cummax()-s)/s.cummax()).max()*100; nt=len(tr)
    wr=(np.array([x[1] for x in tr])>0).mean()*100 if tr else 0
    pos=sum(x[1] for x in tr if x[1]>0); neg=-sum(x[1] for x in tr if x[1]<0)
    pf=pos/neg if neg>0 else 9.99
    return s,tot,dd,nt,wr,pf,tr


def per_year(s, tr):
    """Return per-year equity growth %."""
    yr={}
    for y in range(2020,2027):
        seg=s[s.index.year==y]
        if len(seg)>1:
            yr[y]=(seg.iloc[-1]/seg.iloc[0]-1)*100
    return yr


def main():
    d=prep(o.load_asset('sber_option_indicators_daily.csv','sber_daily.csv','sber_futoi_signals.csv'))

    configs=[
        ("FIXED FIZ<=1.6 (current)",
            lambda x:x['fiz_lsr']<=1.6, lambda x:(x['fiz_lsr']>=3.0)|(x['vrp']<-5),10,90,None),
        ("ADAPT FIZ rank<=20%",
            lambda x:x['fiz_rank']<=20, lambda x:(x['fiz_rank']>=70)|(x['vrp']<-5),10,90,None),
        ("ADAPT FIZ rank<=25%",
            lambda x:x['fiz_rank']<=25, lambda x:(x['fiz_rank']>=60)|(x['vrp']<-5),10,90,None),
        ("ADAPT FIZ rank<=15% (strict)",
            lambda x:x['fiz_rank']<=15, lambda x:(x['fiz_rank']>=70)|(x['vrp']<-5),10,90,None),
        ("ADAPT rank<=25 + stop 12%",
            lambda x:x['fiz_rank']<=25, lambda x:(x['fiz_rank']>=60)|(x['vrp']<-5),10,90,0.12),
        ("ADAPT rank<=20 + stop 10%",
            lambda x:x['fiz_rank']<=20, lambda x:(x['fiz_rank']>=70)|(x['vrp']<-5),10,90,0.10),
    ]

    print(f"{'config':<32}{'tot':>7}{'DD':>6}{'N':>4}{'WR':>5}{'PF':>6}  per-year %")
    print("="*120)
    for nm,ef,xf,mh,xh,stop in configs:
        s,tot,dd,nt,wr,pf,tr=bt(d, ef(d).fillna(False), xf(d).fillna(False), mh,xh,1.0,stop)
        yr=per_year(s,tr)
        ys=" ".join(f"{y}:{v:+.0f}" for y,v in yr.items())
        print(f"{nm:<32}{tot:>+6.0f}%{dd:>5.0f}%{nt:>4}{wr:>4.0f}%{pf:>6.2f}  {ys}")

    print("\n(per-year = equity % change within each calendar year)")


if __name__ == "__main__":
    main()
