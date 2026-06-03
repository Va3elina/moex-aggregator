"""RTS analysis: RVI validation + per-year signal test targeting 40%/yr."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')


def load():
    ind=pd.read_csv(ROOT/'rts_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    px=pd.read_csv(ROOT/'rts_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    close=px['close'] if 'close' in px.columns else px.iloc[:,0]
    df=pd.DataFrame(index=ind.index); df['close']=close.reindex(ind.index).ffill()
    df=df.join(ind, how='left')
    df['ret']=df['close'].pct_change(); df['rv30']=df['ret'].rolling(30).std()*np.sqrt(252)*100
    df['vrp']=df['atm_iv']-df['rv30']
    ft=pd.read_csv(ROOT/'rts_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr']=(ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    # RVI
    rvi=pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE')['close']
    df['rvi']=rvi.reindex(df.index).ffill()
    return df[df['atm_iv'].notna()].copy()


def bt(df, em, xm, minh, maxh, lev=1.0, stop=None):
    c=df['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(df)
    cash=INIT; eq=np.full(n,INIT,float); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            chg=c[i]/ep-1; eq[i]=cash*(1+chg*lev); h=i-ei; do=False
            if stop is not None and chg*lev<=-stop: do=True
            elif h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=(c[i]*(1-SLIP)/ep-1)*lev-2*FEE; cash*=(1+r); tr.append((df.index[i],r*100)); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP); inp=True
    s=pd.Series(eq,index=df.index); tot=(s.iloc[-1]/INIT-1)*100
    dd=((s.cummax()-s)/s.cummax()).max()*100; nt=len(tr)
    wr=(np.array([x[1] for x in tr])>0).mean()*100 if tr else 0
    pos=sum(x[1] for x in tr if x[1]>0); neg=-sum(x[1] for x in tr if x[1]<0)
    pf=pos/neg if neg>0 else 9.99
    yr={}
    for y in range(2020,2027):
        seg=s[s.index.year==y]
        if len(seg)>1: yr[y]=(seg.iloc[-1]/seg.iloc[0]-1)*100
    return tot,dd,nt,wr,pf,yr


def main():
    df=load()
    bh=(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100
    print(f"RTS {df.index[0].date()}->{df.index[-1].date()} ({len(df)}d), B&H {bh:+.0f}%")

    # RVI validation
    v=df[['atm_iv','rvi','varswap_iv']].dropna()
    print(f"\nVALIDATION: corr(ATM_IV, RVI)={v['atm_iv'].corr(v['rvi']):.3f}  "
          f"corr(varswap, RVI)={v['varswap_iv'].corr(v['rvi']):.3f}")
    print(f"  ATM_IV median {df['atm_iv'].median():.1f}  RVI median {df['rvi'].median():.1f}")

    # forward returns by signal
    df['fwd20']=df['close'].pct_change(20).shift(-20)*100
    df['fwd60']=df['close'].pct_change(60).shift(-60)*100
    print("\nForward 60d by signal quintile (Q1 low -> Q5 high):")
    for col in ['vrp','rvi','fiz_lsr','atm_iv','rr25']:
        s=df[[col,'fwd60']].dropna()
        if len(s)<100: continue
        s['q']=pd.qcut(s[col].rank(method='first'),5,labels=[1,2,3,4,5])
        means=[s[s['q']==q]['fwd60'].mean() for q in [1,2,3,4,5]]
        print(f"  {col:<10} Q1..Q5: "+" ".join(f"{m:+.1f}" for m in means)+f"  (spread {means[-1]-means[0]:+.1f})")

    print("\n"+"="*100)
    print("BACKTESTS — targeting 40%/yr (fmt: tot/DD/N/WR/PF, then per-year)")
    print("="*100)
    cfgs=[
        ("FIZ<=q20 contrarian, exit FIZ>=q70|VRP<-5, 10-90, 1x",
            lambda d:d['fiz_lsr']<=d['fiz_lsr'].rolling(252,min_periods=60).quantile(.2),
            lambda d:(d['fiz_lsr']>=d['fiz_lsr'].rolling(252,min_periods=60).quantile(.7))|(d['vrp']<-5),10,90,1.0,None),
        ("RVI>=40 panic LONG, exit RVI<30, 10-90, 1x",
            lambda d:d['rvi']>=40, lambda d:d['rvi']<30,10,90,1.0,None),
        ("VRP>=8 LONG, exit VRP<0, 10-90, 1x",
            lambda d:d['vrp']>=8, lambda d:d['vrp']<0,10,90,1.0,None),
        ("RVI>=35 LONG, exit RVI<28, 10-120, 1.5x",
            lambda d:d['rvi']>=35, lambda d:d['rvi']<28,10,120,1.5,0.15),
        ("RVI>=40 LONG 2x stop15",
            lambda d:d['rvi']>=40, lambda d:d['rvi']<30,10,120,2.0,0.15),
    ]
    for nm,ef,xf,mh,xh,lev,st in cfgs:
        tot,dd,nt,wr,pf,yr=bt(df, ef(df).fillna(False), xf(df).fillna(False), mh,xh,lev,st)
        ys=" ".join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
        print(f"  {nm}\n    tot={tot:+.0f}% DD={dd:.0f}% N={nt} WR={wr:.0f}% PF={pf:.2f}  {ys}")


if __name__ == "__main__":
    main()
