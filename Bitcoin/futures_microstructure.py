"""Futures microstructure signals (NO options, NO futoi/OI).

Signals built from futures + spot daily data:
  1. Basis = (front_future - spot) / spot, annualized
  2. Calendar spread = front_future - next_quarter_future
  3. CVD proxy = cumulative sign(close-open)*volume on the futures
  4. Carry residual = basis_annualized - RUSFAR

Tested on SBER, GAZP, Si, RTS.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')

MONTH = {'H':3, 'M':6, 'U':9, 'Z':12}  # quarterly futures month codes


def parse_fut(secid, prefix):
    """SRH0 -> (month=3, year_digit=0). Skip spreads (two contracts) and clearing."""
    if not secid.startswith(prefix): return None
    rest = secid[len(prefix):]
    if len(rest) != 2: return None  # spreads like SRH1SRM1 are len 6; _CLT skipped
    if rest[0] not in MONTH or not rest[1].isdigit(): return None
    return MONTH[rest[0]], int(rest[1])


def resolve_year(yd, ref_year):
    cands=[2010+yd, 2020+yd, 2030+yd]
    return min(cands, key=lambda y: abs(y-ref_year))


def build_futures_curve(fut_csv, prefix):
    """For each date, identify front and next-quarter futures by expiry."""
    f = pd.read_csv(ROOT/fut_csv, parse_dates=['TRADEDATE'])
    f = f[f['CLOSE'].notna() & (f['CLOSE']>0)].copy()
    parsed = f['SECID'].apply(lambda s: parse_fut(s, prefix))
    f = f[parsed.notna()].copy()
    f['mon'] = parsed.apply(lambda x: x[0])
    f['yd'] = parsed.apply(lambda x: x[1])
    f['ref_year'] = f['TRADEDATE'].dt.year
    f['full_year'] = f.apply(lambda r: resolve_year(int(r['yd']), int(r['ref_year'])), axis=1)
    f['expiry'] = f.apply(lambda r: pd.Timestamp(datetime(int(r['full_year']), int(r['mon']), 15)), axis=1)
    f['dte'] = (f['expiry'] - f['TRADEDATE']).dt.days
    f = f[f['dte'] > 0].copy()
    return f


def daily_curve(f):
    """Return per-date: front_px, next_px, front_dte, front_vol, front_oi, front_open."""
    rows=[]
    for d, g in f.groupby('TRADEDATE'):
        g = g.sort_values('dte')
        if len(g) < 1: continue
        front = g.iloc[0]
        nxt = g.iloc[1] if len(g) >= 2 else None
        row = {'date': d, 'front_px': front['CLOSE'], 'front_dte': front['dte'],
               'front_open': front['OPEN'], 'front_vol': front['VOLUME'],
               'front_oi': front['OPENPOSITION'], 'front_secid': front['SECID']}
        if nxt is not None:
            row['next_px'] = nxt['CLOSE']; row['next_dte'] = nxt['dte']
        rows.append(row)
    return pd.DataFrame(rows).set_index('date').sort_index()


def build_signals(asset, fut_csv, prefix, spot_csv, spot_col='close', spot_scale=1.0, fut_scale=1.0):
    f = build_futures_curve(fut_csv, prefix)
    curve = daily_curve(f)
    # scale futures prices to spot units
    for c in ['front_px','front_open','next_px']:
        if c in curve.columns: curve[c] = curve[c] / fut_scale
    spot = pd.read_csv(ROOT/spot_csv, parse_dates=['date']).set_index('date')[spot_col]
    df = curve.copy()
    df['spot'] = spot.reindex(df.index).ffill() * spot_scale
    # 1. Basis
    df['basis'] = (df['front_px'] - df['spot']) / df['spot']
    df['basis_ann'] = df['basis'] * 365 / df['front_dte'].clip(lower=1) * 100
    # 2. Calendar spread (front - next), normalized
    if 'next_px' in df.columns:
        df['cal_spread'] = (df['next_px'] - df['front_px']) / df['front_px'] * 100
    else:
        df['cal_spread'] = np.nan
    # 3. CVD proxy from daily candle direction
    df['signed_vol'] = np.sign(df['front_px'] - df['front_open']) * df['front_vol']
    df['cvd'] = df['signed_vol'].cumsum()
    df['cvd_20d'] = df['signed_vol'].rolling(20).sum()  # 20-day momentum of flow
    # price for trading = spot (we trade the underlying/continuous)
    df['close'] = df['spot']
    df['ret'] = df['close'].pct_change()
    return df.dropna(subset=['close','basis']).copy()


def bt(df, em, xm, minh=10, maxh=90, stop=0.08, lev=1.0, side=1):
    c=df['close'].values; em=np.asarray(em); xm=np.asarray(xm); n=len(df)
    cash=INIT; eq=np.full(n,INIT,float); inp=False; ei=0; ep=0; tr=[]
    for i in range(n):
        if inp:
            chg=(c[i]/ep-1)*side; eq[i]=cash*(1+chg*lev); h=i-ei; do=False
            if stop is not None and chg<=-stop: do=True
            elif h>=maxh: do=True
            elif h>=minh and xm[i]: do=True
            if do:
                r=chg*lev-2*FEE; cash*=(1+r); tr.append((df.index[i],r*100)); inp=False; eq[i]=cash
        else: eq[i]=cash
        if not inp and em[i]: ei=i; ep=c[i]*(1+SLIP*side); inp=True
    s=pd.Series(eq,index=df.index)
    tot=(s.iloc[-1]/INIT-1)*100; dd=((s.cummax()-s)/s.cummax()).max()*100
    nt=len(tr); wr=(np.array([x[1] for x in tr])>0).mean()*100 if tr else 0
    pos=sum(x[1] for x in tr if x[1]>0); neg=-sum(x[1] for x in tr if x[1]<0)
    pf=pos/neg if neg>0 else 9.99
    n_yrs=(s.index[-1]-s.index[0]).days/365.25
    cagr=((s.iloc[-1]/INIT)**(1/n_yrs)-1)*100 if n_yrs>0 else 0
    yr={}
    for y in range(2020,2027):
        seg=s[s.index.year==y]
        if len(seg)>1: yr[y]=(seg.iloc[-1]/seg.iloc[0]-1)*100
    return cagr, dd, nt, wr, pf, yr


def tercile_edge(df, col, direction, period=None):
    sub = df.copy()
    if period == 'train': sub=sub[sub.index<SPLIT]
    elif period == 'test': sub=sub[sub.index>=SPLIT]
    sub['fwd60']=sub['close'].pct_change(60).shift(-60)*100
    s=sub[[col,'fwd60']].dropna()
    if len(s)<40 or s[col].nunique()<3: return np.nan
    s['t']=pd.qcut(s[col].rank(method='first'),3,labels=['lo','mid','hi'])
    hi=s[s['t']=='hi']['fwd60'].mean(); lo=s[s['t']=='lo']['fwd60'].mean()
    return (hi-lo) if direction=='high' else (lo-hi)


def analyze(asset, df):
    print(f"\n{'='*90}\n{asset}: {df.index[0].date()}→{df.index[-1].date()} ({len(df)}d) B&H {(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100:+.0f}%")
    print('='*90)
    print(f"  basis_ann: median {df['basis_ann'].median():+.1f}% range [{df['basis_ann'].quantile(.05):+.0f},{df['basis_ann'].quantile(.95):+.0f}]")
    if df['cal_spread'].notna().sum()>50:
        print(f"  cal_spread: median {df['cal_spread'].median():+.2f}%")

    # Edge per signal (train/test)
    print(f"\n  60d tercile edge (train/test):")
    for col, dirs in [('basis_ann','low'),('basis_ann','high'),('cal_spread','low'),('cal_spread','high'),
                      ('cvd_20d','high'),('cvd_20d','low')]:
        tr_e=tercile_edge(df,col,dirs,'train'); te_e=tercile_edge(df,col,dirs,'test')
        if not (np.isnan(tr_e) or np.isnan(te_e)):
            rob='ROBUST' if (tr_e>0 and te_e>0) else ''
            print(f"    {col:<12} {dirs:<5}: train {tr_e:+.1f}% test {te_e:+.1f}%  {rob}")

    # Backtests
    train=df[df.index<SPLIT]
    b_lo=train['basis_ann'].quantile(0.2); b_hi=train['basis_ann'].quantile(0.8)
    cvd_hi=train['cvd_20d'].quantile(0.7); cvd_lo=train['cvd_20d'].quantile(0.3)
    print(f"\n  Backtests (CAGR/DD/N/WR/PF, per-year):")
    cfgs=[
        ('Basis backwardation LONG (basis<=q20)', (df['basis_ann']<=b_lo), (df['basis_ann']>=0), 1),
        ('Basis contango LONG (basis>=q80)', (df['basis_ann']>=b_hi), (df['basis_ann']<=0), 1),
        ('CVD momentum LONG (cvd20>=q70)', (df['cvd_20d']>=cvd_hi), (df['cvd_20d']<cvd_lo), 1),
        ('CVD weak SHORT (cvd20<=q30)', (df['cvd_20d']<=cvd_lo), (df['cvd_20d']>cvd_hi), -1),
    ]
    for label, em, xm, side in cfgs:
        cagr,dd,nt,wr,pf,yr = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 0.08, 1.0, side)
        ys=' '.join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
        print(f"    {label:<42} {cagr:+5.1f}% DD{dd:>3.0f}% N{nt:>3} WR{wr:>3.0f}% PF{pf:.2f}  {ys}")


def main():
    # fut_scale: divide futures price to match spot units.
    #   SBER/GAZP futures = kopecks×100 of stock (1 contract = 100 shares) -> /100
    #   Si: si_daily 'close' is futures points already -> spot=close, basis~0; use rub_per_usd spot, fut/1000
    #   RTS: index points, spot rts_daily 'close' also index -> /1.0
    configs = [
        ('SBER', 'sber_futures_history_merged.csv', 'SR', 'sber_daily.csv', 'close', 1.0, 100.0),
        ('GAZP', 'gazp_futures_history_merged.csv', 'GZ', 'gazp_daily.csv', 'close', 1.0, 100.0),
        ('Si',   'si_futures_history_merged.csv',   'Si', 'si_daily.csv', 'rub_per_usd', 1.0, 1000.0),
        ('RTS',  'rts_futures_history_merged.csv',  'RI', 'rts_daily.csv', 'close', 1.0, 1.0),
    ]
    for asset, fc, pfx, sc, scol, scale, fut_scale in configs:
        try:
            df = build_signals(asset, fc, pfx, sc, scol, scale, fut_scale)
            analyze(asset, df)
            df.to_csv(ROOT/f'{asset.lower()}_microstructure.csv')
        except Exception as e:
            print(f"\n{asset}: ERROR {e}")
            import traceback; traceback.print_exc()


if __name__ == "__main__":
    main()
