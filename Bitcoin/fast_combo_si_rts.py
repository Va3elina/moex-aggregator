"""Fast-wins combo backtest for Si and RTS using already-identified robust signals.

While deep agents work on full grid search, test obvious pair combinations.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT=100; FEE=0.0003; SLIP=0.0001
SPLIT=pd.Timestamp('2023-01-01')


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
    # RVI for RTS
    if name == 'RTS':
        rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE')['close']
        df['rvi'] = rvi.reindex(df.index).ffill()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    return df[df['atm_iv'].notna()].copy()


def bt(df, em, xm, minh=10, maxh=90, stop=0.08, lev=1.0):
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
    return tot, dd, nt, wr, pf, cagr, yr


def threshold_train(train, col, q):
    return train[col].quantile(q)


def test_combos(name, df, configs):
    print(f"\n{'='*100}")
    print(f"{name}: B&H {(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100:+.0f}%")
    print('='*100)
    train = df[df.index < SPLIT]
    test_bh = (df[df.index>=SPLIT]['close'].iloc[-1]/df[df.index>=SPLIT]['close'].iloc[0]-1)*100
    print(f"TEST B&H {test_bh:+.0f}%\n")
    print(f"{'config':<60} {'CAGR':>6} {'DD':>5} {'WR':>5} {'PF':>6} {'N':>4}  per-year")
    print("-"*120)
    results = []
    for label, ef, xf, minh, maxh, stop, lev in configs:
        em = ef(df).fillna(False)
        xm = xf(df).fillna(False)
        tot, dd, nt, wr, pf, cagr, yr = bt(df, em, xm, minh, maxh, stop, lev)
        ys = ' '.join(f"{y%100}:{v:+.0f}" for y,v in yr.items())
        print(f"{label:<60} {cagr:>+5.1f}% {dd:>4.0f}% {wr:>4.0f}% {pf:>5.2f} {nt:>4}  {ys}")
        results.append((label, cagr, dd, wr, pf, nt))
    return results


def main():
    # === Si: max_pain + gex + FIZ ===
    si = load('Si', 'si_option_indicators_daily.csv', 'si_daily.csv', 'si_futoi_history.csv')
    si_train = si[si.index < SPLIT]
    # train thresholds for robust signals
    mp_thr = threshold_train(si_train, 'max_pain_dist_pct', 0.3)  # LOW=bullish: bottom 30%
    gex_thr = threshold_train(si_train, 'gex_norm', 0.7)  # HIGH=bullish: top 30%
    pcv_thr = threshold_train(si_train, 'pc_vol_ratio', 0.7)
    sm_thr  = threshold_train(si_train, 'straddle_move_pct', 0.3)  # LOW=bullish

    si_configs = [
        # baseline FIZ
        ("FIZ<=1.4 + VRP-exit (baseline)",
         lambda d: d['fiz_lsr']<=1.4,
         lambda d: (d['fiz_lsr']>=3.0)|(d['vrp']<-5), 10, 60, 0.08, 1.0),
        # Single max_pain (best robust on Si)
        ("max_pain_dist <= q30(train)",
         lambda d: d['max_pain_dist_pct'] <= mp_thr,
         lambda d: d['max_pain_dist_pct'] > 0, 10, 60, 0.08, 1.0),
        # gex_norm
        ("gex_norm >= q70(train)",
         lambda d: d['gex_norm'] >= gex_thr,
         lambda d: d['gex_norm'] < 0, 10, 60, 0.08, 1.0),
        # PAIRS
        ("FIZ<=1.4 AND max_pain<=q30",
         lambda d: (d['fiz_lsr']<=1.4) & (d['max_pain_dist_pct']<=mp_thr),
         lambda d: (d['fiz_lsr']>=3.0)|(d['vrp']<-5), 10, 90, 0.08, 1.0),
        ("FIZ<=1.4 OR max_pain<=q30",
         lambda d: (d['fiz_lsr']<=1.4) | (d['max_pain_dist_pct']<=mp_thr),
         lambda d: (d['fiz_lsr']>=3.0) & (d['max_pain_dist_pct']>0), 10, 90, 0.08, 1.0),
        ("max_pain<=q30 AND gex>=q70",
         lambda d: (d['max_pain_dist_pct']<=mp_thr) & (d['gex_norm']>=gex_thr),
         lambda d: (d['max_pain_dist_pct']>0)|(d['gex_norm']<0), 10, 90, 0.08, 1.0),
        ("FIZ<=1.4 + max_pain exit",
         lambda d: d['fiz_lsr']<=1.4,
         lambda d: (d['fiz_lsr']>=3.0)|(d['max_pain_dist_pct']>5), 10, 90, 0.08, 1.0),
        ("Triple AND: FIZ+max_pain+gex",
         lambda d: (d['fiz_lsr']<=1.4)&(d['max_pain_dist_pct']<=mp_thr)&(d['gex_norm']>=gex_thr),
         lambda d: (d['fiz_lsr']>=3.0), 10, 120, 0.08, 1.0),
        # With 2x lev
        ("FIZ<=1.4 + max_pain exit, lev=2x",
         lambda d: d['fiz_lsr']<=1.4,
         lambda d: (d['fiz_lsr']>=3.0)|(d['max_pain_dist_pct']>5), 10, 90, 0.08, 2.0),
        ("max_pain<=q30, lev=2x",
         lambda d: d['max_pain_dist_pct'] <= mp_thr,
         lambda d: d['max_pain_dist_pct'] > 0, 10, 60, 0.08, 2.0),
    ]
    si_res = test_combos('Si', si, si_configs)

    # === RTS: varswap + rn_p_down + RVI + kill-switch ===
    rts = load('RTS', 'rts_option_indicators_daily.csv', 'rts_daily.csv', 'rts_futoi_history.csv')
    rts_train = rts[rts.index < SPLIT]
    vs_thr = threshold_train(rts_train, 'varswap_iv', 0.7)
    iv_thr = threshold_train(rts_train, 'atm_iv', 0.7)
    bf_thr = threshold_train(rts_train, 'bf25', 0.7)
    pdown_thr = threshold_train(rts_train, 'rn_p_down10', 0.7)
    rvi_panic = 40

    # Kill-switch helper: True 30 days after RVI hit 80
    def with_killswitch(rts_df, sig):
        rvi80_dates = rts_df.index[rts_df['rvi'] >= 80]
        kill_mask = pd.Series(False, index=rts_df.index)
        for d in rvi80_dates:
            end = d + pd.Timedelta(days=30)
            kill_mask.loc[d:end] = True
        return sig & ~kill_mask

    rts_configs = [
        # baseline
        ("RVI>=40 baseline",
         lambda d: d['rvi']>=40,
         lambda d: d['rvi']<28, 10, 90, 0.10, 1.0),
        # Best single robust on RTS
        ("varswap_iv >= q70",
         lambda d: d['varswap_iv']>=vs_thr,
         lambda d: d['varswap_iv']<vs_thr*0.8, 10, 60, 0.08, 1.0),
        ("varswap_iv >= q70 + killswitch",
         lambda d: with_killswitch(d, d['varswap_iv']>=vs_thr),
         lambda d: d['varswap_iv']<vs_thr*0.8, 10, 60, 0.08, 1.0),
        ("rn_p_down10 >= q70 + killswitch",
         lambda d: with_killswitch(d, d['rn_p_down10']>=pdown_thr),
         lambda d: d['rn_p_down10']<pdown_thr*0.8, 10, 90, 0.08, 1.0),
        # PAIRS
        ("varswap>=q70 AND atm_iv>=q70 + killswitch",
         lambda d: with_killswitch(d, (d['varswap_iv']>=vs_thr)&(d['atm_iv']>=iv_thr)),
         lambda d: (d['varswap_iv']<vs_thr*0.8), 10, 90, 0.08, 1.0),
        ("varswap>=q70 AND rn_p_down>=q70 + killswitch",
         lambda d: with_killswitch(d, (d['varswap_iv']>=vs_thr)&(d['rn_p_down10']>=pdown_thr)),
         lambda d: d['varswap_iv']<vs_thr*0.8, 10, 90, 0.08, 1.0),
        ("RVI>=40 AND varswap>=q70 + killswitch",
         lambda d: with_killswitch(d, (d['rvi']>=40)&(d['varswap_iv']>=vs_thr)),
         lambda d: d['rvi']<28, 10, 90, 0.10, 1.0),
        # With EMA200 regime
        ("varswap>=q70 above EMA200 + killswitch",
         lambda d: with_killswitch(d, (d['varswap_iv']>=vs_thr)&(d['close']>d['ema200'])),
         lambda d: d['varswap_iv']<vs_thr*0.8, 10, 90, 0.08, 1.0),
        # Voting ensemble
        ("Vote >= 2 of (varswap, atm_iv, rn_p_down) + KS",
         lambda d: with_killswitch(d,
            ((d['varswap_iv']>=vs_thr).astype(int) +
             (d['atm_iv']>=iv_thr).astype(int) +
             (d['rn_p_down10']>=pdown_thr).astype(int) >= 2)),
         lambda d: (d['varswap_iv']<vs_thr*0.7) | (d['rvi']<28), 10, 90, 0.08, 1.0),
        # Leverage
        ("varswap>=q70 + killswitch, lev=1.5x",
         lambda d: with_killswitch(d, d['varswap_iv']>=vs_thr),
         lambda d: d['varswap_iv']<vs_thr*0.8, 10, 60, 0.08, 1.5),
    ]
    rts_res = test_combos('RTS', rts, rts_configs)


if __name__ == "__main__":
    main()
