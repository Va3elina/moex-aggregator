"""RTS options-signal backtester — full pipeline analysis.

Spot = RTS index (rts_daily.csv, RTSI index points).
ATM IV from rts_option_indicators_daily.csv (Black-76 inversion).
RVI = rvi_daily.csv (official MOEX RTS implied-vol index) — used to VALIDATE ATM IV.
FIZ/YUR positioning from rts_futoi_signals.csv.

Signals tested (train/test split 2023-01-01):
  1. FIZ direction (on RTS retail is TREND/momentum, NOT contrarian — see analyze_rts_futoi)
  2. VRP high/low
  3. RVI panic (RVI>=40 LONG)
  4. Trend / momentum (RTS trends hard)
  5. Combos
Per-year breakdown for the best config. Leverage up to 2x.

Goal: ~40%/year, DD<=20%, high WR. Honesty over pretty numbers.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')


def load():
    ind = pd.read_csv(ROOT/'rts_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    rts = pd.read_csv(ROOT/'rts_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    futoi = pd.read_csv(ROOT/'rts_futoi_signals.csv', parse_dates=['date']).set_index('date').sort_index()
    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE').sort_index()

    df = rts[['close']].copy()
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    df['atm_iv'] = ind['atm_iv'].reindex(df.index)
    df['vrp'] = df['atm_iv'] - df['rv30']
    df['rvi'] = rvi['close'].reindex(df.index).ffill()
    df['fiz_lsr'] = (futoi['fiz_long'] / futoi['fiz_short'].replace(0, 1)).reindex(df.index)
    df['yur_lsr'] = (futoi['yur_long'] / futoi['yur_short'].replace(0, 1)).reindex(df.index)
    # trend features
    df['sma50'] = df['close'].rolling(50).mean()
    df['sma200'] = df['close'].rolling(200).mean()
    df['mom60'] = df['close'].pct_change(60) * 100
    return df


def bt(df, em, xm, minh, maxh, lev=1.0):
    c = df['close'].values
    em = np.asarray(em); xm = np.asarray(xm); n = len(df)
    cash = INIT; eq = np.full(n, INIT, float); inp = False; ei = 0; ep = 0; tr = []
    for i in range(n):
        if inp:
            base = c[i]/ep - 1; eq[i] = cash*(1+base*lev); h = i-ei; do = False
            if h >= maxh: do = True
            elif h >= minh and xm[i]: do = True
            if do:
                r = (c[i]*(1-SLIP)/ep - 1)*lev - 2*FEE
                cash *= (1+r); tr.append(r*100); inp = False; eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i]:
            ei = i; ep = c[i]*(1+SLIP); inp = True
    s = pd.Series(eq, index=df.index)
    tot = (s.iloc[-1]/INIT - 1)*100
    dd = ((s.cummax()-s)/s.cummax()).max()*100
    nt = len(tr)
    wr = (np.array(tr) > 0).mean()*100 if tr else 0
    pf = (sum(x for x in tr if x > 0)/-sum(x for x in tr if x < 0)) if any(x < 0 for x in tr) else 9.99
    return tot, dd, nt, wr, pf, s


def fmt(r):
    return f"{r[0]:+.0f}% DD{r[1]:.0f}% N{r[2]} WR{r[3]:.0f}% PF{r[4]:.2f}"


def split_bt(df, ef, xf, mh, xh, lev=1.0):
    out = []
    for seg in [df[df.index < SPLIT], df[df.index >= SPLIT]]:
        if len(seg) < 30:
            out.append((0, 0, 0, 0, 0, None)); continue
        out.append(bt(seg, ef(seg).fillna(False), xf(seg).fillna(False), mh, xh, lev))
    return out


def per_year(df, ef, xf, mh, xh, lev=1.0):
    em = ef(df).fillna(False); xm = xf(df).fillna(False)
    _, _, _, _, _, eq = bt(df, em, xm, mh, xh, lev)
    rows = []
    cagr_geom = 1.0
    for yr, idx in eq.groupby(eq.index.year).groups.items():
        seg = eq.loc[idx]
        yret = (seg.iloc[-1]/seg.iloc[0] - 1)*100
        ydd = ((seg.cummax()-seg)/seg.cummax()).max()*100
        rows.append((yr, yret, ydd))
        cagr_geom *= (1 + yret/100)
    return rows, eq


def main():
    df = load().copy()
    valid = df[df['atm_iv'].notna()]
    print(f"Period (ATM IV avail): {valid.index[0].date()} -> {valid.index[-1].date()}  ({len(valid)} days)")
    bh = (valid['close'].iloc[-1]/valid['close'].iloc[0] - 1)*100
    print(f"RTS index B&H over options window: {bh:+.0f}%\n")

    # ---------- VALIDATION: ATM IV vs RVI ----------
    v = df.dropna(subset=['atm_iv', 'rvi'])
    corr = v['atm_iv'].corr(v['rvi'])
    print("="*70)
    print(f"VALIDATION  corr(ATM_IV, RVI) = {corr:.3f}   [target > 0.80]   n={len(v)}")
    print(f"  ATM_IV  median={v['atm_iv'].median():.1f}  p5={v['atm_iv'].quantile(.05):.1f}  p95={v['atm_iv'].quantile(.95):.1f}")
    print(f"  RVI     median={v['rvi'].median():.1f}  p5={v['rvi'].quantile(.05):.1f}  p95={v['rvi'].quantile(.95):.1f}")
    print(f"  ratio ATM_IV/RVI median = {(v['atm_iv']/v['rvi']).median():.3f}")
    print("="*70)

    # ---------- quintile diagnostics ----------
    dfx = valid.copy()
    for h in [20, 60]:
        dfx[f'fwd{h}'] = dfx['close'].pct_change(h).shift(-h)*100
    for sig in ['vrp', 'atm_iv', 'rvi', 'fiz_lsr', 'mom60']:
        s = dfx.dropna(subset=[sig, 'fwd60'])
        if len(s) < 50: continue
        s['q'] = pd.qcut(s[sig], 5, labels=['Q1','Q2','Q3','Q4','Q5'], duplicates='drop')
        print(f"\nForward 60d by {sig} quintile:")
        for b in s['q'].cat.categories:
            sub = s[s['q'] == b]
            r60 = sub['fwd60'].dropna()
            print(f"  {b}  n={len(sub):>3}  60d={r60.mean():>+6.1f}%  P_up={ (r60>0).mean()*100:>3.0f}%  ({sig}<= {sub[sig].max():.1f})")

    # ---------- SIGNAL BACKTESTS (train/test) ----------
    print("\n" + "="*90)
    print("SIGNAL BACKTESTS  (train <2023 | test >=2023)   fmt: total DD N WR PF")
    print("="*90)
    d = df.dropna(subset=['atm_iv']).copy()

    def show(name, ef, xf, mh, xh, lev=1.0):
        tr, te = split_bt(d, ef, xf, mh, xh, lev)
        print(f"{name:<42} TRAIN {fmt(tr):<34} TEST {fmt(te)}")

    # 1. FIZ momentum (retail long = trend on RTS): enter when fiz high, exit when fiz low
    print("\n-- FIZ momentum (retail-long = trend, RTS direction) --")
    for thr in [1.2, 1.5, 1.8]:
        show(f"FIZ>={thr} enter / <0.8 exit", lambda x,t=thr:(x['fiz_lsr']>=t),
             lambda x:(x['fiz_lsr']<0.8), 5, 90)
    # contrarian (SBER-style) for comparison
    show("FIZ<=0.6 enter / >=2.0 exit (contrarian)", lambda x:(x['fiz_lsr']<=0.6),
         lambda x:(x['fiz_lsr']>=2.0), 10, 90)

    # 2. VRP
    print("\n-- VRP --")
    for thr in [0, 5, 10]:
        show(f"VRP>={thr} enter / VRP<{thr-10} exit", lambda x,t=thr:(x['vrp']>=t),
             lambda x,t=thr:(x['vrp']<t-10), 10, 60)
    for thr in [-5, 0, 5]:
        show(f"VRP<={thr} enter / VRP>{thr+15} exit (crypto)", lambda x,t=thr:(x['vrp']<=t),
             lambda x,t=thr:(x['vrp']>t+15), 10, 60)

    # 3. RVI panic
    print("\n-- RVI panic (high vol = LONG mean-revert) --")
    for thr in [30, 40, 50]:
        show(f"RVI>={thr} enter / RVI<{thr*0.7:.0f} exit", lambda x,t=thr:(x['rvi']>=t),
             lambda x,t=thr:(x['rvi']<t*0.7), 5, 60)

    # 4. Trend/momentum
    print("\n-- Trend / momentum --")
    show("close>SMA50>SMA200 / exit close<SMA50", lambda x:((x['close']>x['sma50'])&(x['sma50']>x['sma200'])),
         lambda x:(x['close']<x['sma50']), 5, 250)
    show("mom60>0 enter / mom60<0 exit", lambda x:(x['mom60']>0), lambda x:(x['mom60']<0), 5, 250)

    # 5. Combos
    print("\n-- Combos --")
    show("trend + FIZ>=1.2", lambda x:((x['close']>x['sma50'])&(x['fiz_lsr']>=1.2)),
         lambda x:(x['close']<x['sma50']), 5, 250)
    show("trend OR RVI>=40 / exit close<SMA50 & RVI<30",
         lambda x:((x['close']>x['sma50'])|(x['rvi']>=40)),
         lambda x:((x['close']<x['sma50'])&(x['rvi']<30)), 5, 250)

    # ---------- BEST CONFIG candidates with per-year + leverage ----------
    print("\n" + "="*90)
    print("BEST-CONFIG SEARCH (full sample, per-year, leverage)")
    print("="*90)
    candidates = {
        'trend(SMA50>200)+RVI40 long':
            (lambda x:((x['close']>x['sma50'])&(x['sma50']>x['sma200']))|(x['rvi']>=40),
             lambda x:((x['close']<x['sma50'])&(x['rvi']<30)), 5, 250),
        'trend SMA50>200':
            (lambda x:((x['close']>x['sma50'])&(x['sma50']>x['sma200'])),
             lambda x:(x['close']<x['sma50']), 5, 250),
        'FIZ>=1.2 momentum':
            (lambda x:(x['fiz_lsr']>=1.2), lambda x:(x['fiz_lsr']<0.8), 5, 90),
        'mom60>0 + RVI40':
            (lambda x:((x['mom60']>0)|(x['rvi']>=40)), lambda x:((x['mom60']<0)&(x['rvi']<30)), 5, 250),
    }
    for name, (ef, xf, mh, xh) in candidates.items():
        for lev in [1.0, 1.5, 2.0]:
            full = bt(d, ef(d).fillna(False), xf(d).fillna(False), mh, xh, lev)
            yrs, _ = per_year(d, ef, xf, mh, xh, lev)
            ann = [f"{y}:{r:+.0f}%(dd{dd:.0f})" for y, r, dd in yrs]
            print(f"\n{name} | lev{lev}: TOTAL {fmt(full)}")
            print("   " + "  ".join(ann))


if __name__ == "__main__":
    main()
