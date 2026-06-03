"""Comprehensive Si (USD/RUB futures) options-signal backtest & optimizer.

Si specifics tested here:
  - FIZ (retail) are net LONG USD ~93% of days (median LSR ~3.5). Contrarian-fade
    of fiz = mostly SHORT USD, which fights the secular ruble devaluation. So we
    test BOTH: fiz-contrarian (short on fiz euphoria) AND fiz-trend (long with fiz)
    AND yur-follow (smart money). Plus VRP (both sides), trend-following, combos.
  - Long USD/RUB = bet on ruble weakness. The series trends hard on devaluations
    (2020 COVID, 2022 war, 2023-24 weakness), so trend/momentum may dominate.

Outputs per config: total %, maxDD %, N trades, win-rate, profit-factor,
PLUS per-year return of the best config (every year).

Price: si_daily.csv rub_per_usd (continuous front-month).
IV/VRP: si_atm_iv_daily.csv (ATM IV via Black-76) - RV30 (sqrt(252) annualized).
Positioning: si_futoi_history.csv (fiz_lsr, yur_lsr).

Train 2020-2022 / Test 2023-2026.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003   # per side
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')


def load():
    si = pd.read_csv(ROOT/'si_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    df = pd.DataFrame(index=si.index)
    df['close'] = si['rub_per_usd']
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    df['rv10'] = df['ret'].rolling(10).std() * np.sqrt(252) * 100
    # IV / VRP
    ivp = ROOT/'si_atm_iv_daily.csv'
    if ivp.exists():
        iv = pd.read_csv(ivp, parse_dates=['date']).set_index('date').sort_index()
        df['atm_iv'] = iv['atm_iv'].reindex(df.index)
        df['vrp'] = df['atm_iv'] - df['rv30']
        df['iv_pct'] = df['atm_iv'].rolling(252, min_periods=60).rank(pct=True)
        df['vrp_pct'] = df['vrp'].rolling(252, min_periods=60).rank(pct=True)
    # positioning
    ft = pd.read_csv(ROOT/'si_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr'] = ft['fiz_lsr'].reindex(df.index) if 'fiz_lsr' in ft.columns else \
        (ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    df['yur_lsr'] = ft['yur_lsr'].reindex(df.index) if 'yur_lsr' in ft.columns else \
        (ft['yur_long']/ft['yur_short'].replace(0,1)).reindex(df.index)
    df['fiz_lsr_z'] = (df['fiz_lsr'] - df['fiz_lsr'].rolling(120, min_periods=40).mean()) / \
                       df['fiz_lsr'].rolling(120, min_periods=40).std()
    # trend features
    df['ma_fast'] = df['close'].rolling(20).mean()
    df['ma_slow'] = df['close'].rolling(60).mean()
    df['mom60'] = df['close'].pct_change(60)
    df['mom120'] = df['close'].pct_change(120)
    return df


def bt(df, em, xm=None, minh=1, maxh=10**9, lev=1.0, direction=1):
    """Backtest. direction=+1 long USD, -1 short USD.
    em/xm: boolean entry/exit masks aligned to df. If xm None, exits on maxhold only."""
    c = df['close'].values
    em = np.asarray(em, dtype=bool)
    xm = np.asarray(xm, dtype=bool) if xm is not None else np.zeros(len(df), bool)
    n = len(df)
    cash = INIT; eq = np.full(n, INIT, float); inp = False; ei = 0; ep = 0.0
    tr = []; ret_by_year = {}
    last_eq = INIT
    for i in range(n):
        if inp:
            base = (c[i]/ep - 1) * direction
            eq[i] = cash * (1 + base*lev)
            h = i - ei; do = False
            if h >= maxh: do = True
            elif h >= minh and xm[i]: do = True
            if do:
                r = ((c[i]*(1 - SLIP*direction)/ep - 1) * direction) * lev - 2*FEE
                cash *= (1 + r); tr.append(r*100); inp = False; eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i]:
            ei = i; ep = c[i]*(1 + SLIP*direction); inp = True
    s = pd.Series(eq, index=df.index)
    tot = (s.iloc[-1]/INIT - 1)*100
    dd = ((s.cummax()-s)/s.cummax()).max()*100
    nt = len(tr)
    wr = (np.array(tr) > 0).mean()*100 if tr else 0
    pf = (sum(x for x in tr if x > 0)/-sum(x for x in tr if x < 0)) if any(x < 0 for x in tr) else 9.99
    # CAGR
    yrs = (s.index[-1]-s.index[0]).days/365.25
    cagr = ((s.iloc[-1]/INIT)**(1/yrs)-1)*100 if yrs > 0 and s.iloc[-1] > 0 else float('nan')
    return dict(eq=s, tot=tot, dd=dd, n=nt, wr=wr, pf=pf, cagr=cagr)


def per_year(eq):
    """Annual return from equity curve."""
    out = {}
    for y, g in eq.groupby(eq.index.year):
        if len(g) > 1:
            out[y] = (g.iloc[-1]/g.iloc[0]-1)*100
    return out


def fmt(r):
    return f"tot={r['tot']:+7.0f}% CAGR={r['cagr']:+5.0f}% DD={r['dd']:4.0f}% N={r['n']:3} WR={r['wr']:3.0f}% PF={r['pf']:.2f}"


def main():
    df = load()
    have_iv = 'atm_iv' in df.columns and df['atm_iv'].notna().sum() > 50
    d = df.dropna(subset=['close','fiz_lsr']).copy()
    print(f"Period {d.index[0].date()} → {d.index[-1].date()} ({len(d)} days)")
    bh = (d['close'].iloc[-1]/d['close'].iloc[0]-1)*100
    print(f"Si Buy&Hold (long USD): {bh:+.0f}%   start={d['close'].iloc[0]:.2f} end={d['close'].iloc[-1]:.2f} RUB/USD")
    print(f"FIZ LSR: median={d['fiz_lsr'].median():.2f} p10={d['fiz_lsr'].quantile(.1):.2f} p90={d['fiz_lsr'].quantile(.9):.2f} | fiz net-long days={ (d['fiz_lsr']>1).mean()*100:.0f}%")
    print(f"YUR LSR: median={d['yur_lsr'].median():.2f}")
    if have_iv:
        iv = d['atm_iv'].dropna()
        print(f"ATM IV: median={iv.median():.1f}% p5={iv.quantile(.05):.1f}% p95={iv.quantile(.95):.1f}% max={iv.max():.1f}%")
        print(f"VRP: median={d['vrp'].median():.1f} (ATM_IV - RV30)")
    print()

    tr = d[d.index < SPLIT]; te = d[d.index >= SPLIT]
    configs = []  # (name, builder) builder(seg)->result on whole-period uses d

    def run(name, em_fn, xm_fn, minh, maxh, lev, dirn):
        r_full = bt(d, em_fn(d), xm_fn(d) if xm_fn else None, minh, maxh, lev, dirn)
        r_tr = bt(tr, em_fn(tr), xm_fn(tr) if xm_fn else None, minh, maxh, lev, dirn)
        r_te = bt(te, em_fn(te), xm_fn(te) if xm_fn else None, minh, maxh, lev, dirn)
        configs.append((name, r_full, r_tr, r_te))
        return r_full

    # ---------- 1. TREND FOLLOWING (long USD on uptrend) ----------
    print("="*120); print("1) TREND-FOLLOWING (long USD/RUB) — ride ruble devaluation"); print("="*120)
    run("MA20>MA60 long, exit MA20<MA60", lambda x:(x['ma_fast']>x['ma_slow']),
        lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 1.0, 1)
    run("MA20>MA60 long LEV2", lambda x:(x['ma_fast']>x['ma_slow']),
        lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 2.0, 1)
    run("mom60>0 long, exit mom60<0", lambda x:(x['mom60']>0), lambda x:(x['mom60']<0), 1, 10**9, 1.0, 1)
    run("mom120>0 long, exit mom120<0", lambda x:(x['mom120']>0), lambda x:(x['mom120']<0), 1, 10**9, 1.0, 1)

    # ---------- 2. FIZ signals (both directions) ----------
    print("\n"+"="*120); print("2) FIZ positioning signals"); print("="*120)
    # contrarian: fade retail. fiz very long -> short USD. fiz low -> long USD.
    run("FIZ-contrarian: SHORT when LSR>=p90, exit LSR<=median", lambda x:(x['fiz_lsr']>=x['fiz_lsr'].quantile(.9)),
        lambda x:(x['fiz_lsr']<=x['fiz_lsr'].median()), 1, 60, 1.0, -1)
    run("FIZ-contrarian: LONG when LSR<=p20, exit LSR>=p60", lambda x:(x['fiz_lsr']<=x['fiz_lsr'].quantile(.2)),
        lambda x:(x['fiz_lsr']>=x['fiz_lsr'].quantile(.6)), 1, 90, 1.0, 1)
    # trend with retail: fiz piling long -> momentum long USD
    run("FIZ-trend: LONG when LSR>=p70, exit LSR<=p30", lambda x:(x['fiz_lsr']>=x['fiz_lsr'].quantile(.7)),
        lambda x:(x['fiz_lsr']<=x['fiz_lsr'].quantile(.3)), 1, 90, 1.0, 1)
    # yur smart money: yur net long -> long USD
    run("YUR-follow: LONG when yur_lsr>=1.0, exit <0.8", lambda x:(x['yur_lsr']>=1.0),
        lambda x:(x['yur_lsr']<0.8), 1, 120, 1.0, 1)
    run("YUR-follow: SHORT when yur_lsr<=p20, exit>=median", lambda x:(x['yur_lsr']<=x['yur_lsr'].quantile(.2)),
        lambda x:(x['yur_lsr']>=x['yur_lsr'].median()), 1, 90, 1.0, -1)

    # ---------- 3. VRP signals ----------
    if have_iv:
        print("\n"+"="*120); print("3) VRP / IV signals"); print("="*120)
        run("VRP-high: LONG when vrp_pct>=.7, exit <.4", lambda x:(x['vrp_pct']>=.7), lambda x:(x['vrp_pct']<.4), 1, 60, 1.0, 1)
        run("VRP-low: LONG when vrp_pct<=.3, exit >.5", lambda x:(x['vrp_pct']<=.3), lambda x:(x['vrp_pct']>.5), 1, 60, 1.0, 1)
        run("IV-high mean-revert SHORT: iv_pct>=.85, exit<.5", lambda x:(x['iv_pct']>=.85), lambda x:(x['iv_pct']<.5), 1, 30, 1.0, -1)
        run("IV-low LONG (calm trend): iv_pct<=.3 & trend, exit MA cross", lambda x:((x['iv_pct']<=.3)&(x['ma_fast']>x['ma_slow'])),
            lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 1.0, 1)

    # ---------- 4. COMBOS: trend + positioning / vrp filter ----------
    print("\n"+"="*120); print("4) COMBOS"); print("="*120)
    run("Trend(MA) AND fiz building (LSR>=p50)", lambda x:((x['ma_fast']>x['ma_slow'])&(x['fiz_lsr']>=x['fiz_lsr'].median())),
        lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 1.0, 1)
    run("Trend(MA) AND yur_lsr>=0.9", lambda x:((x['ma_fast']>x['ma_slow'])&(x['yur_lsr']>=0.9)),
        lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 1.0, 1)
    if have_iv:
        run("Trend(MA) AND vrp>0 filter LEV1.5", lambda x:((x['ma_fast']>x['ma_slow'])&(x['vrp']>0)),
            lambda x:(x['ma_fast']<x['ma_slow']), 1, 10**9, 1.5, 1)
    run("Trend(mom120) AND fiz>=p50 LEV2", lambda x:((x['mom120']>0)&(x['fiz_lsr']>=x['fiz_lsr'].median())),
        lambda x:(x['mom120']<0), 1, 10**9, 2.0, 1)

    # ---------- print all configs ----------
    print("\n"+"="*120)
    print(f"{'CONFIG':<52}{'FULL':<46}{'TRAIN/TEST CAGR'}")
    print("="*120)
    for name, rf, rtr, rte in configs:
        print(f"{name:<52}{fmt(rf):<46}tr={rtr['cagr']:+.0f}% te={rte['cagr']:+.0f}%")

    # ---------- pick best by: test CAGR>=15, DD<=25, robust (train&test positive) ----------
    print("\n"+"="*120)
    print("BEST CONFIG SELECTION (target ~40%/yr, DD<=20%, robust train+test)")
    print("="*120)
    def score(c):
        _, rf, rtr, rte = c
        if rte['cagr'] != rte['cagr'] or rtr['cagr'] != rtr['cagr']:
            return -1e9
        # penalize DD over 20, reward closeness to 40 CAGR full, require both halves positive
        robust = 1 if (rtr['cagr'] > 0 and rte['cagr'] > 0) else 0
        ddpen = max(0, rf['dd']-20)*2
        return robust*1000 - abs(rf['cagr']-40) - ddpen + min(rf['cagr'],60)
    ranked = sorted(configs, key=score, reverse=True)
    for name, rf, rtr, rte in ranked[:6]:
        print(f"  {name:<50} {fmt(rf)}  tr={rtr['cagr']:+.0f}% te={rte['cagr']:+.0f}%")

    best = ranked[0]
    name, rf, rtr, rte = best
    print("\n"+"="*120)
    print(f"WINNER: {name}")
    print(f"  FULL: {fmt(rf)}")
    print(f"  TRAIN(2020-22): CAGR={rtr['cagr']:+.0f}% DD={rtr['dd']:.0f}% N={rtr['n']}")
    print(f"  TEST (2023-26): CAGR={rte['cagr']:+.0f}% DD={rte['dd']:.0f}% N={rte['n']}")
    print("  PER-YEAR returns:")
    py = per_year(rf['eq'])
    for y in sorted(py):
        print(f"    {y}: {py[y]:+7.1f}%")
    bh_py = per_year(d['close'])  # buy&hold per year for comparison
    print("  (Buy&Hold per-year for reference:)")
    for y in sorted(bh_py):
        print(f"    {y}: {bh_py[y]:+7.1f}%")


if __name__ == "__main__":
    main()
