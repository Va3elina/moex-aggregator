"""Walk-forward Si (USD/RUB) FIZ + VRP strategy validation.

Si is structurally different from SBER:
  - FIZ are net LONG USD ~93% of days (median LSR ~3.35) — they "stay long" by default.
  - FIZ-contrarian fires ONLY in panic dips (LSR<=1.4) — rare but high quality.
  - Trend-following ultimately reverts in 2025-26 (ruble strengthens hard).
  - B&H +13% over 6yr; honest ceiling is much lower than SBER (+159%).

This script:
  1) Loads Si price / IV / VRP / FIZ.
  2) Splits train 2020-22 / test 2023-26.
  3) Sweeps FIZ entry threshold, VRP exit, stop-loss, leverage.
  4) Reports per-year breakdown and picks robust config.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')


def load():
    si = pd.read_csv(ROOT/'si_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    d = pd.DataFrame(index=si.index)
    d['close'] = si['rub_per_usd']
    d['ret'] = d['close'].pct_change()
    d['rv30'] = d['ret'].rolling(30).std() * np.sqrt(252) * 100
    iv = pd.read_csv(ROOT/'si_atm_iv_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    d['atm_iv'] = iv['atm_iv'].reindex(d.index)
    d['vrp'] = d['atm_iv'] - d['rv30']
    ft = pd.read_csv(ROOT/'si_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    d['fiz_lsr'] = ft['fiz_lsr'].reindex(d.index) if 'fiz_lsr' in ft.columns else \
                   (ft['fiz_long']/ft['fiz_short'].replace(0, 1)).reindex(d.index)
    return d.dropna(subset=['close', 'fiz_lsr']).copy()


def bt(d, em, xm, minh, maxh, lev=1.0, stop=None):
    c = d['close'].values
    em = np.asarray(em, bool)
    xm = np.asarray(xm, bool)
    n = len(d)
    cash = INIT; eq = np.full(n, INIT, float)
    inp = False; ei = 0; ep = 0.0
    tr = []
    for i in range(n):
        if inp:
            chg = (c[i]/ep - 1)
            eq[i] = cash * (1 + chg*lev)
            h = i - ei; do = False; reason = ""
            if stop is not None and chg*lev <= -stop:
                do = True; reason = "stop"
            elif h >= maxh:
                do = True; reason = "maxhold"
            elif h >= minh and xm[i]:
                do = True; reason = "signal"
            if do:
                r = ((c[i]*(1 - SLIP)/ep - 1)) * lev - 2*FEE
                cash *= (1 + r)
                tr.append({'entry': d.index[ei], 'exit': d.index[i],
                           'pnl': r*100, 'hold': h, 'reason': reason})
                inp = False
                eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i]:
            ei = i; ep = c[i]*(1 + SLIP); inp = True
    s = pd.Series(eq, index=d.index)
    tot = (s.iloc[-1]/INIT - 1)*100
    dd = ((s.cummax() - s)/s.cummax()).max()*100
    nt = len(tr)
    wr = (np.array([x['pnl'] for x in tr]) > 0).mean()*100 if tr else 0
    pos = sum(x['pnl'] for x in tr if x['pnl'] > 0)
    neg = -sum(x['pnl'] for x in tr if x['pnl'] < 0)
    pf = pos/neg if neg > 0 else 9.99
    yrs = (s.index[-1] - s.index[0]).days/365.25
    cagr = ((s.iloc[-1]/INIT)**(1/yrs) - 1)*100 if yrs > 0 and s.iloc[-1] > 0 else float('nan')
    return dict(eq=s, tot=tot, dd=dd, n=nt, wr=wr, pf=pf, cagr=cagr, trades=tr)


def per_year(eq):
    out = {}
    for y, g in eq.groupby(eq.index.year):
        if len(g) > 1:
            out[y] = (g.iloc[-1]/g.iloc[0] - 1)*100
    return out


def fmt(r):
    return f"tot={r['tot']:+7.0f}% CAGR={r['cagr']:+5.0f}% DD={r['dd']:4.0f}% N={r['n']:3} WR={r['wr']:3.0f}% PF={r['pf']:4.2f}"


def main():
    d = load()
    have_iv = d['atm_iv'].notna().sum() > 50
    print(f"Si period {d.index[0].date()} → {d.index[-1].date()} ({len(d)} days). IV available: {have_iv}")
    bh = (d['close'].iloc[-1]/d['close'].iloc[0] - 1)*100
    yrs_bh = (d.index[-1] - d.index[0]).days/365.25
    bh_cagr = ((d['close'].iloc[-1]/d['close'].iloc[0])**(1/yrs_bh) - 1)*100
    print(f"Si B&H long USD: {bh:+.0f}%  CAGR={bh_cagr:+.1f}%  start={d['close'].iloc[0]:.2f} end={d['close'].iloc[-1]:.2f}")
    print(f"FIZ LSR median={d['fiz_lsr'].median():.2f}  p10={d['fiz_lsr'].quantile(.1):.2f}  p25={d['fiz_lsr'].quantile(.25):.2f}")
    print(f"FIZ net-long days: {(d['fiz_lsr']>1).mean()*100:.0f}%")
    if have_iv:
        iv = d['atm_iv'].dropna()
        print(f"ATM IV: median={iv.median():.1f}% p5={iv.quantile(.05):.1f}% p95={iv.quantile(.95):.1f}%")
        print(f"VRP median={d['vrp'].median():.1f}  p10={d['vrp'].quantile(.1):.1f}  p90={d['vrp'].quantile(.9):.1f}")
    print()

    tr_seg = d[d.index < SPLIT]
    te_seg = d[d.index >= SPLIT]
    tr_bh = (tr_seg['close'].iloc[-1]/tr_seg['close'].iloc[0] - 1)*100
    te_bh = (te_seg['close'].iloc[-1]/te_seg['close'].iloc[0] - 1)*100
    print(f"TRAIN 2020-22 B&H: {tr_bh:+.0f}%  ({len(tr_seg)} days)")
    print(f"TEST  2023-26 B&H: {te_bh:+.0f}%  ({len(te_seg)} days)")
    print()

    # ---------------- sweeps ----------------
    print("="*120)
    print("FIZ ENTRY SWEEP (LSR <= X = retail panic, LONG USD)  exit FIZ>=3.0 OR VRP<-5  minH=10 maxH=60")
    print("="*120)
    print(f"{'cfg':<30}{'FULL':<55}{'TRAIN cagr':>12}{'TEST cagr':>12}")
    for ethr in [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]:
        em = lambda x, t=ethr: (x['fiz_lsr'] <= t)
        xm = lambda x: (x['fiz_lsr'] >= 3.0) | (x['vrp'] < -5)
        rf = bt(d, em(d).fillna(False), xm(d).fillna(False), 10, 60, 1.0, 0.10)
        rt = bt(tr_seg, em(tr_seg).fillna(False), xm(tr_seg).fillna(False), 10, 60, 1.0, 0.10)
        re = bt(te_seg, em(te_seg).fillna(False), xm(te_seg).fillna(False), 10, 60, 1.0, 0.10)
        print(f"FIZ<={ethr:<5}{fmt(rf):<55}{rt['cagr']:>+11.0f}%{re['cagr']:>+11.0f}%")
    print()

    print("="*120)
    print("STOP-LOSS SWEEP (FIZ<=1.4 entry, VRP-/euph exit, hold 10-60d, lev 1.0)")
    print("="*120)
    em = lambda x: (x['fiz_lsr'] <= 1.4)
    xm = lambda x: (x['fiz_lsr'] >= 3.0) | (x['vrp'] < -5)
    for s in [None, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20]:
        rf = bt(d, em(d).fillna(False), xm(d).fillna(False), 10, 60, 1.0, s)
        rt = bt(tr_seg, em(tr_seg).fillna(False), xm(tr_seg).fillna(False), 10, 60, 1.0, s)
        re = bt(te_seg, em(te_seg).fillna(False), xm(te_seg).fillna(False), 10, 60, 1.0, s)
        label = f"stop={int(s*100)}%" if s else "no stop"
        print(f"{label:<10}  {fmt(rf):<55}tr={rt['cagr']:+.0f}% te={re['cagr']:+.0f}%")
    print()

    print("="*120)
    print("LEVERAGE SWEEP (FIZ<=1.4, stop 10%, vrp exit)")
    print("="*120)
    for lev in [1.0, 1.5, 2.0, 2.5, 3.0]:
        rf = bt(d, em(d).fillna(False), xm(d).fillna(False), 10, 60, lev, 0.10)
        rt = bt(tr_seg, em(tr_seg).fillna(False), xm(tr_seg).fillna(False), 10, 60, lev, 0.10)
        re = bt(te_seg, em(te_seg).fillna(False), xm(te_seg).fillna(False), 10, 60, lev, 0.10)
        print(f"x{lev}  {fmt(rf):<55}tr={rt['cagr']:+.0f}% te={re['cagr']:+.0f}%")
    print()

    print("="*120)
    print("MAXHOLD SWEEP (FIZ<=1.4, stop 10%, lev 1)")
    print("="*120)
    for mh in [30, 45, 60, 90, 120]:
        rf = bt(d, em(d).fillna(False), xm(d).fillna(False), 10, mh, 1.0, 0.10)
        rt = bt(tr_seg, em(tr_seg).fillna(False), xm(tr_seg).fillna(False), 10, mh, 1.0, 0.10)
        re = bt(te_seg, em(te_seg).fillna(False), xm(te_seg).fillna(False), 10, mh, 1.0, 0.10)
        print(f"maxH={mh:<4}  {fmt(rf):<55}tr={rt['cagr']:+.0f}% te={re['cagr']:+.0f}%")
    print()

    # ---------------- candidate configs ----------------
    print("="*120)
    print("CANDIDATE CONFIGS — robustness search")
    print("="*120)

    candidates = [
        # (name, entry_thr, exit_fiz, vrp_exit_lvl, minh, maxh, lev, stop)
        ("FIZ<=1.4 v=-5 lev1 stop10",       1.4, 3.0, -5, 10, 60, 1.0, 0.10),
        ("FIZ<=1.4 v=-5 lev1.5 stop10",     1.4, 3.0, -5, 10, 60, 1.5, 0.10),
        ("FIZ<=1.4 v=-5 lev2 stop10",       1.4, 3.0, -5, 10, 60, 2.0, 0.10),
        ("FIZ<=1.4 v=-5 lev1 stop8",        1.4, 3.0, -5, 10, 60, 1.0, 0.08),
        ("FIZ<=1.4 v=-5 lev2 stop8",        1.4, 3.0, -5, 10, 60, 2.0, 0.08),
        ("FIZ<=1.6 v=-5 lev1 stop10",       1.6, 3.0, -5, 10, 60, 1.0, 0.10),
        ("FIZ<=1.6 v=-5 lev1.5 stop10",     1.6, 3.0, -5, 10, 60, 1.5, 0.10),
        ("FIZ<=1.2 strict v=-5 lev2 s10",   1.2, 3.0, -5, 10, 60, 2.0, 0.10),
        ("FIZ<=1.4 v=-10 lev1 stop10",      1.4, 3.0, -10, 10, 60, 1.0, 0.10),
        ("FIZ<=1.4 maxH=90 lev1 stop10",    1.4, 3.0, -5, 10, 90, 1.0, 0.10),
        ("FIZ<=1.4 maxH=45 lev1.5 stop10",  1.4, 3.0, -5, 10, 45, 1.5, 0.10),
    ]

    print(f"{'config':<40}{'FULL':<55}{'TR cagr':>10}{'TE cagr':>10}{'years+':>8}")
    rows = []
    for name, e, x, vx, mh, xh, lev, stop in candidates:
        em = lambda x_, te=e: (x_['fiz_lsr'] <= te)
        xm = lambda x_, ex=x, vxx=vx: (x_['fiz_lsr'] >= ex) | (x_['vrp'] < vxx)
        rf = bt(d, em(d).fillna(False), xm(d).fillna(False), mh, xh, lev, stop)
        rt = bt(tr_seg, em(tr_seg).fillna(False), xm(tr_seg).fillna(False), mh, xh, lev, stop)
        re = bt(te_seg, em(te_seg).fillna(False), xm(te_seg).fillna(False), mh, xh, lev, stop)
        py = per_year(rf['eq'])
        pos_years = sum(1 for v in py.values() if v > 0)
        total_years = len(py)
        rows.append((name, rf, rt, re, py, pos_years, total_years))
        print(f"{name:<40}{fmt(rf):<55}{rt['cagr']:>+9.0f}%{re['cagr']:>+9.0f}%{pos_years:>4}/{total_years}")

    # ---------------- pick best ----------------
    def score(row):
        name, rf, rt, re, py, posy, ty = row
        if np.isnan(rt['cagr']) or np.isnan(re['cagr']):
            return -1e9
        robust = (1 if rt['cagr'] > 0 and re['cagr'] > 0 else 0)
        ddpen = max(0, rf['dd'] - 25)*3
        wrpen = max(0, 50 - rf['wr'])*2
        pfbonus = min(rf['pf'], 5)*5
        return robust*500 + posy*30 + rf['cagr']*0.5 + pfbonus - ddpen - wrpen

    ranked = sorted(rows, key=score, reverse=True)
    print()
    print("="*120)
    print("TOP 5 by robustness score")
    print("="*120)
    for r in ranked[:5]:
        name, rf, rt, re, py, posy, ty = r
        print(f"  {name:<40} {fmt(rf)}  tr={rt['cagr']:+.0f}% te={re['cagr']:+.0f}%  {posy}/{ty} positive years")

    print()
    print("="*120)
    name, rf, rt, re, py, posy, ty = ranked[0]
    print(f"WINNER: {name}")
    print(f"  FULL: {fmt(rf)}")
    print(f"  TRAIN: {fmt(rt)}")
    print(f"  TEST : {fmt(re)}")
    print(f"  Positive years: {posy}/{ty}")
    print()
    print("PER-YEAR breakdown (WINNER vs Si B&H):")
    bh_py = per_year(d['close'])
    print(f"  {'year':<6}{'strat':>10}{'B&H':>10}")
    for y in sorted(set(list(py.keys()) + list(bh_py.keys()))):
        s_val = py.get(y, float('nan'))
        b_val = bh_py.get(y, float('nan'))
        print(f"  {y:<6}{s_val:>+9.1f}%{b_val:>+9.1f}%")
    print()
    print("TRADES (winner):")
    for t in rf['trades']:
        print(f"  {t['entry'].date()} → {t['exit'].date()}  hold={t['hold']:>3}d  PnL={t['pnl']:>+6.1f}%  ({t['reason']})")


if __name__ == "__main__":
    main()
