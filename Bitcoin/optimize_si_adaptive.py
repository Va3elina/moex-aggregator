"""Adaptive Si (USD/RUB) FIZ + VRP strategy — rolling-percentile thresholds.

Si specifics:
  - FIZ are net LONG USD ~93% of days; LSR drifts a lot, so use ROLLING rank.
  - Ruble is NOT one-way: strong devaluation 2020/2022, sharp RECOVERY 2025-26.
    So pure long-USD trend fails; positioning mean-reversion + a regime filter helps.
  - Contrarian LONG entry = fiz capitulates (rank low) = local USD bottom.
  - Optional VRP exit (sell into vol spikes) once IV available.

Targets: positive most years, DD<=20%, leverage up to 2x allowed.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100; FEE = 0.0003; SLIP = 0.0001


def load():
    si = pd.read_csv(ROOT/'si_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    d = pd.DataFrame(index=si.index)
    d['close'] = si['rub_per_usd']
    d['ret'] = d['close'].pct_change()
    d['rv30'] = d['ret'].rolling(30).std()*np.sqrt(252)*100
    ft = pd.read_csv(ROOT/'si_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    d['fiz_lsr'] = ft['fiz_lsr'].reindex(d.index) if 'fiz_lsr' in ft.columns else \
        (ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(d.index)
    ivp = ROOT/'si_atm_iv_daily.csv'
    if ivp.exists():
        iv = pd.read_csv(ivp, parse_dates=['date']).set_index('date').sort_index()
        d['atm_iv'] = iv['atm_iv'].reindex(d.index)
        d['vrp'] = d['atm_iv'] - d['rv30']
    else:
        d['atm_iv'] = np.nan; d['vrp'] = np.nan
    win = 252
    d['fiz_rank'] = d['fiz_lsr'].rolling(win, min_periods=60).apply(
        lambda x: (x < x.iloc[-1]).mean()*100, raw=False)  # 0=lowest in window
    d['ma_fast'] = d['close'].rolling(20).mean()
    d['ma_slow'] = d['close'].rolling(60).mean()
    return d.dropna(subset=['close','fiz_lsr']).copy()


def bt(d, em, xm, minh, maxh, lev=1.0, stop=None, direction=1):
    c = d['close'].values; em = np.asarray(em, bool); xm = np.asarray(xm, bool); n = len(d)
    cash = INIT; eq = np.full(n, INIT, float); inp = False; ei = 0; ep = 0.0; tr = []
    for i in range(n):
        if inp:
            chg = (c[i]/ep-1)*direction; eq[i] = cash*(1+chg*lev); h = i-ei; do = False
            if stop is not None and chg*lev <= -stop: do = True
            elif h >= maxh: do = True
            elif h >= minh and xm[i]: do = True
            if do:
                r = ((c[i]*(1-SLIP*direction)/ep-1)*direction)*lev - 2*FEE
                cash *= (1+r); tr.append((d.index[i], r*100)); inp = False; eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i]: ei = i; ep = c[i]*(1+SLIP*direction); inp = True
    s = pd.Series(eq, index=d.index); tot = (s.iloc[-1]/INIT-1)*100
    dd = ((s.cummax()-s)/s.cummax()).max()*100; nt = len(tr)
    wr = (np.array([x[1] for x in tr]) > 0).mean()*100 if tr else 0
    pos = sum(x[1] for x in tr if x[1] > 0); neg = -sum(x[1] for x in tr if x[1] < 0)
    pf = pos/neg if neg > 0 else 9.99
    yrs = (s.index[-1]-s.index[0]).days/365.25
    cagr = ((s.iloc[-1]/INIT)**(1/yrs)-1)*100 if yrs > 0 and s.iloc[-1] > 0 else float('nan')
    return s, tot, dd, nt, wr, pf, cagr, tr


def per_year(s):
    return {y: (g.iloc[-1]/g.iloc[0]-1)*100 for y, g in s.groupby(s.index.year) if len(g) > 1}


def main():
    d = load()
    have_iv = d['atm_iv'].notna().sum() > 50
    print(f"Period {d.index[0].date()} → {d.index[-1].date()} ({len(d)} days). IV available: {have_iv}")
    # exit clause helper
    def vexit(x, base):
        return (base) | (x['vrp'] < -5) if have_iv else base

    configs = [
        ("ADAPT FIZ rank<=20 long", lambda x: x['fiz_rank'] <= 20,
            lambda x: vexit(x, x['fiz_rank'] >= 70), 10, 90, None, 1, 1.0),
        ("ADAPT FIZ rank<=25 long", lambda x: x['fiz_rank'] <= 25,
            lambda x: vexit(x, x['fiz_rank'] >= 60), 10, 90, None, 1, 1.0),
        ("ADAPT FIZ rank<=15 long (strict)", lambda x: x['fiz_rank'] <= 15,
            lambda x: vexit(x, x['fiz_rank'] >= 70), 10, 90, None, 1, 1.0),
        ("ADAPT rank<=20 + stop 12%", lambda x: x['fiz_rank'] <= 20,
            lambda x: vexit(x, x['fiz_rank'] >= 70), 10, 90, 0.12, 1, 1.0),
        ("ADAPT rank<=20 LEV2", lambda x: x['fiz_rank'] <= 20,
            lambda x: vexit(x, x['fiz_rank'] >= 70), 10, 90, 0.15, 1, 2.0),
        ("ADAPT rank<=20 + MA-trend filter LEV1.5",
            lambda x: (x['fiz_rank'] <= 25) & (x['ma_fast'] > x['ma_slow']*0.97),
            lambda x: vexit(x, x['fiz_rank'] >= 60), 10, 90, 0.12, 1, 1.5),
    ]
    if have_iv:
        configs.append(("VRP-short vol: SHORT-USD-proxy when vrp>p80",
            lambda x: x['vrp'] > x['vrp'].quantile(.8),
            lambda x: x['vrp'] < x['vrp'].median(), 5, 30, 0.10, -1, 1.0))

    print(f"\n{'config':<40}{'tot':>7}{'CAGR':>6}{'DD':>6}{'N':>4}{'WR':>5}{'PF':>6}  per-year %")
    print("="*130)
    best = None
    for nm, ef, xf, mh, xh, stop, dirn, lev in configs:
        s, tot, dd, nt, wr, pf, cagr, tr = bt(d, ef(d).fillna(False), xf(d).fillna(False),
                                              mh, xh, lev, stop, dirn)
        yr = per_year(s)
        ys = " ".join(f"{y}:{v:+.0f}" for y, v in sorted(yr.items()))
        print(f"{nm:<40}{tot:>+6.0f}%{cagr:>+5.0f}%{dd:>5.0f}%{nt:>4}{wr:>4.0f}%{pf:>6.2f}  {ys}")
        # robust score: positive in most years, DD bounded, CAGR up
        npos = sum(1 for v in yr.values() if v > -2)
        sc = npos*100 - max(0, dd-20)*3 + min(cagr, 50)
        if best is None or sc > best[0]:
            best = (sc, nm, s, tot, dd, nt, wr, pf, cagr, yr)

    print("\n" + "="*130)
    _, nm, s, tot, dd, nt, wr, pf, cagr, yr = best
    print(f"BEST (robust): {nm}")
    print(f"  total={tot:+.0f}%  CAGR={cagr:+.0f}%  DD={dd:.0f}%  N={nt}  WR={wr:.0f}%  PF={pf:.2f}")
    print("  per-year:")
    for y in sorted(yr):
        print(f"    {y}: {yr[y]:+7.1f}%")
    print("\n(per-year = equity % change within each calendar year)")


if __name__ == "__main__":
    main()
