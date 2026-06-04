"""RTS refine — find honest config that defends 2022 with cash/short hedge.

Focus: profitable on TEST + survives TRAIN + minimal trades to avoid noise.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
TRAIN_END = pd.Timestamp('2022-12-31')
TEST_START = pd.Timestamp('2023-01-01')


def load():
    ind = pd.read_csv(ROOT/'rts_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    px = pd.read_csv(ROOT/'rts_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    df = pd.DataFrame(index=ind.index)
    df['close'] = px['close'].reindex(ind.index).ffill()
    df = df.join(ind, how='left')
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    df['vrp'] = df['atm_iv'] - df['rv30']
    ft = pd.read_csv(ROOT/'rts_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr'] = (ft['fiz_long'] / ft['fiz_short'].replace(0, 1)).reindex(df.index)
    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE')['close']
    df['rvi'] = rvi.reindex(df.index).ffill()
    df['ma200'] = df['close'].rolling(200).mean()
    return df[df['atm_iv'].notna()].copy()


def bt(df, em, xm, minh, maxh, lev=1.0, stop=None, direction='long'):
    c = df['close'].values
    em = np.asarray(em); xm = np.asarray(xm)
    n = len(df)
    cash = INIT
    eq = np.full(n, INIT, float)
    inp = False; ei = 0; ep = 0
    tr = []
    sign = 1 if direction == 'long' else -1
    for i in range(n):
        if inp:
            chg = sign * (c[i]/ep - 1)
            eq[i] = cash * (1 + chg * lev)
            h = i - ei
            do = False
            if stop is not None and chg * lev <= -stop:
                do = True
            elif h >= maxh:
                do = True
            elif h >= minh and xm[i]:
                do = True
            if do:
                exit_px = c[i] * (1 - sign*SLIP)
                r = sign * (exit_px/ep - 1) * lev - 2*FEE
                cash *= (1 + r)
                tr.append((df.index[i], r*100))
                inp = False
                eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i]:
            ei = i
            ep = c[i] * (1 + sign*SLIP)
            inp = True
    s = pd.Series(eq, index=df.index)
    tot = (s.iloc[-1]/INIT - 1) * 100
    dd = ((s.cummax() - s)/s.cummax()).max() * 100
    nt = len(tr)
    wr = (np.array([x[1] for x in tr]) > 0).mean() * 100 if tr else 0
    pos = sum(x[1] for x in tr if x[1] > 0)
    neg = -sum(x[1] for x in tr if x[1] < 0)
    pf = pos/neg if neg > 0 else 9.99
    yr = {}
    for y in range(2020, 2027):
        seg = s[s.index.year == y]
        if len(seg) > 1:
            yr[y] = (seg.iloc[-1]/seg.iloc[0] - 1) * 100
    return tot, dd, nt, wr, pf, yr, s, tr


def yrline(yr):
    return " ".join(f"{y%100}:{v:+.0f}" for y, v in yr.items())


def bt_dual(df, long_em, long_xm, short_em, short_xm, minh, maxh, lev=1.0, stop=None):
    """Long + opportunistic short hedge in bear regimes."""
    c = df['close'].values
    le = np.asarray(long_em); lx = np.asarray(long_xm)
    se = np.asarray(short_em); sx = np.asarray(short_xm)
    n = len(df)
    cash = INIT
    eq = np.full(n, INIT, float)
    pos = 0  # 0=flat, 1=long, -1=short
    ei = 0; ep = 0
    tr = []
    for i in range(n):
        if pos != 0:
            chg = pos * (c[i]/ep - 1)
            eq[i] = cash * (1 + chg * lev)
            h = i - ei
            do = False
            xm = lx[i] if pos == 1 else sx[i]
            if stop is not None and chg * lev <= -stop:
                do = True
            elif h >= maxh:
                do = True
            elif h >= minh and xm:
                do = True
            if do:
                exit_px = c[i] * (1 - pos*SLIP)
                r = pos * (exit_px/ep - 1) * lev - 2*FEE
                cash *= (1 + r)
                tr.append((df.index[i], r*100, pos))
                pos = 0
                eq[i] = cash
        else:
            eq[i] = cash
        if pos == 0:
            if le[i]:
                ei = i; ep = c[i] * (1 + SLIP); pos = 1
            elif se[i]:
                ei = i; ep = c[i] * (1 - SLIP); pos = -1
    s = pd.Series(eq, index=df.index)
    tot = (s.iloc[-1]/INIT - 1) * 100
    dd = ((s.cummax() - s)/s.cummax()).max() * 100
    nt = len(tr)
    nl = sum(1 for t in tr if t[2] == 1)
    ns = sum(1 for t in tr if t[2] == -1)
    wr = (np.array([x[1] for x in tr]) > 0).mean() * 100 if tr else 0
    posp = sum(x[1] for x in tr if x[1] > 0)
    neg = -sum(x[1] for x in tr if x[1] < 0)
    pf = posp/neg if neg > 0 else 9.99
    yr = {}
    for y in range(2020, 2027):
        seg = s[s.index.year == y]
        if len(seg) > 1:
            yr[y] = (seg.iloc[-1]/seg.iloc[0] - 1) * 100
    return tot, dd, nt, nl, ns, wr, pf, yr, s


def main():
    df = load()
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"RTS B&H {bh:+.0f}%")

    print("\n" + "="*100)
    print("REFINED CONFIGS")
    print("="*100)

    cfgs = [
        ("E1 RVI>=40 + ATM_IV>=q80 stop15 1x",
         lambda d: (d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8)),
         lambda d: d['rvi'] < 28, 10, 90, 1.0, 0.15),

        ("E2 RVI>=42 + ATM_IV>=q85 stop15 1x",
         lambda d: (d['rvi'] >= 42) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.85)),
         lambda d: d['rvi'] < 28, 10, 90, 1.0, 0.15),

        ("E3 RVI>=45 + ATM_IV>=q85 stop12 1x — strict",
         lambda d: (d['rvi'] >= 45) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.85)),
         lambda d: d['rvi'] < 28, 10, 90, 1.0, 0.12),

        ("E4 RVI>=40 + ATM_IV q80 stop10 1x tighter",
         lambda d: (d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8)),
         lambda d: d['rvi'] < 30, 10, 60, 1.0, 0.10),

        ("E5 RVI>=40 + ATM_IV q80 stop15 1.3x",
         lambda d: (d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8)),
         lambda d: d['rvi'] < 28, 10, 90, 1.3, 0.15),

        ("F2 VRP>=10 stop10 1x tight (was DD42)",
         lambda d: d['vrp'] >= 10,
         lambda d: d['vrp'] < 0, 10, 60, 1.0, 0.10),

        # COMBO: RVI panic LONG + FIZ extreme LONG (both bullish on RTS)
        ("J1 (RVI>=40 + IVq80) OR (FIZ>=q90 + uptrend), exit RVI<28 or FIZ<q50, stop15",
         lambda d: ((d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8))) |
                   ((d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.9)) & (d['close'] > d['ma200'])),
         lambda d: (d['rvi'] < 28) | (d['fiz_lsr'] <= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.5)),
         10, 90, 1.0, 0.15),

        ("J2 RVI>=40+IVq80 OR FIZ>=q88, exit RVI<28|FIZ<q40, stop12 1x",
         lambda d: ((d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8))) |
                   (d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.88)),
         lambda d: (d['rvi'] < 28) | (d['fiz_lsr'] <= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.4)),
         10, 90, 1.0, 0.12),
    ]

    results = []
    for cfg in cfgs:
        nm, ef, xf, mh, xh, lev, st = cfg
        tot, dd, nt, wr, pf, yr, eq, tr = bt(df, ef(df).fillna(False), xf(df).fillna(False), mh, xh, lev, st, 'long')
        eq_train = eq[eq.index <= TRAIN_END]
        eq_test = eq[eq.index >= TEST_START]
        tot_train = (eq_train.iloc[-1]/INIT - 1) * 100 if len(eq_train) > 0 else 0
        if len(eq_test) > 1:
            base_test = eq_test.iloc[0]
            tot_test = (eq_test.iloc[-1]/base_test - 1) * 100
            dd_test = ((eq_test.cummax() - eq_test)/eq_test.cummax()).max() * 100
        else:
            tot_test, dd_test = 0, 0
        # 2022 specific drawdown
        eq22 = eq[eq.index.year == 2022]
        dd_2022 = ((eq22.cummax() - eq22)/eq22.cummax()).max() * 100 if len(eq22) > 1 else 0
        results.append((nm, tot, dd, nt, wr, pf, tot_train, tot_test, dd_test, dd_2022, yr))
        print(f"\n  {nm}")
        print(f"    ALL  tot={tot:+.0f}% DD={dd:.0f}% N={nt} WR={wr:.0f}% PF={pf:.2f}")
        print(f"    TRAIN(20-22) {tot_train:+.0f}% | TEST(23-26) {tot_test:+.0f}% DD={dd_test:.0f}% | DD_2022={dd_2022:.0f}%")
        print(f"    per-year: {yrline(yr)}")

    # Best honest: tot positive, dd_test<30, every year not catastrophic
    print("\n" + "="*100)
    print("SHORTLIST: tot>=0, dd_test<35")
    for r in results:
        nm, tot, dd, nt, wr, pf, ttr, tts, ddt, dd22, yr = r
        if tot >= 0 and ddt < 35:
            print(f"  {nm}  tot={tot:+.0f}  DD={dd:.0f}  TEST={tts:+.0f}  TEST_DD={ddt:.0f}  2022_DD={dd22:.0f}")
    print("="*100)

    # ----- DUAL test: long bull, short bear -----
    print("\n" + "="*100)
    print("DUAL: long RVI panic + short FIZ euphoria into MA200-break")
    print("="*100)
    dual_cfgs = [
        ("DUAL1 long=RVI>=40+IVq80, short=FIZ>=q90+price<MA50, exit symm, stop12",
         lambda d: (d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8)),
         lambda d: d['rvi'] < 28,
         lambda d: (d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.9)) & (d['close'] < d['close'].rolling(50).mean()),
         lambda d: d['rvi'] >= 35,
         10, 60, 1.0, 0.12),
    ]
    for cfg in dual_cfgs:
        nm, le, lx, se, sx, mh, xh, lev, st = cfg
        tot, dd, nt, nl, ns, wr, pf, yr, eq = bt_dual(df,
            le(df).fillna(False), lx(df).fillna(False),
            se(df).fillna(False), sx(df).fillna(False), mh, xh, lev, st)
        print(f"\n  {nm}")
        print(f"    tot={tot:+.0f}% DD={dd:.0f}% N={nt} (long={nl} short={ns}) WR={wr:.0f}% PF={pf:.2f}")
        print(f"    per-year: {yrline(yr)}")


if __name__ == "__main__":
    main()
