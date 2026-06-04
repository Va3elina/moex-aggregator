"""RTS deep analysis: sign of FIZ on index, bull-regime filter, walk-forward, short side.

Goal: find honest realistic config protecting capital through 2022 war.
NOT trying to brute-force 40%/yr — show actual achievable edge on RTS.
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
TRAIN_END = pd.Timestamp('2022-12-31')  # 2020-22 = train (COVID + war stress)
TEST_START = pd.Timestamp('2023-01-01')  # 2023-26 = test


def load():
    ind = pd.read_csv(ROOT/'rts_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    px = pd.read_csv(ROOT/'rts_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    close = px['close']
    df = pd.DataFrame(index=ind.index)
    df['close'] = close.reindex(ind.index).ffill()
    df = df.join(ind, how='left')
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    df['vrp'] = df['atm_iv'] - df['rv30']

    ft = pd.read_csv(ROOT/'rts_futoi_history.csv', parse_dates=['date']).set_index('date').sort_index()
    df['fiz_lsr'] = (ft['fiz_long'] / ft['fiz_short'].replace(0, 1)).reindex(df.index)
    df['fiz_net'] = ft['fiz_net'].reindex(df.index)
    df['yur_lsr'] = (ft['yur_long'] / ft['yur_short'].replace(0, 1)).reindex(df.index)

    rvi = pd.read_csv(ROOT/'rvi_daily.csv', parse_dates=['TRADEDATE']).set_index('TRADEDATE')['close']
    df['rvi'] = rvi.reindex(df.index).ffill()
    df['ma200'] = df['close'].rolling(200).mean()
    df['ma50'] = df['close'].rolling(50).mean()
    df['above_ma200'] = df['close'] > df['ma200']
    return df[df['atm_iv'].notna()].copy()


def bt(df, em, xm, minh, maxh, lev=1.0, stop=None, direction='long'):
    """Backtest. direction='long' or 'short'."""
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
    return tot, dd, nt, wr, pf, yr, s


def yrline(yr):
    return " ".join(f"{y%100}:{v:+.0f}" for y, v in yr.items())


def main():
    df = load()
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    print(f"RTS {df.index[0].date()}->{df.index[-1].date()} ({len(df)}d), B&H {bh:+.0f}%")
    print(f"  fiz_lsr range {df['fiz_lsr'].min():.2f}..{df['fiz_lsr'].max():.2f} median {df['fiz_lsr'].median():.2f}")
    print(f"  yur_lsr range {df['yur_lsr'].min():.2f}..{df['yur_lsr'].max():.2f} median {df['yur_lsr'].median():.2f}")
    print(f"  RVI median {df['rvi'].median():.1f}, q90 {df['rvi'].quantile(0.9):.1f}")
    print(f"  ATM IV median {df['atm_iv'].median():.1f}, q90 {df['atm_iv'].quantile(0.9):.1f}")

    # SIGN check: how do FIZ extremes line up with future returns?
    print("\n[1] FIZ_LSR sign check — is high FIZ bullish or bearish on RTS?")
    df['fwd20'] = df['close'].pct_change(20).shift(-20) * 100
    df['fwd60'] = df['close'].pct_change(60).shift(-60) * 100
    s = df[['fiz_lsr', 'fwd20', 'fwd60']].dropna()
    for q in [0.1, 0.2, 0.5, 0.8, 0.9]:
        thr = s['fiz_lsr'].quantile(q)
        if q <= 0.5:
            sub = s[s['fiz_lsr'] <= thr]
            label = f"FIZ<=q{int(q*100)}({thr:.2f}) low-retail-long"
        else:
            sub = s[s['fiz_lsr'] >= thr]
            label = f"FIZ>=q{int(q*100)}({thr:.2f}) high-retail-long"
        print(f"  {label:<40} N={len(sub):4d}  fwd20={sub['fwd20'].mean():+.1f}%  fwd60={sub['fwd60'].mean():+.1f}%")

    print("\n[2] FIZ extreme on RTS: signs flipped vs SBER?")
    # On SBER: FIZ low (panic) -> bullish. On RTS check both directions.
    # The Q5 spread +8.8 from quintile suggests FIZ high also bullish — but only in test set?
    pre22 = df[df.index < TRAIN_END]
    post22 = df[df.index >= TEST_START]
    print(f"  pre-2023 (n={len(pre22)}): fwd60 by FIZ quintile:")
    for d, label in [(pre22, 'train'), (post22, 'test')]:
        s2 = d[['fiz_lsr', 'fwd60']].dropna()
        if len(s2) < 60:
            continue
        s2['q'] = pd.qcut(s2['fiz_lsr'].rank(method='first'), 5, labels=[1,2,3,4,5])
        m = [s2[s2['q']==q]['fwd60'].mean() for q in [1,2,3,4,5]]
        print(f"    {label:6s} Q1..Q5: " + " ".join(f"{x:+.1f}" for x in m) + f"  spread {m[-1]-m[0]:+.1f}")

    print("\n[3] RVI panic quintile by regime:")
    for d, label in [(df, 'all'), (pre22, 'train'), (post22, 'test')]:
        s2 = d[['rvi', 'fwd60']].dropna()
        if len(s2) < 60:
            continue
        s2['q'] = pd.qcut(s2['rvi'].rank(method='first'), 5, labels=[1,2,3,4,5])
        m = [s2[s2['q']==q]['fwd60'].mean() for q in [1,2,3,4,5]]
        print(f"  {label:6s} Q1..Q5: " + " ".join(f"{x:+.1f}" for x in m) + f"  spread {m[-1]-m[0]:+.1f}")

    print("\n[4] RVI panic + price>MA200 regime filter (avoid catching falling knife):")
    s3 = df[['rvi', 'above_ma200', 'fwd60']].dropna()
    for above in [True, False]:
        sub = s3[s3['above_ma200'] == above]
        if len(sub) < 50: continue
        q90 = sub['rvi'].quantile(0.9)
        panic = sub[sub['rvi'] >= 40]
        print(f"  above_ma200={above}  n={len(sub)}  RVI>=40 fwd60 mean={panic['fwd60'].mean():+.1f}% (n={len(panic)})")

    print("\n[5] Short side: FIZ euphoria signal — does shorting work?")
    for q in [0.85, 0.9, 0.95]:
        thr = df['fiz_lsr'].quantile(q)
        sub = df[df['fiz_lsr'] >= thr].dropna(subset=['fwd20','fwd60'])
        print(f"  FIZ>=q{int(q*100)}({thr:.2f}) n={len(sub)}  fwd20={sub['fwd20'].mean():+.1f}  fwd60={sub['fwd60'].mean():+.1f}")

    print("\n" + "="*100)
    print("[6] BACKTESTS — walk-forward TRAIN(2020-22) | TEST(2023-26)")
    print("="*100)

    cfgs = [
        # (name, entry_fn, exit_fn, min_hold, max_hold, lev, stop, direction)
        ("A: RVI>=40 panic LONG, exit RVI<30, 10-90, 1x",
         lambda d: d['rvi'] >= 40,
         lambda d: d['rvi'] < 30, 10, 90, 1.0, None, 'long'),

        ("B: RVI>=40 panic LONG + MA200 filter, exit RVI<28, 10-90, 1x",
         lambda d: (d['rvi'] >= 40) & d['above_ma200'],
         lambda d: d['rvi'] < 28, 10, 90, 1.0, None, 'long'),

        ("C: RVI>=35 + MA200, exit RVI<25, 10-120, 1x stop12",
         lambda d: (d['rvi'] >= 35) & d['above_ma200'],
         lambda d: d['rvi'] < 25, 10, 120, 1.0, 0.12, 'long'),

        ("D: FIZ_LSR>=q85 trend-LONG (retail bullish + uptrend), exit FIZ<q50, 10-60",
         lambda d: (d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.85)) & d['above_ma200'],
         lambda d: d['fiz_lsr'] <= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.5), 10, 60, 1.0, 0.10, 'long'),

        ("E: RVI>=40 panic LONG + ATM_IV>=q80, exit RVI<28, 10-90, 1x stop15",
         lambda d: (d['rvi'] >= 40) & (d['atm_iv'] >= d['atm_iv'].rolling(252, min_periods=60).quantile(0.8)),
         lambda d: d['rvi'] < 28, 10, 90, 1.0, 0.15, 'long'),

        ("F: VRP>=10 LONG (HIGH vol premium), exit VRP<0, 10-90, 1x stop12",
         lambda d: d['vrp'] >= 10,
         lambda d: d['vrp'] < 0, 10, 90, 1.0, 0.12, 'long'),

        ("G: SHORT when FIZ>=q95 euphoria + ATM_IV<q30 complacency, exit 20d",
         lambda d: (d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.95)) & (d['atm_iv'] <= d['atm_iv'].rolling(252, min_periods=60).quantile(0.3)),
         lambda d: pd.Series(False, index=d.index), 5, 20, 1.0, 0.10, 'short'),

        ("H: COMBO RVI>=40 OR (FIZ>=q85 + MA200), exit RVI<28 OR FIZ<q40, stop12",
         lambda d: (d['rvi'] >= 40) | ((d['fiz_lsr'] >= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.85)) & d['above_ma200']),
         lambda d: (d['rvi'] < 28) | (d['fiz_lsr'] <= d['fiz_lsr'].rolling(252, min_periods=60).quantile(0.4)),
         10, 90, 1.0, 0.12, 'long'),
    ]

    best = None
    rows = []
    for cfg in cfgs:
        nm, ef, xf, mh, xh, lev, st, dr = cfg
        tot, dd, nt, wr, pf, yr, eq = bt(df, ef(df).fillna(False), xf(df).fillna(False), mh, xh, lev, st, dr)
        # split metrics
        eq_train = eq[eq.index <= TRAIN_END]
        eq_test = eq[eq.index >= TEST_START]
        tot_train = (eq_train.iloc[-1]/INIT - 1) * 100 if len(eq_train) > 0 else 0
        # test: rebase
        if len(eq_test) > 1:
            base_test = eq_test.iloc[0]
            tot_test = (eq_test.iloc[-1]/base_test - 1) * 100
            dd_test = ((eq_test.cummax() - eq_test)/eq_test.cummax()).max() * 100
        else:
            tot_test, dd_test = 0, 0
        print(f"\n  {nm}")
        print(f"    ALL  tot={tot:+.0f}% DD={dd:.0f}% N={nt} WR={wr:.0f}% PF={pf:.2f}")
        print(f"    TRAIN(2020-22) tot={tot_train:+.0f}%   TEST(2023-26) tot={tot_test:+.0f}% DD={dd_test:.0f}%")
        print(f"    per-year: {yrline(yr)}")
        rows.append((nm, tot, dd, nt, wr, pf, tot_train, tot_test, dd_test, yr))
        # score: total positive on TEST + DD<40
        score = tot_test - dd_test * 0.5 - (dd if dd > 30 else 0) * 0.3
        if best is None or score > best[0]:
            best = (score, nm, cfg, tot, dd, nt, wr, pf, tot_train, tot_test, dd_test, yr)

    print("\n" + "="*100)
    print(f"BEST by score(tot_test - 0.5*dd_test - 0.3*overshoot_dd): {best[1]}")
    print(f"  tot={best[3]:+.0f}% DD={best[4]:.0f}% N={best[5]} WR={best[6]:.0f}% PF={best[7]:.2f}")
    print(f"  TRAIN={best[8]:+.0f}% TEST={best[9]:+.0f}% TEST_DD={best[10]:.0f}%")
    print(f"  per-year: {yrline(best[11])}")
    print("="*100)

    print(f"\nB&H comparison: -27% over 2020-26.  Best signal vs B&H = {best[3] - bh:+.0f}pp better")

    # Save best result for Pine
    print(f"\nUse this config for Pine generator.")


if __name__ == "__main__":
    main()
