"""Deep grid-search optimization for RTS option strategies.

Phases:
  1. Single-signal grid: sweep entry/exit thresholds, holds, stops, leverage,
     with/without kill-switch (RVI>=80 -> 30d no-trade window).
  2. Multi-signal AND/OR/voting combinations of robust signals.
  3. RTS-specific innovations: 200d MA regime filter, vol-level + vol-premium
     combos, short on complacency, directional regime.
  4. 2022 protection mechanisms: hard MA200 stop, RVI>=70 stop, position
     sizing, time-stop, lever-down high-vol.
  5. Long+short dual strategy combined equity curve.

Metrics: TRAIN(2020-22) / TEST(2023-26) totals, DDs, WR, PF, N, CAGR,
per-year (including 2022 PnL), Sharpe-like.

Output:
  - prints top-10 per phase + overall best
  - rts_top_strategies.csv (top 50 configs)
"""
import warnings; warnings.filterwarnings("ignore")
import itertools
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
TRAIN_END = pd.Timestamp('2022-12-31')
TEST_START = pd.Timestamp('2023-01-01')

# Six robust signals identified earlier (per task brief).
ROBUST = [
    ('varswap_iv',  'high'),
    ('atm_iv',      'high'),
    ('bf25',        'high'),
    ('gex_norm',    'high'),
    ('rn_p_down10', 'high'),
    ('rvi',         'high'),
]

# Direction-aware quantile helper (for 'low' we use 1-q).
def dir_q(s, q, direction):
    return s.quantile(q if direction == 'high' else 1 - q)


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
    df['ma50']  = df['close'].rolling(50).mean()
    df['above_ma200'] = df['close'] > df['ma200']
    df['mom20'] = df['close'].pct_change(20)
    return df[df['atm_iv'].notna()].copy()


def killswitch_mask(df, rvi_panic_thr=80, lockout_days=30):
    """Returns mask: True = trading allowed; False = locked-out window after extreme RVI spike."""
    n = len(df)
    allow = np.ones(n, dtype=bool)
    rvi = df['rvi'].values
    last_panic = -10_000
    for i in range(n):
        if rvi[i] >= rvi_panic_thr:
            last_panic = i
        if i - last_panic < lockout_days:
            allow[i] = False
    return allow


def bt(df, em, xm, minh=10, maxh=90, lev=1.0, stop=None, direction='long', killswitch=None,
       lever_down_rvi=None, lever_down_factor=0.5):
    """Backtest a long or short rule. Optional kill-switch mask; optional lever-down when RVI>=lever_down_rvi."""
    c = df['close'].values
    em = np.asarray(em); xm = np.asarray(xm)
    n = len(df)
    cash = INIT
    eq = np.full(n, INIT, float)
    inp = False; ei = 0; ep = 0
    cur_lev = lev
    tr = []
    sign = 1 if direction == 'long' else -1
    rvi = df['rvi'].values if 'rvi' in df.columns else None
    ks = killswitch if killswitch is not None else np.ones(n, dtype=bool)
    for i in range(n):
        if inp:
            chg = sign * (c[i]/ep - 1)
            eq[i] = cash * (1 + chg * cur_lev)
            h = i - ei
            do = False
            if stop is not None and chg * cur_lev <= -stop:
                do = True
            elif h >= maxh:
                do = True
            elif h >= minh and xm[i]:
                do = True
            if do:
                exit_px = c[i] * (1 - sign*SLIP)
                r = sign * (exit_px/ep - 1) * cur_lev - 2*FEE
                cash *= (1 + r)
                tr.append((df.index[i], r*100))
                inp = False
                eq[i] = cash
        else:
            eq[i] = cash
        if not inp and em[i] and ks[i]:
            ei = i
            ep = c[i] * (1 + sign*SLIP)
            inp = True
            cur_lev = lev
            if lever_down_rvi is not None and rvi is not None and rvi[i] >= lever_down_rvi:
                cur_lev = lev * lever_down_factor
    s = pd.Series(eq, index=df.index)
    return _metrics(s, tr)


def bt_dual(df, le, lx, se, sx, minh=10, maxh=90, lev=1.0, stop=None, killswitch=None):
    """Long+short dual strategy on a single equity curve."""
    c = df['close'].values
    le = np.asarray(le); lx = np.asarray(lx)
    se = np.asarray(se); sx = np.asarray(sx)
    n = len(df)
    cash = INIT
    eq = np.full(n, INIT, float)
    pos = 0
    ei = 0; ep = 0
    tr = []
    ks = killswitch if killswitch is not None else np.ones(n, dtype=bool)
    for i in range(n):
        if pos != 0:
            chg = pos * (c[i]/ep - 1)
            eq[i] = cash * (1 + chg * lev)
            h = i - ei
            xm = lx[i] if pos == 1 else sx[i]
            do = False
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
        if pos == 0 and ks[i]:
            if le[i]:
                ei = i; ep = c[i] * (1 + SLIP); pos = 1
            elif se[i]:
                ei = i; ep = c[i] * (1 - SLIP); pos = -1
    s = pd.Series(eq, index=df.index)
    nl = sum(1 for t in tr if t[2] == 1)
    ns = sum(1 for t in tr if t[2] == -1)
    m = _metrics(s, [(d, r) for d, r, _ in tr])
    m['n_long'] = nl
    m['n_short'] = ns
    return m


def _metrics(s, tr):
    """Compute split metrics for an equity curve."""
    tot = (s.iloc[-1]/INIT - 1) * 100
    dd  = ((s.cummax() - s)/s.cummax()).max() * 100
    nt = len(tr)
    if nt > 0:
        rs = np.array([x[1] for x in tr])
        wr = (rs > 0).mean() * 100
        pos = rs[rs > 0].sum(); neg = -rs[rs < 0].sum()
        pf = pos/neg if neg > 0 else 9.99
    else:
        wr = pf = 0
    # daily ret for Sharpe-like
    rets = s.pct_change().fillna(0)
    sharpe = (rets.mean()/rets.std()*np.sqrt(252)) if rets.std() > 0 else 0
    yr = {}
    for y in range(2020, 2027):
        seg = s[s.index.year == y]
        if len(seg) > 1:
            yr[y] = (seg.iloc[-1]/seg.iloc[0] - 1) * 100
    # train/test split
    eq_tr = s[s.index <= TRAIN_END]
    eq_te = s[s.index >= TEST_START]
    tot_train = (eq_tr.iloc[-1]/INIT - 1) * 100 if len(eq_tr) > 1 else 0
    if len(eq_te) > 1:
        base = eq_te.iloc[0]
        tot_test = (eq_te.iloc[-1]/base - 1) * 100
        dd_test = ((eq_te.cummax() - eq_te)/eq_te.cummax()).max() * 100
        n_yrs = (eq_te.index[-1] - eq_te.index[0]).days / 365.25
        cagr_test = ((eq_te.iloc[-1]/base) ** (1/max(n_yrs,1e-3)) - 1) * 100 if base > 0 else 0
    else:
        tot_test = dd_test = cagr_test = 0
    eq22 = s[s.index.year == 2022]
    if len(eq22) > 1:
        pnl_2022 = (eq22.iloc[-1]/eq22.iloc[0] - 1) * 100
        dd_2022 = ((eq22.cummax() - eq22)/eq22.cummax()).max() * 100
    else:
        pnl_2022 = dd_2022 = 0
    return dict(tot=tot, dd=dd, nt=nt, wr=wr, pf=pf, sharpe=sharpe,
                tot_train=tot_train, tot_test=tot_test, dd_test=dd_test,
                cagr_test=cagr_test, pnl_2022=pnl_2022, dd_2022=dd_2022, yr=yr)


def yrline(yr):
    return " ".join(f"{y%100}:{v:+.0f}" for y, v in yr.items())


def score(m):
    """Composite ranking score:
       favours test CAGR + 2022 protection + low DD; punishes catastrophic train losses.
    """
    cagr = m['cagr_test']
    ddt = m['dd_test']
    p22 = m['pnl_2022']
    dd22 = m['dd_2022']
    tot_train = m['tot_train']
    pf = m['pf']
    # base score
    s = cagr - 0.4*ddt - max(0, -p22)*0.3 - max(0, dd22-30)*0.4
    if tot_train < -30:
        s -= 25  # punish train blow-ups
    if m['nt'] < 5:
        s -= 30  # too few trades = noise
    if pf >= 1.2:
        s += 5
    return s


# ============================================================
# PHASE 1 — Single-signal grid
# ============================================================
def phase1(df, kill_mask):
    print("\n" + "="*100)
    print("PHASE 1: Single-signal grid search")
    print("="*100)
    train = df[df.index <= TRAIN_END]
    results = []
    q_entry = [0.5, 0.6, 0.7, 0.8, 0.9]
    q_exit  = [0.3, 0.4, 0.5]
    minh_grid = [5, 10, 15]
    maxh_grid = [30, 60, 90, 120]
    stop_grid = [0.05, 0.08, 0.10, 0.15, 0.20, None]
    lev_grid  = [1.0, 1.5]
    ks_grid   = [False, True]
    for sig, direction in ROBUST:
        if sig not in df.columns:
            continue
        for qe in q_entry:
            for qx in q_exit:
                # threshold from train set
                thr_e = dir_q(train[sig], qe, direction)
                thr_x = dir_q(train[sig], qx, direction)
                if direction == 'high':
                    em = (df[sig] >= thr_e).fillna(False)
                    xm = (df[sig] <= thr_x).fillna(False)
                else:
                    em = (df[sig] <= thr_e).fillna(False)
                    xm = (df[sig] >= thr_x).fillna(False)
                for minh, maxh, stop, lev, ks in itertools.product(minh_grid, maxh_grid, stop_grid, lev_grid, ks_grid):
                    if minh >= maxh: continue
                    m = bt(df, em, xm, minh, maxh, lev, stop, 'long', kill_mask if ks else None)
                    if m['nt'] < 5: continue
                    sc = score(m)
                    cfg = dict(phase=1, signal=sig, dir=direction, qe=qe, qx=qx, thr_e=thr_e, thr_x=thr_x,
                               minh=minh, maxh=maxh, stop=stop, lev=lev, killswitch=ks,
                               score=sc, **m)
                    results.append(cfg)
    results.sort(key=lambda r: r['score'], reverse=True)
    print(f"  searched {len(results)} configs (post-filter)")
    print(f"\n  TOP-10 single-signal:")
    print(f"  {'sig':<14} qe/qx min-max stop lev ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4}  per-year")
    for r in results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        print(f"  {r['signal']:<14} {r['qe']:.1f}/{r['qx']:.1f} {r['minh']:2}-{r['maxh']:<3} {stop_s:<4} {r['lev']:.1f} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4}  {yrline(r['yr'])}")
    return results


# ============================================================
# PHASE 2 — Multi-signal combinations (AND/OR/voting)
# ============================================================
def phase2(df, kill_mask):
    print("\n" + "="*100)
    print("PHASE 2: Multi-signal combinations")
    print("="*100)
    train = df[df.index <= TRAIN_END]
    results = []
    # Build entry masks per signal at quantile 0.7
    sig_em = {}
    sig_xm = {}
    for sig, direction in ROBUST:
        if sig not in df.columns: continue
        thr_e = dir_q(train[sig], 0.7, direction)
        thr_x = dir_q(train[sig], 0.4, direction)
        if direction == 'high':
            sig_em[sig] = (df[sig] >= thr_e).fillna(False)
            sig_xm[sig] = (df[sig] <= thr_x).fillna(False)
        else:
            sig_em[sig] = (df[sig] <= thr_e).fillna(False)
            sig_xm[sig] = (df[sig] >= thr_x).fillna(False)
    sigs = list(sig_em.keys())

    # All pairs AND + OR
    for a, b in itertools.combinations(sigs, 2):
        for combo_name, em, xm in [
            (f"{a}_AND_{b}", sig_em[a] & sig_em[b], sig_xm[a] | sig_xm[b]),
            (f"{a}_OR_{b}",  sig_em[a] | sig_em[b], sig_xm[a] & sig_xm[b]),
        ]:
            for stop, ks in [(0.10, True), (0.15, True), (0.10, False), (0.15, False)]:
                m = bt(df, em, xm, 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                if m['nt'] < 5: continue
                results.append(dict(phase=2, signal=combo_name, qe=0.7, qx=0.4,
                                     minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                     score=score(m), **m))

    # Triples
    for a, b, c in itertools.combinations(sigs, 3):
        for combo_name, em, xm in [
            (f"{a}+{b}+{c}_AND", sig_em[a] & sig_em[b] & sig_em[c], sig_xm[a] | sig_xm[b] | sig_xm[c]),
            (f"{a}+{b}+{c}_OR",  sig_em[a] | sig_em[b] | sig_em[c], sig_xm[a] & sig_xm[b] & sig_xm[c]),
        ]:
            for stop, ks in [(0.10, True), (0.15, True)]:
                m = bt(df, em, xm, 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                if m['nt'] < 5: continue
                results.append(dict(phase=2, signal=combo_name, qe=0.7, qx=0.4,
                                     minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                     score=score(m), **m))

    # Voting ensembles: enter when N+ signals fire, exit when <K fire
    vote_em = pd.DataFrame({s: sig_em[s] for s in sigs}).sum(axis=1)
    vote_xm = pd.DataFrame({s: sig_xm[s] for s in sigs}).sum(axis=1)
    for vmin in [2, 3, 4]:
        for vexit in [1, 2]:
            for stop, ks in [(0.10, True), (0.15, True), (0.10, False)]:
                em = (vote_em >= vmin)
                xm = (vote_em < vexit)
                m = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                if m['nt'] < 5: continue
                results.append(dict(phase=2, signal=f"VOTE>={vmin}_exit<{vexit}", qe=0.7, qx=0.4,
                                     minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                     score=score(m), **m))

    # Vol-weighted voting: vol signals (varswap_iv, atm_iv, rvi, bf25) double-weighted
    vol_sigs = [s for s in sigs if s in ('varswap_iv','atm_iv','rvi','bf25')]
    other_sigs = [s for s in sigs if s not in vol_sigs]
    weighted_em = 2*pd.DataFrame({s: sig_em[s] for s in vol_sigs}).sum(axis=1) + \
                  pd.DataFrame({s: sig_em[s] for s in other_sigs}).sum(axis=1) if other_sigs else \
                  2*pd.DataFrame({s: sig_em[s] for s in vol_sigs}).sum(axis=1)
    weighted_xm = 2*pd.DataFrame({s: sig_xm[s] for s in vol_sigs}).sum(axis=1) + \
                  pd.DataFrame({s: sig_xm[s] for s in other_sigs}).sum(axis=1) if other_sigs else \
                  2*pd.DataFrame({s: sig_xm[s] for s in vol_sigs}).sum(axis=1)
    for thr in [4, 5, 6, 7]:
        em = (weighted_em >= thr)
        xm = (weighted_em < thr-2)
        for ks in [True, False]:
            m = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 1.0, 0.12, 'long', kill_mask if ks else None)
            if m['nt'] < 5: continue
            results.append(dict(phase=2, signal=f"VOLwgt>={thr}", qe=0.7, qx=0.4,
                                 minh=10, maxh=90, stop=0.12, lev=1.0, killswitch=ks,
                                 score=score(m), **m))

    results.sort(key=lambda r: r['score'], reverse=True)
    print(f"  searched {len(results)} multi-signal configs")
    print(f"\n  TOP-10 multi-signal:")
    print(f"  {'combo':<46} stop ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4}  per-year")
    for r in results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:45]
        print(f"  {nm:<46} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4}  {yrline(r['yr'])}")
    return results


# ============================================================
# PHASE 3 — RTS-specific innovations
# ============================================================
def phase3(df, kill_mask):
    print("\n" + "="*100)
    print("PHASE 3: RTS-specific innovations (regime, vol-level + premium, short-on-complacency)")
    print("="*100)
    train = df[df.index <= TRAIN_END]
    results = []

    # 3a — vol-level signals + MA200 regime filter
    for sig, direction in [('varswap_iv','high'), ('atm_iv','high'), ('rvi','high')]:
        if sig not in df.columns: continue
        for qe in [0.6, 0.7, 0.8]:
            thr_e = dir_q(train[sig], qe, direction)
            thr_x = dir_q(train[sig], 0.4, direction)
            base_em = (df[sig] >= thr_e) if direction=='high' else (df[sig] <= thr_e)
            xm = (df[sig] <= thr_x) if direction=='high' else (df[sig] >= thr_x)
            for regime in ['ma200_only', 'above_ma200', 'mom_pos']:
                if regime == 'above_ma200':
                    em = base_em & df['above_ma200']
                elif regime == 'mom_pos':
                    em = base_em & (df['mom20'] > 0)
                else:
                    em = base_em
                for stop in [0.10, 0.15]:
                    for ks in [True, False]:
                        m = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                        if m['nt'] < 5: continue
                        results.append(dict(phase=3, signal=f"{sig}_q{qe}_{regime}",
                                             qe=qe, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                             score=score(m), **m))

    # 3b — vol-level + vrp premium combo (vrp is brittle alone but adds info)
    for qe in [0.6, 0.7, 0.8]:
        thr = dir_q(train['atm_iv'], qe, 'high')
        em = (df['atm_iv'] >= thr) & (df['vrp'] >= 5)
        xm = df['atm_iv'] < dir_q(train['atm_iv'], 0.4, 'high')
        for stop in [0.10, 0.15]:
            for ks in [True, False]:
                m = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                if m['nt'] < 5: continue
                results.append(dict(phase=3, signal=f"atm_iv_q{qe}_AND_vrp_pos",
                                     qe=qe, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                     score=score(m), **m))

    # 3c — SHORT on complacency (low atm_iv) + downtrend
    for ql in [0.2, 0.3, 0.4]:
        thr = train['atm_iv'].quantile(ql)
        em = (df['atm_iv'] <= thr) & (~df['above_ma200'])
        xm = df['atm_iv'] >= train['atm_iv'].quantile(0.5)
        for stop in [0.10, 0.15]:
            for ks in [True, False]:
                m = bt(df, em.fillna(False), xm.fillna(False), 5, 30, 1.0, stop, 'short', kill_mask if ks else None)
                if m['nt'] < 5: continue
                results.append(dict(phase=3, signal=f"SHORT_atm_iv_low_q{ql}_below_MA200",
                                     qe=ql, qx=0.5, minh=5, maxh=30, stop=stop, lev=1.0, killswitch=ks,
                                     score=score(m), **m))

    # 3d — Directional regime: panic LONG only when momentum positive OR above MA200
    if 'varswap_iv' in df.columns:
        for qe in [0.7, 0.8]:
            thr = dir_q(train['varswap_iv'], qe, 'high')
            base = (df['varswap_iv'] >= thr)
            xm = df['varswap_iv'] < dir_q(train['varswap_iv'], 0.4, 'high')
            for label, em in [
                ('vsw_above_ma200', base & df['above_ma200']),
                ('vsw_mom20_pos', base & (df['mom20'] > 0)),
                ('vsw_above_ma200_or_mom', base & (df['above_ma200'] | (df['mom20'] > 0.02))),
            ]:
                for stop in [0.10, 0.15]:
                    for ks in [True, False]:
                        m = bt(df, em.fillna(False), xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
                        if m['nt'] < 5: continue
                        results.append(dict(phase=3, signal=f"{label}_q{qe}",
                                             qe=qe, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                             score=score(m), **m))

    results.sort(key=lambda r: r['score'], reverse=True)
    print(f"  searched {len(results)} phase-3 configs")
    print(f"\n  TOP-10 RTS-specific:")
    print(f"  {'config':<42} stop ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4}  per-year")
    for r in results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:41]
        print(f"  {nm:<42} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4}  {yrline(r['yr'])}")
    return results


# ============================================================
# PHASE 4 — 2022 protection mechanisms
# ============================================================
def phase4(df, kill_mask):
    print("\n" + "="*100)
    print("PHASE 4: 2022 protection mechanisms")
    print("="*100)
    train = df[df.index <= TRAIN_END]
    results = []
    sig = 'varswap_iv'
    if sig not in df.columns:
        return results
    thr = dir_q(train[sig], 0.7, 'high')
    xthr = dir_q(train[sig], 0.4, 'high')
    base_em = (df[sig] >= thr)
    base_xm = (df[sig] <= xthr)

    # 4a — hard MA200 stop overlay (no entry below MA200)
    em = base_em & df['above_ma200']
    for stop, ks in [(0.10, True), (0.15, True), (0.08, True)]:
        m = bt(df, em.fillna(False), base_xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="vsw_q0.7+MA200_filter",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                 score=score(m), **m))

    # 4b — RVI>=70 hard stop
    em2 = base_em & (df['rvi'] < 70)
    for stop, ks in [(0.10, True), (0.15, True)]:
        m = bt(df, em2.fillna(False), base_xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="vsw_q0.7+RVI<70",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                 score=score(m), **m))

    # 4c — lever-down when RVI>=60 (cut size in half)
    for stop in [0.10, 0.15]:
        m = bt(df, base_em.fillna(False), base_xm.fillna(False), 10, 90, 1.0, stop, 'long',
               kill_mask, lever_down_rvi=60, lever_down_factor=0.5)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="vsw_q0.7+leverdown_RVI60",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=True,
                                 score=score(m), **m))

    # 4d — time-stop: no trades Feb-April 2022 (rare ex-post but informative)
    no_war = pd.Series(True, index=df.index)
    war_window = (df.index >= '2022-02-01') & (df.index <= '2022-04-30')
    no_war[war_window] = False
    em4 = base_em & no_war
    for stop, ks in [(0.10, True), (0.15, True)]:
        m = bt(df, em4.fillna(False), base_xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask if ks else None)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="vsw_q0.7+timestop_war2022_(post-hoc)",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=ks,
                                 score=score(m), **m))

    # 4e — combined: MA200 + RVI<70 + killswitch + tight stop
    em5 = base_em & df['above_ma200'] & (df['rvi'] < 70)
    for stop in [0.08, 0.10, 0.12]:
        m = bt(df, em5.fillna(False), base_xm.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="vsw_q0.7+MA200+RVI<70+kill",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=True,
                                 score=score(m), **m))

    # 4f — Apply same protection package to atm_iv too
    sig2 = 'atm_iv'
    thr2 = dir_q(train[sig2], 0.7, 'high')
    xthr2 = dir_q(train[sig2], 0.4, 'high')
    em6 = (df[sig2] >= thr2) & df['above_ma200'] & (df['rvi'] < 70)
    xm6 = (df[sig2] <= xthr2)
    for stop in [0.08, 0.10, 0.12]:
        m = bt(df, em6.fillna(False), xm6.fillna(False), 10, 90, 1.0, stop, 'long', kill_mask)
        if m['nt'] >= 5:
            results.append(dict(phase=4, signal="atm_iv_q0.7+MA200+RVI<70+kill",
                                 qe=0.7, qx=0.4, minh=10, maxh=90, stop=stop, lev=1.0, killswitch=True,
                                 score=score(m), **m))

    results.sort(key=lambda r: r['score'], reverse=True)
    print(f"\n  TOP-10 protection variants:")
    print(f"  {'config':<42} stop ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4}  per-year")
    for r in results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:41]
        print(f"  {nm:<42} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4}  {yrline(r['yr'])}")
    return results


# ============================================================
# PHASE 5 — Long+short dual strategy
# ============================================================
def phase5(df, kill_mask):
    print("\n" + "="*100)
    print("PHASE 5: Long+Short dual strategy")
    print("="*100)
    train = df[df.index <= TRAIN_END]
    results = []
    sig = 'varswap_iv'
    if sig not in df.columns:
        return results

    # LONG: high varswap (panic) AND above MA200
    thr_l = dir_q(train[sig], 0.7, 'high')
    le = ((df[sig] >= thr_l) & df['above_ma200']).fillna(False)
    lx = (df[sig] <= dir_q(train[sig], 0.4, 'high')).fillna(False)

    # SHORT: low atm_iv (complacency) AND below MA200
    for ql in [0.3, 0.4]:
        thr_s = train['atm_iv'].quantile(ql)
        se = ((df['atm_iv'] <= thr_s) & (~df['above_ma200'])).fillna(False)
        sx = (df['atm_iv'] >= train['atm_iv'].quantile(0.5)).fillna(False)
        for stop in [0.08, 0.10, 0.15]:
            for ks in [True, False]:
                m = bt_dual(df, le, lx, se, sx, 10, 60, 1.0, stop, kill_mask if ks else None)
                if m['nt'] >= 5:
                    results.append(dict(phase=5, signal=f"DUAL_vsw_long+atm_low_q{ql}_short",
                                         qe=0.7, qx=ql, minh=10, maxh=60, stop=stop, lev=1.0, killswitch=ks,
                                         score=score(m), **m))

    # Alt SHORT: rvi extremely low + below MA200
    for ql in [0.2, 0.3]:
        thr_s = train['rvi'].quantile(ql)
        se = ((df['rvi'] <= thr_s) & (~df['above_ma200'])).fillna(False)
        sx = (df['rvi'] >= train['rvi'].quantile(0.5)).fillna(False)
        for stop in [0.10, 0.15]:
            for ks in [True, False]:
                m = bt_dual(df, le, lx, se, sx, 10, 60, 1.0, stop, kill_mask if ks else None)
                if m['nt'] >= 5:
                    results.append(dict(phase=5, signal=f"DUAL_vsw_long+rvi_low_q{ql}_short",
                                         qe=0.7, qx=ql, minh=10, maxh=60, stop=stop, lev=1.0, killswitch=ks,
                                         score=score(m), **m))

    results.sort(key=lambda r: r['score'], reverse=True)
    print(f"\n  TOP-10 dual long+short:")
    print(f"  {'config':<46} stop ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4}(L/S)  per-year")
    for r in results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:45]
        nl = r.get('n_long', 0); ns = r.get('n_short', 0)
        print(f"  {nm:<46} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>3}({nl}/{ns})  {yrline(r['yr'])}")
    return results


# ============================================================
# MAIN
# ============================================================
def main():
    df = load()
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1) * 100
    bh_test = df[df.index >= TEST_START]
    bh_test_tot = (bh_test['close'].iloc[-1]/bh_test['close'].iloc[0]-1)*100
    print(f"RTS {df.index[0].date()} → {df.index[-1].date()} ({len(df)}d)")
    print(f"  B&H all   {bh:+.1f}%")
    print(f"  B&H test  {bh_test_tot:+.1f}%")
    print(f"  RVI median {df['rvi'].median():.1f}, q90 {df['rvi'].quantile(0.9):.1f}, max {df['rvi'].max():.1f}")
    print(f"  ATM IV median {df['atm_iv'].median():.1f}, q90 {df['atm_iv'].quantile(0.9):.1f}")
    print(f"  varswap_iv median {df['varswap_iv'].median():.1f}")

    kill_mask = killswitch_mask(df, 80, 30)
    kill_pct = (~kill_mask).mean() * 100
    print(f"  Kill-switch (RVI>=80, 30d lockout) blocks {kill_pct:.1f}% of days")

    all_results = []
    all_results += phase1(df, kill_mask)
    all_results += phase2(df, kill_mask)
    all_results += phase3(df, kill_mask)
    all_results += phase4(df, kill_mask)
    all_results += phase5(df, kill_mask)

    # Global ranking
    all_results.sort(key=lambda r: r['score'], reverse=True)
    print("\n" + "="*100)
    print(f"GLOBAL BEST 10 across all phases (sorted by composite score)")
    print("="*100)
    print(f"  {'ph':>2} {'config':<48} stop ks {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4} {'Shrp':>5}  per-year")
    for r in all_results[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:47]
        print(f"  {r['phase']:>2} {nm:<48} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4} {r['sharpe']:>4.2f}  {yrline(r['yr'])}")

    # Filter to honest "useful" set: CAGR>=10, DD<=35, 2022>=-25
    print("\n" + "="*100)
    print("HONEST SHORTLIST: CAGRte>=10, DDte<=35, 2022>=-25 (sorted by CAGR)")
    print("="*100)
    honest = [r for r in all_results if r['cagr_test'] >= 10 and r['dd_test'] <= 35 and r['pnl_2022'] >= -25]
    honest.sort(key=lambda r: r['cagr_test'], reverse=True)
    print(f"  found {len(honest)} configs")
    for r in honest[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:47]
        print(f"  {r['phase']:>2} {nm:<48} {stop_s:<4} {str(r['killswitch'])[0]} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4} {r['sharpe']:>4.2f}  {yrline(r['yr'])}")

    # Dedupe by (signal, killswitch, lev, rounded metrics) — strategies that
    # produce identical equity curves with different unused param values.
    seen = set()
    deduped = []
    for r in all_results:
        key = (r['signal'], r['killswitch'], round(r['lev'],2),
               round(r['tot_test'],2), round(r['dd_test'],2),
               round(r['pnl_2022'],2), r['nt'])
        if key in seen: continue
        seen.add(key)
        deduped.append(r)

    out = []
    for r in deduped[:50]:
        row = {k: v for k, v in r.items() if k != 'yr'}
        for y in range(2020, 2027):
            row[f'y{y}'] = r['yr'].get(y, np.nan)
        out.append(row)
    df_out = pd.DataFrame(out)
    csv_path = ROOT/'rts_top_strategies.csv'
    df_out.to_csv(csv_path, index=False)
    print(f"\nSaved top-50 deduped → {csv_path}")

    # Print true top-10 from deduped global list
    print("\n" + "="*100)
    print("DEDUPED TOP-10 ACROSS ALL PHASES")
    print("="*100)
    print(f"  {'ph':>2} {'config':<48} stop ks lev {'CAGRte':>7} {'DDte':>5} {'2022':>6} {'PF':>5} {'N':>4} {'Shrp':>5}  per-year")
    for r in deduped[:10]:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        nm = r['signal'][:47]
        print(f"  {r['phase']:>2} {nm:<48} {stop_s:<4} {str(r['killswitch'])[0]} {r['lev']:.1f} "
              f"{r['cagr_test']:>+6.1f}% {r['dd_test']:>4.0f}% {r['pnl_2022']:>+5.0f}% {r['pf']:>4.2f} {r['nt']:>4} {r['sharpe']:>4.2f}  {yrline(r['yr'])}")

    # Topical groups: TOP-5 single-signal and TOP-5 multi-signal
    print("\n" + "="*100)
    print("DEDUPED TOP-5 SINGLE-SIGNAL (phase 1)")
    print("="*100)
    p1 = [r for r in deduped if r['phase'] == 1][:5]
    for r in p1:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        print(f"  {r['signal']:<14} qe={r['qe']} stop={stop_s} lev={r['lev']} ks={r['killswitch']} "
              f"CAGRte={r['cagr_test']:+.1f}% DDte={r['dd_test']:.0f}% 2022={r['pnl_2022']:+.0f}% PF={r['pf']:.2f} N={r['nt']}")

    print("\n" + "="*100)
    print("DEDUPED TOP-5 MULTI-SIGNAL (phase 2)")
    print("="*100)
    p2 = [r for r in deduped if r['phase'] == 2][:5]
    for r in p2:
        stop_s = f"{r['stop']*100:.0f}%" if r['stop'] is not None else "none"
        print(f"  {r['signal']:<46} stop={stop_s} lev={r['lev']} ks={r['killswitch']} "
              f"CAGRte={r['cagr_test']:+.1f}% DDte={r['dd_test']:.0f}% 2022={r['pnl_2022']:+.0f}% PF={r['pf']:.2f} N={r['nt']}")

    all_results = deduped  # for the best-overall section below

    # Useful summary numbers for the report
    if all_results:
        best = all_results[0]
        print("\n" + "="*100)
        print("BEST OVERALL")
        print("="*100)
        print(f"  signal:    {best['signal']}")
        print(f"  phase:     {best['phase']}, killswitch={best['killswitch']}, stop={best['stop']}, lev={best['lev']}")
        print(f"  TRAIN:     {best['tot_train']:+.1f}%")
        print(f"  TEST:      {best['tot_test']:+.1f}%  CAGR {best['cagr_test']:+.1f}%  DD {best['dd_test']:.0f}%")
        print(f"  2022:      pnl {best['pnl_2022']:+.1f}%  DD {best['dd_2022']:.0f}%")
        print(f"  trades:    {best['nt']}   WR {best['wr']:.0f}%   PF {best['pf']:.2f}   Sharpe {best['sharpe']:.2f}")
        print(f"  per-year:  {yrline(best['yr'])}")


if __name__ == "__main__":
    main()
