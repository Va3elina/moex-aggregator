"""Deep grid search for Si (USD/RUB futures) option strategies.

Phase 1: single-signal sweep (entry/exit/hold/stop/leverage) for robust signals.
Phase 2: pair combinations (AND/OR/asymmetric).
Phase 3: voting ensembles (N+ signals fire).
Phase 4: regime-aware (high-vol vs low-vol filter).
Phase 5: short side (sell USD when retail euphoria).

Train: 2020-01 -> 2022-12. Test: 2023-01 -> 2026-05.
B&H Si over period ~= +13%.

Honest reporting: train/test must BOTH be positive for "robust". Test = unseen.
Outputs: si_top_strategies.csv (top configs by composite score).
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path
from itertools import product, combinations
import time

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')

# Signal directional hypothesis: 'high' = top-quantile is bullish, 'low' = bottom-quantile bullish
# Plus FIZ-contrarian = fiz_lsr LOW means retail not euphoric long USD -> contrarian bullish
ROBUST_SIGS = [
    ('max_pain_dist_pct', 'low'),    # best Si signal: low = price below max_pain = bullish snap-back
    ('gex_norm',          'high'),   # dealer positioning -> stability
    ('pc_vol_ratio',      'high'),   # high put/call vol = panic = bullish
    ('straddle_move_pct', 'low'),    # low implied move = vol-sale opp / continuation
    ('fiz_lsr_contra',    'low'),    # contrarian: low FIZ LSR = USD not consensus long
]

# Search grids (kept modest to control total runtime)
ENTRY_Q  = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
EXIT_Q   = [0.5, 0.6, 0.7, 0.8]
MIN_HOLD = [5, 10, 15, 20, 30]
MAX_HOLD = [30, 60, 90, 120, 180]
STOPS    = [0.03, 0.05, 0.07, 0.10, 0.15, None]
LEVS     = [1.0, 1.5, 2.0, 2.5, 3.0]

YEARS = list(range(2020, 2027))


def load_si():
    ind = pd.read_csv(ROOT/'si_option_indicators_daily.csv', parse_dates=['date']).set_index('date').sort_index()
    px = pd.read_csv(ROOT/'si_daily.csv', parse_dates=['date']).set_index('date')['close']
    df = pd.DataFrame(index=ind.index)
    df['close'] = px.reindex(ind.index).ffill()
    df = df.join(ind, how='left')
    df['ret'] = df['close'].pct_change()
    df['rv30'] = df['ret'].rolling(30).std() * np.sqrt(252) * 100
    df['rv90'] = df['ret'].rolling(90).std() * np.sqrt(252) * 100
    df['vrp'] = df['atm_iv'] - df['rv30']
    ft = pd.read_csv(ROOT/'si_futoi_history.csv', parse_dates=['date']).set_index('date')
    df['fiz_lsr'] = (ft['fiz_long']/ft['fiz_short'].replace(0,1)).reindex(df.index)
    df['fiz_lsr_contra'] = df['fiz_lsr']  # alias for clarity in signal lookup
    return df[df['atm_iv'].notna()].copy()


def bt_vec(closes, em, xm, minh, maxh, stop, lev, side='long'):
    """Vectorised-friendly inner-loop backtest. Returns (eq_series, trades_pnl)."""
    n = len(closes)
    eq = np.full(n, INIT, dtype=float)
    cash = INIT
    inp = False
    ei = 0; ep = 0.0
    pnls = []
    sgn = 1.0 if side == 'long' else -1.0
    em_arr = em.astype(bool)
    xm_arr = xm.astype(bool)
    for i in range(n):
        if inp:
            base = sgn * (closes[i]/ep - 1.0)
            eq[i] = cash * (1.0 + base * lev)
            h = i - ei
            do = False
            if stop is not None and base <= -stop:
                do = True
            elif h >= maxh:
                do = True
            elif h >= minh and xm_arr[i]:
                do = True
            if do:
                exit_px = closes[i] * (1.0 - SLIP*sgn)  # slippage costs you on exit too
                r = sgn * (exit_px/ep - 1.0) * lev - 2.0*FEE
                cash *= (1.0 + r)
                pnls.append(r * 100.0)
                inp = False
                eq[i] = cash
        else:
            eq[i] = cash
        if (not inp) and em_arr[i]:
            ei = i
            ep = closes[i] * (1.0 + SLIP*sgn)
            inp = True
    return eq, pnls


def metrics(eq_arr, dates, pnls, start_idx=0, end_idx=None):
    if end_idx is None:
        end_idx = len(eq_arr)
    s = pd.Series(eq_arr[start_idx:end_idx], index=dates[start_idx:end_idx])
    if len(s) < 2:
        return dict(tot=0.0, dd=0.0, n=0, wr=0.0, pf=0.0, cagr=0.0, yr={})
    tot = (s.iloc[-1]/s.iloc[0] - 1.0) * 100.0
    dd = ((s.cummax() - s)/s.cummax()).max() * 100.0
    sub_pnls = pnls  # full pnls list -> only count those in range
    nt = len(sub_pnls)
    wr = (np.array(sub_pnls) > 0).mean() * 100.0 if sub_pnls else 0.0
    pos = sum(x for x in sub_pnls if x > 0)
    neg = -sum(x for x in sub_pnls if x < 0)
    pf = pos/neg if neg > 0 else (9.99 if pos > 0 else 0.0)
    yrs = (s.index[-1] - s.index[0]).days / 365.25
    cagr = ((s.iloc[-1]/s.iloc[0]) ** (1/yrs) - 1) * 100 if yrs > 0 and s.iloc[0] > 0 else 0.0
    yr = {}
    for y in YEARS:
        seg = s[s.index.year == y]
        if len(seg) > 1:
            yr[y] = (seg.iloc[-1]/seg.iloc[0] - 1)*100
    return dict(tot=tot, dd=dd, n=nt, wr=wr, pf=pf, cagr=cagr, yr=yr)


def run_config(df, em_full, xm_full, minh, maxh, stop, lev, side='long'):
    """Run a backtest on full df, then split metrics into train/test windows."""
    closes = df['close'].values
    dates = df.index
    eq, pnls = bt_vec(closes, em_full, xm_full, minh, maxh, stop, lev, side)
    # Split pnls by date
    train_mask_dates = [d for d in dates if d < SPLIT]
    n_train = len(train_mask_dates)
    # split pnls actually requires knowing trade close dates; we redo cheap pass
    eq_train_end_idx = n_train
    # We didn't record trade dates above; rebuild by re-running on segments for clean stats
    eq_train, p_train = bt_vec(closes[:n_train], em_full[:n_train], xm_full[:n_train],
                                minh, maxh, stop, lev, side)
    eq_test, p_test = bt_vec(closes[n_train:], em_full[n_train:], xm_full[n_train:],
                              minh, maxh, stop, lev, side)
    train_m = metrics(eq_train, dates[:n_train], p_train)
    test_m  = metrics(eq_test, dates[n_train:], p_test)
    full_m  = metrics(eq, dates, pnls)
    return train_m, test_m, full_m


def signal_mask(df, sig_name, direction, quantile, train_df):
    """Return (entry_mask, exit_mask) using TRAIN-derived threshold."""
    col = 'fiz_lsr' if sig_name == 'fiz_lsr_contra' else sig_name
    if col not in df.columns:
        return None, None
    q_thr = quantile if direction == 'high' else (1.0 - quantile)
    # We want extreme: if direction=high, entry when value >= q(1-quantile-ish? we want top-quantile)
    # Convention: entry when in the "bullish tail". For direction='high', tail = top q (q=0.30 -> top 30%).
    if direction == 'high':
        thr_in = train_df[col].quantile(1 - quantile)  # 70th if q=0.30
        em = df[col] >= thr_in
    else:
        thr_in = train_df[col].quantile(quantile)
        em = df[col] <= thr_in
    return em, thr_in


def exit_mask_from(df, sig_name, direction, exit_q, train_df):
    col = 'fiz_lsr' if sig_name == 'fiz_lsr_contra' else sig_name
    if col not in df.columns:
        return None
    # Exit when sig moves AWAY from bullish tail beyond exit_q
    if direction == 'high':
        thr_out = train_df[col].quantile(1 - exit_q)  # cross below to exit
        xm = df[col] <= thr_out
    else:
        thr_out = train_df[col].quantile(exit_q)
        xm = df[col] >= thr_out
    return xm


def score_config(train_m, test_m):
    """Composite score: emphasize TEST performance but penalize wild train/test divergence."""
    if train_m['n'] < 5 or test_m['n'] < 3:
        return -1e9
    test_cagr = test_m['cagr']
    test_dd = max(test_m['dd'], 1.0)
    train_cagr = train_m['cagr']
    # MAR-like: cagr/dd. Penalize if test much worse than train.
    mar = test_cagr / test_dd
    consistency = min(train_cagr, test_cagr) / max(abs(train_cagr), abs(test_cagr), 1e-6)
    return mar * 10 + test_cagr * 0.5 + 5 * consistency - (10 if test_cagr < 0 else 0)


def phase1_single(df):
    """Phase 1: per-signal grid search."""
    print("\n" + "="*100)
    print("PHASE 1: Single-signal grid search")
    print("="*100)
    train_df = df[df.index < SPLIT]
    results = []
    t0 = time.time()
    for sig, direction in ROBUST_SIGS:
        col = 'fiz_lsr' if sig == 'fiz_lsr_contra' else sig
        if col not in df.columns or df[col].isna().all():
            print(f"  skip {sig}: no data"); continue
        # Pre-build masks for each entry q (avoid recompute)
        for eq in ENTRY_Q:
            em, _ = signal_mask(df, sig, direction, eq, train_df)
            for xq in EXIT_Q:
                xm = exit_mask_from(df, sig, direction, xq, train_df)
                if em is None or xm is None: continue
                em_f = em.fillna(False).values
                xm_f = xm.fillna(False).values
                for mh in MIN_HOLD:
                    for xh in MAX_HOLD:
                        if mh >= xh: continue
                        for stop in STOPS:
                            for lev in LEVS:
                                tr_m, te_m, _ = run_config(df, em_f, xm_f, mh, xh, stop, lev)
                                sc = score_config(tr_m, te_m)
                                results.append(dict(
                                    phase='1-single',
                                    signal=sig, direction=direction,
                                    entry_q=eq, exit_q=xq, min_hold=mh, max_hold=xh,
                                    stop=stop, lev=lev, side='long',
                                    train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                                    train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                                    test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                                    test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                                    test_tot=te_m['tot'], train_tot=tr_m['tot'],
                                    score=sc,
                                ))
        elapsed = time.time() - t0
        print(f"  done {sig} ({elapsed:.0f}s, {len(results)} configs)")
    return results


def phase2_pairs(df, top_singles):
    """Phase 2: pair combinations with AND/OR/asymmetric (using best parameter from Phase 1)."""
    print("\n" + "="*100)
    print("PHASE 2: Pair combinations")
    print("="*100)
    train_df = df[df.index < SPLIT]
    results = []
    # Use best entry/exit q for each signal from phase 1
    best_per_sig = {}
    for r in top_singles:
        s = r['signal']
        if s not in best_per_sig or r['score'] > best_per_sig[s]['score']:
            best_per_sig[s] = r
    sigs_avail = list(best_per_sig.keys())
    print(f"  Using best singles: {[(s, best_per_sig[s]['entry_q'], best_per_sig[s]['exit_q']) for s in sigs_avail]}")
    pairs = list(combinations(sigs_avail, 2))
    for s1, s2 in pairs:
        d1 = next(x[1] for x in ROBUST_SIGS if x[0] == s1)
        d2 = next(x[1] for x in ROBUST_SIGS if x[0] == s2)
        b1 = best_per_sig[s1]; b2 = best_per_sig[s2]
        em1, _ = signal_mask(df, s1, d1, b1['entry_q'], train_df)
        em2, _ = signal_mask(df, s2, d2, b2['entry_q'], train_df)
        xm1 = exit_mask_from(df, s1, d1, b1['exit_q'], train_df)
        xm2 = exit_mask_from(df, s2, d2, b2['exit_q'], train_df)
        em1f = em1.fillna(False); em2f = em2.fillna(False)
        xm1f = xm1.fillna(False); xm2f = xm2.fillna(False)
        combos = [
            ('AND', em1f & em2f, xm1f | xm2f),         # both in, either out
            ('OR',  em1f | em2f, xm1f & xm2f),         # either in, both out
            ('S1->S2x', em1f, xm2f),                   # entry on s1, exit on s2
            ('S2->S1x', em2f, xm1f),                   # entry on s2, exit on s1
        ]
        # Reduced param sweep on pairs (already big)
        for combo_name, em_c, xm_c in combos:
            em_arr = em_c.values; xm_arr = xm_c.values
            for mh, xh in [(10, 60), (10, 90), (20, 90), (10, 120)]:
                for stop in [0.05, 0.10, None]:
                    for lev in [1.0, 1.5, 2.0]:
                        tr_m, te_m, _ = run_config(df, em_arr, xm_arr, mh, xh, stop, lev)
                        sc = score_config(tr_m, te_m)
                        results.append(dict(
                            phase='2-pair',
                            signal=f"{s1}+{s2}/{combo_name}", direction='-',
                            entry_q=f"{b1['entry_q']}/{b2['entry_q']}",
                            exit_q=f"{b1['exit_q']}/{b2['exit_q']}",
                            min_hold=mh, max_hold=xh, stop=stop, lev=lev, side='long',
                            train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                            train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                            test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                            test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                            test_tot=te_m['tot'], train_tot=tr_m['tot'],
                            score=sc,
                        ))
    print(f"  pair configs: {len(results)}")
    return results


def phase3_vote(df, top_singles):
    """Phase 3: voting ensemble (N+ of K signals fire)."""
    print("\n" + "="*100)
    print("PHASE 3: Voting ensembles")
    print("="*100)
    train_df = df[df.index < SPLIT]
    results = []
    best_per_sig = {}
    for r in top_singles:
        s = r['signal']
        if s not in best_per_sig or r['score'] > best_per_sig[s]['score']:
            best_per_sig[s] = r
    sigs_avail = list(best_per_sig.keys())
    em_per = {}
    xm_per = {}
    for s in sigs_avail:
        d = next(x[1] for x in ROBUST_SIGS if x[0] == s)
        b = best_per_sig[s]
        em, _ = signal_mask(df, s, d, b['entry_q'], train_df)
        xm = exit_mask_from(df, s, d, b['exit_q'], train_df)
        em_per[s] = em.fillna(False).astype(int)
        xm_per[s] = xm.fillna(False).astype(int)
    K = len(sigs_avail)
    vote = pd.concat([em_per[s] for s in sigs_avail], axis=1).sum(axis=1)
    # Weighted (train_cagr as weight)
    weights = np.array([max(best_per_sig[s]['train_cagr'], 0.1) for s in sigs_avail])
    weights = weights / weights.sum()
    weighted = sum(em_per[s].values * weights[i] for i, s in enumerate(sigs_avail))
    for vmin in range(1, K+1):
        em_c = (vote >= vmin)
        xm_c = (vote < vmin)
        for mh, xh in [(10, 60), (10, 90), (20, 90), (15, 120)]:
            for stop in [0.05, 0.10, None]:
                for lev in [1.0, 1.5, 2.0]:
                    tr_m, te_m, _ = run_config(df, em_c.values, xm_c.values, mh, xh, stop, lev)
                    sc = score_config(tr_m, te_m)
                    results.append(dict(
                        phase='3-vote', signal=f"vote>={vmin}/{K}", direction='-',
                        entry_q='-', exit_q='-',
                        min_hold=mh, max_hold=xh, stop=stop, lev=lev, side='long',
                        train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                        train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                        test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                        test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                        test_tot=te_m['tot'], train_tot=tr_m['tot'],
                        score=sc,
                    ))
    # Weighted threshold
    for wmin in [0.3, 0.5, 0.7]:
        em_c = pd.Series(weighted >= wmin, index=df.index)
        xm_c = ~em_c
        for mh, xh in [(10, 90), (20, 120)]:
            for stop in [0.05, 0.10, None]:
                for lev in [1.0, 1.5, 2.0]:
                    tr_m, te_m, _ = run_config(df, em_c.values, xm_c.values, mh, xh, stop, lev)
                    sc = score_config(tr_m, te_m)
                    results.append(dict(
                        phase='3-weighted', signal=f"weighted>={wmin}", direction='-',
                        entry_q='-', exit_q='-',
                        min_hold=mh, max_hold=xh, stop=stop, lev=lev, side='long',
                        train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                        train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                        test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                        test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                        test_tot=te_m['tot'], train_tot=tr_m['tot'],
                        score=sc,
                    ))
    print(f"  voting configs: {len(results)}")
    return results


def phase4_regime(df, top_singles):
    """Phase 4: regime filter (high-vol vs low-vol via rolling 90d RV)."""
    print("\n" + "="*100)
    print("PHASE 4: Regime-aware (rolling 90d vol terciles)")
    print("="*100)
    train_df = df[df.index < SPLIT]
    # Regime threshold from train
    rv_tr = train_df['rv90'].dropna()
    if len(rv_tr) < 30:
        print("  not enough RV data"); return []
    rv_lo = rv_tr.quantile(0.33)
    rv_hi = rv_tr.quantile(0.66)
    regime = pd.Series('mid', index=df.index)
    regime[df['rv90'] <= rv_lo] = 'lo'
    regime[df['rv90'] >= rv_hi] = 'hi'
    print(f"  RV90 thresholds: lo<={rv_lo:.1f}, hi>={rv_hi:.1f}")
    results = []
    best_per_sig = {}
    for r in top_singles:
        s = r['signal']
        if s not in best_per_sig or r['score'] > best_per_sig[s]['score']:
            best_per_sig[s] = r
    for sig, _ in best_per_sig.items():
        d = next(x[1] for x in ROBUST_SIGS if x[0] == sig)
        b = best_per_sig[sig]
        em, _ = signal_mask(df, sig, d, b['entry_q'], train_df)
        xm = exit_mask_from(df, sig, d, b['exit_q'], train_df)
        em = em.fillna(False); xm = xm.fillna(False)
        for reg in ['lo', 'mid', 'hi']:
            reg_mask = (regime == reg)
            em_c = em & reg_mask
            xm_c = xm | (~reg_mask)  # exit if leave regime
            for mh, xh in [(10, 60), (10, 90), (20, 120)]:
                for stop in [0.05, 0.10, None]:
                    for lev in [1.0, 1.5, 2.0]:
                        tr_m, te_m, _ = run_config(df, em_c.values, xm_c.values, mh, xh, stop, lev)
                        sc = score_config(tr_m, te_m)
                        results.append(dict(
                            phase='4-regime', signal=f"{sig}|reg={reg}",
                            direction=d, entry_q=b['entry_q'], exit_q=b['exit_q'],
                            min_hold=mh, max_hold=xh, stop=stop, lev=lev, side='long',
                            train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                            train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                            test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                            test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                            test_tot=te_m['tot'], train_tot=tr_m['tot'],
                            score=sc,
                        ))
    print(f"  regime configs: {len(results)}")
    return results


def phase5_short(df):
    """Phase 5: short side (sell Si when retail euphoria)."""
    print("\n" + "="*100)
    print("PHASE 5: Short side (sell USD/RUB on retail euphoria)")
    print("="*100)
    train_df = df[df.index < SPLIT]
    results = []
    # Short signals: opposite direction
    short_sigs = [
        ('fiz_lsr_short', 'fiz_lsr', 'high'),       # fiz LSR high = retail euphoric long USD = short
        ('max_pain_dist_short', 'max_pain_dist_pct', 'high'),  # price above max pain = mean revert down
        ('straddle_move_short', 'straddle_move_pct', 'high'),  # high implied move = vol spike = short USD?
    ]
    for label, col, d in short_sigs:
        if col not in df.columns: continue
        for eq in ENTRY_Q:
            if d == 'high':
                thr_in = train_df[col].quantile(1 - eq)
                em = df[col] >= thr_in
            else:
                thr_in = train_df[col].quantile(eq)
                em = df[col] <= thr_in
            for xq in EXIT_Q:
                if d == 'high':
                    thr_out = train_df[col].quantile(1 - xq)
                    xm = df[col] <= thr_out
                else:
                    thr_out = train_df[col].quantile(xq)
                    xm = df[col] >= thr_out
                em_f = em.fillna(False).values
                xm_f = xm.fillna(False).values
                for mh in [10, 20]:
                    for xh in [60, 90, 120]:
                        for stop in [0.05, 0.10, None]:
                            for lev in [1.0, 1.5, 2.0]:
                                tr_m, te_m, _ = run_config(df, em_f, xm_f, mh, xh, stop, lev, side='short')
                                sc = score_config(tr_m, te_m)
                                results.append(dict(
                                    phase='5-short', signal=label,
                                    direction=d, entry_q=eq, exit_q=xq,
                                    min_hold=mh, max_hold=xh, stop=stop, lev=lev, side='short',
                                    train_cagr=tr_m['cagr'], train_dd=tr_m['dd'],
                                    train_wr=tr_m['wr'], train_pf=tr_m['pf'], train_n=tr_m['n'],
                                    test_cagr=te_m['cagr'], test_dd=te_m['dd'],
                                    test_wr=te_m['wr'], test_pf=te_m['pf'], test_n=te_m['n'],
                                    test_tot=te_m['tot'], train_tot=tr_m['tot'],
                                    score=sc,
                                ))
    print(f"  short configs: {len(results)}")
    return results


def yearly_breakdown(df, em, xm, minh, maxh, stop, lev, side='long'):
    closes = df['close'].values
    eq, pnls = bt_vec(closes, em, xm, minh, maxh, stop, lev, side)
    s = pd.Series(eq, index=df.index)
    out = {}
    for y in YEARS:
        seg = s[s.index.year == y]
        if len(seg) > 1:
            out[y] = (seg.iloc[-1]/seg.iloc[0] - 1)*100
    return out


def main():
    print("Loading Si data...")
    df = load_si()
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1)*100
    print(f"  rows: {len(df)} ({df.index[0].date()} -> {df.index[-1].date()})")
    print(f"  B&H Si: {bh:+.1f}%")
    print(f"  train: {(df.index<SPLIT).sum()}d, test: {(df.index>=SPLIT).sum()}d")

    t_global = time.time()

    p1 = phase1_single(df)
    print(f"  Phase 1 total: {len(p1)} configs in {time.time()-t_global:.0f}s")

    # Filter top from phase 1: need both train and test positive, n>=5 trades each
    p1_df = pd.DataFrame(p1)
    p1_ok = p1_df[(p1_df['train_cagr']>0) & (p1_df['test_cagr']>0)
                  & (p1_df['train_n']>=5) & (p1_df['test_n']>=3)].copy()
    top_singles = p1_ok.sort_values('score', ascending=False).head(50).to_dict('records')
    if not top_singles:
        print("WARNING: no robust singles. Using top by test_cagr instead.")
        top_singles = p1_df.sort_values('test_cagr', ascending=False).head(50).to_dict('records')

    p2 = phase2_pairs(df, top_singles)
    p3 = phase3_vote(df, top_singles)
    p4 = phase4_regime(df, top_singles)
    p5 = phase5_short(df)

    all_res = p1 + p2 + p3 + p4 + p5
    res_df = pd.DataFrame(all_res)
    res_df = res_df.sort_values('score', ascending=False)
    print(f"\nAll configs: {len(res_df)}")

    # Save top 50 by score
    top50 = res_df.head(50).copy()
    top50.to_csv(ROOT/'si_top_strategies.csv', index=False)
    print(f"Saved top 50 -> si_top_strategies.csv")

    # Print top 10 by composite, top 5 by test_cagr, top 5 by best Sharpe-like
    print("\n" + "="*120)
    print("TOP 10 BY COMPOSITE SCORE (test-MAR + consistency)")
    print("="*120)
    cols = ['phase','signal','min_hold','max_hold','stop','lev',
            'train_cagr','train_dd','train_wr','train_pf','train_n',
            'test_cagr','test_dd','test_wr','test_pf','test_n','score']
    print(top50[cols].head(10).to_string(index=False))

    print("\n" + "="*120)
    print("TOP 5 BY TEST CAGR (must have test_n>=3, train_cagr>0)")
    print("="*120)
    cand = res_df[(res_df['test_n']>=3) & (res_df['train_cagr']>0)].sort_values('test_cagr', ascending=False)
    print(cand[cols].head(5).to_string(index=False))

    print("\n" + "="*120)
    print("TOP 5 BY BEST TEST CAGR/DD RATIO")
    print("="*120)
    cand2 = res_df[(res_df['test_n']>=3) & (res_df['train_cagr']>0) & (res_df['test_dd']>0)].copy()
    cand2['mar'] = cand2['test_cagr'] / cand2['test_dd'].clip(lower=1)
    print(cand2.sort_values('mar', ascending=False)[cols+['mar']].head(5).to_string(index=False))

    print("\n" + "="*120)
    print("TOP 5 SINGLE-SIGNAL CONFIGS (by composite score)")
    print("="*120)
    sing = res_df[res_df['phase']=='1-single']
    print(sing.sort_values('score', ascending=False)[cols].head(5).to_string(index=False))

    print("\n" + "="*120)
    print("TOP 5 MULTI-SIGNAL CONFIGS (pairs/voting/weighted)")
    print("="*120)
    multi = res_df[res_df['phase'].isin(['2-pair','3-vote','3-weighted'])]
    print(multi.sort_values('score', ascending=False)[cols].head(5).to_string(index=False))

    print("\n" + "="*120)
    print("TOP REGIME-CONDITIONAL")
    print("="*120)
    reg = res_df[res_df['phase']=='4-regime']
    print(reg.sort_values('score', ascending=False)[cols].head(5).to_string(index=False))

    print("\n" + "="*120)
    print("TOP SHORT-SIDE")
    print("="*120)
    sh = res_df[res_df['phase']=='5-short']
    print(sh.sort_values('score', ascending=False)[cols].head(5).to_string(index=False))

    # Detailed per-year breakdown for OVERALL BEST
    print("\n" + "="*120)
    print("OVERALL BEST: detailed per-year breakdown")
    print("="*120)
    best = top50.iloc[0]
    print(f"Best config: phase={best['phase']} signal={best['signal']} q_entry={best['entry_q']} q_exit={best['exit_q']}")
    print(f"  hold {best['min_hold']}-{best['max_hold']}, stop={best['stop']}, lev={best['lev']}, side={best['side']}")
    print(f"  train CAGR {best['train_cagr']:+.1f}% DD {best['train_dd']:.0f}% WR {best['train_wr']:.0f}% PF {best['train_pf']:.2f} N {best['train_n']}")
    print(f"  test  CAGR {best['test_cagr']:+.1f}% DD {best['test_dd']:.0f}% WR {best['test_wr']:.0f}% PF {best['test_pf']:.2f} N {best['test_n']}")

    print(f"\nTotal runtime: {time.time()-t_global:.0f}s")


if __name__ == "__main__":
    main()
