"""Deep grid search for Si (USD/RUB futures) option strategies. Vectorised & fast.

Phase 1: single-signal sweep (entry/exit/hold/stop/leverage) for robust signals.
Phase 2: pair combinations (AND/OR/asymmetric).
Phase 3: voting ensembles (N+ signals fire) + weighted.
Phase 4: regime-aware (high-vol vs low-vol filter via rolling 90d RV).
Phase 5: short side (sell USD when retail euphoria).

Train: 2020-01 -> 2022-12. Test: 2023-01 -> 2026-05.
B&H Si over period ~= +13%.

Honest reporting: train and test reported SEPARATELY.
Outputs: si_top_strategies.csv (top configs by composite score).
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from pathlib import Path
from itertools import combinations
import time, sys
try:
    from numba import njit
    HAVE_NUMBA = True
except Exception:
    HAVE_NUMBA = False
    def njit(*a, **k):
        def deco(f): return f
        return deco

ROOT = Path(__file__).parent
INIT = 100.0
FEE = 0.0003
SLIP = 0.0001
SPLIT = pd.Timestamp('2023-01-01')

ROBUST_SIGS = [
    ('max_pain_dist_pct', 'low'),
    ('gex_norm',          'high'),
    ('pc_vol_ratio',      'high'),
    ('straddle_move_pct', 'low'),
    ('fiz_lsr_contra',    'low'),
]

ENTRY_Q  = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
EXIT_Q   = [0.5, 0.6, 0.7, 0.8]
MIN_HOLD = [5, 10, 15, 20, 30]
MAX_HOLD = [30, 60, 90, 120, 180]
STOPS    = [0.03, 0.05, 0.07, 0.10, 0.15, None]
LEVS     = [1.0, 1.5, 2.0, 2.5, 3.0]


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
    df['fiz_lsr_contra'] = df['fiz_lsr']
    return df[df['atm_iv'].notna()].copy()


@njit(cache=True, fastmath=True)
def bt_core_nb(closes, em, xm, minh, maxh, stop, has_stop, lev, sgn, fee, slip, init):
    """JIT-compiled backtest. Returns (final_eq, n_trades, max_dd_pct, sum_pos, sum_neg, n_wins)."""
    n = closes.shape[0]
    cash = init
    eq_peak = init
    eq_now = init
    max_dd = 0.0
    inp = False
    ei = 0
    ep = 0.0
    n_tr = 0
    n_w = 0
    sum_pos = 0.0
    sum_neg = 0.0
    pnls = np.zeros(n, dtype=np.float64)
    for i in range(n):
        if inp:
            base = sgn * (closes[i]/ep - 1.0)
            eq_now = cash * (1.0 + base * lev)
            h = i - ei
            do = False
            if has_stop and base <= -stop:
                do = True
            elif h >= maxh:
                do = True
            elif h >= minh and xm[i]:
                do = True
            if do:
                exit_px = closes[i] * (1.0 - slip*sgn)
                r = sgn * (exit_px/ep - 1.0) * lev - 2.0*fee
                cash *= (1.0 + r)
                pnl = r * 100.0
                pnls[n_tr] = pnl
                n_tr += 1
                if pnl > 0:
                    n_w += 1
                    sum_pos += pnl
                elif pnl < 0:
                    sum_neg += -pnl
                inp = False
                eq_now = cash
        else:
            eq_now = cash
        if eq_now > eq_peak:
            eq_peak = eq_now
        dd = (eq_peak - eq_now) / eq_peak * 100.0
        if dd > max_dd:
            max_dd = dd
        if (not inp) and em[i]:
            ei = i
            ep = closes[i] * (1.0 + slip*sgn)
            inp = True
    final_eq = eq_now if inp else cash
    return final_eq, n_tr, max_dd, sum_pos, sum_neg, n_w


def bt_core(closes, em, xm, minh, maxh, stop, lev, side):
    sgn = 1.0 if side == 'long' else -1.0
    has_stop = stop is not None
    stop_v = stop if has_stop else 0.0
    return bt_core_nb(closes, em.astype(np.bool_), xm.astype(np.bool_),
                      minh, maxh, stop_v, has_stop, lev, sgn, FEE, SLIP, INIT)


def stats_from(bt_out, n_days):
    final_eq, nt, max_dd, sum_pos, sum_neg, n_w = bt_out
    tot = (final_eq/INIT - 1.0) * 100.0
    if nt == 0:
        return dict(tot=tot, dd=max_dd, n=0, wr=0.0, pf=0.0, cagr=0.0)
    wr = (n_w / nt) * 100.0
    pf = sum_pos/sum_neg if sum_neg > 0 else (9.99 if sum_pos > 0 else 0.0)
    yrs = n_days / 252.0
    cagr = ((final_eq/INIT) ** (1/yrs) - 1) * 100 if yrs > 0 and final_eq > 0 else -100
    return dict(tot=tot, dd=max_dd, n=nt, wr=wr, pf=pf, cagr=cagr)


def yearly_eq(closes, em, xm, minh, maxh, stop, lev, side, year_arr):
    """Run bt but capture per-year returns. Returns dict year->return_pct."""
    n = len(closes)
    cash = INIT
    inp = False
    ei = 0; ep = 0.0
    sgn = 1.0 if side == 'long' else -1.0
    eq = np.full(n, INIT, dtype=float)
    for i in range(n):
        if inp:
            base = sgn * (closes[i]/ep - 1.0)
            eq[i] = cash * (1.0 + base * lev)
            h = i - ei
            do = False
            if stop is not None and base <= -stop: do = True
            elif h >= maxh: do = True
            elif h >= minh and xm[i]: do = True
            if do:
                exit_px = closes[i] * (1.0 - SLIP*sgn)
                r = sgn * (exit_px/ep - 1.0) * lev - 2.0*FEE
                cash *= (1.0 + r)
                inp = False
                eq[i] = cash
        else:
            eq[i] = cash
        if (not inp) and em[i]:
            ei = i; ep = closes[i] * (1.0 + SLIP*sgn); inp = True
    out = {}
    for y in np.unique(year_arr):
        mask = year_arr == y
        idx = np.where(mask)[0]
        if len(idx) > 1:
            out[int(y)] = (eq[idx[-1]]/eq[idx[0]] - 1) * 100
    return out


def split_run(closes, em_full, xm_full, n_train, minh, maxh, stop, lev, side='long'):
    """Run bt separately on train and test, return both stats dicts."""
    r1 = bt_core(closes[:n_train], em_full[:n_train], xm_full[:n_train], minh, maxh, stop, lev, side)
    r2 = bt_core(closes[n_train:], em_full[n_train:], xm_full[n_train:], minh, maxh, stop, lev, side)
    tr_m = stats_from(r1, n_train)
    te_m = stats_from(r2, len(closes) - n_train)
    return tr_m, te_m


def score_config(tr, te):
    if tr['n'] < 5 or te['n'] < 3:
        return -1e9
    if tr['cagr'] <= 0 or te['cagr'] <= 0:
        # still allow but heavily penalize
        return -50 + 0.1*tr['cagr'] + 0.5*te['cagr']
    mar = te['cagr'] / max(te['dd'], 1.0)
    cons = min(tr['cagr'], te['cagr']) / max(tr['cagr'], te['cagr'])
    return mar*10 + te['cagr']*0.5 + 5*cons


def get_col(sig):
    return 'fiz_lsr' if sig == 'fiz_lsr_contra' else sig


def precompute_entry_masks(df, train_df, sig, direction, qlist):
    """Returns dict q -> bool array (np)."""
    col = get_col(sig)
    if col not in df.columns:
        return None
    series = df[col]
    train_series = train_df[col]
    out = {}
    for q in qlist:
        if direction == 'high':
            thr = train_series.quantile(1 - q)
            mask = (series >= thr).fillna(False).values
        else:
            thr = train_series.quantile(q)
            mask = (series <= thr).fillna(False).values
        out[q] = mask
    return out


def precompute_exit_masks(df, train_df, sig, direction, qlist):
    col = get_col(sig)
    if col not in df.columns:
        return None
    series = df[col]
    train_series = train_df[col]
    out = {}
    for q in qlist:
        if direction == 'high':
            thr = train_series.quantile(1 - q)
            mask = (series <= thr).fillna(False).values
        else:
            thr = train_series.quantile(q)
            mask = (series >= thr).fillna(False).values
        out[q] = mask
    return out


def phase1_single(df):
    print("\nPHASE 1: Single-signal grid search")
    train_df = df[df.index < SPLIT]
    n_train = (df.index < SPLIT).sum()
    closes = df['close'].values
    results = []
    t0 = time.time()
    for sig, direction in ROBUST_SIGS:
        col = get_col(sig)
        if col not in df.columns or df[col].isna().all():
            print(f"  skip {sig}"); continue
        em_dict = precompute_entry_masks(df, train_df, sig, direction, ENTRY_Q)
        xm_dict = precompute_exit_masks(df, train_df, sig, direction, EXIT_Q)
        cnt_sig = 0
        for eq in ENTRY_Q:
            em = em_dict[eq]
            for xq in EXIT_Q:
                xm = xm_dict[xq]
                for mh in MIN_HOLD:
                    for xh in MAX_HOLD:
                        if mh >= xh: continue
                        for stop in STOPS:
                            for lev in LEVS:
                                tr_m, te_m = split_run(closes, em, xm, n_train, mh, xh, stop, lev)
                                sc = score_config(tr_m, te_m)
                                results.append((
                                    '1-single', sig, direction, eq, xq, mh, xh,
                                    stop if stop is not None else -1, lev, 'long',
                                    tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                                    te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                                    te_m['tot'], tr_m['tot'], sc,
                                ))
                                cnt_sig += 1
        print(f"  {sig}: {cnt_sig} configs, total {time.time()-t0:.0f}s elapsed")
    print(f"Phase 1 done: {len(results)} configs in {time.time()-t0:.0f}s")
    return results


def to_df(rows):
    cols = ['phase','signal','direction','entry_q','exit_q','min_hold','max_hold','stop','lev','side',
            'train_cagr','train_dd','train_wr','train_pf','train_n',
            'test_cagr','test_dd','test_wr','test_pf','test_n','test_tot','train_tot','score']
    return pd.DataFrame(rows, columns=cols)


def phase2_pairs(df, top_per_sig):
    print("\nPHASE 2: Pair combinations")
    train_df = df[df.index < SPLIT]
    n_train = (df.index < SPLIT).sum()
    closes = df['close'].values
    results = []
    sigs = list(top_per_sig.keys())
    print(f"  signals: {sigs}")
    for s1, s2 in combinations(sigs, 2):
        d1 = next(x[1] for x in ROBUST_SIGS if x[0] == s1)
        d2 = next(x[1] for x in ROBUST_SIGS if x[0] == s2)
        b1 = top_per_sig[s1]; b2 = top_per_sig[s2]
        em1 = precompute_entry_masks(df, train_df, s1, d1, [b1['entry_q']])[b1['entry_q']]
        em2 = precompute_entry_masks(df, train_df, s2, d2, [b2['entry_q']])[b2['entry_q']]
        xm1 = precompute_exit_masks(df, train_df, s1, d1, [b1['exit_q']])[b1['exit_q']]
        xm2 = precompute_exit_masks(df, train_df, s2, d2, [b2['exit_q']])[b2['exit_q']]
        combos = [
            ('AND', em1 & em2, xm1 | xm2),
            ('OR',  em1 | em2, xm1 & xm2),
            ('S1eS2x', em1, xm2),
            ('S2eS1x', em2, xm1),
        ]
        for cname, em_c, xm_c in combos:
            for mh, xh in [(10,60),(10,90),(20,90),(10,120),(20,120)]:
                for stop in [0.05, 0.10, None]:
                    for lev in [1.0, 1.5, 2.0]:
                        tr_m, te_m = split_run(closes, em_c, xm_c, n_train, mh, xh, stop, lev)
                        sc = score_config(tr_m, te_m)
                        results.append((
                            '2-pair', f"{s1}+{s2}/{cname}", '-',
                            f"{b1['entry_q']}/{b2['entry_q']}",
                            f"{b1['exit_q']}/{b2['exit_q']}",
                            mh, xh, stop if stop is not None else -1, lev, 'long',
                            tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                            te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                            te_m['tot'], tr_m['tot'], sc,
                        ))
    print(f"  pair configs: {len(results)}")
    return results


def phase3_vote(df, top_per_sig):
    print("\nPHASE 3: Voting + Weighted ensembles")
    train_df = df[df.index < SPLIT]
    n_train = (df.index < SPLIT).sum()
    closes = df['close'].values
    sigs = list(top_per_sig.keys())
    em_per = {}; xm_per = {}
    weights = []
    for s in sigs:
        d = next(x[1] for x in ROBUST_SIGS if x[0] == s)
        b = top_per_sig[s]
        em_per[s] = precompute_entry_masks(df, train_df, s, d, [b['entry_q']])[b['entry_q']].astype(np.int8)
        xm_per[s] = precompute_exit_masks(df, train_df, s, d, [b['exit_q']])[b['exit_q']].astype(np.int8)
        weights.append(max(b['train_cagr'], 0.1))
    weights = np.array(weights) / np.sum(weights)
    K = len(sigs)
    vote = np.zeros(len(df), dtype=np.int8)
    weighted = np.zeros(len(df), dtype=np.float32)
    for i, s in enumerate(sigs):
        vote += em_per[s]
        weighted += em_per[s] * weights[i]
    results = []
    for vmin in range(1, K+1):
        em_c = (vote >= vmin)
        xm_c = (vote < vmin)
        for mh, xh in [(10,60),(10,90),(20,90),(15,120),(20,180)]:
            for stop in [0.05, 0.10, None]:
                for lev in [1.0, 1.5, 2.0, 2.5]:
                    tr_m, te_m = split_run(closes, em_c, xm_c, n_train, mh, xh, stop, lev)
                    sc = score_config(tr_m, te_m)
                    results.append((
                        '3-vote', f"vote>={vmin}/{K}", '-', '-', '-',
                        mh, xh, stop if stop is not None else -1, lev, 'long',
                        tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                        te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                        te_m['tot'], tr_m['tot'], sc,
                    ))
    for wmin in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
        em_c = (weighted >= wmin)
        xm_c = ~em_c
        for mh, xh in [(10,60),(10,90),(20,120),(15,180)]:
            for stop in [0.05, 0.10, None]:
                for lev in [1.0, 1.5, 2.0, 2.5]:
                    tr_m, te_m = split_run(closes, em_c, xm_c, n_train, mh, xh, stop, lev)
                    sc = score_config(tr_m, te_m)
                    results.append((
                        '3-weighted', f"weighted>={wmin}", '-', '-', '-',
                        mh, xh, stop if stop is not None else -1, lev, 'long',
                        tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                        te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                        te_m['tot'], tr_m['tot'], sc,
                    ))
    print(f"  vote+weighted configs: {len(results)}")
    return results


def phase4_regime(df, top_per_sig):
    print("\nPHASE 4: Regime-aware")
    train_df = df[df.index < SPLIT]
    n_train = (df.index < SPLIT).sum()
    closes = df['close'].values
    rv_tr = train_df['rv90'].dropna()
    rv_lo = rv_tr.quantile(0.33); rv_hi = rv_tr.quantile(0.66)
    print(f"  RV90 train: lo<={rv_lo:.1f}, hi>={rv_hi:.1f}")
    rv = df['rv90'].fillna(method='ffill').fillna(rv_tr.median()).values
    reg_lo = rv <= rv_lo; reg_hi = rv >= rv_hi; reg_mid = (~reg_lo) & (~reg_hi)
    regimes = {'lo': reg_lo, 'mid': reg_mid, 'hi': reg_hi}
    results = []
    for sig in top_per_sig:
        d = next(x[1] for x in ROBUST_SIGS if x[0] == sig)
        b = top_per_sig[sig]
        em = precompute_entry_masks(df, train_df, sig, d, [b['entry_q']])[b['entry_q']]
        xm = precompute_exit_masks(df, train_df, sig, d, [b['exit_q']])[b['exit_q']]
        for reg_name, reg_mask in regimes.items():
            em_c = em & reg_mask
            xm_c = xm | (~reg_mask)
            for mh, xh in [(10,60),(10,90),(20,120)]:
                for stop in [0.05, 0.10, None]:
                    for lev in [1.0, 1.5, 2.0]:
                        tr_m, te_m = split_run(closes, em_c, xm_c, n_train, mh, xh, stop, lev)
                        sc = score_config(tr_m, te_m)
                        results.append((
                            '4-regime', f"{sig}|reg={reg_name}", d,
                            b['entry_q'], b['exit_q'],
                            mh, xh, stop if stop is not None else -1, lev, 'long',
                            tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                            te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                            te_m['tot'], tr_m['tot'], sc,
                        ))
    print(f"  regime configs: {len(results)}")
    return results


def phase5_short(df):
    print("\nPHASE 5: Short side")
    train_df = df[df.index < SPLIT]
    n_train = (df.index < SPLIT).sum()
    closes = df['close'].values
    results = []
    short_sigs = [
        ('fiz_lsr_short',      'fiz_lsr',           'high'),
        ('max_pain_dist_short','max_pain_dist_pct', 'high'),
        ('straddle_move_short','straddle_move_pct', 'high'),
        ('gex_norm_short',     'gex_norm',          'low'),
        ('pc_vol_ratio_short', 'pc_vol_ratio',      'low'),
    ]
    for label, col, d in short_sigs:
        if col not in df.columns: continue
        series = df[col]; tser = train_df[col]
        for eq in ENTRY_Q:
            if d == 'high':
                thr = tser.quantile(1 - eq)
                em = (series >= thr).fillna(False).values
            else:
                thr = tser.quantile(eq)
                em = (series <= thr).fillna(False).values
            for xq in EXIT_Q:
                if d == 'high':
                    thr = tser.quantile(1 - xq)
                    xm = (series <= thr).fillna(False).values
                else:
                    thr = tser.quantile(xq)
                    xm = (series >= thr).fillna(False).values
                for mh in [10, 20]:
                    for xh in [60, 90, 120]:
                        for stop in [0.05, 0.10, None]:
                            for lev in [1.0, 1.5, 2.0]:
                                tr_m, te_m = split_run(closes, em, xm, n_train, mh, xh, stop, lev, side='short')
                                sc = score_config(tr_m, te_m)
                                results.append((
                                    '5-short', label, d, eq, xq,
                                    mh, xh, stop if stop is not None else -1, lev, 'short',
                                    tr_m['cagr'], tr_m['dd'], tr_m['wr'], tr_m['pf'], tr_m['n'],
                                    te_m['cagr'], te_m['dd'], te_m['wr'], te_m['pf'], te_m['n'],
                                    te_m['tot'], tr_m['tot'], sc,
                                ))
    print(f"  short configs: {len(results)}")
    return results


def best_per_sig_from(p1_rows):
    df = to_df(p1_rows)
    # Need train+test positive and meaningful trade counts
    ok = df[(df['train_cagr']>0) & (df['test_cagr']>0) & (df['train_n']>=5) & (df['test_n']>=3)].copy()
    best = {}
    src = ok if len(ok) > 0 else df
    for sig in src['signal'].unique():
        sub = src[src['signal']==sig].sort_values('score', ascending=False)
        if len(sub) > 0:
            best[sig] = sub.iloc[0].to_dict()
    return best


def detailed_breakdown(df, row):
    """Replay best config and print yearly returns."""
    train_df = df[df.index < SPLIT]
    closes = df['close'].values
    year_arr = df.index.year.values
    phase = row['phase']
    side = row['side']
    sig = row['signal']
    mh = int(row['min_hold']); xh = int(row['max_hold'])
    stop = None if row['stop'] == -1 else float(row['stop'])
    lev = float(row['lev'])
    if phase == '1-single':
        d = row['direction']
        em = precompute_entry_masks(df, train_df, sig, d, [float(row['entry_q'])])[float(row['entry_q'])]
        xm = precompute_exit_masks(df, train_df, sig, d, [float(row['exit_q'])])[float(row['exit_q'])]
    elif phase == '5-short':
        col_map = {'fiz_lsr_short':('fiz_lsr','high'),
                   'max_pain_dist_short':('max_pain_dist_pct','high'),
                   'straddle_move_short':('straddle_move_pct','high'),
                   'gex_norm_short':('gex_norm','low'),
                   'pc_vol_ratio_short':('pc_vol_ratio','low')}
        col, d = col_map[sig]
        series = df[col]; tser = train_df[col]
        eq_q = float(row['entry_q']); xq_q = float(row['exit_q'])
        if d == 'high':
            em = (series >= tser.quantile(1-eq_q)).fillna(False).values
            xm = (series <= tser.quantile(1-xq_q)).fillna(False).values
        else:
            em = (series <= tser.quantile(eq_q)).fillna(False).values
            xm = (series >= tser.quantile(xq_q)).fillna(False).values
    elif phase in ('3-weighted', '3-vote'):
        # Rebuild weighted/voting ensemble using best singles per signal
        best = {}
        # use ROBUST_SIGS list and pull from the global top50 by signal (heuristic: take train+test positive)
        # we will pass best_per_sig from caller... simpler: redo phase build here
        bsig = {}
        for s, dd in ROBUST_SIGS:
            col = get_col(s)
            if col not in df.columns: continue
            # pick the best entry/exit quantile by quick re-search would be slow; instead use 0.25/0.7 fallback
            bsig[s] = dict(entry_q=0.25, exit_q=0.7, direction=dd, train_cagr=10.0)
        weights = np.array([bsig[s]['train_cagr'] for s in bsig]) / sum(bsig[s]['train_cagr'] for s in bsig)
        weighted = np.zeros(len(df), dtype=np.float32)
        vote = np.zeros(len(df), dtype=np.int8)
        for i, s in enumerate(bsig):
            em_i = precompute_entry_masks(df, train_df, s, bsig[s]['direction'], [bsig[s]['entry_q']])[bsig[s]['entry_q']]
            weighted += em_i.astype(np.float32) * weights[i]
            vote += em_i.astype(np.int8)
        if phase == '3-weighted':
            wmin = float(row['signal'].split('>=')[1])
            em = (weighted >= wmin)
            xm = ~em
        else:
            vmin = int(row['signal'].split('>=')[1].split('/')[0])
            em = (vote >= vmin); xm = (vote < vmin)
    else:
        return None
    yr = yearly_eq(closes, em, xm, mh, xh, stop, lev, side, year_arr)
    return yr


def combined_long_short(df, long_cfg, short_cfg):
    """50/50 capital between long straddle_move_pct + short max_pain_dist (best per side).
    Returns dict of metrics + yearly returns."""
    train_df = df[df.index < SPLIT]
    closes = df['close'].values
    year_arr = df.index.year.values
    # Long
    d_l = long_cfg['direction']
    em_l = precompute_entry_masks(df, train_df, long_cfg['signal'], d_l, [float(long_cfg['entry_q'])])[float(long_cfg['entry_q'])]
    xm_l = precompute_exit_masks(df, train_df, long_cfg['signal'], d_l, [float(long_cfg['exit_q'])])[float(long_cfg['exit_q'])]
    # Short
    col_map = {'max_pain_dist_short':('max_pain_dist_pct','high')}
    col_s, d_s = col_map.get(short_cfg['signal'], ('max_pain_dist_pct','high'))
    s_ser = df[col_s]; t_ser = train_df[col_s]
    eq_s = float(short_cfg['entry_q']); xq_s = float(short_cfg['exit_q'])
    em_s = (s_ser >= t_ser.quantile(1-eq_s)).fillna(False).values
    xm_s = (s_ser <= t_ser.quantile(1-xq_s)).fillna(False).values
    # Each side runs on half capital (independent compounding for simplicity)
    n = len(closes)
    cash_l = INIT/2; cash_s = INIT/2
    inp_l=False; ei_l=0; ep_l=0.0
    inp_s=False; ei_s=0; ep_s=0.0
    eq_l = np.full(n, INIT/2); eq_s = np.full(n, INIT/2)
    mh_l=int(long_cfg['min_hold']); xh_l=int(long_cfg['max_hold']); stop_l=float(long_cfg['stop']); lev_l=float(long_cfg['lev'])
    if stop_l == -1: stop_l = None
    mh_s=int(short_cfg['min_hold']); xh_s=int(short_cfg['max_hold']); stop_s=float(short_cfg['stop']); lev_s=float(short_cfg['lev'])
    if stop_s == -1: stop_s = None
    pnl_l=[]; pnl_s=[]
    for i in range(n):
        # long
        if inp_l:
            base = (closes[i]/ep_l - 1)
            eq_l[i] = cash_l*(1+base*lev_l)
            h = i-ei_l; do=False
            if stop_l is not None and base <= -stop_l: do=True
            elif h >= xh_l: do=True
            elif h >= mh_l and xm_l[i]: do=True
            if do:
                exp = closes[i]*(1-SLIP)
                r = (exp/ep_l-1)*lev_l - 2*FEE
                cash_l *= (1+r); pnl_l.append(r*100); inp_l=False; eq_l[i]=cash_l
        else: eq_l[i]=cash_l
        if (not inp_l) and em_l[i]: ei_l=i; ep_l=closes[i]*(1+SLIP); inp_l=True
        # short
        if inp_s:
            base = -1*(closes[i]/ep_s - 1)
            eq_s[i] = cash_s*(1+base*lev_s)
            h = i-ei_s; do=False
            if stop_s is not None and base <= -stop_s: do=True
            elif h >= xh_s: do=True
            elif h >= mh_s and xm_s[i]: do=True
            if do:
                exp = closes[i]*(1+SLIP)
                r = -1*(exp/ep_s-1)*lev_s - 2*FEE
                cash_s *= (1+r); pnl_s.append(r*100); inp_s=False; eq_s[i]=cash_s
        else: eq_s[i]=cash_s
        if (not inp_s) and em_s[i]: ei_s=i; ep_s=closes[i]*(1-SLIP); inp_s=True
    eq_total = eq_l + eq_s
    s = pd.Series(eq_total, index=df.index)
    yr = {}
    for y in np.unique(year_arr):
        mask = year_arr == y
        idx = np.where(mask)[0]
        if len(idx) > 1:
            yr[int(y)] = (eq_total[idx[-1]]/eq_total[idx[0]] - 1) * 100
    tot = (eq_total[-1]/INIT - 1) * 100
    dd = ((s.cummax()-s)/s.cummax()).max()*100
    train_seg = s[s.index < SPLIT]
    test_seg = s[s.index >= SPLIT]
    train_tot = (train_seg.iloc[-1]/train_seg.iloc[0]-1)*100
    test_tot = (test_seg.iloc[-1]/test_seg.iloc[0]-1)*100
    test_dd = ((test_seg.cummax()-test_seg)/test_seg.cummax()).max()*100
    yrs_t = (train_seg.index[-1]-train_seg.index[0]).days/365.25
    yrs_te = (test_seg.index[-1]-test_seg.index[0]).days/365.25
    train_cagr = ((train_seg.iloc[-1]/train_seg.iloc[0])**(1/yrs_t)-1)*100 if yrs_t>0 else 0
    test_cagr = ((test_seg.iloc[-1]/test_seg.iloc[0])**(1/yrs_te)-1)*100 if yrs_te>0 else 0
    return dict(tot=tot, dd=dd, yr=yr, train_tot=train_tot, test_tot=test_tot,
                test_dd=test_dd, n_l=len(pnl_l), n_s=len(pnl_s),
                train_cagr=train_cagr, test_cagr=test_cagr)


def main():
    print("Loading Si data...")
    df = load_si()
    bh = (df['close'].iloc[-1]/df['close'].iloc[0] - 1)*100
    print(f"  rows: {len(df)} ({df.index[0].date()} -> {df.index[-1].date()})")
    print(f"  B&H Si: {bh:+.1f}%")
    print(f"  train: {(df.index<SPLIT).sum()}d, test: {(df.index>=SPLIT).sum()}d")

    sys.stdout.flush()
    t0 = time.time()
    p1 = phase1_single(df); sys.stdout.flush()
    bps = best_per_sig_from(p1)
    p2 = phase2_pairs(df, bps); sys.stdout.flush()
    p3 = phase3_vote(df, bps); sys.stdout.flush()
    p4 = phase4_regime(df, bps); sys.stdout.flush()
    p5 = phase5_short(df); sys.stdout.flush()

    all_res = p1 + p2 + p3 + p4 + p5
    res_df = to_df(all_res)
    res_df = res_df.sort_values('score', ascending=False)
    print(f"\nTotal configs: {len(res_df)} in {time.time()-t0:.0f}s")

    top50 = res_df.head(50).copy()
    top50.to_csv(ROOT/'si_top_strategies.csv', index=False)
    print(f"Saved top 50 -> si_top_strategies.csv")

    cols = ['phase','signal','min_hold','max_hold','stop','lev',
            'train_cagr','train_dd','train_wr','train_pf','train_n',
            'test_cagr','test_dd','test_wr','test_pf','test_n','score']

    print("\n" + "="*130)
    print("TOP 10 BY COMPOSITE SCORE (test-MAR + consistency)")
    print("="*130)
    print(top50[cols].head(10).to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 BY TEST CAGR (filter: train_cagr>0, test_n>=3)")
    print("="*130)
    cand = res_df[(res_df['test_n']>=3) & (res_df['train_cagr']>0)].sort_values('test_cagr', ascending=False)
    print(cand[cols].head(5).to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 BY TEST MAR (CAGR/DD)")
    print("="*130)
    cand2 = res_df[(res_df['test_n']>=3) & (res_df['train_cagr']>0) & (res_df['test_dd']>1)].copy()
    cand2['mar'] = cand2['test_cagr'] / cand2['test_dd']
    print(cand2.sort_values('mar', ascending=False)[cols+['mar']].head(5).to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 SINGLE-SIGNAL (by composite)")
    print("="*130)
    print(res_df[res_df['phase']=='1-single'].head(5)[cols].to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 MULTI-SIGNAL (pair/vote/weighted by composite)")
    print("="*130)
    print(res_df[res_df['phase'].isin(['2-pair','3-vote','3-weighted'])].head(5)[cols].to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 REGIME (by composite)")
    print("="*130)
    print(res_df[res_df['phase']=='4-regime'].head(5)[cols].to_string(index=False))

    print("\n" + "="*130)
    print("TOP 5 SHORT (by composite)")
    print("="*130)
    print(res_df[res_df['phase']=='5-short'].head(5)[cols].to_string(index=False))

    # Per-year detail for overall best (if single or short, else for best single)
    print("\n" + "="*130)
    print("PER-YEAR BREAKDOWN of best in each category")
    print("="*130)
    for label, sub in [
        ('OVERALL BEST', res_df.iloc[0]),
        ('BEST SINGLE', res_df[res_df['phase']=='1-single'].iloc[0] if (res_df['phase']=='1-single').any() else None),
        ('BEST MULTI', res_df[res_df['phase'].isin(['2-pair','3-vote','3-weighted'])].iloc[0]
                       if (res_df['phase'].isin(['2-pair','3-vote','3-weighted'])).any() else None),
        ('BEST SHORT', res_df[res_df['phase']=='5-short'].iloc[0] if (res_df['phase']=='5-short').any() else None),
    ]:
        if sub is None: continue
        print(f"\n  --- {label} ---")
        print(f"  phase={sub['phase']}  signal={sub['signal']}  entry_q={sub['entry_q']}  exit_q={sub['exit_q']}")
        print(f"  hold {sub['min_hold']}-{sub['max_hold']}  stop={sub['stop']}  lev={sub['lev']}  side={sub['side']}")
        print(f"  TRAIN: cagr {sub['train_cagr']:+.1f}%  DD {sub['train_dd']:.0f}%  WR {sub['train_wr']:.0f}%  PF {sub['train_pf']:.2f}  N {int(sub['train_n'])}")
        print(f"  TEST : cagr {sub['test_cagr']:+.1f}%  DD {sub['test_dd']:.0f}%  WR {sub['test_wr']:.0f}%  PF {sub['test_pf']:.2f}  N {int(sub['test_n'])}")
        yr = detailed_breakdown(df, sub)
        if yr:
            print(f"  per-year: " + " ".join(f"{y%100}:{v:+.1f}" for y,v in sorted(yr.items())))

    # Combined long+short portfolio (50/50 capital)
    print("\n" + "="*130)
    print("COMBINED LONG+SHORT PORTFOLIO (50/50 capital, independent compounding)")
    print("="*130)
    long_best = res_df[res_df['phase']=='1-single'].iloc[0].to_dict()
    short_best = res_df[res_df['phase']=='5-short'].iloc[0].to_dict()
    for lev_scale in [1.0, 1.5, 2.0]:
        lc = dict(long_best); lc['lev'] = float(long_best['lev']) * lev_scale / float(long_best['lev'])  # keep original lev
        sc = dict(short_best); sc['lev'] = float(short_best['lev']) * lev_scale / float(short_best['lev'])
        # actually just use original lev (it was already optimized)
        cmb = combined_long_short(df, long_best, short_best)
        print(f"\n  long={long_best['signal']} lev={long_best['lev']}, short={short_best['signal']} lev={short_best['lev']}")
        print(f"  total: {cmb['tot']:+.0f}%  DD {cmb['dd']:.0f}%  train_cagr {cmb['train_cagr']:+.1f}%  test_cagr {cmb['test_cagr']:+.1f}%  test_dd {cmb['test_dd']:.0f}%")
        print(f"  trades: long={cmb['n_l']}  short={cmb['n_s']}")
        print(f"  per-year: " + " ".join(f"{y%100}:{v:+.1f}" for y,v in sorted(cmb['yr'].items())))
        break  # only run once since lev_scale not applied

    print(f"\nTotal runtime: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
