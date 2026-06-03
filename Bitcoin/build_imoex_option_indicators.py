"""Master builder: ALL options-derived indicators for IMOEX (MIX), daily 2020-2026.

Adapted from build_sber_option_indicators.py. Differences for the index:
  - SECID prefix MX (parse_imoex_secid), ASSETCODE=MIX.
  - Underlying = MX index futures (MXM6...), quoted as IMOEX x100. Strikes also x100.
    Black-76 is scale-invariant (F and K in the same units), so atm_iv etc. come out
    natively. We rescale GEX/dollar_gamma into index-point terms.
  - IMOEX options effectively single 'B' series (no 2022 redenom), but we still keep
    settle_group separation by series+month+year+week so weeklies/quarterlies don't mix.

Output: imoex_option_indicators_daily.csv
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path
from parse_imoex_secid import parse_secid, resolve_year
from black76_iv import black76_iv, black76_greeks

try:
    from option_advanced_metrics import (variance_swap_rate, breeden_litzenberger_density,
                                          straddle_implied_move)
    HAVE_ADV = True
except Exception:
    HAVE_ADV = False

ROOT = Path(__file__).parent
TARGET_DTE = 30
MIN_DTE, MAX_DTE = 7, 75
# MX index future point value: lot = 1 contract on IMOEX x100. We express GEX in
# index points (divide x100 strikes back) so magnitudes are interpretable.
CONTRACT_MULT = 1


def load_chain():
    parts = []
    for fn in ['imoex_options_history.csv', 'imoex_options_history_part2.csv']:
        p = ROOT / fn
        if p.exists():
            parts.append(pd.read_csv(p))
    df = pd.concat(parts, ignore_index=True) if parts else pd.read_csv(ROOT / 'imoex_options_history.csv')
    df = df.drop_duplicates(subset=['TRADEDATE', 'SECID'])
    df = df[df['SETTLEPRICE'].notna() & (df['SETTLEPRICE'] > 0)].copy()
    parsed = df['SECID'].apply(parse_secid)
    df = df[parsed.notna()].copy()
    pdf = pd.DataFrame(list(parsed[parsed.notna()])); pdf.index = df.index
    df['strike'] = pdf['strike']; df['otype'] = pdf['option_type']
    df['expiry_code'] = pdf['expiry_code']; df['series'] = pdf['series']
    df['settle_group'] = (pdf['series'] + pdf['exp_month'].astype(str).str.zfill(2)
                          + pdf['exp_year_digit'].astype(str) + pdf['week'])
    df['oi'] = pd.to_numeric(df['OPENPOSITION'], errors='coerce').fillna(0)
    df['vol'] = pd.to_numeric(df['VOLUME'], errors='coerce').fillna(0)
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    return df


def load_expiry_map():
    m = pd.read_csv(ROOT / 'imoex_expiry_map.csv')
    return dict(zip(m['expiry_code'], pd.to_datetime(m['expiry_date'])))


def load_rate():
    r = pd.read_csv(ROOT / 'rusfar_daily.csv', parse_dates=['date']).set_index('date')
    col = 'rate_pct' if 'rate_pct' in r.columns else r.columns[0]
    return (r[col] / 100.0).sort_index()


def derive_forward(K, C, P, disc):
    cp = C - P
    valid = ~np.isnan(cp)
    if valid.sum() < 2:
        return None
    b, a = np.polyfit(K[valid], cp[valid], 1)
    if abs(disc) < 1e-9:
        return None
    return a / disc


def build_surface(grp, F, T, r, disc):
    calls = grp[grp['otype'] == 'C'].set_index('strike')
    puts = grp[grp['otype'] == 'P'].set_index('strike')
    common = sorted(set(calls.index) & set(puts.index))
    if len(common) < 3:
        return None
    rows = []
    def cell(frame, K, col):
        v = frame.loc[K, col]
        return float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)
    for K in common:
        c_px = cell(calls, K, 'SETTLEPRICE'); p_px = cell(puts, K, 'SETTLEPRICE')
        oi_c = cell(calls, K, 'oi'); oi_p = cell(puts, K, 'oi')
        vol_c = cell(calls, K, 'vol'); vol_p = cell(puts, K, 'vol')
        if K >= F:
            iv = black76_iv(c_px, F, K, T, r, 'call')
        else:
            iv = black76_iv(p_px, F, K, T, r, 'put')
        rows.append((K, iv, c_px, p_px, oi_c, oi_p, vol_c, vol_p))
    arr = pd.DataFrame(rows, columns=['K', 'iv', 'c', 'p', 'oi_c', 'oi_p', 'vol_c', 'vol_p'])
    arr = arr.dropna(subset=['iv'])
    arr = arr[(arr['iv'] > 0.03) & (arr['iv'] < 3.0)]
    if len(arr) < 3:
        return None
    gam, dlt = [], []
    for _, rr in arr.iterrows():
        g = black76_greeks(F, rr['K'], T, rr['iv'], r, 'call')
        gam.append(g['gamma']); dlt.append(g['delta'])
    arr['gamma'] = gam; arr['delta'] = dlt
    return arr


def interp_at_delta(arr, target_delta):
    a = arr.sort_values('delta')
    d = a['delta'].values; iv = a['iv'].values
    if target_delta < d.min() or target_delta > d.max():
        return np.nan
    return float(np.interp(target_delta, d, iv))


def max_pain(arr, F):
    Ks = arr['K'].values
    best_K, best_pain = None, np.inf
    for K0 in Ks:
        pain = np.sum(np.maximum(K0 - Ks, 0) * arr['oi_p'].values) + \
               np.sum(np.maximum(Ks - K0, 0) * arr['oi_c'].values)
        if pain < best_pain:
            best_pain = pain; best_K = K0
    return best_K


def build():
    chain = load_chain()
    exp_map = load_expiry_map()
    rate = load_rate()
    print(f"Chain: {len(chain)} rows, {chain['TRADEDATE'].nunique()} days")

    out = []
    for td, day in chain.groupby('TRADEDATE'):
        r = rate.asof(td)
        if pd.isna(r):
            r = 0.12

        tenor_ivs = {}
        groups = []
        for code, grp in day.groupby('settle_group'):
            ed = exp_map.get(grp['expiry_code'].iloc[0])
            if ed is None:
                continue
            dte = (ed - td).days
            if dte < 3 or dte > 250:
                continue
            groups.append((dte, grp))
        if not groups:
            continue

        for dte, grp in groups:
            T = dte / 365.0; disc = np.exp(-r * T)
            calls = grp[grp['otype'] == 'C'].set_index('strike')['SETTLEPRICE']
            puts = grp[grp['otype'] == 'P'].set_index('strike')['SETTLEPRICE']
            common = sorted(set(calls.index) & set(puts.index))
            if len(common) < 2:
                continue
            K = np.array(common, float)
            C = calls.reindex(common).values.astype(float)
            P = puts.reindex(common).values.astype(float)
            F = derive_forward(K, C, P, disc)
            if F is None or F <= 0:
                continue
            ai = int(np.argmin(np.abs(K - F)))
            iv_c = black76_iv(C[ai], F, K[ai], T, r, 'call')
            iv_p = black76_iv(P[ai], F, K[ai], T, r, 'put')
            ivs = [v for v in (iv_c, iv_p) if v and not np.isnan(v)]
            if ivs:
                tenor_ivs[dte] = np.mean(ivs) * 100

        def iv_at(target):
            if not tenor_ivs:
                return np.nan
            dtes = np.array(sorted(tenor_ivs.keys()))
            vals = np.array([tenor_ivs[d] for d in dtes])
            if target < dtes.min() or target > dtes.max():
                return float(vals[np.argmin(np.abs(dtes - target))])
            return float(np.interp(target, dtes, vals))

        iv7, iv30, iv90 = iv_at(7), iv_at(30), iv_at(90)

        cand = [(dte, grp) for dte, grp in groups if MIN_DTE <= dte <= MAX_DTE]
        if not cand:
            continue
        dte, grp = min(cand, key=lambda x: abs(x[0] - TARGET_DTE))
        T = dte / 365.0; disc = np.exp(-r * T)
        calls0 = grp[grp['otype'] == 'C'].set_index('strike')['SETTLEPRICE']
        puts0 = grp[grp['otype'] == 'P'].set_index('strike')['SETTLEPRICE']
        common0 = sorted(set(calls0.index) & set(puts0.index))
        if len(common0) < 3:
            continue
        Kf = np.array(common0, float)
        Cf = calls0.reindex(common0).values.astype(float)
        Pf = puts0.reindex(common0).values.astype(float)
        F = derive_forward(Kf, Cf, Pf, disc)
        if F is None or F <= 0 or F < Kf.min()*0.5 or F > Kf.max()*2:
            continue

        arr = build_surface(grp, F, T, r, disc)
        if arr is None:
            continue

        atm_i = int(np.argmin(np.abs(arr['K'].values - F)))
        atm_iv = arr['iv'].values[atm_i] * 100

        iv_25c = interp_at_delta(arr, 0.25)
        iv_25p = interp_at_delta(arr, 0.75)
        rr25 = (iv_25c - iv_25p) * 100 if not (np.isnan(iv_25c) or np.isnan(iv_25p)) else np.nan
        bf25 = ((iv_25c + iv_25p)/2 * 100 - atm_iv) if not (np.isnan(iv_25c) or np.isnan(iv_25p)) else np.nan

        a_sort = arr.sort_values('K')
        logK = np.log(a_sort['K'].values / F)
        ivv = a_sort['iv'].values
        near = np.abs(logK) < 0.15
        skew_slope = np.polyfit(logK[near], ivv[near], 1)[0] if near.sum() >= 3 else np.nan

        # GEX in index points: gamma is per unit underlying; F is in x100 units, so
        # F^2*0.01 already gives x100^2 scaling -> divide by 100^2 to bring to index pts,
        # then /1e6 for readability. Sign: dealers short calls (-), long puts (+).
        gamma = arr['gamma'].values
        gex_call = -np.sum(arr['oi_c'].values * gamma) * F**2 * 0.01
        gex_put = +np.sum(arr['oi_p'].values * gamma) * F**2 * 0.01
        gex = (gex_call + gex_put) / (100.0**2)
        gex_norm = gex / 1e6
        dollar_gamma = np.sum((arr['oi_c'].values + arr['oi_p'].values) * gamma) * F / 100.0

        flip_strike = float(arr['K'].values[np.argmax(arr['gamma'].values)]) / 100.0

        mp = max_pain(arr, F)
        mp_dist = (mp / F - 1) * 100 if mp else np.nan

        straddle_move = (Cf[atm_i] + Pf[atm_i]) / F * 100 if atm_i < len(Cf) else np.nan
        if HAVE_ADV:
            try:
                sm = straddle_implied_move(arr['c'].values[atm_i], arr['p'].values[atm_i], F, T)
                straddle_move = sm if not np.isnan(sm) else straddle_move
            except Exception:
                pass

        varswap_iv = np.nan
        if HAVE_ADV:
            try:
                varswap_iv = variance_swap_rate(F, arr['K'].values, arr['c'].values,
                                                 arr['p'].values, T, r)
            except Exception:
                pass

        rn_p_down10 = rn_p_up10 = rn_skew = np.nan
        if HAVE_ADV:
            try:
                bl = breeden_litzenberger_density(F, arr['K'].values, arr['c'].values, T, r)
                if isinstance(bl, dict):
                    rn_p_down10 = bl.get('rn_p_down_10', np.nan)
                    rn_p_up10 = bl.get('rn_p_up_10', np.nan)
                    rn_skew = bl.get('rn_skew', np.nan)
            except Exception:
                pass

        oi_c_all = day[day['otype'] == 'C']['oi'].sum()
        oi_p_all = day[day['otype'] == 'P']['oi'].sum()
        vol_c_all = day[day['otype'] == 'C']['vol'].sum()
        vol_p_all = day[day['otype'] == 'P']['vol'].sum()
        pc_oi = oi_p_all / oi_c_all if oi_c_all > 0 else np.nan
        pc_vol = vol_p_all / vol_c_all if vol_c_all > 0 else np.nan

        out.append({
            'date': td, 'F': F / 100.0, 'T_days': dte, 'series': grp['series'].iloc[0],
            'atm_iv': atm_iv, 'rr25': rr25, 'bf25': bf25, 'skew_slope': skew_slope,
            'iv_7d': iv7, 'iv_30d': iv30, 'iv_90d': iv90,
            'term_ratio_30_90': iv30/iv90 if (iv30 and iv90 and iv90>0) else np.nan,
            'gex_norm': gex_norm, 'dollar_gamma': dollar_gamma, 'flip_strike': flip_strike,
            'max_pain': (mp/100.0 if mp else np.nan), 'max_pain_dist_pct': mp_dist,
            'straddle_move_pct': straddle_move,
            'varswap_iv': varswap_iv,
            'pc_oi_ratio': pc_oi, 'pc_vol_ratio': pc_vol,
            'rn_p_down10': rn_p_down10, 'rn_p_up10': rn_p_up10, 'rn_skew': rn_skew,
        })

    res = pd.DataFrame(out).sort_values('date')
    res.to_csv(ROOT / 'imoex_option_indicators_daily.csv', index=False)
    print(f"\nBuilt {len(res)} days of full indicators")
    if len(res):
        print(f"Range: {res['date'].min().date()} -> {res['date'].max().date()}")
        for c in ['atm_iv', 'rr25', 'bf25', 'term_ratio_30_90', 'gex_norm',
                  'max_pain_dist_pct', 'straddle_move_pct', 'varswap_iv', 'pc_oi_ratio']:
            if c in res.columns:
                v = res[c].dropna()
                if len(v):
                    print(f"  {c:<20} n={len(v):>4}  median={v.median():>+8.2f}  "
                          f"p5={v.quantile(.05):>+8.2f}  p95={v.quantile(.95):>+8.2f}")
    return res


if __name__ == "__main__":
    build()
