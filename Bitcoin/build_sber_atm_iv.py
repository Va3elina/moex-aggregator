"""Build daily ATM IV series for SBER from option settlement prices.

Method (robust to B/C series redenomination):
  For each (trade_date, expiry_code):
    1. Pair calls & puts by strike (using parsed SECID).
    2. Derive forward F via put-call parity regression:
         C - P = disc*(F - K)  →  linear in K, slope = -disc, intercept = disc*F
       Using near-money strikes (most reliable).
    3. ATM strike = nearest to F.
    4. Black-76 invert IV for ATM call and put; ATM IV = average.
  Then pick the expiry nearest to 30 calendar days as the daily ATM IV.

Output: sber_atm_iv_daily.csv
  date, F, T_days, atm_strike, atm_iv, series, expiry_code, n_strikes
"""
import warnings; warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from parse_sber_secid import parse_secid, resolve_year
from black76_iv import black76_iv

ROOT = Path(__file__).parent
TARGET_DTE = 30
MIN_DTE, MAX_DTE = 7, 75


def load_chain():
    # Prefer merged; fall back to part1 only
    for fname in ['sber_options_history_merged.csv', 'sber_options_history.csv']:
        p = ROOT / fname
        if p.exists():
            print(f"Loading {fname}...")
            df = pd.read_csv(p)
            break
    else:
        raise FileNotFoundError("No options history CSV found")

    df = df[df['SETTLEPRICE'].notna() & (df['SETTLEPRICE'] > 0)].copy()
    # Parse SECID
    parsed = df['SECID'].apply(parse_secid)
    df = df[parsed.notna()].copy()
    pdf = pd.DataFrame(list(parsed[parsed.notna()]))
    pdf.index = df.index
    df['strike'] = pdf['strike']
    df['otype'] = pdf['option_type']
    df['expiry_code'] = pdf['expiry_code']
    df['series'] = pdf['series']
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    return df


def load_expiry_map():
    p = ROOT / 'sber_expiry_map.csv'
    if not p.exists():
        return {}
    m = pd.read_csv(p)
    return dict(zip(m['expiry_code'], pd.to_datetime(m['expiry_date'])))


def load_rate():
    p = ROOT / 'rusfar_daily.csv'
    r = pd.read_csv(p, parse_dates=['date']).set_index('date')
    col = 'rate_pct' if 'rate_pct' in r.columns else r.columns[0]
    return (r[col] / 100.0)  # decimal


def derive_forward(strikes, call_p, put_p, disc):
    """Put-call parity regression: (C - P) = disc*F - disc*K.
    Returns F or None."""
    cp = call_p - put_p
    # Use strikes where both exist and |cp| reasonable
    valid = ~np.isnan(cp)
    if valid.sum() < 2:
        return None
    K = strikes[valid]
    Y = cp[valid]
    # Linear fit Y = a + b*K, with b = -disc, a = disc*F
    b, a = np.polyfit(K, Y, 1)
    if abs(disc) < 1e-9:
        return None
    F = a / disc
    return F


def build():
    chain = load_chain()
    exp_map = load_expiry_map()
    rate = load_rate()
    print(f"Chain rows: {len(chain)}, dates: {chain['TRADEDATE'].nunique()}, "
          f"expiry codes mapped: {len(exp_map)}")

    results = []
    skipped_no_expiry = 0

    for trade_date, day in chain.groupby('TRADEDATE'):
        ref_year = trade_date.year
        r = rate.reindex([trade_date]).ffill().iloc[0] if trade_date in rate.index else \
            rate.asof(trade_date)
        if pd.isna(r):
            r = 0.10

        # Evaluate each expiry_code present this day
        candidates = []
        for code, grp in day.groupby('expiry_code'):
            exp_date = exp_map.get(code)
            if exp_date is None:
                # fallback: approximate from month/year via parse of code
                p = parse_secid(grp['SECID'].iloc[0])
                if p is None:
                    continue
                full_year = resolve_year(p['exp_year_digit'], ref_year)
                exp_date = pd.Timestamp(datetime(full_year, p['exp_month'], 15))
                skipped_no_expiry += 1
            dte = (exp_date - trade_date).days
            if dte < MIN_DTE or dte > MAX_DTE:
                continue
            candidates.append((code, exp_date, dte, grp))

        if not candidates:
            continue
        # Pick expiry nearest target DTE
        code, exp_date, dte, grp = min(candidates, key=lambda x: abs(x[2] - TARGET_DTE))

        T = dte / 365.0
        disc = np.exp(-r * T)

        # Pivot calls/puts by strike
        calls = grp[grp['otype'] == 'C'].set_index('strike')['SETTLEPRICE']
        puts = grp[grp['otype'] == 'P'].set_index('strike')['SETTLEPRICE']
        common = sorted(set(calls.index) & set(puts.index))
        if len(common) < 2:
            continue
        strikes = np.array(common, dtype=float)
        call_p = calls.reindex(common).values.astype(float)
        put_p = puts.reindex(common).values.astype(float)

        F = derive_forward(strikes, call_p, put_p, disc)
        if F is None or F <= 0 or F < strikes.min() * 0.5 or F > strikes.max() * 2:
            continue

        # ATM strike nearest F
        atm_i = int(np.argmin(np.abs(strikes - F)))
        K_atm = strikes[atm_i]

        iv_c = black76_iv(call_p[atm_i], F, K_atm, T, r, 'call')
        iv_p = black76_iv(put_p[atm_i], F, K_atm, T, r, 'put')
        ivs = [v for v in (iv_c, iv_p) if v is not None and not np.isnan(v)]
        if not ivs:
            continue
        atm_iv = float(np.mean(ivs)) * 100  # percent

        if atm_iv < 5 or atm_iv > 200:
            continue

        results.append({
            'date': trade_date, 'F': F, 'T_days': dte, 'atm_strike': K_atm,
            'atm_iv': atm_iv, 'series': grp['series'].iloc[0],
            'expiry_code': code, 'n_strikes': len(common),
        })

    out = pd.DataFrame(results).sort_values('date')
    out.to_csv(ROOT / 'sber_atm_iv_daily.csv', index=False)
    print(f"\nATM IV built: {len(out)} days")
    if len(out):
        print(f"  Range: {out['date'].min().date()} → {out['date'].max().date()}")
        print(f"  ATM IV: min={out['atm_iv'].min():.1f}% "
              f"median={out['atm_iv'].median():.1f}% max={out['atm_iv'].max():.1f}%")
        print(f"  Series used: {out['series'].value_counts().to_dict()}")
    return out


if __name__ == "__main__":
    build()
