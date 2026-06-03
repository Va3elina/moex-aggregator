"""
build_iv_term_structure.py
==========================
Build a daily multi-tenor ATM implied-volatility term structure for SBER.

For each trading day and each target tenor in {7, 14, 30, 60, 90} days:
    1) Pick the two listed expiries that bracket the target tenor
       (if only one side exists, use the nearest single expiry — no extrapolation).
    2) Compute the ATM IV on each bracketing expiry via the same procedure
       used in build_atm_iv_history.py (n-strikes around F, sqrt-mean of
       call+put per strike, linear interp in K to F).
    3) Linearly interpolate IV in TIME (in calendar days) between the two
       bracketing expiries to land on the target tenor.

Output: sber_iv_term_structure.csv
    date, iv_7d, iv_14d, iv_30d, iv_60d, iv_90d   (all in % vol)

Note on the time interpolation:
    For term-structure work the textbook trick is to interpolate VARIANCE
    in time (sigma^2 * T), which is roughly forward-vol consistent. We do
    that — variance is what's additive across non-overlapping time windows,
    not vol itself. The CSV still reports the resulting sigma in % per
    annum.

Run (after data agent has staged the files):
    python3 build_iv_term_structure.py \
        --options sber_options_history.parquet \
        --futures sber_futures_history.parquet \
        --rate    rusfar_daily.csv \
        --out     sber_iv_term_structure.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from black76_iv import black76_iv
from build_atm_iv_history import (
    _IV_MIN,
    _IV_MAX,
    _atm_iv_for_slice,
    load_futures,
    load_options,
    load_rate,
)

DEFAULT_TENORS = (7, 14, 30, 60, 90)


# ---------------------------------------------------------------------------
# Per-day work
# ---------------------------------------------------------------------------
def _iv_per_expiry(
    day_chain: pd.DataFrame,
    F: float,
    trade_date: pd.Timestamp,
    r: float,
    n_strikes: int,
) -> pd.DataFrame:
    """Compute ATM IV for every listed expiry on this date.

    Returns a DataFrame with one row per expiry: T_days, atm_iv, n_used.
    """
    rows = []
    for expiry, slice_ in day_chain.groupby("expiry"):
        T_days = (expiry - trade_date).days
        if T_days <= 0:
            continue
        T_years = T_days / 365.0
        atm_iv, _atm_k, n_used = _atm_iv_for_slice(
            slice_, F=F, T_years=T_years, r=r, n_strikes=n_strikes,
        )
        if np.isnan(atm_iv) or not (_IV_MIN <= atm_iv <= _IV_MAX):
            continue
        rows.append({"T_days": T_days, "iv": atm_iv, "n_used": n_used})
    return pd.DataFrame(rows).sort_values("T_days").reset_index(drop=True)


def _interp_iv_in_variance(curve: pd.DataFrame, target_days: int) -> float:
    """Interpolate sigma at `target_days` using variance-time interpolation.

    `curve` must have columns T_days, iv (sigma), sorted by T_days.
    Returns NaN if target lies outside the curve and no extrapolation.
    """
    if curve.empty:
        return float("nan")

    Ts = curve["T_days"].to_numpy(dtype=float)
    ivs = curve["iv"].to_numpy(dtype=float)
    var_t = (ivs ** 2) * Ts  # total variance to expiry

    if target_days <= Ts.min():
        # Don't extrapolate the very front: use the nearest if it's within 5 days,
        # else NaN. (e.g. weekly options often missing → 7d tenor genuinely absent.)
        return float(ivs[0]) if (Ts.min() - target_days) <= 5 else float("nan")
    if target_days >= Ts.max():
        # Same on the back: only nearest if within ~10 days.
        return float(ivs[-1]) if (target_days - Ts.max()) <= 10 else float("nan")

    interp_var_t = np.interp(target_days, Ts, var_t)
    sigma = np.sqrt(interp_var_t / target_days)
    return float(sigma)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def build_term_structure(
    options: pd.DataFrame,
    futures: pd.DataFrame,
    rates: pd.DataFrame,
    tenors: tuple[int, ...] = DEFAULT_TENORS,
    n_strikes: int = 5,
) -> pd.DataFrame:
    rates_indexed = rates.set_index("date")["rate"].sort_index()
    futures_indexed = futures.set_index("date")["F"].sort_index()

    out_rows = []
    for trade_date, day_chain in options.groupby("date"):
        if trade_date not in futures_indexed.index:
            continue
        F = float(futures_indexed.loc[trade_date])

        if trade_date in rates_indexed.index:
            r = float(rates_indexed.loc[trade_date])
        else:
            prior = rates_indexed.loc[:trade_date]
            if prior.empty:
                continue
            r = float(prior.iloc[-1])

        curve = _iv_per_expiry(day_chain, F=F, trade_date=trade_date, r=r, n_strikes=n_strikes)
        if curve.empty:
            continue

        row = {"date": trade_date.date().isoformat()}
        for tenor in tenors:
            sigma = _interp_iv_in_variance(curve, target_days=tenor)
            row[f"iv_{tenor}d"] = (
                round(sigma * 100.0, 4) if np.isfinite(sigma) else np.nan
            )
        out_rows.append(row)

    out = pd.DataFrame(out_rows).sort_values("date").reset_index(drop=True)
    return out


def _print_stats(df: pd.DataFrame, tenors: tuple[int, ...]) -> None:
    print("\n=== IV term-structure summary ===")
    print(f"  rows: {len(df)}")
    if df.empty:
        print("================================\n")
        return
    print(f"  date range: {df['date'].iloc[0]} → {df['date'].iloc[-1]}")
    for tenor in tenors:
        col = f"iv_{tenor}d"
        s = df[col].dropna()
        if s.empty:
            print(f"  {col:<8} : no observations")
        else:
            print(
                f"  {col:<8} : n={len(s):>5}  "
                f"mean={s.mean():6.2f}%  std={s.std():5.2f}%  "
                f"min={s.min():6.2f}%  max={s.max():6.2f}%"
            )
    # Useful derived ratio for quick eyeball
    if "iv_30d" in df and "iv_90d" in df:
        ratio = (df["iv_30d"] / df["iv_90d"]).dropna()
        if not ratio.empty:
            print(
                f"  iv_30d/iv_90d ratio: mean={ratio.mean():.3f}  "
                f"std={ratio.std():.3f}  "
                f"(<1 = contango, >1 = backwardation)"
            )
    print("================================\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--options", default="sber_options_history.parquet", type=Path)
    ap.add_argument("--futures", default="sber_futures_history.parquet", type=Path)
    ap.add_argument("--rate",    default="rusfar_daily.csv", type=Path)
    ap.add_argument("--out",     default="sber_iv_term_structure.csv", type=Path)
    ap.add_argument(
        "--tenors",
        default=",".join(str(t) for t in DEFAULT_TENORS),
        help="Comma-separated list of target tenors in calendar days (default 7,14,30,60,90)",
    )
    ap.add_argument("--n-strikes", type=int, default=5)
    args = ap.parse_args()

    tenors = tuple(int(x) for x in args.tenors.split(","))

    print(f"Loading options : {args.options}")
    opts = load_options(args.options)
    print(f"  {len(opts):,} rows, {opts['date'].nunique()} trading days, "
          f"{opts['expiry'].nunique()} expiries")

    print(f"Loading futures : {args.futures}")
    fut = load_futures(args.futures)
    print(f"  {len(fut):,} daily settles")

    print(f"Loading rates   : {args.rate}")
    rates = load_rate(args.rate)
    print(f"  {len(rates):,} rate observations")

    out = build_term_structure(opts, fut, rates, tenors=tenors, n_strikes=args.n_strikes)
    out.to_csv(args.out, index=False)
    print(f"\nWrote {len(out):,} rows → {args.out}")
    _print_stats(out, tenors)
    return 0


if __name__ == "__main__":
    sys.exit(main())
