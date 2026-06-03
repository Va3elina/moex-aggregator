"""
build_atm_iv_history.py
=======================
Build a daily ~30-day ATM implied-volatility series for SBER by inverting
Black-76 from MOEX option SETTLEPRICE.

Input files (produced by the data-fetch agent):
    sber_options_history.parquet  (or .csv)
        Required columns (case-insensitive will be normalised):
            date        — trade date (yyyy-mm-dd)
            expiry      — option expiration date
            strike      — strike price
            option_type — 'C'/'P' (or 'call'/'put')
            settle      — settlement price (SETTLEPRICE)
        Optional:
            volume, oi, secid
    sber_futures_history.parquet  (or .csv)
        Required: date, settle  (futures settlement / последняя расчётная цена)
        Optional: expiry (нужен только если хотим брать ровный фронт-контракт
        под опционную серию; иначе берём один daily settle)
    rusfar_daily.csv
        Required: date, rate  (RUSFAR в долях, например 0.16 = 16 %).
        Допускается также колонка `rusfar` или `value`.

Output:
    sber_atm_iv_daily.csv
        date, F, T_days, atm_strike, atm_iv_pct, n_strikes_used, expiry_used

Methodology:
    1) For each trading day pick the nearest expiry in [20, 40] calendar days
       (target ~30d ATM IV).
    2) Take the 5 strikes nearest to F (futures settle).
    3) Invert IV per (strike, type) via Black-76 with r = RUSFAR (cont. comp.).
    4) Linearly interpolate sigma(K) at K = F.
    5) When both put and call IVs exist at the chosen ATM strike, take the
       sqrt-mean ( sqrt((iv_c^2+iv_p^2)/2) ) — more stable than either leg.
    6) Drop days with iv < 5 % or > 200 % (most likely broken settles).

Run:
    python3 build_atm_iv_history.py \
        --options sber_options_history.parquet \
        --futures sber_futures_history.parquet \
        --rate    rusfar_daily.csv \
        --out     sber_atm_iv_daily.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from black76_iv import black76_iv


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------
def _read_table(path: Path) -> pd.DataFrame:
    """Read parquet if available, fall back to CSV (with parquet path → csv)."""
    if not path.exists():
        # Allow user to point at sber_options_history.parquet and silently
        # pick up sber_options_history.csv if parquet is missing.
        alt = path.with_suffix(".csv") if path.suffix == ".parquet" else path.with_suffix(".parquet")
        if alt.exists():
            path = alt
        else:
            raise FileNotFoundError(f"Neither {path} nor {alt} exists")

    if path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as e:
            raise ImportError(
                "pyarrow/fastparquet not installed; either `pip install pyarrow` "
                f"or provide a CSV instead of {path}"
            ) from e
    return pd.read_csv(path)


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _coerce_option_type(s: pd.Series) -> pd.Series:
    """Normalise to 'call' / 'put'."""
    m = {"c": "call", "call": "call", "p": "put", "put": "put",
         "C": "call", "P": "put"}
    return s.astype(str).str.lower().map(m)


def load_options(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_table(path))
    # Common column aliases
    rename = {}
    for src, dst in [
        ("optiontype", "option_type"),
        ("type", "option_type"),
        ("settleprice", "settle"),
        ("settle_price", "settle"),
        ("price_settle", "settle"),
        ("expiration", "expiry"),
        ("expirationdate", "expiry"),
        ("lasttradedate", "expiry"),
        ("trade_date", "date"),
        ("tradedate", "date"),
    ]:
        if src in df.columns and dst not in df.columns:
            rename[src] = dst
    df = df.rename(columns=rename)

    need = {"date", "expiry", "strike", "option_type", "settle"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"Options file missing columns: {sorted(missing)}; have {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.normalize()
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["settle"] = pd.to_numeric(df["settle"], errors="coerce")
    df["option_type"] = _coerce_option_type(df["option_type"])
    df = df.dropna(subset=["date", "expiry", "strike", "settle", "option_type"])
    df = df[df["settle"] > 0]
    return df


def load_futures(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_table(path))
    rename = {}
    for src, dst in [
        ("settleprice", "settle"),
        ("settle_price", "settle"),
        ("close", "settle"),
        ("last", "settle"),
        ("trade_date", "date"),
        ("tradedate", "date"),
    ]:
        if src in df.columns and dst not in df.columns:
            rename[src] = dst
    df = df.rename(columns=rename)

    if "date" not in df.columns or "settle" not in df.columns:
        raise ValueError(f"Futures file needs date,settle; have {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["settle"] = pd.to_numeric(df["settle"], errors="coerce")
    df = df.dropna(subset=["date", "settle"])
    # If multiple futures per day (different expiries), keep the front month
    # (= soonest expiry that is still > date). If no expiry col, average.
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"]).dt.normalize()
        df = df[df["expiry"] > df["date"]]
        df = df.sort_values(["date", "expiry"]).groupby("date", as_index=False).first()
    else:
        df = df.groupby("date", as_index=False)["settle"].mean()
    return df[["date", "settle"]].rename(columns={"settle": "F"})


def load_rate(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_table(path))
    rename = {}
    for src in ("rusfar", "value", "rate_pct", "rate_percent"):
        if src in df.columns and "rate" not in df.columns:
            rename[src] = "rate"
    df = df.rename(columns=rename)
    if "date" not in df.columns or "rate" not in df.columns:
        raise ValueError(f"Rate file needs date,rate; have {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    # If rate looks like % (e.g. 16.0 instead of 0.16) — auto-rescale.
    if df["rate"].dropna().median() > 1.0:
        df["rate"] = df["rate"] / 100.0
    df = df.dropna(subset=["date", "rate"]).sort_values("date")
    return df[["date", "rate"]]


# ---------------------------------------------------------------------------
# Core: ATM IV for a single (date, expiry) slice
# ---------------------------------------------------------------------------
def _atm_iv_for_slice(
    chain: pd.DataFrame,
    F: float,
    T_years: float,
    r: float,
    n_strikes: int = 5,
) -> tuple[float, float, int]:
    """Return (atm_iv, atm_strike, n_used) for one expiry slice.

    `chain` already filtered to one (date, expiry). NaN if not computable.
    """
    if chain.empty or F <= 0 or T_years <= 0:
        return (np.nan, np.nan, 0)

    # Invert IV strike-by-strike, side-by-side.
    chain = chain.copy()
    chain["iv"] = chain.apply(
        lambda row: black76_iv(
            price=row["settle"],
            F=F,
            K=row["strike"],
            T=T_years,
            r=r,
            option_type=row["option_type"],
        ),
        axis=1,
    )

    # Keep only the strikes nearest to F.
    uniq_strikes = np.array(sorted(chain["strike"].unique()))
    order = np.argsort(np.abs(uniq_strikes - F))
    keep = set(uniq_strikes[order[:n_strikes]])
    chain = chain[chain["strike"].isin(keep)]
    chain = chain[chain["iv"].between(_IV_MIN, _IV_MAX, inclusive="both")]
    if chain.empty:
        return (np.nan, np.nan, 0)

    # Collapse to one IV per strike: prefer the put+call sqrt-mean.
    by_strike = (
        chain.pivot_table(index="strike", columns="option_type", values="iv", aggfunc="mean")
        .reset_index()
    )
    if "call" not in by_strike.columns:
        by_strike["call"] = np.nan
    if "put" not in by_strike.columns:
        by_strike["put"] = np.nan
    both = by_strike[["call", "put"]].notna().all(axis=1)
    by_strike["iv_combo"] = np.where(
        both,
        np.sqrt((by_strike["call"].fillna(0) ** 2 + by_strike["put"].fillna(0) ** 2) / 2.0),
        by_strike[["call", "put"]].mean(axis=1, skipna=True),
    )
    by_strike = by_strike.dropna(subset=["iv_combo"]).sort_values("strike")
    if by_strike.empty:
        return (np.nan, np.nan, 0)

    strikes = by_strike["strike"].to_numpy(dtype=float)
    ivs = by_strike["iv_combo"].to_numpy(dtype=float)

    # Linear interpolation in strike → sigma at K = F.
    if F <= strikes.min() or F >= strikes.max():
        # F outside available wing: pick the IV at the nearest strike (no extrap).
        idx = int(np.argmin(np.abs(strikes - F)))
        atm_iv = float(ivs[idx])
        atm_strike = float(strikes[idx])
    else:
        atm_iv = float(np.interp(F, strikes, ivs))
        # The "ATM strike" reported is whichever listed strike is closest to F.
        atm_strike = float(strikes[int(np.argmin(np.abs(strikes - F)))])

    return (atm_iv, atm_strike, int(len(by_strike)))


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
_IV_MIN = 0.05   # 5 %  — anything below this on SBER is almost certainly broken
_IV_MAX = 2.00   # 200 %


def build_atm_iv(
    options: pd.DataFrame,
    futures: pd.DataFrame,
    rates: pd.DataFrame,
    *,
    tenor_min_days: int = 20,
    tenor_max_days: int = 40,
    n_strikes: int = 5,
) -> pd.DataFrame:
    """Loop over every trade date, compute one ATM IV per day."""
    rates_indexed = rates.set_index("date")["rate"].sort_index()
    futures_indexed = futures.set_index("date")["F"].sort_index()

    out_rows = []
    for trade_date, day_chain in options.groupby("date"):
        if trade_date not in futures_indexed.index:
            continue
        F = float(futures_indexed.loc[trade_date])

        # Risk-free: prefer exact day, otherwise last known rate <= trade_date.
        if trade_date in rates_indexed.index:
            r = float(rates_indexed.loc[trade_date])
        else:
            prior = rates_indexed.loc[:trade_date]
            if prior.empty:
                continue
            r = float(prior.iloc[-1])

        # Find candidate expiries within the tenor window.
        days_to_expiry = (day_chain["expiry"] - trade_date).dt.days
        mask = (days_to_expiry >= tenor_min_days) & (days_to_expiry <= tenor_max_days)
        cands = day_chain[mask]
        if cands.empty:
            # Fall back: pick the expiry closest to 30d outside the window
            # (helps days right before/after series rollover).
            absd = (days_to_expiry - 30).abs()
            if absd.empty:
                continue
            chosen_expiry = day_chain.loc[absd.idxmin(), "expiry"]
            cands = day_chain[day_chain["expiry"] == chosen_expiry]
        else:
            # Pick the single expiry closest to 30 days inside the window.
            absd = ((cands["expiry"] - trade_date).dt.days - 30).abs()
            chosen_expiry = cands.loc[absd.idxmin(), "expiry"]
            cands = cands[cands["expiry"] == chosen_expiry]

        T_days = (chosen_expiry - trade_date).days
        T_years = T_days / 365.0

        atm_iv, atm_strike, n_used = _atm_iv_for_slice(
            cands, F=F, T_years=T_years, r=r, n_strikes=n_strikes,
        )
        if np.isnan(atm_iv):
            continue

        out_rows.append({
            "date": trade_date.date().isoformat(),
            "F": round(F, 4),
            "T_days": int(T_days),
            "atm_strike": round(atm_strike, 4),
            "atm_iv_pct": round(atm_iv * 100.0, 4),
            "n_strikes_used": n_used,
            "expiry_used": chosen_expiry.date().isoformat(),
        })

    out = pd.DataFrame(out_rows).sort_values("date").reset_index(drop=True)

    # Quality filter (already inside _atm_iv_for_slice, belt-and-braces).
    if not out.empty:
        out = out[
            (out["atm_iv_pct"] >= _IV_MIN * 100)
            & (out["atm_iv_pct"] <= _IV_MAX * 100)
        ].reset_index(drop=True)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _print_stats(df: pd.DataFrame, total_days: int) -> None:
    print("\n=== ATM IV build summary ===")
    print(f"  trading days in chain : {total_days}")
    print(f"  days with valid ATM IV: {len(df)}  ({len(df)/max(total_days,1):.1%})")
    if not df.empty:
        print(f"  mean ATM IV           : {df['atm_iv_pct'].mean():.2f} %")
        print(f"  std  ATM IV           : {df['atm_iv_pct'].std():.2f} %")
        print(f"  min / max             : {df['atm_iv_pct'].min():.2f} % / {df['atm_iv_pct'].max():.2f} %")
        print(f"  median T_days         : {df['T_days'].median():.0f}")
        print(f"  date range            : {df['date'].iloc[0]} → {df['date'].iloc[-1]}")
    print("============================\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--options", default="sber_options_history.parquet", type=Path)
    ap.add_argument("--futures", default="sber_futures_history.parquet", type=Path)
    ap.add_argument("--rate",    default="rusfar_daily.csv", type=Path)
    ap.add_argument("--out",     default="sber_atm_iv_daily.csv", type=Path)
    ap.add_argument("--tenor-min", type=int, default=20)
    ap.add_argument("--tenor-max", type=int, default=40)
    ap.add_argument("--n-strikes", type=int, default=5)
    args = ap.parse_args()

    print(f"Loading options : {args.options}")
    opts = load_options(args.options)
    print(f"  {len(opts):,} rows, {opts['date'].nunique()} trading days, "
          f"{opts['expiry'].nunique()} expiries, "
          f"date range {opts['date'].min().date()} → {opts['date'].max().date()}")

    print(f"Loading futures : {args.futures}")
    fut = load_futures(args.futures)
    print(f"  {len(fut):,} daily settles")

    print(f"Loading rates   : {args.rate}")
    rates = load_rate(args.rate)
    print(f"  {len(rates):,} rate observations")

    out = build_atm_iv(
        opts, fut, rates,
        tenor_min_days=args.tenor_min,
        tenor_max_days=args.tenor_max,
        n_strikes=args.n_strikes,
    )

    out.to_csv(args.out, index=False)
    print(f"\nWrote {len(out):,} rows → {args.out}")
    _print_stats(out, total_days=opts["date"].nunique())
    return 0


if __name__ == "__main__":
    sys.exit(main())
