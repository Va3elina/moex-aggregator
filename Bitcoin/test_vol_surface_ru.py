"""
MOEX FORTS vol surface prototype.

What this does
--------------
1. Pulls the live option board for an underlying (default: SBER) from MOEX ISS.
2. Reads MOEX's published clearing IV (`VOLAT` field) for every strike on every expiry.
3. Computes delta for every strike using Black-76 (options on futures) so we can
   pick the 25-delta call / 25-delta put on each smile.
4. Builds:
   - Term structure of ATM IV (7d / 30d / 90d interpolated).
   - 25-delta risk reversal RR25 = IV(25dC) - IV(25dP).
   - 25-delta butterfly BF25 = (IV(25dC) + IV(25dP))/2 - IV_ATM.
   - Full smile per expiry.
5. Computes a fallback IV via Black-76 inversion from the option `SETTLEPRICE`
   for cases when MOEX VOLAT is missing (used for historical reconstruction).

Notes about MOEX option data (verified live):
- MOEX publishes ONE `VOLAT` per strike (same for call+put: put-call parity).
- No greeks in any ISS endpoint, but we can compute them from VOLAT + spot + T.
- The /optionboard endpoint is a current snapshot only — no IV history.
- The /history endpoint goes back to ~2015 and gives OHLC + SETTLEPRICE + OI per
  contract per day, BUT NO VOLAT — so for backtest we must Black-76-invert IV
  from SETTLEPRICE ourselves (function `iv_from_price_black76` below).
- Underlying for SBER options is the SBER futures contract (SiM6/SRM6 etc.).
  The chain endpoint already exposes the underlying settlement price, so the
  Black-76 forward F is available without extra lookups.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, date

import numpy as np
import pandas as pd
import requests
from scipy.stats import norm
from scipy.optimize import brentq

ISS_BASE = "https://iss.moex.com/iss"
UNDERLYING = "SBER"   # change to GAZP / AFKS / RTS / Si etc.
RISK_FREE = 0.18      # CBR key rate ~18% in 2026, only used cosmetically (Black-76 doesn't need it for delta on futures except for discounting)


# ---------------------------------------------------------------------------
# Black-76 (options on futures) — Bachelier-style, with discount factor.
# ---------------------------------------------------------------------------

def _d1d2(F: float, K: float, T: float, sigma: float) -> tuple[float, float]:
    if T <= 0 or sigma <= 0:
        return float("nan"), float("nan")
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    d2 = d1 - v
    return d1, d2


def black76_price(F: float, K: float, T: float, sigma: float, r: float, is_call: bool) -> float:
    if T <= 0:
        return max(0.0, (F - K) if is_call else (K - F))
    d1, d2 = _d1d2(F, K, T, sigma)
    disc = math.exp(-r * T)
    if is_call:
        return disc * (F * norm.cdf(d1) - K * norm.cdf(d2))
    return disc * (K * norm.cdf(-d2) - F * norm.cdf(-d1))


def black76_delta(F: float, K: float, T: float, sigma: float, r: float, is_call: bool) -> float:
    if T <= 0 or sigma <= 0:
        return float("nan")
    d1, _ = _d1d2(F, K, T, sigma)
    disc = math.exp(-r * T)
    return disc * (norm.cdf(d1) if is_call else -norm.cdf(-d1))


def iv_from_price_black76(price: float, F: float, K: float, T: float, r: float, is_call: bool) -> float:
    """Invert Black-76 to get IV from a market price. Returns NaN if not solvable."""
    if price <= 0 or T <= 0 or F <= 0 or K <= 0:
        return float("nan")
    intrinsic = math.exp(-r * T) * max(0.0, (F - K) if is_call else (K - F))
    if price < intrinsic - 1e-6:
        return float("nan")
    f = lambda s: black76_price(F, K, T, s, r, is_call) - price
    try:
        return brentq(f, 1e-4, 5.0, maxiter=100, xtol=1e-5)
    except (ValueError, RuntimeError):
        return float("nan")


# ---------------------------------------------------------------------------
# MOEX ISS fetchers
# ---------------------------------------------------------------------------

def iss_get(path: str, **params) -> dict:
    params = {"iss.meta": "off", **params}
    r = requests.get(f"{ISS_BASE}{path}.json", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_expirations(asset: str) -> pd.DataFrame:
    js = iss_get(f"/statistics/engines/futures/markets/options/assets/{asset}")
    cols = js["expirations"]["columns"]
    rows = js["expirations"]["data"]
    df = pd.DataFrame(rows, columns=cols)
    df["expiration_date"] = pd.to_datetime(df["expiration_date"]).dt.date
    return df


def fetch_optionboard(asset: str, expiry: date | None = None) -> pd.DataFrame:
    params = {}
    if expiry is not None:
        params["expiration_date"] = expiry.isoformat()
    js = iss_get(
        f"/statistics/engines/futures/markets/options/assets/{asset}/optionboard",
        **params,
    )
    parts = []
    for side, is_call in (("call", True), ("put", False)):
        if side not in js:
            continue
        cols = js[side]["columns"]
        rows = js[side]["data"]
        df = pd.DataFrame(rows, columns=cols)
        df["is_call"] = is_call
        parts.append(df)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not out.empty:
        out["expiration_date"] = expiry
    return out


def fetch_underlying_price(asset: str) -> float:
    """Get current underlying spot (or futures settle) from the asset_volumes stat."""
    js = iss_get("/statistics/engines/futures/markets/options/assets")
    cols = js["asset_volumes"]["columns"]
    for row in js["asset_volumes"]["data"]:
        rec = dict(zip(cols, row))
        if rec["asset"] == asset:
            return float(rec["asset_last_price"])
    raise ValueError(f"asset {asset} not found in asset_volumes")


# ---------------------------------------------------------------------------
# Surface math
# ---------------------------------------------------------------------------

@dataclass
class SmileMetrics:
    expiry: date
    T_years: float
    n_strikes: int
    atm_iv: float
    rr25: float
    bf25: float
    iv25c: float
    iv25p: float
    slope: float           # d(IV)/d(logK) near ATM
    smile: pd.DataFrame    # full per-strike IV


def smile_for_expiry(board: pd.DataFrame, spot: float, today: date) -> SmileMetrics | None:
    """Compute term-structure metrics for one expiry."""
    if board.empty:
        return None
    expiry = board["expiration_date"].iloc[0]
    T = max((expiry - today).days, 0) / 365.0
    if T <= 0:
        return None

    # MOEX gives one VOLAT per strike (same for call+put). Use call rows.
    df = (
        board[board["is_call"]][["STRIKE", "VOLAT", "OPENPOSITION", "VOLTODAY"]]
        .dropna(subset=["VOLAT"])
        .copy()
    )
    df = df[df["VOLAT"] > 0]
    if len(df) < 3:
        return None
    df = df.sort_values("STRIKE").reset_index(drop=True)
    df["sigma"] = df["VOLAT"] / 100.0
    df["delta_call"] = df.apply(
        lambda r: black76_delta(spot, r["STRIKE"], T, r["sigma"], RISK_FREE, True),
        axis=1,
    )
    df["delta_put"] = df["delta_call"] - math.exp(-RISK_FREE * T)
    df["logK"] = np.log(df["STRIKE"] / spot)

    # ATM IV: interpolate to logK=0
    atm_iv = float(np.interp(0.0, df["logK"], df["sigma"]))

    # 25-delta call IV: interpolate sigma at delta_call=0.25
    sub_c = df[df["delta_call"].between(0.05, 0.95)].sort_values("delta_call")
    sub_p = df[df["delta_put"].between(-0.95, -0.05)].sort_values("delta_put")
    iv25c = (
        float(np.interp(0.25, sub_c["delta_call"], sub_c["sigma"]))
        if len(sub_c) >= 2
        else float("nan")
    )
    iv25p = (
        float(np.interp(-0.25, sub_p["delta_put"], sub_p["sigma"]))
        if len(sub_p) >= 2
        else float("nan")
    )

    rr25 = iv25c - iv25p
    bf25 = 0.5 * (iv25c + iv25p) - atm_iv

    # Local skew slope: regress sigma on logK over the ATM neighborhood
    near = df[df["logK"].abs() < 0.10]
    if len(near) >= 3:
        slope = float(np.polyfit(near["logK"], near["sigma"], 1)[0])
    else:
        slope = float("nan")

    return SmileMetrics(
        expiry=expiry,
        T_years=T,
        n_strikes=len(df),
        atm_iv=atm_iv,
        rr25=rr25,
        bf25=bf25,
        iv25c=iv25c,
        iv25p=iv25p,
        slope=slope,
        smile=df[["STRIKE", "logK", "sigma", "delta_call", "delta_put", "OPENPOSITION", "VOLTODAY"]],
    )


def term_structure(metrics: list[SmileMetrics]) -> pd.DataFrame:
    rows = [
        dict(
            expiry=m.expiry,
            T_years=round(m.T_years, 4),
            T_days=round(m.T_years * 365, 1),
            n_strikes=m.n_strikes,
            atm_iv=round(m.atm_iv * 100, 2),
            iv25c=round(m.iv25c * 100, 2),
            iv25p=round(m.iv25p * 100, 2),
            rr25_pp=round(m.rr25 * 100, 2),
            bf25_pp=round(m.bf25 * 100, 2),
            slope=round(m.slope, 3),
        )
        for m in metrics
        if m is not None
    ]
    return pd.DataFrame(rows).sort_values("T_years").reset_index(drop=True)


def interpolate_atm_at_horizon(ts: pd.DataFrame, days: int) -> float:
    """Linear interp ATM IV to a target horizon in days."""
    if ts.empty:
        return float("nan")
    return float(np.interp(days, ts["T_days"], ts["atm_iv"]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(asset: str = UNDERLYING) -> None:
    today = datetime.now().date()
    print(f"=== Vol surface for {asset} @ {today} ===")

    spot = fetch_underlying_price(asset)
    print(f"Underlying spot/last: {spot}")

    exps = fetch_expirations(asset)
    # filter only future expiries & cap at first 6 to keep it fast
    exps = exps[pd.to_datetime(exps["expiration_date"]) > pd.Timestamp(today)].head(6)
    print(f"Expirations to process: {list(exps['expiration_date'])}")

    metrics: list[SmileMetrics] = []
    for _, row in exps.iterrows():
        board = fetch_optionboard(asset, row["expiration_date"])
        m = smile_for_expiry(board, spot, today)
        if m is not None:
            metrics.append(m)

    ts = term_structure(metrics)
    print("\n--- Term structure (ATM IV, RR25, BF25 per expiry) ---")
    print(ts.to_string(index=False))

    iv7  = interpolate_atm_at_horizon(ts, 7)
    iv30 = interpolate_atm_at_horizon(ts, 30)
    iv90 = interpolate_atm_at_horizon(ts, 90)
    print("\n--- Interpolated ATM IV term points ---")
    print(f"  IV(7d)  = {iv7:.2f}%")
    print(f"  IV(30d) = {iv30:.2f}%")
    print(f"  IV(90d) = {iv90:.2f}%")
    if iv30 and iv90:
        print(f"  IV30/IV90 = {iv30/iv90:.3f}  ({'BACKWARD' if iv30>iv90 else 'CONTANGO'})")

    # Show smile of the nearest reasonable expiry (>=14d)
    target = next((m for m in metrics if m.T_years * 365 >= 14), metrics[0] if metrics else None)
    if target is not None:
        print(f"\n--- Smile @ {target.expiry} (T={target.T_years*365:.0f}d) ---")
        sm = target.smile.copy()
        sm["sigma_pct"] = (sm["sigma"] * 100).round(2)
        print(
            sm[["STRIKE", "logK", "sigma_pct", "delta_call", "delta_put", "OPENPOSITION"]]
            .round({"logK": 4, "delta_call": 3, "delta_put": 3})
            .to_string(index=False)
        )


if __name__ == "__main__":
    import sys
    arg = sys.argv[1] if len(sys.argv) > 1 else UNDERLYING
    main(arg)
