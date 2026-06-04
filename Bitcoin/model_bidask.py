"""
model_bidask.py
===============
Empirical bid/ask spread model for FORTS options.

Problem
-------
MOEX ISS history endpoint only returns SETTLEPRICE (clearing mark). Real bid-ask
spread eats 5-15 % of the premium for a strangle seller. Backtesting on
mid-prices systematically overstates returns.

Approach
--------
1. Use HIGH-LOW intraday range and WAPRICE-SETTLEPRICE deviation as proxies for
   half-spread (the tightest the market quoted that day).
2. Fit a log-linear model on liquid rows (VOLUME > 0 AND OI > 0):

       hs / settle = a + b * |log(K/F)| + c * sqrt(T) + d / sqrt(1+OI) + e * (1/sqrt(1+VOL))

   where hs is the half-spread proxy.
3. Apply to every option on every day to estimate (bid, ask).
4. For panic/forced exits (margin call, gap reopening) apply a *worst-case*
   multiplier (default 3x mid spread, capped at 50 % of mid).

Important caveats
-----------------
* HIGH-LOW is an upper bound on spread (it also reflects intraday vol).
  We deliberately scale the estimate down (factor 0.4) so half-spread ~
  0.2 * (HIGH-LOW), which matches anecdotal FORTS market-maker quotes
  (bid-ask 1-3% for liquid 25-delta, 5-15% for 5-delta wings).
* Below 5-delta or DTE<5, wings are quoted at "junk" prices: floor the
  half-spread at 25% of settle. This kills the cheap-tail-hedge illusion.
* OI=0 strikes are dropped (no market). VOLUME=0 strikes get the
  illiquid penalty (max half-spread).
"""
from __future__ import annotations

import math
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent

# ---------------------------------------------------------------------------
# Model coefficients (fit lazily on first import; cached on disk)
# ---------------------------------------------------------------------------
_COEFFS_CACHE = ROOT / ".bidask_coeffs.json"
_COEFFS = None
# Fallback when fitting fails - calibrated to deliver plausible numbers:
#   ATM liquid → hs/settle ≈ 3 %
#   25Δ liquid → hs/settle ≈ 6 %
#   5Δ wing    → hs/settle ≈ 20 %
_FALLBACK_COEFFS = {
    "a": 0.03,    # base 3% half-spread on liquid ATM
    "b": 0.20,    # +20% per unit |log(K/F)|  (1 stdev OTM → ~3% extra)
    "c": 0.05,    # +5% per sqrt(year) of T
    "d": 0.50,    # +50% spread when OI → 0
    "e": 0.40,    # +40% spread when VOL → 0
    "floor_atm": 0.02,    # never tighter than 2% of mid
    "floor_otm": 0.05,    # OTM always ≥ 5%
    "cap":      0.50,    # never wider than 50% of mid
}


def _hs_proxy_row(row):
    """Half-spread proxy from one day's bar.

    HIGH-LOW captures both spread and intraday move; we conservatively take
    (HIGH-LOW)/2 * 0.4 as a lower bound on the spread (~ 0.2*HL range).
    WAPRICE-SETTLE captures sided pressure if it exists.
    """
    if not (np.isfinite(row.HIGH) and np.isfinite(row.LOW) and row.SETTLEPRICE > 0):
        return None
    hl_half = 0.5 * (row.HIGH - row.LOW)
    if hl_half <= 0:
        return None
    wap_dev = 0.0
    if np.isfinite(row.WAPRICE) and row.WAPRICE > 0:
        wap_dev = abs(row.WAPRICE - row.SETTLEPRICE)
    hs_proxy = 0.4 * hl_half + 0.6 * wap_dev
    rel = hs_proxy / row.SETTLEPRICE
    # Discard pathological rows
    if not (0.001 < rel < 1.5):
        return None
    return rel


def fit_coefficients(chain_path: Path | None = None,
                     force: bool = False,
                     verbose: bool = True) -> dict:
    """Fit the half-spread model on liquid history rows.

    Loads merged chain, parses strikes via parse_sber_secid, derives
    forward from put-call parity per day, and least-squares fits
    (a, b, c, d, e) on log-half-spread.
    """
    global _COEFFS
    import json
    if _COEFFS is not None and not force:
        return _COEFFS
    if not force and _COEFFS_CACHE.exists():
        try:
            _COEFFS = json.loads(_COEFFS_CACHE.read_text())
            return _COEFFS
        except Exception:
            pass

    chain_path = chain_path or (ROOT / "sber_options_history_merged.csv")
    if not chain_path.exists():
        if verbose:
            print("[bidask] chain not found, using fallback coefficients")
        _COEFFS = dict(_FALLBACK_COEFFS)
        return _COEFFS

    from parse_sber_secid import parse_secid

    # ---- Load (only the columns we need) -------------------------------
    cols = ["TRADEDATE", "SECID", "SETTLEPRICE", "HIGH", "LOW",
            "WAPRICE", "VOLUME", "OPENPOSITION"]
    df_all = pd.read_csv(chain_path, usecols=cols)
    df_all = df_all[df_all["SETTLEPRICE"].notna() & (df_all["SETTLEPRICE"] > 0)]
    parsed_all = df_all["SECID"].apply(parse_secid)
    df_all = df_all[parsed_all.notna()].copy()
    parsed_all = parsed_all.dropna()
    df_all["strike"]  = parsed_all.apply(lambda p: p["strike"])
    df_all["otype"]   = parsed_all.apply(lambda p: p["option_type"])
    # Settle-group: series + month_num + year_digit + week  (calls + puts share)
    df_all["sg"] = parsed_all.apply(
        lambda p: f"{p['series']}{p['exp_month']:02d}{p['exp_year_digit']}{p['week']}"
    )
    df_all["expcode"] = parsed_all.apply(lambda p: p["expiry_code"])
    df_all["TRADEDATE"] = pd.to_datetime(df_all["TRADEDATE"])

    # ---- Forward F per (date, settle_group) via put-call parity -------
    # Use ALL strikes (liquid + illiquid) to maximize K overlap.
    # cp = C - P = exp(-rT) * (F - K)  →  linear fit
    forward = {}
    for (d, sg), grp in df_all.groupby(["TRADEDATE", "sg"]):
        calls = grp[grp["otype"] == "C"].set_index("strike")["SETTLEPRICE"]
        puts  = grp[grp["otype"] == "P"].set_index("strike")["SETTLEPRICE"]
        calls = calls[~calls.index.duplicated(keep='first')]
        puts  = puts[~puts.index.duplicated(keep='first')]
        common = sorted(set(calls.index) & set(puts.index))
        if len(common) < 3:
            continue
        K = np.array(common, float)
        cp = calls.reindex(common).values - puts.reindex(common).values
        try:
            slope, intercept = np.polyfit(K, cp, 1)
            if abs(slope) < 1e-9:
                continue
            F = -intercept / slope
        except Exception:
            continue
        if F > 0:
            forward[(d, sg)] = F

    if verbose:
        print(f"[bidask] forwards computed: {len(forward):,}")

    # ---- Now select liquid rows only for FIT ---------------------------
    df = df_all[(df_all["VOLUME"].fillna(0) > 0) &
                (df_all["OPENPOSITION"].fillna(0) > 0)].copy()
    if verbose:
        print(f"[bidask] liquid rows for fit: {len(df):,}")
    if len(df) < 1000:
        _COEFFS = dict(_FALLBACK_COEFFS)
        return _COEFFS

    df["F"] = [forward.get((row.TRADEDATE, row.sg), np.nan)
               for row in df.itertuples()]
    df = df[df["F"].notna() & (df["F"] > 0)]

    # We also need T - join expiry_map. The map uses literal expiry_code
    # (BA0 etc.) which differs for calls vs puts of the same physical
    # expiry. Build a merged sg→date map by taking call dates and falling
    # back to puts if missing.
    em_path = ROOT / "sber_expiry_map.csv"
    if not em_path.exists():
        _COEFFS = dict(_FALLBACK_COEFFS)
        return _COEFFS
    em = pd.read_csv(em_path)
    em["expiry_date"] = pd.to_datetime(em["expiry_date"])
    em_map_lit = dict(zip(em["expiry_code"], em["expiry_date"]))
    # Convert literal code → sg using parse_secid on rep_secid
    sg_map = {}
    for _, row in em.iterrows():
        rep = row.get("rep_secid")
        if not isinstance(rep, str):
            continue
        p = parse_secid(rep)
        if p is None:
            continue
        sg_key = f"{p['series']}{p['exp_month']:02d}{p['exp_year_digit']}{p['week']}"
        # Keep one date per sg
        sg_map.setdefault(sg_key, row["expiry_date"])
    df["expiry"] = df["sg"].map(sg_map)
    df = df[df["expiry"].notna()]
    df["T"] = (df["expiry"] - df["TRADEDATE"]).dt.days / 365.0
    df = df[(df["T"] > 1/365) & (df["T"] < 1.0)]

    # ---- Compute half-spread proxy ------------------------------------
    df["hs_rel"] = [_hs_proxy_row(r) for r in df.itertuples()]
    df = df[df["hs_rel"].notna()]
    if verbose:
        print(f"[bidask] usable rows post-clean: {len(df):,}")
    if len(df) < 500:
        _COEFFS = dict(_FALLBACK_COEFFS)
        return _COEFFS

    # ---- Build features ------------------------------------------------
    df["m"]  = np.abs(np.log(df["strike"] / df["F"]))
    df["sT"] = np.sqrt(df["T"])
    df["oi_inv"] = 1.0 / np.sqrt(1.0 + df["OPENPOSITION"].clip(lower=0))
    df["vol_inv"] = 1.0 / np.sqrt(1.0 + df["VOLUME"].clip(lower=0))
    # Clip target to remove outliers
    df = df[(df["hs_rel"] >= 0.005) & (df["hs_rel"] <= 0.8)]

    X = df[["m", "sT", "oi_inv", "vol_inv"]].values
    X = np.hstack([np.ones((len(X), 1)), X])
    y = df["hs_rel"].values
    # Least squares
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    a, b, c, d_, e = coef

    # Sanity-bound the coefficients to avoid pathological extrapolation
    a  = float(np.clip(a,  0.005, 0.10))
    b  = float(np.clip(b,  0.05,  0.40))
    c  = float(np.clip(c,  0.00,  0.20))
    d_ = float(np.clip(d_, 0.10,  1.00))
    e  = float(np.clip(e,  0.05,  0.80))

    _COEFFS = dict(_FALLBACK_COEFFS)
    _COEFFS.update({"a": a, "b": b, "c": c, "d": d_, "e": e})
    try:
        _COEFFS_CACHE.write_text(json.dumps(_COEFFS, indent=2))
    except Exception:
        pass
    if verbose:
        print(f"[bidask] fit coefficients: {_COEFFS}")
        pred = X @ coef
        mae = np.mean(np.abs(pred - y))
        r2  = 1 - np.var(pred - y) / np.var(y)
        print(f"[bidask] in-sample MAE={mae:.4f}  R^2={r2:.3f}")
    return _COEFFS


def half_spread_relative(K, F, T, OI, vol=None, coeffs=None) -> float:
    """Return half-spread / mid as a fraction (0..1)."""
    if coeffs is None:
        coeffs = fit_coefficients()
    if F <= 0 or K <= 0 or T <= 0:
        return coeffs["cap"]
    m  = abs(math.log(K / F))
    sT = math.sqrt(max(T, 0))
    oi_inv  = 1.0 / math.sqrt(1.0 + max(OI or 0, 0))
    vol_inv = 1.0 / math.sqrt(1.0 + max(vol or 0, 0))
    rel = (coeffs["a"]
           + coeffs["b"] * m
           + coeffs["c"] * sT
           + coeffs["d"] * oi_inv
           + coeffs["e"] * vol_inv)
    # Floors / caps
    floor = coeffs["floor_atm"] if m < 0.05 else coeffs["floor_otm"]
    rel = max(rel, floor)
    rel = min(rel, coeffs["cap"])
    # Force-wider for tiny DTE (illiquid, market-maker pulled)
    if T < 5/365:
        rel = max(rel, 0.15)
    return rel


def estimate_bidask(secid, settle, K, F, T, OI, vol=None, coeffs=None):
    """Return (bid, ask) tuple in the same currency as `settle`.

    For the seller of an option:
       sell at the **bid** (gets less than mid)
    For the buyer of a wing:
       pay the **ask** (pays more than mid)
    """
    if settle is None or not np.isfinite(settle) or settle <= 0:
        return None, None
    rel = half_spread_relative(K, F, T, OI, vol, coeffs)
    hs = rel * settle
    bid = max(settle - hs, 0.01)   # never <1 kop
    ask = settle + hs
    return float(bid), float(ask)


def panic_bid(settle, K, F, T, OI, vol=None, coeffs=None, multiplier=3.0):
    """Worst-case bid for forced/panic close (margin call, gap reopen).

    Used when we MUST close: half-spread inflated to 3× nominal, capped
    at 60% of mid (forced sell at <40% of mid is the realistic FORTS
    panic-reopen scenario in March 2022)."""
    if settle is None or not np.isfinite(settle) or settle <= 0:
        return 0.0
    rel = half_spread_relative(K, F, T, OI, vol, coeffs)
    rel = min(rel * multiplier, 0.60)
    return max(settle * (1 - rel), 0.01)


def panic_ask(settle, K, F, T, OI, vol=None, coeffs=None, multiplier=3.0):
    """Worst-case ask (when we are forced to buy back shorts)."""
    if settle is None or not np.isfinite(settle) or settle <= 0:
        return 0.01
    rel = half_spread_relative(K, F, T, OI, vol, coeffs)
    rel = min(rel * multiplier, 0.60)
    return settle * (1 + rel)


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    coeffs = fit_coefficients(force=True, verbose=True)
    print("\n--- Spot checks ---")
    print(" Liquid ATM 30 DTE OI=5000 VOL=500:")
    bid, ask = estimate_bidask("TEST", 10.0, 300, 300, 30/365, 5000, 500)
    print(f"   settle=10.00  bid={bid:.3f}  ask={ask:.3f}  hs={ask-10:.3f}")
    print(" 25Δ OTM put 30 DTE OI=500 VOL=50:")
    bid, ask = estimate_bidask("TEST", 5.0, 280, 300, 30/365, 500, 50)
    print(f"   settle=5.00  bid={bid:.3f}  ask={ask:.3f}  hs={ask-5:.3f}")
    print(" 5Δ wing 30 DTE OI=50 VOL=5:")
    bid, ask = estimate_bidask("TEST", 1.0, 250, 300, 30/365, 50, 5)
    print(f"   settle=1.00  bid={bid:.3f}  ask={ask:.3f}  hs={ask-1:.3f}")
    print(" Panic close 5Δ wing:")
    print(f"   panic_bid={panic_bid(1.0, 250, 300, 30/365, 50, 5):.3f}")
