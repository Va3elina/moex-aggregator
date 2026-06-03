"""
black76_iv.py
=============
Black-76 pricing, Greeks, and implied-volatility inversion for MOEX-style
options on futures (e.g. SBRF / GAZR / RIM6 series).

Black-76 is the variant of Black-Scholes for European options written on a
FORWARD or FUTURES contract (Black, 1976). The underlying is the forward
price F (not the spot), and the carry term disappears from d1/d2:

    d1 = (ln(F/K) + 0.5 * sigma^2 * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    call = exp(-r*T) * (F * N(d1) - K * N(d2))
    put  = exp(-r*T) * (K * N(-d2) - F * N(-d1))

For MOEX-style options on futures this is the textbook model: the only
discounting is the risk-free factor on the premium, F replaces the
spot-with-carry.

Usage
-----
    from black76_iv import black76_price, black76_iv, black76_greeks

    p = black76_price(F=300, K=300, T=30/365, sigma=0.30, r=0.15)
    iv = black76_iv(price=p, F=300, K=300, T=30/365, r=0.15)
    g = black76_greeks(F=300, K=300, T=30/365, sigma=0.30, r=0.15)

Run as a script to execute the unit tests:
    python3 black76_iv.py
"""
from __future__ import annotations

import math
from typing import Literal

import numpy as np
from scipy.optimize import brentq
from scipy.stats import norm

OptionType = Literal["call", "put", "c", "p", "C", "P"]


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------
def _norm_type(option_type: OptionType) -> str:
    t = str(option_type).lower()
    if t in ("c", "call"):
        return "call"
    if t in ("p", "put"):
        return "put"
    raise ValueError(f"Unknown option_type={option_type!r} (use 'call' or 'put')")


def _intrinsic(F: float, K: float, r: float, T: float, option_type: str) -> float:
    """Discounted intrinsic value — the no-arbitrage lower bound for a
    European option on a future."""
    disc = math.exp(-r * T) if T > 0 else 1.0
    if option_type == "call":
        return disc * max(F - K, 0.0)
    return disc * max(K - F, 0.0)


def black76_price(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float,
    option_type: OptionType = "call",
) -> float:
    """Black-76 price for a European option on a futures contract.

    Parameters
    ----------
    F : forward / futures price
    K : strike
    T : time to expiry in YEARS (e.g. 30/365)
    sigma : volatility (e.g. 0.30 for 30 %)
    r : risk-free continuously-compounded rate (e.g. 0.15 for 15 %)
    option_type : 'call' or 'put'

    Returns
    -------
    Theoretical premium in the same currency as F/K.
    """
    ot = _norm_type(option_type)

    # Expired or degenerate inputs — fall back to (discounted) intrinsic.
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return _intrinsic(F, K, r, max(T, 0.0), ot)

    sqrtT = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    disc = math.exp(-r * T)

    if ot == "call":
        return disc * (F * norm.cdf(d1) - K * norm.cdf(d2))
    return disc * (K * norm.cdf(-d2) - F * norm.cdf(-d1))


# ---------------------------------------------------------------------------
# Greeks (analytical)
# ---------------------------------------------------------------------------
def black76_greeks(
    F: float,
    K: float,
    T: float,
    sigma: float,
    r: float,
    option_type: OptionType = "call",
) -> dict:
    """Analytical Black-76 Greeks.

    Delta is dPrice / dF (the futures delta — not the spot delta).
    Vega is per 1.0 of vol (i.e. divide by 100 for "per vol point").
    Theta is per YEAR (divide by 365 for daily decay).
    """
    ot = _norm_type(option_type)

    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        # Degenerate: return zeros except a step-function delta
        if ot == "call":
            delta = 1.0 if F > K else 0.0
        else:
            delta = -1.0 if F < K else 0.0
        return {"delta": delta, "gamma": 0.0, "vega": 0.0, "theta": 0.0}

    sqrtT = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    disc = math.exp(-r * T)
    pdf_d1 = norm.pdf(d1)

    # Gamma and vega have the same expression for calls and puts.
    gamma = disc * pdf_d1 / (F * sigma * sqrtT)
    vega = disc * F * pdf_d1 * sqrtT  # per unit of sigma

    if ot == "call":
        delta = disc * norm.cdf(d1)
        theta = (
            -disc * F * pdf_d1 * sigma / (2.0 * sqrtT)
            + r * disc * (F * norm.cdf(d1) - K * norm.cdf(d2)) * (-1.0)
        )
        # Simplify: theta = -F*phi(d1)*sigma*exp(-rT)/(2 sqrt(T))
        #                  - r*exp(-rT)*(F N(d1) - K N(d2))
        # The second term is - r * price.
        # (Both signs already baked in above.)
    else:
        delta = -disc * norm.cdf(-d1)
        theta = (
            -disc * F * pdf_d1 * sigma / (2.0 * sqrtT)
            - r * disc * (K * norm.cdf(-d2) - F * norm.cdf(-d1)) * (-1.0)
        )

    return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta}


# ---------------------------------------------------------------------------
# IV inversion
# ---------------------------------------------------------------------------
_IV_LOWER = 1e-3   # 0.1 %
_IV_UPPER = 5.0    # 500 %


def black76_iv(
    price: float,
    F: float,
    K: float,
    T: float,
    r: float,
    option_type: OptionType = "call",
    *,
    lower: float = _IV_LOWER,
    upper: float = _IV_UPPER,
    tol: float = 1e-6,
) -> float:
    """Invert Black-76 to recover implied volatility from a market price.

    Uses Brent's method on [lower, upper]. Returns NaN when the price is
    outside the no-arbitrage band, the contract has expired, or the root
    bracket is invalid (deep ITM/OTM with stale settle, illiquid wings, ...).
    """
    ot = _norm_type(option_type)

    # ----- pre-flight sanity ------------------------------------------------
    if not (np.isfinite(price) and np.isfinite(F) and np.isfinite(K)
            and np.isfinite(T) and np.isfinite(r)):
        return float("nan")
    if T <= 0 or F <= 0 or K <= 0 or price <= 0:
        return float("nan")

    disc = math.exp(-r * T)
    intrinsic = _intrinsic(F, K, r, T, ot)
    # Upper no-arbitrage bound:
    #   call <= F * exp(-rT)         (price never exceeds discounted forward)
    #   put  <= K * exp(-rT)
    upper_bound = disc * (F if ot == "call" else K)
    if price < intrinsic - 1e-8 or price > upper_bound + 1e-8:
        # Quote violates no-arb — likely stale or wrong sign. Skip.
        return float("nan")

    # ----- objective --------------------------------------------------------
    def obj(sigma: float) -> float:
        return black76_price(F, K, T, sigma, r, ot) - price

    f_lo = obj(lower)
    f_hi = obj(upper)
    if f_lo * f_hi > 0:
        # Same sign at both ends → no root inside [lower, upper].
        # Almost always means deep ITM with price ~ intrinsic (so any small
        # sigma already over/undershoots) or quote inconsistent with model.
        return float("nan")

    try:
        return brentq(obj, lower, upper, xtol=tol, maxiter=100)
    except (ValueError, RuntimeError):
        return float("nan")


# ---------------------------------------------------------------------------
# Vectorised helper (handy for chain-wide inversion)
# ---------------------------------------------------------------------------
def black76_iv_vec(prices, F, K, T, r, option_types):
    """Vectorised wrapper around black76_iv. All array-likes broadcast
    against `prices`. Returns a numpy array of IVs (NaN on failure)."""
    prices = np.asarray(prices, dtype=float)
    F = np.broadcast_to(np.asarray(F, dtype=float), prices.shape)
    K = np.broadcast_to(np.asarray(K, dtype=float), prices.shape)
    T = np.broadcast_to(np.asarray(T, dtype=float), prices.shape)
    r = np.broadcast_to(np.asarray(r, dtype=float), prices.shape)
    option_types = np.broadcast_to(np.asarray(option_types), prices.shape)

    out = np.empty(prices.shape, dtype=float)
    flat_iter = zip(
        prices.ravel(), F.ravel(), K.ravel(),
        T.ravel(), r.ravel(), option_types.ravel(),
    )
    for i, (p, f_, k_, t_, r_, ot_) in enumerate(flat_iter):
        out.flat[i] = black76_iv(p, f_, k_, t_, r_, ot_)
    return out


# ---------------------------------------------------------------------------
# Unit tests (run `python3 black76_iv.py`)
# ---------------------------------------------------------------------------
def _run_tests() -> int:
    """Self-contained sanity checks. Returns 0 on success, 1 on failure."""
    failed = 0

    def check(name, ok, detail=""):
        nonlocal failed
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}{(' — ' + detail) if detail else ''}")
        if not ok:
            failed += 1

    print("Black-76 unit tests")
    print("-------------------")

    # --- Test 1: ATM call price (F=K=100, T=0.25, sigma=0.20, r=0.05) ---
    # Hand-calc: d1 = 0.5*0.20*sqrt(0.25)= 0.05; d2 = -0.05
    # N(0.05)=0.51994, N(-0.05)=0.48006
    # call = exp(-0.0125) * (100*0.51994 - 100*0.48006)
    #      = 0.98758 * 3.9878 = 3.938  (≈ 3.99 per spec)
    p_atm = black76_price(F=100, K=100, T=0.25, sigma=0.20, r=0.05, option_type="call")
    check(
        "ATM call ~ 3.94",
        abs(p_atm - 3.94) < 0.10,
        f"got {p_atm:.4f}",
    )

    # --- Test 2: IV inversion round-trip ---
    iv_back = black76_iv(price=p_atm, F=100, K=100, T=0.25, r=0.05, option_type="call")
    check(
        "IV round-trip recovers 0.20 ± 0.001",
        abs(iv_back - 0.20) < 1e-3,
        f"got {iv_back:.6f}",
    )

    # --- Test 3: Greeks at ATM ---
    g = black76_greeks(F=100, K=100, T=0.25, sigma=0.20, r=0.05, option_type="call")
    # ATM call delta on a future: disc * N(d1) ≈ 0.98758 * 0.51994 ≈ 0.5135
    check(
        "ATM call delta ~ 0.51",
        abs(g["delta"] - 0.51) < 0.02,
        f"got {g['delta']:.4f}",
    )
    # Gamma at ATM: disc * phi(d1) / (F*sigma*sqrt(T))
    #            ≈ 0.98758 * 0.39844 / (100*0.20*0.5)
    #            ≈ 0.03934
    check(
        "ATM gamma ~ 0.0393",
        abs(g["gamma"] - 0.0393) < 1e-3,
        f"got {g['gamma']:.6f}",
    )

    # --- Test 4: Put-call parity (forward version) ---
    #   C - P = exp(-rT) * (F - K)
    F, K, T, sigma, r = 320.0, 310.0, 45 / 365, 0.35, 0.16
    c = black76_price(F, K, T, sigma, r, "call")
    p = black76_price(F, K, T, sigma, r, "put")
    parity = math.exp(-r * T) * (F - K)
    check(
        "Put-call parity holds",
        abs((c - p) - parity) < 1e-8,
        f"diff={(c - p) - parity:.2e}",
    )

    # --- Test 5: Deep OTM should still invert cleanly when within band ---
    p_otm = black76_price(F=300, K=400, T=30/365, sigma=0.45, r=0.16, option_type="call")
    iv_otm = black76_iv(p_otm, F=300, K=400, T=30/365, r=0.16, option_type="call")
    check(
        "Deep OTM IV round-trip",
        abs(iv_otm - 0.45) < 1e-3,
        f"got {iv_otm:.6f}",
    )

    # --- Test 6: Expired option → NaN, not crash ---
    iv_dead = black76_iv(price=5.0, F=100, K=100, T=0.0, r=0.05, option_type="call")
    check(
        "Expired (T=0) returns NaN",
        math.isnan(iv_dead),
        f"got {iv_dead}",
    )

    # --- Test 7: Arbitrage-violating quote → NaN ---
    iv_bad = black76_iv(price=200.0, F=100, K=100, T=0.25, r=0.05, option_type="call")
    check(
        "Price above no-arb upper bound returns NaN",
        math.isnan(iv_bad),
        f"got {iv_bad}",
    )

    # --- Test 8: Put price > 0 and inverts ---
    p_put = black76_price(F=300, K=320, T=30/365, sigma=0.40, r=0.16, option_type="put")
    iv_put = black76_iv(p_put, F=300, K=320, T=30/365, r=0.16, option_type="put")
    check(
        "Put IV round-trip",
        abs(iv_put - 0.40) < 1e-3,
        f"got {iv_put:.6f}",
    )

    print("-------------------")
    if failed == 0:
        print(f"All tests passed.")
    else:
        print(f"{failed} test(s) FAILED.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(_run_tests())
