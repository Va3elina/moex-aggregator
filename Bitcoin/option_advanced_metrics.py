"""
option_advanced_metrics.py
==========================
Model-free metrics extracted from an options chain on a FUTURES underlying
(MOEX FORTS, Black-76 world). All functions are pure: they operate on numpy
arrays / scalars, read no files, and degrade to NaN on bad input.

Two families of metrics are provided:

  1. Variance-swap / model-free implied vol      (VIX-style, whole-smile)
  2. Breeden-Litzenberger risk-neutral density   (from d2C/dK2)

plus a handful of cheap-but-useful single-number diagnostics:
  3. straddle_implied_move        (ATM straddle as % move)
  4. risk_reversal_25d            (25-delta skew)
  5. butterfly_25d                (25-delta convexity / smile curvature)

Conventions
-----------
F  : forward / futures price (already PCP-implied upstream)
K  : strike(s)
C  : call price(s)            (settlement / mid)
P  : put price(s)
T  : time to expiry in YEARS
r  : risk-free rate (continuously compounded, e.g. 0.16 = 16 %)

Discounting is exp(-r*T) on the premium, exactly as Black-76. Because these
are options on a future, the model-free variance formula uses the SAME VIX
machinery: the forward replaces (spot * e^{rT}) so the only thing the e^{rT}
factor does is undo the discounting embedded in the settlement premia.

Run as a script for self-contained unit tests on a synthetic smile:
    python3 option_advanced_metrics.py
"""
from __future__ import annotations

import numpy as np

NAN = float("nan")


# ---------------------------------------------------------------------------
# small shared helpers
# ---------------------------------------------------------------------------
def _clean_sort(K, *arrays):
    """Coerce to float arrays, drop rows with non-finite strike or any
    non-finite payload, and sort by strike ascending. Returns
    (K_sorted, [arr_sorted, ...]). On failure returns (None, []).
    """
    K = np.asarray(K, dtype=float)
    arrays = [np.asarray(a, dtype=float) for a in arrays]

    n = K.shape[0]
    if n == 0 or any(a.shape[0] != n for a in arrays):
        return None, []

    good = np.isfinite(K)
    for a in arrays:
        good &= np.isfinite(a)
    if good.sum() < 1:
        return None, []

    K = K[good]
    arrays = [a[good] for a in arrays]

    order = np.argsort(K)
    K = K[order]
    arrays = [a[order] for a in arrays]

    # collapse duplicate strikes (keep first) — duplicates break dK weights
    uniq = np.concatenate(([True], np.diff(K) > 0))
    K = K[uniq]
    arrays = [a[uniq] for a in arrays]
    return K, arrays


def _dk_weights(K):
    """Trapezoidal-style strike spacing weights dK_i used by VIX:
       dK_i = (K_{i+1} - K_{i-1}) / 2 for interior points,
       one-sided at the two endpoints. Works on a non-uniform grid.
    """
    K = np.asarray(K, dtype=float)
    n = K.shape[0]
    dk = np.empty(n, dtype=float)
    if n == 1:
        dk[0] = 0.0
        return dk
    dk[1:-1] = (K[2:] - K[:-2]) / 2.0
    dk[0] = K[1] - K[0]
    dk[-1] = K[-1] - K[-2]
    return dk


# ---------------------------------------------------------------------------
# 1. Model-free implied variance (VIX-style) on a futures underlying
# ---------------------------------------------------------------------------
def variance_swap_rate(F, K, C, P, T, r):
    """Model-free implied volatility (%, annualised) from the whole smile.

    Replicates the CBOE VIX integral discretised over the listed strikes,
    using OTM options only: puts for K < F, calls for K >= F.

        sigma2 = (2 * e^{rT} / T) * sum_i [ dK_i / K_i^2 * Q(K_i) ]
                 - (1 / T) * (F / K0 - 1)^2

    where Q(K) is the OTM premium at strike K, dK_i the strike spacing
    weight, and K0 the largest listed strike <= F.

    Returns sqrt(sigma2) * 100. NaN if < 3 usable strikes or sigma2 < 0.
    """
    if not (np.isfinite(F) and np.isfinite(T) and np.isfinite(r)) or F <= 0 or T <= 0:
        return NAN

    K, (C, P) = _clean_sort(K, C, P)
    if K is None or K.shape[0] < 3:
        return NAN

    # K0 = nearest listed strike at or below the forward.
    below = K[K <= F]
    if below.size == 0:
        # forward sits below the whole grid — fall back to lowest strike
        K0 = K[0]
    else:
        K0 = below.max()

    # OTM selection: puts strictly below K0, calls at/above K0.
    # (At K0 itself we take the call leg; the (F/K0 - 1)^2 term corrects the
    #  ATM in-the-moneyness, matching CBOE's convention.)
    Q = np.where(K < K0, P, C)

    # Guard: prices must be finite & non-negative to enter the sum.
    valid = np.isfinite(Q) & (Q >= 0)
    if valid.sum() < 3:
        return NAN
    Kv = K[valid]
    Qv = Q[valid]

    dk = _dk_weights(Kv)
    disc = np.exp(r * T)

    integral = np.sum(dk / (Kv * Kv) * Qv)
    sigma2 = (2.0 * disc / T) * integral - (1.0 / T) * (F / K0 - 1.0) ** 2

    if not np.isfinite(sigma2) or sigma2 < 0:
        return NAN
    return float(np.sqrt(sigma2) * 100.0)


# ---------------------------------------------------------------------------
# 2. Breeden-Litzenberger risk-neutral density
# ---------------------------------------------------------------------------
def breeden_litzenberger_density(F, K, C, T, r):
    """Risk-neutral PDF of the terminal futures price from call curvature.

        pdf(K) = e^{rT} * d2C/dK2

    Second derivative is taken with a non-uniform central finite difference,
    so the density lives on the interior strikes (n-2 points). It is then
    clipped at zero (curvature noise can go slightly negative) and rescaled
    so that sum(pdf * dK) == 1.

    Returns a dict:
        strikes_mid : interior strikes (the density support)
        density     : normalised pdf on strikes_mid
        rn_mean     : sum K * pdf * dK
        rn_std      : sqrt(sum (K-mean)^2 * pdf * dK)
        rn_skew     : sum ((K-mean)/std)^3 * pdf * dK
        rn_p_down_10: prob(K < 0.9 F)
        rn_p_up_10  : prob(K > 1.1 F)

    All scalar stats are NaN (and arrays empty) if < 3 strikes or the density
    fails to normalise (non-positive total mass).
    """
    empty = {
        "strikes_mid": np.array([], dtype=float),
        "density": np.array([], dtype=float),
        "rn_mean": NAN,
        "rn_std": NAN,
        "rn_skew": NAN,
        "rn_p_down_10": NAN,
        "rn_p_up_10": NAN,
    }

    if not (np.isfinite(F) and np.isfinite(T) and np.isfinite(r)) or F <= 0 or T <= 0:
        return empty

    K, (C,) = _clean_sort(K, C)
    if K is None or K.shape[0] < 3:
        return empty

    # Non-uniform second derivative on interior nodes i = 1..n-2:
    #   f''_i = 2/(h_l+h_r) * [ f_{i+1}/h_r - f_i*(1/h_l + 1/h_r) + f_{i-1}/h_l ]
    # with h_l = K_i - K_{i-1}, h_r = K_{i+1} - K_i.
    Kl, Ki, Kr = K[:-2], K[1:-1], K[2:]
    Cl, Ci, Cr = C[:-2], C[1:-1], C[2:]
    hl = Ki - Kl
    hr = Kr - Ki
    d2C = (2.0 / (hl + hr)) * (Cr / hr - Ci * (1.0 / hl + 1.0 / hr) + Cl / hl)

    disc = np.exp(r * T)
    pdf = disc * d2C

    # Curvature can dip slightly negative from settlement noise / arb
    # violations; clip to a valid (non-negative) density.
    pdf = np.where(np.isfinite(pdf), pdf, 0.0)
    pdf = np.clip(pdf, 0.0, None)

    strikes_mid = Ki
    dk = _dk_weights(strikes_mid)

    mass = np.sum(pdf * dk)
    if not np.isfinite(mass) or mass <= 0:
        return empty

    pdf = pdf / mass  # normalise so sum(pdf*dk) == 1

    rn_mean = float(np.sum(strikes_mid * pdf * dk))
    var = float(np.sum((strikes_mid - rn_mean) ** 2 * pdf * dk))
    rn_std = float(np.sqrt(var)) if var > 0 else NAN
    if np.isfinite(rn_std) and rn_std > 0:
        rn_skew = float(np.sum(((strikes_mid - rn_mean) / rn_std) ** 3 * pdf * dk))
    else:
        rn_skew = NAN

    rn_p_down_10 = float(np.sum((pdf * dk)[strikes_mid < 0.9 * F]))
    rn_p_up_10 = float(np.sum((pdf * dk)[strikes_mid > 1.1 * F]))

    return {
        "strikes_mid": strikes_mid,
        "density": pdf,
        "rn_mean": rn_mean,
        "rn_std": rn_std,
        "rn_skew": rn_skew,
        "rn_p_down_10": rn_p_down_10,
        "rn_p_up_10": rn_p_up_10,
    }


# ---------------------------------------------------------------------------
# 3. Straddle implied move
# ---------------------------------------------------------------------------
def straddle_implied_move(atm_call, atm_put, F, T, T_days=None):
    """ATM straddle premium as an implied % move to expiry.

        implied_move_pct = (atm_call + atm_put) / F * 100

    If T_days (calendar days to expiry) is given, also annualise:
        implied_move_annualized = implied_move_pct * sqrt(365 / T_days)

    Returns dict {implied_move_pct, implied_move_annualized}. Fields are NaN
    on bad input. (T is accepted for API symmetry; only F and the premia are
    needed for the point estimate.)
    """
    out = {"implied_move_pct": NAN, "implied_move_annualized": NAN}
    if not (np.isfinite(atm_call) and np.isfinite(atm_put) and np.isfinite(F)):
        return out
    if F <= 0 or atm_call < 0 or atm_put < 0:
        return out

    pct = (atm_call + atm_put) / F * 100.0
    out["implied_move_pct"] = float(pct)

    if T_days is not None and np.isfinite(T_days) and T_days > 0:
        out["implied_move_annualized"] = float(pct * np.sqrt(365.0 / T_days))
    return out


# ---------------------------------------------------------------------------
# 4 & 5. Delta-based skew / convexity (25-delta)
# ---------------------------------------------------------------------------
def _iv_at_call_delta(strikes, ivs, deltas, target_delta):
    """Interpolate IV at a target CALL delta.

    Call delta is monotone DECREASING in strike (high strike -> low delta),
    so we sort by delta ascending and interpolate IV vs delta. Returns NaN
    if the target lies outside the available delta range (no extrapolation).
    """
    s, (ivs, deltas) = _clean_sort(strikes, ivs, deltas)
    if s is None or s.shape[0] < 2:
        return NAN

    # sort by delta ascending for np.interp (needs increasing x)
    order = np.argsort(deltas)
    d = deltas[order]
    v = ivs[order]

    # collapse duplicate deltas
    uniq = np.concatenate(([True], np.diff(d) > 0))
    d = d[uniq]
    v = v[uniq]
    if d.shape[0] < 2:
        return NAN

    if target_delta < d.min() or target_delta > d.max():
        return NAN  # outside coverage — do not extrapolate
    return float(np.interp(target_delta, d, v))


def risk_reversal_25d(strikes, ivs, deltas):
    """25-delta risk reversal: IV(25d call) - IV(25d put), in vol points.

    The 25-delta put corresponds to a 0.75 call delta (put delta = call
    delta - 1 = -0.25). Negative RR25 => puts richer than calls (downside
    skew), the usual equity-index signature.

    Returns NaN if the delta grid does not bracket both 0.25 and 0.75.
    """
    iv_call_25 = _iv_at_call_delta(strikes, ivs, deltas, 0.25)
    iv_put_25 = _iv_at_call_delta(strikes, ivs, deltas, 0.75)
    if not (np.isfinite(iv_call_25) and np.isfinite(iv_put_25)):
        return NAN
    # IVs may be supplied in fractions (0.25) or % (25); return in the same
    # units as the input, scaled to "points" (multiply by 100 if fractional).
    scale = 100.0 if max(abs(iv_call_25), abs(iv_put_25)) < 5.0 else 1.0
    return float((iv_call_25 - iv_put_25) * scale)


def butterfly_25d(strikes, ivs, deltas, atm_iv):
    """25-delta butterfly: mean of the 25d wings minus ATM IV.

        BF25 = (IV(25d call) + IV(25d put)) / 2 - atm_iv

    Positive BF25 => the wings are richer than ATM (smile convexity).
    Returns NaN if either 25d wing is unavailable or atm_iv is invalid.
    """
    if not np.isfinite(atm_iv):
        return NAN
    iv_call_25 = _iv_at_call_delta(strikes, ivs, deltas, 0.25)
    iv_put_25 = _iv_at_call_delta(strikes, ivs, deltas, 0.75)
    if not (np.isfinite(iv_call_25) and np.isfinite(iv_put_25)):
        return NAN

    wings = (iv_call_25 + iv_put_25) / 2.0
    # Match units: if wings look fractional but atm_iv looks like %, scale.
    wings_frac = wings < 5.0
    atm_frac = atm_iv < 5.0
    if wings_frac and not atm_frac:
        wings *= 100.0
    elif (not wings_frac) and atm_frac:
        atm_iv *= 100.0
    return float(wings - atm_iv)


# ---------------------------------------------------------------------------
# Unit tests on a synthetic smile (run: python3 option_advanced_metrics.py)
# ---------------------------------------------------------------------------
def _run_tests() -> int:
    from black76_iv import black76_price, black76_greeks

    failed = 0

    def check(name, ok, detail=""):
        nonlocal failed
        tag = "PASS" if ok else "FAIL"
        print(f"  [{tag}] {name}{(' — ' + detail) if detail else ''}")
        if not ok:
            failed += 1

    print("option_advanced_metrics unit tests")
    print("----------------------------------")

    # ---- synthetic market ------------------------------------------------
    F = 300.0
    r = 0.16
    T_days = 30.0
    T = T_days / 365.0
    K = np.arange(240.0, 360.0 + 1e-9, 5.0)

    # Smile: base 25 %, linear put skew (richer downside), mild smile curvature.
    base = 0.25
    moneyness = (K - F) / F                      # negative below F
    skew_slope = -0.40                           # IV rises as moneyness falls
    convex = 0.30                                 # smile curvature
    IV = base + skew_slope * moneyness + convex * moneyness ** 2

    # Black-76 prices for the whole chain.
    C = np.array([black76_price(F, k, T, s, r, "call") for k, s in zip(K, IV)])
    P = np.array([black76_price(F, k, T, s, r, "put") for k, s in zip(K, IV)])
    deltas = np.array([black76_greeks(F, k, T, s, r, "call")["delta"]
                       for k, s in zip(K, IV)])

    atm_idx = int(np.argmin(np.abs(K - F)))
    atm_iv = IV[atm_idx]
    print(f"  setup: F={F}, T={T_days:.0f}d, ATM IV={atm_iv*100:.2f}%, "
          f"{K.size} strikes [{K[0]:.0f}..{K[-1]:.0f}]")

    # ---- 1. variance swap rate ------------------------------------------
    mfiv = variance_swap_rate(F, K, C, P, T, r)
    print(f"  variance_swap_rate (model-free IV) = {mfiv:.3f}%")
    check("MFIV finite", np.isfinite(mfiv), f"{mfiv}")
    check("MFIV in 24-29% band (ATM + convexity premium)",
          24.0 <= mfiv <= 29.0, f"{mfiv:.3f}%")
    check("MFIV >= ATM IV (smile convexity lifts it)",
          mfiv >= atm_iv * 100.0 - 0.5, f"mfiv={mfiv:.2f} atm={atm_iv*100:.2f}")

    # ---- 2. Breeden-Litzenberger density --------------------------------
    bl = breeden_litzenberger_density(F, K, C, T, r)
    sm = bl["strikes_mid"]
    dens = bl["density"]
    integral = float(np.sum(dens * _dk_weights(sm)))
    print(f"  BL density: integral={integral:.4f}, rn_mean={bl['rn_mean']:.3f}, "
          f"rn_std={bl['rn_std']:.3f}, rn_skew={bl['rn_skew']:+.4f}")
    print(f"             p_down_10={bl['rn_p_down_10']:.4f}, "
          f"p_up_10={bl['rn_p_up_10']:.4f}")
    check("BL density integrates to ~1.0", abs(integral - 1.0) < 1e-6,
          f"{integral:.6f}")
    check("BL rn_mean ~ F (within 1%)", abs(bl["rn_mean"] - F) < 0.01 * F,
          f"{bl['rn_mean']:.3f} vs {F}")
    check("BL density non-negative", np.all(dens >= 0), "")
    check("BL rn_std > 0", bl["rn_std"] > 0, f"{bl['rn_std']:.3f}")
    # Put skew => fatter left tail => negative risk-neutral skew expected.
    check("BL rn_skew negative (downside skew)", bl["rn_skew"] < 0,
          f"{bl['rn_skew']:+.4f}")
    check("BL p_down_10 > p_up_10 (left-skewed)",
          bl["rn_p_down_10"] > bl["rn_p_up_10"],
          f"{bl['rn_p_down_10']:.4f} vs {bl['rn_p_up_10']:.4f}")

    # cross-check: rn_std vs ATM lognormal sigma*F*sqrt(T)
    approx_std = atm_iv * F * np.sqrt(T)
    print(f"             rn_std={bl['rn_std']:.3f} vs lognormal "
          f"approx F*sigma*sqrt(T)={approx_std:.3f}")

    # ---- 3. straddle implied move ---------------------------------------
    sm_move = straddle_implied_move(C[atm_idx], P[atm_idx], F, T, T_days=T_days)
    print(f"  straddle_implied_move: pct={sm_move['implied_move_pct']:.3f}%, "
          f"annualized={sm_move['implied_move_annualized']:.3f}%")
    check("straddle move pct finite & positive",
          sm_move["implied_move_pct"] > 0, f"{sm_move['implied_move_pct']:.3f}")
    # Sanity: ATM straddle ~ 0.8 * F * sigma * sqrt(T) -> as % of F.
    expect_move = 0.8 * atm_iv * np.sqrt(T) * 100.0
    check("straddle move ~ 0.8*sigma*sqrt(T) (rough)",
          abs(sm_move["implied_move_pct"] - expect_move) < 1.5,
          f"got {sm_move['implied_move_pct']:.2f} expect ~{expect_move:.2f}")

    # ---- 4. risk reversal 25d -------------------------------------------
    rr = risk_reversal_25d(K, IV, deltas)
    print(f"  risk_reversal_25d = {rr:+.3f} vol pts")
    check("RR25 finite", np.isfinite(rr), f"{rr}")
    check("RR25 negative (put skew)", rr < 0, f"{rr:+.3f}")

    # ---- 5. butterfly 25d -----------------------------------------------
    bf = butterfly_25d(K, IV, deltas, atm_iv)
    print(f"  butterfly_25d = {bf:+.3f} vol pts")
    check("BF25 finite", np.isfinite(bf), f"{bf}")
    check("BF25 positive (wings richer than ATM)", bf > 0, f"{bf:+.3f}")

    # ---- edge cases ------------------------------------------------------
    print("  edge cases:")
    check("variance_swap < 3 strikes -> NaN",
          np.isnan(variance_swap_rate(F, K[:2], C[:2], P[:2], T, r)))
    check("variance_swap T<=0 -> NaN",
          np.isnan(variance_swap_rate(F, K, C, P, 0.0, r)))
    bl_short = breeden_litzenberger_density(F, K[:2], C[:2], T, r)
    check("BL < 3 strikes -> NaN mean & empty density",
          np.isnan(bl_short["rn_mean"]) and bl_short["density"].size == 0)
    check("straddle bad F -> NaN",
          np.isnan(straddle_implied_move(C[atm_idx], P[atm_idx], 0.0, T)["implied_move_pct"]))
    # delta grid that doesn't reach 0.25 (clip to near-ATM strikes only)
    mid = slice(atm_idx - 1, atm_idx + 2)
    check("RR25 narrow delta range -> NaN",
          np.isnan(risk_reversal_25d(K[mid], IV[mid], deltas[mid])))
    check("BF25 NaN atm_iv -> NaN", np.isnan(butterfly_25d(K, IV, deltas, NAN)))

    print("----------------------------------")
    if failed == 0:
        print("All tests passed.")
    else:
        print(f"{failed} test(s) FAILED.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(_run_tests())
