"""
forts_margin.py
===============
Simplified FORTS / MOEX portfolio initial-margin (ГО) calculator.

What MOEX actually does
-----------------------
The real FORTS margin engine is a SPAN-style scenario simulator (described in
the public document «Методика определения размера обеспечения», fs.moex.com,
e.g. https://fs.moex.com/files/4723).

Per series:
* 16 scenarios — combinations of underlying-price shocks (±a*range,
  ±0.5*a*range, ±0.25*a*range, 0) and volatility shocks (±vol-shift).
* Each scenario re-prices every contract by Black-76 (options) or by
  full bump (futures), nets longs and shorts in the same series,
  and takes the worst portfolio P/L.
* Adds inter-month / inter-commodity credits (we IGNORE these; they
  only reduce margin for diversified books).
* Margin = max(scenario-loss, regulatory floor).

Our simplified model
--------------------
We use a 7×3 grid (vs MOEX's 7×2 + flat) which is conservative:

    spot shocks: −15 %, −10 %, −5 %, 0, +5 %, +10 %, +15 %
    vol  shocks: −30 %, 0, +30 % (relative to per-strike IV)

For each (Δspot, Δvol):
    F' = F * (1 + Δspot)
    iv' = iv * (1 + Δvol), floored at 5 %
    Price every leg with Black-76 at (F', K, T, iv', r)
    P/L = sum_over_legs(qty * (price_now - price_after_shock))

Margin = max(0, -min_pnl) * contract_multiplier

For SBER stock options each contract = 100 shares (FORTS convention),
so PnL per option = (premium_change) * 100.

Caveats
-------
* No inter-month credits — overstates margin for calendar spreads.
* No daily VAR scaling — we use fixed shocks that approximate the
  long-term realized range (-15..+15 %). For high-vol regimes MOEX
  would widen these; for low-vol it would tighten. In 2022 it could
  easily be ±40 %. Our estimate is therefore CONSERVATIVE in calm
  times and OPTIMISTIC in stress (margin will be larger in 2022 than
  we model).
* IV is assumed flat across strikes per leg (smile-flat shock).
* Regulatory floor ignored — small short books may under-margin slightly.
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np
from black76_iv import black76_price

# SBER stock-options contract size (FORTS convention)
SBER_CONTRACT_MULT = 100  # 100 shares per option contract

# Default shock grid
DEFAULT_SPOT_SHOCKS = (-0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15)
DEFAULT_VOL_SHOCKS  = (-0.30, 0.0, 0.30)


def portfolio_margin(
    positions: Iterable[dict],
    F: float,
    T: float,
    r: float,
    contract_mult: int = SBER_CONTRACT_MULT,
    spot_shocks: tuple = DEFAULT_SPOT_SHOCKS,
    vol_shocks:  tuple = DEFAULT_VOL_SHOCKS,
    min_iv: float = 0.05,
) -> dict:
    """Compute portfolio initial margin (ГО) for an options book.

    Parameters
    ----------
    positions : iterable of dicts with keys
                {'K': strike, 'otype': 'C'|'P', 'qty': int (+long / -short),
                 'iv': implied vol at that strike (decimal, e.g. 0.30),
                 'open_px': mid premium at open (RUB)}
    F : current forward price of the underlying
    T : time to expiry (years)
    r : risk-free rate (decimal)
    contract_mult : shares per contract (default 100 for SBER stock options)

    Returns
    -------
    dict with keys:
      margin       — required initial margin (RUB)
      worst_loss   — most negative scenario P/L (RUB, negative number)
      worst_scenario — (spot_shock, vol_shock)
      scenarios    — list of all (spot, vol, pnl) tuples
    """
    if T <= 0:
        return {"margin": 0.0, "worst_loss": 0.0,
                "worst_scenario": (0, 0), "scenarios": []}

    # Pre-compute current values
    cur_vals = []
    for pos in positions:
        iv = max(pos.get("iv", 0.25), min_iv)
        p = black76_price(F, pos["K"], T, iv, r,
                          "call" if pos["otype"] == "C" else "put")
        cur_vals.append(p)

    scenarios = []
    for ds in spot_shocks:
        for dv in vol_shocks:
            F_s = F * (1.0 + ds)
            if F_s <= 0:
                continue
            pnl_per_share = 0.0
            for pos, cur_p in zip(positions, cur_vals):
                iv_s = max(pos.get("iv", 0.25) * (1.0 + dv), min_iv)
                p_s = black76_price(F_s, pos["K"], T, iv_s, r,
                                    "call" if pos["otype"] == "C" else "put")
                # qty > 0 long, qty < 0 short
                pnl_per_share += pos["qty"] * (p_s - cur_p)
            pnl_total = pnl_per_share * contract_mult
            scenarios.append((ds, dv, pnl_total))

    if not scenarios:
        return {"margin": 0.0, "worst_loss": 0.0,
                "worst_scenario": (0, 0), "scenarios": []}

    worst = min(scenarios, key=lambda t: t[2])
    worst_loss = worst[2]
    # Margin = -worst_loss (margin held against worst-case loss)
    margin = max(0.0, -worst_loss)
    return {
        "margin": margin,
        "worst_loss": worst_loss,
        "worst_scenario": (worst[0], worst[1]),
        "scenarios": scenarios,
    }


def simple_margin_per_contract(short_strike, long_strike, credit_per_contract,
                                contract_mult: int = SBER_CONTRACT_MULT) -> float:
    """Conservative quick-margin fallback for credit spreads.

    Used when full portfolio scenarios are unavailable (or as a sanity
    floor).  margin = (wing_width - credit) * contract_multiplier.
    """
    width = abs(long_strike - short_strike)
    return max(0.0, (width - credit_per_contract)) * contract_mult


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Example: Iron Condor on SBER F=300, T=30/365, r=15%
    F, T, r = 300.0, 30/365, 0.15
    positions = [
        # short 320 call (20Δ), short 280 put (20Δ), wings at 340/260
        {"K": 320, "otype": "C", "qty": -1, "iv": 0.30, "open_px": 2.50},
        {"K": 280, "otype": "P", "qty": -1, "iv": 0.30, "open_px": 2.20},
        {"K": 340, "otype": "C", "qty":  1, "iv": 0.32, "open_px": 0.80},
        {"K": 260, "otype": "P", "qty":  1, "iv": 0.34, "open_px": 0.70},
    ]
    res = portfolio_margin(positions, F, T, r)
    print(f"Single Iron Condor on SBER F={F}:")
    print(f"  Required margin: {res['margin']:,.0f} RUB")
    print(f"  Worst loss     : {res['worst_loss']:,.0f} RUB")
    print(f"  Worst scenario : Δspot={res['worst_scenario'][0]:+.0%} "
          f"Δvol={res['worst_scenario'][1]:+.0%}")
    print()
    print("All scenarios:")
    for ds, dv, pnl in sorted(res["scenarios"], key=lambda t: t[2]):
        print(f"  Δspot={ds:+5.0%}  Δvol={dv:+5.0%}  P/L={pnl:+,.0f}")
    print()
    print("Simple fallback margin per contract:")
    print(f"  call wing 340-320 → {simple_margin_per_contract(320, 340, 1.7):,.0f}")
    print(f"  put  wing 260-280 → {simple_margin_per_contract(280, 260, 1.5):,.0f}")
