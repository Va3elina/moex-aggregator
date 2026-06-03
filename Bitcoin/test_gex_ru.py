"""
GEX / Dealer Gamma / Vanna / Charm prototype for MOEX FORTS options.

Goal: replicate the CBOE / Deribit style "greeks aggregation" metrics on RU
options market. We pull the full chain from ISS MOEX, compute Black-Scholes
greeks from the implied volatility that MOEX publishes (IMP column), then
aggregate by strike weighted by Open Interest (OI).

Data source: https://iss.moex.com/iss/engines/futures/markets/options
  - PREVOPENPOSITION  = OI snapshot (prev evening clearing)
  - STRIKE            = option strike
  - OPTIONTYPE        = 'C' / 'P'
  - IMP               = implied volatility (annualised, in %)
  - UNDERLYINGSETTLEPRICE = futures settle price = spot proxy
  - LASTTRADEDATE     = expiry
  - ASSETCODE         = root (SBRF, GAZR, RTS via Si/IMX, etc)

Historical OI: /iss/history/engines/futures/markets/options/securities/<SECID>
  returns daily OPENPOSITION + SETTLEPRICE. This is per-contract though, so
  a full historical backtest = O(strikes * days) requests.

Run:  python3 test_gex_ru.py [ASSETCODE]
Default underlying: SBRF (most liquid stock option). Try GAZR, ROSN, Si, IMX.
"""

from __future__ import annotations

import json
import math
import sys
import urllib.request
from collections import defaultdict
from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy.stats import norm

ISS_OPTIONS = "https://iss.moex.com/iss/engines/futures/markets/options/securities.json?iss.meta=off"
RISK_FREE = 0.16  # CBR key rate proxy, June 2026


def fetch_chain() -> pd.DataFrame:
    with urllib.request.urlopen(ISS_OPTIONS, timeout=30) as r:
        raw = json.load(r)
    block = raw["securities"]
    df = pd.DataFrame(block["data"], columns=block["columns"])
    df["STRIKE"] = pd.to_numeric(df["STRIKE"], errors="coerce")
    df["PREVOPENPOSITION"] = pd.to_numeric(df["PREVOPENPOSITION"], errors="coerce").fillna(0)
    df["IMP"] = pd.to_numeric(df["IMP"], errors="coerce")
    df["UNDERLYINGSETTLEPRICE"] = pd.to_numeric(df["UNDERLYINGSETTLEPRICE"], errors="coerce")
    df["LASTTRADEDATE"] = pd.to_datetime(df["LASTTRADEDATE"], errors="coerce")
    return df


def bs_greeks(S: float, K: float, T: float, sigma: float, r: float = RISK_FREE, q: float = 0.0):
    """Black-Scholes-Merton greeks for a European option on a futures (q ~= r => Black-76).
    Returns (gamma, vanna, charm). We don't need delta itself for aggregation
    (charm uses d2).
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0, 0.0, 0.0
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    pdf = norm.pdf(d1)
    gamma = math.exp(-q * T) * pdf / (S * sigma * sqrtT)
    vanna = -math.exp(-q * T) * pdf * d2 / sigma            # dDelta / dVol
    charm = -math.exp(-q * T) * pdf * (2 * (r - q) * T - d2 * sigma * sqrtT) / (
        2 * T * sigma * sqrtT
    )
    return gamma, vanna, charm


def analyse(df: pd.DataFrame, asset: str, contract_mult: int = 1) -> dict:
    """Aggregate greeks for one ASSETCODE, picking the nearest expiry that
    still has >50% of the OI (front-month focus, как делают US dealers).

    MOEX gotcha: the same ASSETCODE (e.g. 'SBRF') maps to multiple
    UNDERLYINGASSET futures (SRM6/SRU6/SRZ6). We pick the one with the most
    OI on the front expiry — that's the dealer-relevant series.
    Also: IMP is volatility in PRICE UNITS of the underlying (rubles),
    annualised. Convert to % via sigma = IMP / spot.
    """
    sub = df[df["ASSETCODE"] == asset].copy()
    sub = sub[(sub["PREVOPENPOSITION"] > 0) & sub["IMP"].notna() & sub["STRIKE"].notna()]
    sub = sub[sub["UNDERLYINGTYPE"] == "F"]  # futures-style only
    if sub.empty:
        return {"asset": asset, "ok": False, "reason": "no OI/IV rows"}

    today = pd.Timestamp(date.today())
    sub = sub[sub["LASTTRADEDATE"] > today]
    if sub.empty:
        return {"asset": asset, "ok": False, "reason": "all expired"}

    # Pick the (UNDERLYINGASSET, expiry) pair with the largest OI
    oi_by_pair = (
        sub.groupby(["UNDERLYINGASSET", "LASTTRADEDATE"])["PREVOPENPOSITION"].sum().sort_values(ascending=False)
    )
    front_ua, front_exp = oi_by_pair.index[0]
    chain = sub[(sub["UNDERLYINGASSET"] == front_ua) & (sub["LASTTRADEDATE"] == front_exp)].copy()
    spot = float(chain["UNDERLYINGSETTLEPRICE"].dropna().iloc[0])
    T = max((front_exp - today).days, 1) / 365.0

    # Black-Scholes greeks per contract.
    # MOEX IMP = annualised volatility expressed in price units of the
    # underlying. Convert to dimensionless sigma via IMP / spot.
    rows = []
    for _, row in chain.iterrows():
        sigma = float(row["IMP"]) / spot
        gamma, vanna, charm = bs_greeks(spot, float(row["STRIKE"]), T, sigma)
        sign = 1 if row["OPTIONTYPE"] == "C" else -1  # dealer convention: short calls, long puts? we use call-put net
        rows.append(
            {
                "secid": row["SECID"],
                "strike": float(row["STRIKE"]),
                "type": row["OPTIONTYPE"],
                "oi": float(row["PREVOPENPOSITION"]),
                "iv": sigma,
                "gamma": gamma,
                "vanna": vanna,
                "charm": charm,
                "gex_contrib": sign * row["PREVOPENPOSITION"] * contract_mult * gamma * spot * spot * 0.01,
                "ddg_contrib": row["PREVOPENPOSITION"] * contract_mult * gamma * spot,
                "vanna_contrib": sign * row["PREVOPENPOSITION"] * contract_mult * vanna,
                "charm_contrib": sign * row["PREVOPENPOSITION"] * contract_mult * charm,
            }
        )
    g = pd.DataFrame(rows)

    by_strike = g.groupby("strike").agg(
        oi=("oi", "sum"),
        gex=("gex_contrib", "sum"),
        ddg=("ddg_contrib", "sum"),
        vanna=("vanna_contrib", "sum"),
        charm=("charm_contrib", "sum"),
    ).sort_index()

    # Magnet strikes = largest absolute DDG (positive gamma = pinning)
    magnets = by_strike.reindex(by_strike["ddg"].abs().sort_values(ascending=False).index).head(5)

    # Zero-gamma flip strike (regime change): where cumulative GEX crosses 0
    by_strike["cum_gex"] = by_strike["gex"].cumsum()
    flip_strike = None
    cg = by_strike["cum_gex"].values
    strikes_arr = by_strike.index.values
    for i in range(1, len(cg)):
        if cg[i - 1] * cg[i] < 0:
            # linear interp
            x0, x1 = strikes_arr[i - 1], strikes_arr[i]
            y0, y1 = cg[i - 1], cg[i]
            flip_strike = x0 - y0 * (x1 - x0) / (y1 - y0)
            break

    return {
        "asset": asset,
        "ok": True,
        "underlying": front_ua,
        "spot": spot,
        "expiry": front_exp.strftime("%Y-%m-%d"),
        "dte": int((front_exp - today).days),
        "n_strikes": int(by_strike.shape[0]),
        "total_oi": int(by_strike["oi"].sum()),
        "net_gex": float(by_strike["gex"].sum()),
        "net_ddg": float(by_strike["ddg"].sum()),
        "net_vanna": float(by_strike["vanna"].sum()),
        "net_charm": float(by_strike["charm"].sum()),
        "flip_strike": flip_strike,
        "magnets": magnets[["oi", "gex", "ddg"]],
        "by_strike": by_strike,
    }


def print_report(res: dict) -> None:
    if not res["ok"]:
        print(f"[{res['asset']}] FAIL: {res['reason']}")
        return
    a = res["asset"]
    print(f"\n=== GEX REPORT  {a}  (front futures = {res['underlying']}) ===")
    print(f"  front expiry : {res['expiry']}  (DTE={res['dte']})")
    print(f"  spot         : {res['spot']:.2f}")
    print(f"  strikes      : {res['n_strikes']}")
    print(f"  total OI     : {res['total_oi']:,}")
    print(f"  net GEX      : {res['net_gex']:>16,.0f}  (>0 => dealers long gamma, suppress vol)")
    print(f"  net DDG      : {res['net_ddg']:>16,.0f}  $-gamma units (sort of)")
    print(f"  net vanna    : {res['net_vanna']:>16,.0f}")
    print(f"  net charm    : {res['net_charm']:>16,.0f}")
    if res["flip_strike"] is not None:
        print(f"  zero-gamma flip: {res['flip_strike']:.2f}  (regime change strike)")
    print("\n  TOP-5 MAGNET STRIKES (by |DDG|):")
    print(res["magnets"].to_string(float_format=lambda v: f"{v:>14,.0f}"))
    print("\n  GEX DISTRIBUTION (top 10 strikes by |GEX|):")
    top = res["by_strike"].reindex(res["by_strike"]["gex"].abs().sort_values(ascending=False).index).head(10)
    print(top[["oi", "gex", "ddg"]].to_string(float_format=lambda v: f"{v:>14,.0f}"))


def main():
    asset = sys.argv[1] if len(sys.argv) > 1 else "SBRF"
    print(f"Fetching MOEX options chain... (target asset={asset})")
    df = fetch_chain()
    print(f"  got {len(df):,} option contracts across {df['ASSETCODE'].nunique()} underlyings")

    # Run on requested asset
    res = analyse(df, asset)
    print_report(res)

    # Also run on all liquid futures-style underlyings for comparison
    if asset == "SBRF":
        print("\n--- quick scan of top liquid roots ---")
        for a in ["GAZR", "ROSN", "SBRF", "Si", "IMX", "CNY", "MOEX"]:
            r = analyse(df, a)
            if r["ok"]:
                flip = f"{r['flip_strike']:.2f}" if r['flip_strike'] else 'n/a'
                print(
                    f"  {a:6} ({r['underlying']:5})  spot={r['spot']:>10.2f}  OI={r['total_oi']:>10,}  "
                    f"netGEX={r['net_gex']:>18,.0f}  flip={flip}"
                )
            else:
                print(f"  {a:6}  -- {r['reason']}")


if __name__ == "__main__":
    main()
