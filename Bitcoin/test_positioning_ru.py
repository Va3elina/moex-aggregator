"""
test_positioning_ru.py
----------------------
MOEX ISS - Options Positioning / Sentiment metrics for RU underlyings.

Computes:
  1) Put/Call OI ratio (sum OPENPOSITION grouped by OPTIONTYPE)
  2) Put/Call volume ratio (VOLTODAY)
  3) Max-pain strike (Σ OI_call*max(S-K,0) + OI_put*max(K-S,0))
  4) OI concentration: top-5 strikes by total OI

Endpoint:
  https://iss.moex.com/iss/engines/futures/markets/options/securities.json
  -> blocks: securities (STRIKE, OPTIONTYPE, ASSETCODE, UNDERLYINGASSET,
                          UNDERLYINGSETTLEPRICE, LASTDELDATE, ...)
             marketdata (OPENPOSITION, VOLTODAY, VALTODAY, OICHANGE, ...)

Run:
  python3 test_positioning_ru.py SBRF
  python3 test_positioning_ru.py RTS
  python3 test_positioning_ru.py GAZR
"""

from __future__ import annotations

import sys
from datetime import date

import pandas as pd
import requests

ISS = "https://iss.moex.com/iss"


def fetch_options_chain(asset: str | None = None) -> pd.DataFrame:
    """Fetch the full live FORTS options board (ROPD/RPMO/RPMA) and merge
    securities + marketdata blocks. Optionally filter by ASSETCODE.

    NOTE: MOEX paginates results in pages of 1000. We follow `start=`.
    """
    rows = []
    md_rows = []
    start = 0
    while True:
        url = (
            f"{ISS}/engines/futures/markets/options/securities.json"
            f"?iss.meta=off&iss.only=securities,marketdata&start={start}"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        j = r.json()
        sec_cols = j["securities"]["columns"]
        md_cols = j["marketdata"]["columns"]
        sec_data = j["securities"]["data"]
        md_data = j["marketdata"]["data"]
        if not sec_data:
            break
        rows.extend(sec_data)
        md_rows.extend(md_data)
        if len(sec_data) < 1000:
            break
        start += 1000

    sec = pd.DataFrame(rows, columns=sec_cols)
    md = pd.DataFrame(md_rows, columns=md_cols)
    df = sec.merge(md, on=["SECID", "BOARDID"], how="left", suffixes=("", "_md"))
    if asset:
        df = df[df["ASSETCODE"] == asset]
    # Numeric coercions
    for col in ("STRIKE", "OPENPOSITION", "VOLTODAY", "VALTODAY",
                "UNDERLYINGSETTLEPRICE", "PREVOPENPOSITION"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def pick_nearest_expiry(df: pd.DataFrame) -> str:
    """Return LASTDELDATE that has the largest total OI (most liquid front)."""
    g = (
        df.dropna(subset=["LASTDELDATE"])
        .groupby("LASTDELDATE")["OPENPOSITION"]
        .sum()
        .sort_values(ascending=False)
    )
    if g.empty:
        raise RuntimeError("No expiries with OI")
    return g.index[0]


def positioning_report(asset: str) -> None:
    print(f"\n{'='*70}\nMOEX OPTIONS POSITIONING REPORT - {asset}\n{'='*70}")
    df = fetch_options_chain(asset)
    if df.empty:
        print(f"No options for ASSETCODE={asset}")
        return

    # Pick the single most-liquid expiry to make the chain coherent.
    expiry = pick_nearest_expiry(df)
    chain = df[df["LASTDELDATE"] == expiry].copy()
    spot = chain["UNDERLYINGSETTLEPRICE"].dropna().iloc[0]
    underlying = chain["UNDERLYINGASSET"].iloc[0]
    print(f"Underlying future : {underlying}  settle={spot}")
    print(f"Expiry            : {expiry}  ({len(chain)} contracts)")

    calls = chain[chain["OPTIONTYPE"] == "C"]
    puts  = chain[chain["OPTIONTYPE"] == "P"]

    oi_c = calls["OPENPOSITION"].sum()
    oi_p = puts["OPENPOSITION"].sum()
    v_c  = calls["VOLTODAY"].sum()
    v_p  = puts["VOLTODAY"].sum()

    print(f"\n[1] Put/Call OI ratio     = {oi_p/oi_c if oi_c else float('nan'):.3f}"
          f"   (OI_put={int(oi_p)}  OI_call={int(oi_c)})")
    print(f"[2] Put/Call VOL ratio    = {v_p/v_c if v_c else float('nan'):.3f}"
          f"   (V_put={int(v_p)}  V_call={int(v_c)})")

    # Max-pain - aggregate OI per strike
    agg = (
        chain.groupby(["STRIKE", "OPTIONTYPE"])["OPENPOSITION"]
        .sum()
        .unstack(fill_value=0)
        .rename(columns={"C": "OI_C", "P": "OI_P"})
        .reset_index()
        .sort_values("STRIKE")
    )
    if "OI_C" not in agg: agg["OI_C"] = 0
    if "OI_P" not in agg: agg["OI_P"] = 0
    agg["OI_TOTAL"] = agg["OI_C"] + agg["OI_P"]

    strikes = agg["STRIKE"].values
    pain = []
    for K in strikes:
        # Pain at expiry if underlying settled at strike K:
        # call writers lose max(S-K,0) per contract; put writers lose max(K-S,0)
        loss = (agg["OI_C"] * (strikes - K).clip(min=0)).sum() \
             + (agg["OI_P"] * (K - strikes).clip(min=0)).sum()
        pain.append(loss)
    agg["PAIN"] = pain
    max_pain = agg.loc[agg["PAIN"].idxmin(), "STRIKE"]
    print(f"[3] Max-pain strike       = {max_pain}   (spot {spot}, "
          f"diff {(max_pain - spot)/spot*100:+.2f}%)")

    # Top strikes by OI concentration
    top = agg.nlargest(5, "OI_TOTAL")[["STRIKE", "OI_C", "OI_P", "OI_TOTAL"]]
    print(f"\n[4] Top-5 strikes by total OI (support/resistance):")
    print(top.to_string(index=False, float_format=lambda x: f"{x:.0f}"))

    # Unusual activity = high VOL relative to OI per strike/side
    chain["V_OI"] = chain["VOLTODAY"] / chain["OPENPOSITION"].replace(0, pd.NA)
    unusual = (
        chain.dropna(subset=["V_OI"])
        .sort_values("V_OI", ascending=False)
        .head(5)[["SECID", "OPTIONTYPE", "STRIKE", "VOLTODAY",
                  "OPENPOSITION", "V_OI"]]
    )
    print(f"\n[5] Top unusual activity (Volume/OI):")
    print(unusual.to_string(index=False))


if __name__ == "__main__":
    asset = sys.argv[1] if len(sys.argv) > 1 else "SBRF"
    positioning_report(asset)
