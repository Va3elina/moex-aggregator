"""
test_basis_ru.py
----------------
MOEX ISS - Futures Basis & Term Structure for SBER/RTS/GAZR/IMOEX.

Computes:
  - Per-contract basis vs spot (annualised)
  - Term structure (front -> next -> far)
  - Calendar spread (front - next)
  - Backwardation flag

Conventions:
  SBRF futures   : 1 contract = 100 SBER shares   -> price/100 vs SBER spot
  GAZR futures   : 1 contract = 100 GAZP shares   -> price/100 vs GAZP spot
  RTS  (RI)      : USD-points, multiplier 100     -> price/100 vs RTSI index
  MIX  (MX)      : MOEX index points, multiplier 1 -> price vs IMOEX
  Si             : USD/RUB *1000 -> price/1000 vs USDFIX

Endpoints:
  futures board : /iss/engines/futures/markets/forts/securities.json
  spot share    : /iss/engines/stock/markets/shares/boards/TQBR/securities/<TICKER>.json
  index         : /iss/engines/stock/markets/index/securities/<INDEX>.json

Run:
  python3 test_basis_ru.py
"""

from __future__ import annotations

from datetime import date
from typing import Dict

import pandas as pd
import requests

ISS = "https://iss.moex.com/iss"

# (asset_code, spot_resolver, multiplier, spot_label)
UNIVERSE = {
    "SBRF": dict(spot_ticker="SBER", spot_kind="share", mult=100),
    "GAZR": dict(spot_ticker="GAZP", spot_kind="share", mult=100),
    "RTS":  dict(spot_ticker="RTSI", spot_kind="index", mult=100),
    "MIX":  dict(spot_ticker="IMOEX", spot_kind="index", mult=100),
}


def get_share_spot(ticker: str) -> float:
    url = (f"{ISS}/engines/stock/markets/shares/boards/TQBR/securities/"
           f"{ticker}.json?iss.meta=off&iss.only=marketdata"
           f"&marketdata.columns=SECID,LAST,LCURRENTPRICE,LCLOSEPRICE")
    j = requests.get(url, timeout=20).json()
    row = j["marketdata"]["data"][0]
    # Prefer LAST, then LCURRENTPRICE, then LCLOSEPRICE
    for v in row[1:]:
        if v not in (None, 0):
            return float(v)
    raise RuntimeError(f"No spot for {ticker}")


def get_index_spot(secid: str) -> float:
    url = (f"{ISS}/engines/stock/markets/index/securities/{secid}.json"
           f"?iss.meta=off&iss.only=marketdata"
           f"&marketdata.columns=SECID,CURRENTVALUE,LASTVALUE")
    j = requests.get(url, timeout=20).json()
    rows = j["marketdata"]["data"]
    if not rows:
        return float("nan")
    row = rows[0]
    return float(row[1] or row[2])


def get_futures(asset: str) -> pd.DataFrame:
    url = (f"{ISS}/engines/futures/markets/forts/securities.json"
           f"?iss.meta=off&iss.only=securities,marketdata"
           "&securities.columns=SECID,SHORTNAME,ASSETCODE,LASTDELDATE,PREVSETTLEPRICE"
           "&marketdata.columns=SECID,LAST,OPENPOSITION,VOLTODAY,SETTLEPRICE")
    j = requests.get(url, timeout=30).json()
    sec = pd.DataFrame(j["securities"]["data"], columns=j["securities"]["columns"])
    md = pd.DataFrame(j["marketdata"]["data"], columns=j["marketdata"]["columns"])
    df = sec.merge(md, on="SECID", how="left")
    df = df[df["ASSETCODE"] == asset].copy()
    for c in ("LAST", "SETTLEPRICE", "PREVSETTLEPRICE", "OPENPOSITION", "VOLTODAY"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["LASTDELDATE"] = pd.to_datetime(df["LASTDELDATE"])
    df["PX"] = df["LAST"].where(df["LAST"] > 0,
                                df["SETTLEPRICE"].where(df["SETTLEPRICE"] > 0,
                                                        df["PREVSETTLEPRICE"]))
    # Only keep contracts that haven't expired
    df = df[df["LASTDELDATE"] >= pd.Timestamp(date.today())]
    return df.sort_values("LASTDELDATE").reset_index(drop=True)


def basis_report(asset: str) -> None:
    cfg = UNIVERSE[asset]
    print(f"\n{'='*70}\n{asset}  (spot={cfg['spot_ticker']}, mult={cfg['mult']})\n{'='*70}")
    spot = get_share_spot(cfg["spot_ticker"]) if cfg["spot_kind"] == "share" \
           else get_index_spot(cfg["spot_ticker"])
    print(f"Spot {cfg['spot_ticker']} = {spot:.4f}")

    fut = get_futures(asset)
    if fut.empty:
        print("No live futures")
        return

    today = pd.Timestamp(date.today())
    fut["DTE"] = (fut["LASTDELDATE"] - today).dt.days
    fut["F_NORM"] = fut["PX"] / cfg["mult"]
    fut["BASIS_ABS"] = fut["F_NORM"] - spot
    fut["BASIS_%"] = (fut["F_NORM"] - spot) / spot * 100.0
    # Annualised cost-of-carry implied yield (continuous-comp approx, t in years)
    fut["t_yrs"] = fut["DTE"] / 365.0
    fut["ANN_%"] = (fut["F_NORM"] / spot) ** (1.0 / fut["t_yrs"].clip(lower=1/365)) - 1.0
    fut["ANN_%"] *= 100.0

    out = fut[["SECID", "SHORTNAME", "LASTDELDATE", "DTE", "PX",
               "F_NORM", "BASIS_ABS", "BASIS_%", "ANN_%",
               "OPENPOSITION", "VOLTODAY"]].copy()
    out["LASTDELDATE"] = out["LASTDELDATE"].dt.strftime("%Y-%m-%d")
    print(out.to_string(index=False,
                        formatters={"PX": "{:>10.2f}".format,
                                    "F_NORM": "{:>10.4f}".format,
                                    "BASIS_ABS": "{:>+8.4f}".format,
                                    "BASIS_%": "{:>+6.2f}".format,
                                    "ANN_%": "{:>+6.2f}".format,
                                    "OPENPOSITION": "{:>10.0f}".format,
                                    "VOLTODAY": "{:>9.0f}".format}))

    # Term-structure & calendar spread (front vs next liquid)
    liquid = fut[fut["OPENPOSITION"] > 100].sort_values("LASTDELDATE")
    if len(liquid) >= 2:
        front, nxt = liquid.iloc[0], liquid.iloc[1]
        cal = nxt["F_NORM"] - front["F_NORM"]
        struc = "CONTANGO" if cal > 0 else ("BACKWARDATION" if cal < 0 else "FLAT")
        print(f"\nCalendar spread {front['SECID']}->{nxt['SECID']} = "
              f"{cal:+.4f}  ({struc})")
        if len(liquid) >= 3:
            far = liquid.iloc[2]
            inv = nxt["F_NORM"] < front["F_NORM"] and far["F_NORM"] < nxt["F_NORM"]
            print(f"Term-structure: {front['SECID']}={front['F_NORM']:.4f}  "
                  f"{nxt['SECID']}={nxt['F_NORM']:.4f}  "
                  f"{far['SECID']}={far['F_NORM']:.4f}  "
                  f"{'INVERTED (panic)' if inv else 'normal'}")

    # Front basis interpretation
    front = fut.sort_values("LASTDELDATE").iloc[0]
    b = float(front["BASIS_%"])
    if b > 5:
        flag = "DEEP CONTANGO - bullish mania"
    elif b > 1:
        flag = "mild contango"
    elif b < -1:
        flag = "BACKWARDATION - stress / dividend"
    else:
        flag = "near fair"
    print(f"\nFront basis flag: {flag}  (basis={b:+.2f}%)")


if __name__ == "__main__":
    for a in ("SBRF", "GAZR", "RTS", "MIX"):
        basis_report(a)
