"""
fetch_forts_margin_snapshot.py
==============================
Collect MOEX FORTS guarantee-obligation (ГО / initial margin) data for the
front-month futures of SBER (SR), GAZP (GZ), Si, RTS (RI).

IMPORTANT DATA-AVAILABILITY FINDING
-----------------------------------
MOEX ISS exposes INITIALMARGIN (ГО) ONLY in the LIVE securities snapshot:
    /iss/engines/futures/markets/forts/securities/{SECID}.json -> securities block
Fields available there: INITIALMARGIN, IMTIME, BUYSELLFEE, SCALPERFEE,
NEGOTIATEDFEE, HIGHLIMIT, LOWLIMIT, STEPPRICE, PREVSETTLEPRICE.

The HISTORY endpoint
    /iss/history/engines/futures/markets/forts/securities/{SECID}.json
DOES NOT contain any margin column (columns are OHLC / OI / VALUE / WAPRICE
only). Verified 2026-06-04.

=> There is NO public historical ГО time series on ISS.
   Historical margin requires either:
     * Algopack (apim.moex.com / datashop) -- PAID, returns 401 without a key.
     * Building your own daily snapshot archive going forward (this script).

This script therefore appends a DAILY SNAPSHOT row per run. Run it once per
trading day (cron) to accumulate a real margin history over time.

Output: {asset}_margin_history.csv
    columns: date, secid, margin, buysellfee, settleprice, imtime, margin_chg_pct
margin_chg_pct = day-over-day % change vs the previous stored row for that asset.
"""
from __future__ import annotations

import csv
import os
import sys
import time
from datetime import date

import requests

ASSETS = {
    "sber": "SR",
    "gazp": "GZ",
    "si": "Si",
    "rts": "RI",
}

SEC_URL = ("https://iss.moex.com/iss/engines/futures/markets/forts/"
           "securities/{secid}.json?iss.meta=off&iss.only=securities")
# NB: the assetcode= query filter is IGNORED by this ISS endpoint, so we pull
# the whole FORTS list and filter client-side by SECID prefix.
LIST_URL = ("https://iss.moex.com/iss/engines/futures/markets/forts/"
            "securities.json?iss.meta=off&iss.only=securities"
            "&securities.columns=SECID,LASTTRADEDATE,SECTYPE")

# SECID prefix of the futures contracts per asset (2-letter, e.g. SRM6)
PREFIX = {"sber": "SR", "gazp": "GZ", "si": "Si", "rts": "RI"}
MONTH_CODES = set("FGHJKMNQUVXZ")

session = requests.Session()
session.headers.update({"User-Agent": "moex-margin-collector/1.0"})


def get_json(url, retries=4):
    for i in range(retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception as e:  # noqa
            sys.stderr.write(f"  retry {i} {e}\n")
        time.sleep(1.5 * (i + 1))
    return None


_LIST_CACHE = None


def _is_front_month_secid(secid: str, prefix: str) -> bool:
    """True for plain front-month codes like SRM6 (prefix + month + year digit).

    Excludes calendar spreads (e.g. SiZ6SiH7) and week options.
    """
    if not secid.startswith(prefix):
        return False
    tail = secid[len(prefix):]
    return (len(tail) == 2 and tail[0] in MONTH_CODES and tail[1].isdigit())


def front_month_secid(prefix: str) -> str | None:
    """Resolve the nearest non-expired plain futures contract by SECID prefix."""
    global _LIST_CACHE
    if _LIST_CACHE is None:
        j = get_json(LIST_URL)
        _LIST_CACHE = j
    j = _LIST_CACHE
    if not j or not j["securities"]["data"]:
        return None
    cols = j["securities"]["columns"]
    rows = [dict(zip(cols, r)) for r in j["securities"]["data"]]
    rows = [r for r in rows if _is_front_month_secid(r["SECID"], prefix)]
    if not rows:
        return None
    today = date.today().isoformat()
    future = [r for r in rows if (r.get("LASTTRADEDATE") or "") >= today]
    future.sort(key=lambda r: r["LASTTRADEDATE"])
    if future:
        return future[0]["SECID"]
    rows.sort(key=lambda r: r.get("LASTTRADEDATE") or "")
    return rows[-1]["SECID"]


def fetch_margin(secid: str) -> dict | None:
    j = get_json(SEC_URL.format(secid=secid))
    if not j or not j["securities"]["data"]:
        return None
    cols = j["securities"]["columns"]
    r = dict(zip(cols, j["securities"]["data"][0]))
    return {
        "secid": secid,
        "margin": r.get("INITIALMARGIN"),
        "buysellfee": r.get("BUYSELLFEE"),
        "settleprice": r.get("LASTSETTLEPRICE") or r.get("PREVSETTLEPRICE"),
        "imtime": r.get("IMTIME"),
    }


def append_snapshot(asset: str):
    secid = front_month_secid(PREFIX[asset])
    if not secid:
        print(f"  {asset}: could not resolve front-month")
        return
    m = fetch_margin(secid)
    if not m or m["margin"] is None:
        print(f"  {asset}: no margin for {secid}")
        return

    path = f"{asset}_margin_history.csv"
    today = date.today().isoformat()
    prev_margin = None
    existing = []
    if os.path.exists(path):
        with open(path) as fh:
            existing = list(csv.DictReader(fh))
        # last row's margin (any secid) for day-over-day change
        if existing:
            try:
                prev_margin = float(existing[-1]["margin"])
            except (ValueError, KeyError):
                prev_margin = None
        # idempotent: skip if today's snapshot already stored
        if existing and existing[-1].get("date") == today:
            print(f"  {asset}: snapshot for {today} already present, skip")
            return

    chg = ""
    if prev_margin:
        chg = f"{(float(m['margin']) - prev_margin) / prev_margin * 100:.4f}"

    fields = ["date", "secid", "margin", "buysellfee", "settleprice",
              "imtime", "margin_chg_pct"]
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow({
            "date": today,
            "secid": m["secid"],
            "margin": m["margin"],
            "buysellfee": m["buysellfee"],
            "settleprice": m["settleprice"],
            "imtime": m["imtime"],
            "margin_chg_pct": chg,
        })
    print(f"  {asset}: {secid} margin={m['margin']} chg={chg or 'n/a'} -> {path}")


def main():
    print(f"FORTS margin snapshot {date.today().isoformat()}")
    for asset in ASSETS:
        append_snapshot(asset)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
