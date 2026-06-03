"""
test_moex_options_api.py
========================
Reusable probes for MOEX ISS API options endpoints. Use these helpers to
explore the options chain on SBER/GAZP/AFKS/IMOEX (MIX) etc. for the VRP
strategy work.

Docs index:  https://iss.moex.com/iss/reference/
Options board:  ROPD (board_group_id=35 "futures_options")
Engine/Market: engines/futures/markets/options

Underlyings of interest (confirmed 2026-06-03):
    ASSETCODE   underlying        type  # listed
    SBRF        SBER              S     2200    -> stock options on Sberbank
    GAZR        GZM6 (futures)    F     2278    -> options on GAZP futures
    AFKS        AFKS              S      144    -> stock options on Sistema
    MIX         MXM6 (futures)    F      376    -> options on MOEX index futures
    IMX         IMOEX             I     1714    -> options on IMOEX directly
    RTS         RIM6 (futures)    F      468    -> options on RTS futures (RIH/RIM/...)

OPTIONTYPE:    C = Call, P = Put
UNDERLYINGTYPE: F=futures, S=stock, I=index, G=metal (GLDRUB/SLVRUB)
"""

from __future__ import annotations
import json
import os
import ssl
import urllib.parse
import urllib.request
from typing import Any

BASE = "https://iss.moex.com/iss"
UA = {"User-Agent": "moex-aggregator/1.0 (research)"}

# Set MOEX_INSECURE=1 to skip TLS verification (useful inside dev sandboxes
# where the host clock differs from real-world time, breaking cert validity).
_CTX = ssl._create_unverified_context() if os.getenv("MOEX_INSECURE") else None


def _get(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30, context=_CTX) as r:
        # ISS occasionally serves UTF-8 BOM
        return json.loads(r.read().decode("utf-8-sig"))


def _table(block: dict) -> list[dict]:
    return [dict(zip(block["columns"], row)) for row in block["data"]]


# ---------------------------------------------------------------------------
# 1. Snapshot of the full option chain (LIVE + last settlement)
# ---------------------------------------------------------------------------
def list_options(assetcode: str | None = None) -> tuple[list[dict], list[dict]]:
    """
    Returns (securities_meta, marketdata_snapshot).

    Fields in `securities` block include:
        SECID, SHORTNAME, SECNAME, BOARDID, ASSETCODE, UNDERLYINGASSET,
        UNDERLYINGTYPE, STRIKE, OPTIONTYPE (C/P), LASTDELDATE (expiry),
        LASTTRADEDATE, PREVSETTLEPRICE, PREVOPENPOSITION, MINSTEP, STEPPRICE,
        UNDERLYINGSETTLEPRICE, SETTLEPRICE_CLR, IMNP/IMP/IMBUY (margin), ...

    Fields in `marketdata` block include:
        SECID, BID, OFFER, SPREAD, OPEN, HIGH, LOW, LAST, QUANTITY, NUMTRADES,
        SETTLEPRICE, OPENPOSITION (live OI), OICHANGE, VOLTODAY (contracts),
        VALTODAY (rub), LASTCHANGEPRCNT, UPDATETIME, TRADE_SESSION_DATE.
    NB: ISS does NOT publish implied volatility or option greeks via the
    public iss.moex.com endpoints. IV exists only in the FORTS terminal /
    paid Algopack feed, not in /iss/. RVI (composite IV) is the only public
    volatility series.
    """
    url = (
        f"{BASE}/engines/futures/markets/options/securities.json"
        f"?iss.meta=off&iss.only=securities,marketdata"
    )
    data = _get(url)
    secs = _table(data["securities"])
    md = _table(data["marketdata"])
    if assetcode is not None:
        secs = [s for s in secs if s.get("ASSETCODE") == assetcode]
        keep = {s["SECID"] for s in secs}
        md = [m for m in md if m["SECID"] in keep]
    return secs, md


# ---------------------------------------------------------------------------
# 2. Spec for ONE option (instrument card)
# ---------------------------------------------------------------------------
def option_spec(secid: str) -> dict:
    """
    GET https://iss.moex.com/iss/engines/futures/markets/options/securities/{SECID}.json
    Includes per-instrument securities + marketdata.
    """
    url = (
        f"{BASE}/engines/futures/markets/options/securities/"
        f"{urllib.parse.quote(secid)}.json?iss.meta=off"
    )
    return _get(url)


# ---------------------------------------------------------------------------
# 3. Daily history (EOD) for ONE option, by date range
# ---------------------------------------------------------------------------
def option_history(secid: str, date_from: str, date_till: str) -> list[dict]:
    """
    GET https://iss.moex.com/iss/history/engines/futures/markets/options/
        securities/{SECID}.json?from=YYYY-MM-DD&till=YYYY-MM-DD

    Fields (history block):
        BOARDID, TRADEDATE, SECID, OPEN, LOW, HIGH, CLOSE,
        OPENPOSITIONVALUE (rub), VALUE (rub), VOLUME (contracts),
        OPENPOSITION (contracts), SETTLEPRICE, WAPRICE, CHANGE,
        QTY, NUMTRADES.
    Historical depth confirmed back to 2014-01-15 (e.g. BR100BA4_2014).
    Paginate with &start=N (page size = 100 by default).
    """
    rows: list[dict] = []
    start = 0
    while True:
        url = (
            f"{BASE}/history/engines/futures/markets/options/securities/"
            f"{urllib.parse.quote(secid)}.json?iss.meta=off"
            f"&from={date_from}&till={date_till}&start={start}"
        )
        d = _get(url)
        chunk = _table(d["history"])
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < 100:
            break
        start += 100
    return rows


# ---------------------------------------------------------------------------
# 4. Whole-market daily slice (every option that traded on a given day)
# ---------------------------------------------------------------------------
def market_history_day(date: str) -> list[dict]:
    """
    GET https://iss.moex.com/iss/history/engines/futures/markets/options/
        securities.json?date=YYYY-MM-DD&start=N

    Same `history` fields as above. Useful for backfilling the entire chain
    in one pass (loop pages of 100).
    """
    rows: list[dict] = []
    start = 0
    while True:
        url = (
            f"{BASE}/history/engines/futures/markets/options/securities.json"
            f"?iss.meta=off&date={date}&start={start}"
        )
        d = _get(url)
        chunk = _table(d["history"])
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < 100:
            break
        start += 100
    return rows


# ---------------------------------------------------------------------------
# 5. Intraday tape (trades) for one option
# ---------------------------------------------------------------------------
def option_trades(secid: str) -> list[dict]:
    """
    GET https://iss.moex.com/iss/engines/futures/markets/options/securities/
        {SECID}/trades.json
    Fields: TRADENO, TRADEDATE, TRADETIME, PRICE, QUANTITY, BUYSELL,
            OPENPOSITION, OFFMARKETDEAL.
    Only live trades for the current trading session (no deep history).
    """
    url = (
        f"{BASE}/engines/futures/markets/options/securities/"
        f"{urllib.parse.quote(secid)}/trades.json?iss.meta=off"
    )
    return _table(_get(url)["trades"])


if __name__ == "__main__":
    # Quick smoke test: pull liquid SBER options today + 30-day history for the
    # most-traded strike.
    secs, md = list_options(assetcode="SBRF")
    md_by_id = {m["SECID"]: m for m in md}
    liquid = sorted(
        [(md_by_id[s["SECID"]]["VOLTODAY"] or 0, s) for s in secs
         if md_by_id.get(s["SECID"])],
        key=lambda x: x[0],
        reverse=True,
    )[:5]
    print(f"Top 5 SBRF options by VOLTODAY:")
    for v, s in liquid:
        m = md_by_id[s["SECID"]]
        print(
            f"  {s['SECID']:15s} K={s['STRIKE']:>8} {s['OPTIONTYPE']} "
            f"exp={s['LASTDELDATE']} LAST={m['LAST']} OI={m['OPENPOSITION']} "
            f"VOL={v}"
        )

    if liquid:
        target = liquid[0][1]["SECID"]
        print(f"\n30-day history for {target}:")
        hist = option_history(target, "2026-05-01", "2026-06-02")
        for h in hist[-5:]:
            print(
                f"  {h['TRADEDATE']} O={h['OPEN']} H={h['HIGH']} L={h['LOW']} "
                f"C={h['CLOSE']} SETTLE={h['SETTLEPRICE']} "
                f"OI={h['OPENPOSITION']} VOL={h['VOLUME']}"
            )
