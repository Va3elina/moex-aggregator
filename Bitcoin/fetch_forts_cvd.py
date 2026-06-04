"""
fetch_forts_cvd.py
==================
Build a daily CVD (Cumulative Volume Delta) proxy for the FRONT-MONTH futures
of SBER (SR), GAZP (GZ), Si, RTS (RI) from 1-minute candles.

Method
------
True bid/ask volume split needs tick data. We approximate per-minute signed
volume from 1-minute candles:

    signed_vol(minute) = sign(close - open) * volume
        close > open  -> +volume   (buying pressure)
        close < open  -> -volume   (selling pressure)
        close == open ->  0

    daily CVD            = sum of signed_vol over the day
    up_vol / down_vol    = volume on up-minutes / down-minutes
    delta_co_volweighted = sum( (close-open) * volume ) / sum(volume)
                           (volume-weighted close-to-open delta, price units)
    cvd_cumulative       = running sum of daily CVD across the file

NOTE on VALUE: FORTS 1-min candles return value=0 (ISS quirk), so we use the
VOLUME field (number of contracts) as the size measure. That is fine for a CVD
proxy since we only need relative buy/sell pressure.

Front-month resolution
----------------------
We read the existing *_futures_history_merged.csv (all contracts, with daily
VOLUME) and pick, per trading day, the plain front-month contract (SECID like
SRM6) with the highest volume. We then fetch minute candles per contract and
keep only the days on which that contract was the front month -> a continuous
front-month CVD series with no double counting at rolls.

Output: {asset}_cvd_daily.csv
    columns: date, secid, cvd, cvd_cumulative, up_vol, down_vol,
             total_vol, delta_co_vw

Candles endpoint:
    /iss/engines/futures/markets/forts/securities/{SECID}/candles.json
        ?interval=1&from=YYYY-MM-DD&till=YYYY-MM-DD   (paged, 500 rows/page)
"""
from __future__ import annotations

import csv
import os
import re
import sys
import time
from collections import defaultdict

import requests

ASSETS = {"sber": "SR", "gazp": "GZ", "si": "Si", "rts": "RI"}
MONTH_CODES = set("FGHJKMNQUVXZ")

# default window; minute history is heavy, keep to a few years
DEFAULT_FROM = "2023-01-01"

CANDLE_URL = ("https://iss.moex.com/iss/engines/futures/markets/forts/"
              "securities/{secid}/candles.json"
              "?interval=1&from={frm}&till={till}&start={start}&iss.meta=off")

session = requests.Session()
session.headers.update({"User-Agent": "moex-cvd-collector/1.0"})


def get_json(url, retries=4):
    for i in range(retries):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 200:
                return r.json()
            sys.stderr.write(f"  http {r.status_code}\n")
        except Exception as e:  # noqa
            sys.stderr.write(f"  retry {i}: {e}\n")
        time.sleep(1.5 * (i + 1))
    return None


def is_front_secid(secid: str, prefix: str) -> bool:
    if not secid.startswith(prefix):
        return False
    tail = secid[len(prefix):]
    return len(tail) == 2 and tail[0] in MONTH_CODES and tail[1].isdigit()


def front_month_by_date(asset: str, prefix: str, start_from: str):
    """Return {date -> secid} of the highest-volume front-month per day."""
    path = f"{asset}_futures_history_merged.csv"
    if not os.path.exists(path):
        path = f"{asset}_futures_history.csv"
    best = {}  # date -> (vol, secid)
    with open(path) as fh:
        for row in csv.DictReader(fh):
            d = row["TRADEDATE"]
            if d < start_from:
                continue
            sec = row["SECID"]
            if not is_front_secid(sec, prefix):
                continue
            try:
                vol = float(row["VOLUME"])
            except (ValueError, KeyError):
                vol = 0.0
            if d not in best or vol > best[d][0]:
                best[d] = (vol, sec)
    return {d: v[1] for d, v in best.items()}


def fetch_candles(secid: str, frm: str, till: str):
    """Yield candle dicts across all pages."""
    start = 0
    while True:
        j = get_json(CANDLE_URL.format(secid=secid, frm=frm, till=till,
                                       start=start))
        if not j:
            return
        c = j["candles"]
        cols = c["columns"]
        data = c["data"]
        if not data:
            return
        for r in data:
            yield dict(zip(cols, r))
        if len(data) < 500:
            return
        start += 500
        time.sleep(0.25)


def daily_cvd_for_contract(secid: str, frm: str, till: str):
    """Aggregate per-minute signed volume into per-day stats for one contract."""
    agg = defaultdict(lambda: {"cvd": 0.0, "up": 0.0, "down": 0.0,
                               "vol": 0.0, "co_vw_num": 0.0})
    for cdl in fetch_candles(secid, frm, till):
        day = cdl["begin"][:10]
        o = cdl["open"]
        cl = cdl["close"]
        vol = cdl["volume"] or 0
        a = agg[day]
        if cl > o:
            a["cvd"] += vol
            a["up"] += vol
        elif cl < o:
            a["cvd"] -= vol
            a["down"] += vol
        a["vol"] += vol
        a["co_vw_num"] += (cl - o) * vol
    return agg


def build_asset(asset: str, prefix: str, start_from: str):
    print(f"\n=== {asset.upper()} ({prefix}) CVD from {start_from} ===")
    fmap = front_month_by_date(asset, prefix, start_from)
    if not fmap:
        print("  no front-month dates found")
        return
    # group dates by the contract that was front month
    dates_by_sec = defaultdict(list)
    for d, sec in fmap.items():
        dates_by_sec[sec].append(d)

    rows = []  # (date, secid, cvd, up, down, vol, co_vw)
    for sec in sorted(dates_by_sec, key=lambda s: min(dates_by_sec[s])):
        days = sorted(dates_by_sec[sec])
        frm, till = days[0], days[-1]
        print(f"  {sec}: {len(days)} front-month days "
              f"({frm}..{till}) fetching candles...")
        agg = daily_cvd_for_contract(sec, frm, till)
        keep = set(days)
        for d in sorted(agg):
            if d not in keep:
                continue  # only days this contract was the front month
            a = agg[d]
            co_vw = (a["co_vw_num"] / a["vol"]) if a["vol"] else 0.0
            rows.append((d, sec, a["cvd"], a["up"], a["down"], a["vol"], co_vw))
        time.sleep(0.3)

    rows.sort(key=lambda r: r[0])
    # cumulative CVD
    path = f"{asset}_cvd_daily.csv"
    cum = 0.0
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "secid", "cvd", "cvd_cumulative",
                    "up_vol", "down_vol", "total_vol", "delta_co_vw"])
        for d, sec, cvd, up, down, vol, co_vw in rows:
            cum += cvd
            w.writerow([d, sec, f"{cvd:.0f}", f"{cum:.0f}",
                        f"{up:.0f}", f"{down:.0f}", f"{vol:.0f}",
                        f"{co_vw:.4f}"])
    print(f"  -> {path} ({len(rows)} daily rows)")


def main():
    start_from = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_FROM
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for asset, prefix in ASSETS.items():
        if only and only != "all" and asset != only:
            continue
        build_asset(asset, prefix, start_from)


if __name__ == "__main__":
    main()
