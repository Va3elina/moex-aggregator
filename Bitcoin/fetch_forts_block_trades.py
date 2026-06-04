"""
fetch_forts_block_trades.py
===========================
Snapshot today's FORTS trade tape for the front-month futures of
SBER (SR), GAZP (GZ), Si, RTS (RI) and extract block / off-market deals.

DATA-AVAILABILITY FINDING (verified 2026-06-04)
-----------------------------------------------
Endpoint:
    /iss/engines/futures/markets/forts/securities/{SECID}/trades.json
Columns:
    TRADENO, BOARDNAME, SECID, TRADEDATE, TRADETIME, PRICE, QUANTITY,
    SYSTIME, RECNO, OPENPOSITION, OFFMARKETDEAL, BUYSELL, TRADE_SESSION_DATE

* OFFMARKETDEAL  -> 1 for negotiated / off-order-book (block) deals, 0 normal.
* BUYSELL        -> 'B' / 'S' aggressor side (real, not a proxy!).
* The tape is CURRENT TRADING DAY ONLY. The `date=` query parameter is
  IGNORED (always returns today's trades); requesting an expired contract
  returns 0 rows. => There is NO trade-tape history on ISS.

Therefore block trades can only be archived by snapshotting daily. This
script writes/append one CSV row per detected OFF-MARKET deal plus a daily
aggregate summary, so running it once per day (after close) builds a history.

Outputs:
    forts_block_trades.csv        -- one row per off-market deal (append)
        date, secid, tradetime, price, quantity, buysell, tradeno
    forts_trade_summary.csv       -- daily per-asset aggregate (append)
        date, secid, n_trades, total_qty, buy_qty, sell_qty,
        n_block, block_qty, block_qty_pct
"""
from __future__ import annotations

import csv
import os
import sys
import time
from datetime import date

import requests

ASSETS = {"sber": "SR", "gazp": "GZ", "si": "Si", "rts": "RI"}
MONTH_CODES = set("FGHJKMNQUVXZ")

LIST_URL = ("https://iss.moex.com/iss/engines/futures/markets/forts/"
            "securities.json?iss.meta=off&iss.only=securities"
            "&securities.columns=SECID,LASTTRADEDATE")
TRADES_URL = ("https://iss.moex.com/iss/engines/futures/markets/forts/"
              "securities/{secid}/trades.json?iss.meta=off&start={start}")

session = requests.Session()
session.headers.update({"User-Agent": "moex-blocktrades-collector/1.0"})


def get_json(url, retries=4):
    for i in range(retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception as e:  # noqa
            sys.stderr.write(f"  retry {i}: {e}\n")
        time.sleep(1.5 * (i + 1))
    return None


def is_front(secid, prefix):
    if not secid.startswith(prefix):
        return False
    tail = secid[len(prefix):]
    return len(tail) == 2 and tail[0] in MONTH_CODES and tail[1].isdigit()


_LIST = None


def front_month(prefix):
    global _LIST
    if _LIST is None:
        _LIST = get_json(LIST_URL)
    if not _LIST:
        return None
    cols = _LIST["securities"]["columns"]
    rows = [dict(zip(cols, r)) for r in _LIST["securities"]["data"]]
    rows = [r for r in rows if is_front(r["SECID"], prefix)]
    today = date.today().isoformat()
    fut = sorted([r for r in rows if (r.get("LASTTRADEDATE") or "") >= today],
                 key=lambda r: r["LASTTRADEDATE"])
    return fut[0]["SECID"] if fut else (rows[-1]["SECID"] if rows else None)


def fetch_all_trades(secid):
    out, start = [], 0
    while True:
        j = get_json(TRADES_URL.format(secid=secid, start=start))
        if not j:
            break
        cols = j["trades"]["columns"]
        data = j["trades"]["data"]
        if not data:
            break
        out.extend(dict(zip(cols, r)) for r in data)
        if len(data) < 1000:  # ISS trades page size
            break
        start += len(data)
        time.sleep(0.2)
    return out


def append_rows(path, fieldnames, rows):
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def main():
    today = date.today().isoformat()
    print(f"FORTS trade-tape snapshot {today}")
    block_rows, summary_rows = [], []
    for asset, prefix in ASSETS.items():
        secid = front_month(prefix)
        if not secid:
            print(f"  {asset}: no front-month")
            continue
        trades = fetch_all_trades(secid)
        if not trades:
            print(f"  {asset} {secid}: no trades")
            continue
        n = len(trades)
        total_qty = sum(t["QUANTITY"] for t in trades)
        buy_qty = sum(t["QUANTITY"] for t in trades if t["BUYSELL"] == "B")
        sell_qty = total_qty - buy_qty
        blocks = [t for t in trades if t["OFFMARKETDEAL"]]
        block_qty = sum(t["QUANTITY"] for t in blocks)
        pct = (block_qty / total_qty * 100) if total_qty else 0.0
        print(f"  {asset} {secid}: {n} trades, qty={total_qty}, "
              f"block={len(blocks)} ({pct:.2f}% qty)")
        for t in blocks:
            block_rows.append({
                "date": today, "secid": secid,
                "tradetime": t["TRADETIME"], "price": t["PRICE"],
                "quantity": t["QUANTITY"], "buysell": t["BUYSELL"],
                "tradeno": t["TRADENO"],
            })
        summary_rows.append({
            "date": today, "secid": secid, "n_trades": n,
            "total_qty": total_qty, "buy_qty": buy_qty, "sell_qty": sell_qty,
            "n_block": len(blocks), "block_qty": block_qty,
            "block_qty_pct": f"{pct:.4f}",
        })
        time.sleep(0.3)

    if block_rows:
        append_rows("forts_block_trades.csv",
                    ["date", "secid", "tradetime", "price", "quantity",
                     "buysell", "tradeno"], block_rows)
        print(f"  wrote {len(block_rows)} block-deal rows -> "
              "forts_block_trades.csv")
    else:
        print("  no off-market deals detected in today's snapshot so far")
    if summary_rows:
        append_rows("forts_trade_summary.csv",
                    ["date", "secid", "n_trades", "total_qty", "buy_qty",
                     "sell_qty", "n_block", "block_qty", "block_qty_pct"],
                    summary_rows)
        print("  wrote daily summary -> forts_trade_summary.csv")


if __name__ == "__main__":
    main()
