"""CryptosMX: ищем его лимитки прямо в стакане Bybit (история стакана ob500, quote-saver.bycsi.com).

Идея: лесенку он выставляет пачкой одинаковых заявок. В потоке изменений стакана это видно как одно сообщение,
где на нескольких уровнях объём вырос на ОДНО И ТО ЖЕ число. Если такой уровень потом совпадает с ценой его
сделки — мы нашли момент, когда он выставил лесенку, её размер и все уровни (в том числе те, что не исполнились).
Ограничение: ob500 — это ±500 шагов цены (для DOGE около ±1.3%), дальние уровни в момент выставления не видны.

Выход: ob_ladders_<день>.parquet — все «пачки одинаковых прибавок» (≥3 уровня), ob_levels_<день>.parquet —
история прибавок на ценах его сделок.
"""
from __future__ import annotations

import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import orjson
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"
OB = ROOT / "data" / "ob"
URL = "https://quote-saver.bycsi.com/orderbook/linear/{s}/{d}_{s}_ob500.data.zip"


def scan(sym: str, day: str, targets: dict[str, set[str]], min_levels: int = 3):
    f = OB / f"{sym}_{day}.zip"
    if not f.exists():
        subprocess.run(["curl", "-s", "-o", str(f), URL.format(s=sym, d=day)], check=True)
    p = subprocess.Popen(["unzip", "-p", str(f)], stdout=subprocess.PIPE, bufsize=1 << 22)
    book = {"b": {}, "a": {}}
    ladders, hist = [], []
    n = 0
    for line in p.stdout:
        m = orjson.loads(line)
        n += 1
        d = m["data"]; ts = m["ts"]
        if m["type"] == "snapshot":
            book = {"b": {px: float(q) for px, q in d["b"]}, "a": {px: float(q) for px, q in d["a"]}}
            continue
        for side in ("b", "a"):
            bk = book[side]
            incs = defaultdict(list)
            for px, q in d[side]:
                q = float(q); old = bk.get(px, 0.0)
                inc = q - old
                if q == 0:
                    bk.pop(px, None)
                else:
                    bk[px] = q
                if px in targets[side]:
                    hist.append((ts, side, px, old, q))
                if inc > 0 and old > 0:            # новый объём на уже видимом уровне
                    incs[inc].append(px)
            for inc, pxs in incs.items():
                if len(pxs) >= min_levels:
                    ladders.append((ts, side, inc, len(pxs), ",".join(sorted(pxs))))
    p.wait()
    f.unlink(missing_ok=True)
    L = pd.DataFrame(ladders, columns=["ts", "side", "inc", "n", "prices"])
    H = pd.DataFrame(hist, columns=["ts", "side", "px", "old", "new"])
    for x in (L, H):
        if len(x):
            x["t"] = pd.to_datetime(x.ts, unit="ms")
    return L, H, n


if __name__ == "__main__":
    sym, day = sys.argv[1], sys.argv[2]
    T = pd.read_parquet(OUT / "trades_full.parquet")
    T = T[(T.sym == sym) & (T.t.dt.strftime("%Y-%m-%d").isin([day, (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")]))]
    T["mpx"] = T.master_px.fillna(T.px)
    targets = {"b": set(), "a": set()}
    for s, px in zip(T.side, T.mpx):
        for dt in (-2, -1, 0, 1, 2):                      # его цена ±2 шага (оценка цены мастера неточна)
            targets["b" if s == "buy" else "a"].add(f"{px + dt * 1e-5:.5f}")
    L, H, n = scan(sym, day, targets)
    L.to_parquet(OUT / f"ob_ladders_{sym}_{day}.parquet", index=False)
    H.to_parquet(OUT / f"ob_levels_{sym}_{day}.parquet", index=False)
    print(f"сообщений {n}, пачек одинаковых прибавок (≥3 уровня): {len(L)}, записей по его ценам: {len(H)}")
