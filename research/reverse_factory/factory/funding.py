"""История ставки финансирования Bybit (linear perpetual), кэш data/funding/<SYM>.parquet.
Индекс — время начисления (UTC, наивное), колонка rate (доля за период; плюс — лонги платят шортам)."""
from __future__ import annotations

import json
import os
import time
import urllib.request

import pandas as pd

from .schema import DATA


def _page(sym: str, end_ms: int):
    u = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={sym}&limit=200&endTime={end_ms}"
    for a in range(5):
        try:
            r = json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
            if r.get("retCode") == 0:
                return r["result"]["list"]
        except Exception:
            pass
        time.sleep(2 + 3 * a)
    return []


def funding(sym: str, start, end=None) -> pd.Series:
    start = pd.Timestamp(start)
    end = pd.Timestamp(end) if end is not None else pd.Timestamp.now(tz="UTC").tz_localize(None)
    path = DATA / "funding" / f"{sym}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    cache = pd.read_parquet(path).rate if path.exists() else pd.Series(dtype=float)
    need = cache.empty or cache.index.min() > start + pd.Timedelta(hours=8) or cache.index.max() < end - pd.Timedelta(hours=9)
    if need:
        rows, end_ms = [], int(end.timestamp() * 1000)
        while True:
            L = _page(sym, end_ms)
            if not L:
                break
            rows += L
            oldest = int(L[-1]["fundingRateTimestamp"])
            if oldest <= int(start.timestamp() * 1000) or len(L) < 200:
                break
            end_ms = oldest - 1
            time.sleep(0.15)
        if rows:
            s = pd.Series({pd.to_datetime(int(r["fundingRateTimestamp"]), unit="ms"): float(r["fundingRate"]) for r in rows}).sort_index()
            cache = pd.concat([cache, s]); cache = cache[~cache.index.duplicated(keep="last")].sort_index()
            tmp = path.with_suffix(f".parquet.{os.getpid()}.tmp")
            cache.rename("rate").to_frame().to_parquet(tmp)
            tmp.replace(path)
    return cache[(cache.index >= start) & (cache.index <= end)]
