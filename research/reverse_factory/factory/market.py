"""Свечи Bybit (linear perpetual) с кэшем в data/klines/<SYM>_<interval>.parquet.

Индекс — время ОТКРЫТИЯ свечи, наивное UTC. Колонки o h l c v.
Публичный API Bybit работает без браузера (в отличие от витрины копитрейдинга).
"""
from __future__ import annotations

import json
import os
import time
import urllib.request

import pandas as pd

from .schema import DATA

MIN = {"1": 1, "3": 3, "5": 5, "15": 15, "30": 30, "60": 60, "120": 120, "240": 240,
       "360": 360, "720": 720, "D": 1440}
_missing: set[str] = set()


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["o", "h", "l", "c", "v"], index=pd.DatetimeIndex([], name="ts"), dtype=float)


def tf_to_interval(minutes: int) -> str:
    for k, v in MIN.items():
        if v == minutes:
            return k
    # ближайший меньший стандартный
    return max((k for k, v in MIN.items() if v <= minutes), key=lambda k: MIN[k], default="1")


def _call(sym: str, interval: str, end_ms: int, category="linear"):
    u = (f"https://api.bybit.com/v5/market/kline?category={category}&symbol={sym}"
         f"&interval={interval}&limit=1000&end={end_ms}")
    for a in range(6):
        try:
            r = json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
            if r.get("retCode") == 0:
                return r["result"]["list"]
            if "symbol" in str(r.get("retMsg", "")).lower() or r.get("retCode") == 10001:
                return "INVALID"                     # биржа сказала: такого контракта нет
        except Exception:
            pass
        time.sleep(2 + 3 * a)
    return None


def _fetch(sym: str, interval: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows, end_ms = [], int(end.timestamp() * 1000)
    start_ms = int(start.timestamp() * 1000)
    while True:
        L = _call(sym, interval, end_ms)
        if L == "INVALID":                           # навсегда пропускаем только несуществующий контракт
            _missing.add(sym)
            break
        if L is None:                                # сбой сети после повторов — в следующий раз попробуем снова
            break
        if not L:
            break
        rows += L
        oldest = int(L[-1][0])
        if oldest <= start_ms or len(L) < 1000:
            break
        end_ms = oldest - 1
        time.sleep(0.12)
    if not rows:
        return _empty()
    d = pd.DataFrame(rows, columns=["ts", "o", "h", "l", "c", "v", "t"]).astype(float)
    d["ts"] = pd.to_datetime(d.ts, unit="ms")
    return d.drop_duplicates("ts").set_index("ts").sort_index()[["o", "h", "l", "c", "v"]]


def klines(sym: str, interval: str, start, end=None) -> pd.DataFrame:
    """Свечи за [start, end]. Докачивает недостающее и хранит в кэше."""
    if sym in _missing:
        return _empty()
    start = pd.Timestamp(start)
    end = pd.Timestamp(end) if end is not None else pd.Timestamp.now(tz="UTC").tz_localize(None)
    path = DATA / "klines" / f"{sym}_{interval}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    cache = pd.read_parquet(path) if path.exists() else _empty()
    step = pd.Timedelta(minutes=MIN[interval])
    parts = [cache]
    if cache.empty:
        parts.append(_fetch(sym, interval, start, end))
    else:
        if start < cache.index.min() - step:
            parts.append(_fetch(sym, interval, start, cache.index.min()))
        if end > cache.index.max() + 2 * step:
            parts.append(_fetch(sym, interval, cache.index.max(), end))
    parts = [x for x in parts if len(x)]
    if not parts:
        return _empty()
    full = pd.concat(parts)
    if len(full):
        full = full[~full.index.duplicated(keep="last")].sort_index()
        if len(full) != len(cache):
            tmp = path.with_suffix(f".parquet.{os.getpid()}.tmp")      # атомарная запись: читатели не увидят недописанный файл
            full.to_parquet(tmp)
            tmp.replace(path)
    return full[(full.index >= start - step) & (full.index <= end)]


def available(sym: str) -> bool:
    if sym in _missing:
        return False
    return _call(sym, "D", int(time.time() * 1000)) not in (None, [], "INVALID")
