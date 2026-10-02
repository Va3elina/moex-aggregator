"""CSV-источники.

1. TradingView → «Тестер стратегий» → «Список сделок» → выгрузка CSV.
   Строки идут парами (вход/выход) с общим номером сделки. Время — в часовом поясе
   графика (для Bybit обычно UTC, для MOEX — Москва, --tz 3).
2. Произвольный CSV: --map sym=...,side=...,p_open=...,p_close=...,t_open=...,t_close=...
   side: 1/-1 или long/short/buy/sell; время — строка или число (--time-unit s|ms).
"""
from __future__ import annotations

import re

import numpy as np
import pandas as pd

from ..schema import finalize, to_utc_naive


def _side(v) -> int:
    s = str(v).strip().lower()
    if re.search(r"short|шорт|корот|sell|прода|^-1", s):
        return -1
    return 1


def _find(cols, *keys):
    for c in cols:
        lc = c.lower()
        if all(k in lc for k in keys):
            return c
    return None


def tradingview(path: str, sym: str, tz_hours: float = 0.0) -> pd.DataFrame:
    D = pd.read_csv(path)
    cols = list(D.columns)
    c_no = _find(cols, "#") or _find(cols, "№") or _find(cols, "trade") or _find(cols, "сделк")
    c_type = _find(cols, "type") or _find(cols, "тип")
    c_time = _find(cols, "date") or _find(cols, "дата") or _find(cols, "время")
    c_px = next((c for c in cols if re.match(r"(price|цена)", c.lower())), None)
    c_qty = _find(cols, "contract") or _find(cols, "контракт") or _find(cols, "qty") or _find(cols, "size") or _find(cols, "размер")
    if not all([c_no, c_type, c_time, c_px]):
        raise SystemExit(f"не узнал колонки TradingView: {cols}")
    D["_entry"] = D[c_type].astype(str).str.lower().str.contains("entry|вход")
    rows = []
    for no, g in D.groupby(c_no):
        e, x = g[g._entry], g[~g._entry]
        if e.empty:
            continue
        e = e.iloc[0]
        x = x.iloc[0] if len(x) else None
        rows.append(dict(sym=sym, side=_side(e[c_type]),
                         t_open=e[c_time], t_close=None if x is None else x[c_time],
                         p_open=e[c_px], order_price=e[c_px], p_close=None if x is None else x[c_px],
                         size=e[c_qty] if c_qty else np.nan, pos_id=f"tv{no}"))
    df = pd.DataFrame(rows)
    df["t_open"] = to_utc_naive(df.t_open, tz_hours=tz_hours)
    df["t_close"] = to_utc_naive(df.t_close, tz_hours=tz_hours)
    return finalize(df)


def generic(path: str, mapping: dict, time_unit: str | None = None, tz_hours: float = 0.0) -> pd.DataFrame:
    D = pd.read_csv(path)
    df = pd.DataFrame({k: D[v] for k, v in mapping.items()})
    if "side" in df:
        df["side"] = df["side"].map(_side)
    for c in ["t_open", "t_close"]:
        if c in df:
            df[c] = to_utc_naive(df[c], unit=time_unit, tz_hours=tz_hours)
    if "order_price" not in df and "p_open" in df:
        df["order_price"] = df["p_open"]
    return finalize(df)
