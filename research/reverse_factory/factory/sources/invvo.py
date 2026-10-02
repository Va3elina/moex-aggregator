"""invvo.com — открытый API, ключ не нужен, curl работает.

Проверено 26.09.2026:
  * время на сайте — UTC (сверка цены входа со свечами Bybit по 160 сделкам: ошибка 0,05%);
  * entry_price отформатирован ("80.20 K") — точная цена = cost / cost_lots, и это СРЕДНЯЯ
    позиции: стратегии доливаются, а мульти-стратегии (Algotoria: «14 стратегий») неттируются
    в одну позицию по монете;
  * сигналы отдаются по 6 на страницу; кривая доходности — graph/extended (tw = доходность, %).
"""
from __future__ import annotations

import json
import time
import urllib.request

import numpy as np
import pandas as pd

from ..schema import finalize, to_utc_naive

BASE = "https://invvo.com/api/v1"


def _get(url: str):
    for a in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            return json.load(urllib.request.urlopen(req, timeout=30))
        except Exception:
            time.sleep(2 + 2 * a)
    return None


def meta(sid: int) -> dict | None:
    d = (_get(f"{BASE}/strategy/id?strategy_id={sid}&lang=ru&") or {}).get("result", {}).get("data")
    return d if d and d.get("name") else None


def signals(sid: int) -> list[dict]:
    out = []
    for state in ["close", "open"]:
        p = 0
        while True:
            r = _get(f"{BASE}/strategy/signal?strategy_id={sid}&page={p}&type={state}&webp=1&") or {}
            s = ((r.get("result") or {}).get("data") or {}).get("signals") or []
            if not s:
                break
            for x in s:
                x["_state"] = state
            out += s
            p += 1
            time.sleep(0.15)
    return out


def equity(sid: int, start: str) -> tuple[pd.Series, dict]:
    u = f"{BASE}/strategy/graph/extended?strategy_id={sid}&scale=day&from_date={start}&to_date={pd.Timestamp.now(tz="UTC"):%Y-%m-%d}&webp=1&"
    d = (_get(u) or {}).get("result", {}).get("data") or {}
    if not d.get("tw"):
        return pd.Series(dtype=float), {}
    tw = np.array(d["tw"], float)
    idx = pd.date_range(d["date"]["start"], periods=len(tw), freq="D")
    eq = pd.Series(1 + tw / 100, idx)
    b = (d.get("s") or {}).get("basic") or {}
    num = lambda v: float(str(v).replace(" ", "")) if v not in (None, "") else np.nan
    extra = dict(twr_pct=num(b.get("twr")), max_dd_pct=num(b.get("max_draw_down")),
                 real_leverage_max=float(np.max(np.array(d.get("dl") or [0], float))),
                 profitable_pct=num(((d.get("s") or {}).get("signals") or {}).get("profitable_signals_percent")))
    return eq, extra


def _price(s) -> float:
    s = str(s).replace(" ", "")
    mult = 1e3 if s.endswith("K") else 1e6 if s.endswith("M") else 1
    try:
        return float(s.rstrip("KM")) * mult
    except ValueError:
        return np.nan


def to_trades(sig: list[dict]) -> pd.DataFrame:
    S = pd.DataFrame(sig)
    if S.empty:
        return S
    cost = pd.to_numeric(S["cost"], errors="coerce")
    lots = pd.to_numeric(S["cost_lots"], errors="coerce")
    p_open = (cost / lots).where(lots > 0, S["entry_price"].map(_price))
    side = np.where(S["side"].str.lower() == "long", 1, -1)
    closed = S["_state"] == "close"
    pnl = pd.to_numeric(S["profit_diff_percent"], errors="coerce").where(closed)
    df = pd.DataFrame(dict(
        sym=S["symbol"], side=side,
        t_open=to_utc_naive(S["created_at"], fmt="%d.%m.%Y, %H:%M"),
        t_close=to_utc_naive(S["closed_at"].where(closed), fmt="%d.%m.%Y, %H:%M"),
        p_open=p_open, order_price=np.nan, p_close=np.nan, size=lots, cost=cost,
        lev=pd.to_numeric(S["leverage"], errors="coerce"), pnl_pct=pnl,
        pos_id=S["signal_id"].astype(str)))
    return finalize(df)


def build(sid: int, dump: dict | None = None) -> tuple[pd.DataFrame, dict, pd.Series]:
    """Скачать (или взять из готовой выгрузки) стратегию invvo."""
    if dump and str(sid) in dump:
        m, sig = dump[str(sid)]["meta"], dump[str(sid)]["signals"]
    else:
        m = meta(sid)
        if not m:
            raise SystemExit(f"invvo: стратегии {sid} нет")
        sig = signals(sid)
    tr = to_trades(sig)
    start = str(m.get("run_date") or tr.t_open.min())[:10]
    eq, extra = equity(sid, start)
    info = m.get("info") or {}
    desc = info.get("description") or {}
    text = " ".join(str(desc.get(k) or "") for k in ["strategy", "trader"]).strip()
    import re
    n_strat = re.search(r"(\d+)\s+(?:uncorrelated|независим|некоррел|strateg|стратег)", text, re.I)
    meta_out = dict(
        name=m["name"], source="invvo", source_id=sid, url=f"https://invvo.com/strategy/{sid}",
        description=text, tags=info.get("tags"), active=m.get("is_active"), run_date=m.get("run_date"),
        exchange=m.get("network_type"), market=m.get("network_order_type"), **extra,
        time_note="UTC (проверено сверкой цен со свечами)",
        price_note="p_open = средняя позиции (cost/lots): доливки внутри, при нескольких стратегиях — неттинг",
        netted_suspect=bool(n_strat and int(n_strat.group(1)) > 1),
        history_note="полная история с запуска")
    return tr, meta_out, eq


def list_ids(max_id: int = 100) -> list[tuple[int, str]]:
    out = []
    for sid in range(1, max_id + 1):
        m = meta(sid)
        if m:
            out.append((sid, m["name"]))
        time.sleep(0.2)
    return out
