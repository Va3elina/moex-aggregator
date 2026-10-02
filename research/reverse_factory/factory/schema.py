"""Единый формат цели завода: сделки + паспорт + (необязательно) кривая доходности.

Все времена — наивные UTC. Сделка = одна строка ордера; ордера одной позиции
связаны pos_id (у invvo и CSV каждая строка — своя позиция).

Колонки сделок:
    sym          символ в формате Bybit linear: BTCUSDT
    side         +1 лонг, -1 шорт
    t_open       время входа (первого ордера строки)
    t_close      время выхода (NaT — позиция ещё открыта)
    p_open       цена входа позиции (у Bybit и invvo это СРЕДНЯЯ позиции)
    order_price  фактическая цена этого ордера (если источник её даёт)
    p_close      цена выхода
    size         объём в монетах
    cost         объём в USDT
    lev          плечо из карточки сделки (косметика, не реальный риск)
    pnl_pct      ход цены в сторону позиции, % (без плеча)
    pos_id       ключ позиции (ордера одной позиции)
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ROOT / "targets"
DATA = ROOT / "data"
INBOX = ROOT / "inbox"

COLS = ["sym", "side", "t_open", "t_close", "p_open", "order_price", "p_close",
        "size", "cost", "lev", "pnl_pct", "pos_id"]

MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "XAUUSDT", "XAUTUSDT", "PAXGUSDT"}


def norm_sym(s: str) -> str:
    s = str(s).upper().strip()
    s = re.sub(r"\.P$|_PERP$|PERP$", "", s)
    s = s.replace("/", "").replace("-", "").replace(":", "")
    if not s.endswith(("USDT", "USDC", "USD")):
        s += "USDT"
    return s


def slugify(name: str, fallback: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", str(name).lower()).strip("-")
    return s or fallback


def to_utc_naive(x, unit=None, fmt=None, tz_hours: float = 0.0):
    """Строки/числа → наивное UTC. tz_hours — часовой пояс источника (МСК = 3):
    время источника переводится в UTC вычитанием сдвига."""
    t = pd.to_datetime(x, unit=unit, format=fmt, errors="coerce", utc=True)
    t = t.dt.tz_localize(None) if isinstance(t, pd.Series) else t.tz_localize(None)
    if tz_hours:
        t = t - pd.Timedelta(hours=tz_hours)
    return t


def finalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in COLS:
        if c not in df:
            df[c] = np.nan
    df["sym"] = df["sym"].map(norm_sym)
    df["side"] = df["side"].astype(float).astype(int)
    for c in ["p_open", "order_price", "p_close", "size", "cost", "lev", "pnl_pct"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    need = df["pnl_pct"].isna() & df["p_open"].notna() & df["p_close"].notna()
    df.loc[need, "pnl_pct"] = 100 * df.loc[need, "side"] * (df.loc[need, "p_close"] / df.loc[need, "p_open"] - 1)
    miss = df["p_close"].isna() & df["pnl_pct"].notna() & df["p_open"].notna()
    df.loc[miss, "p_close"] = df.loc[miss, "p_open"] * (1 + df.loc[miss, "side"] * df.loc[miss, "pnl_pct"] / 100)
    df["pos_id"] = df["pos_id"].where(df["pos_id"].notna(), pd.Series(range(len(df)), index=df.index).astype(str))
    df["pos_id"] = df["pos_id"].astype(str)
    return df[COLS].sort_values("t_open").reset_index(drop=True)


def target_dir(slug: str) -> Path:
    d = TARGETS / slug
    (d / "figs").mkdir(parents=True, exist_ok=True)
    return d


def save_target(slug: str, trades: pd.DataFrame, meta: dict, equity: pd.Series | None = None) -> Path:
    d = target_dir(slug)
    finalize(trades).to_parquet(d / "trades.parquet", index=False)
    meta = {**meta, "slug": slug}
    (d / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str))
    if equity is not None and len(equity):
        equity.rename("equity").to_csv(d / "equity.csv", index_label="date")
    return d


def load_target(slug: str):
    d = TARGETS / slug
    tr = pd.read_parquet(d / "trades.parquet")
    meta = json.loads((d / "meta.json").read_text())
    eq = None
    if (d / "equity.csv").exists():
        eq = pd.read_csv(d / "equity.csv", index_col=0, parse_dates=True).iloc[:, 0]
    return tr, meta, eq


def positions(tr: pd.DataFrame) -> pd.DataFrame:
    """Ордера → позиции. Позиция: первый вход, средняя цена, выход, число ордеров,
    направление доливок (вниз — усреднение, вверх — пирамида)."""
    rows = []
    for pid, g in tr.groupby("pos_id", sort=False):
        g = g.sort_values("t_open")
        f = g.iloc[0]
        first_px = f.order_price if pd.notna(f.order_price) else f.p_open
        sizes = g["size"].tolist()
        ratios = [sizes[i] / sizes[i - 1] for i in range(1, len(sizes)) if sizes[i - 1] and pd.notna(sizes[i - 1])]
        adds = g["order_price"].iloc[1:] if g["order_price"].notna().all() else pd.Series(dtype=float)
        add_dir = np.nan
        if len(adds) and pd.notna(first_px):
            add_dir = float(np.sign(((adds / first_px - 1) * f.side).mean()))  # +1 пирамида, -1 усреднение
        avg = g["p_open"].iloc[-1]
        pnl = 100 * f.side * (g["p_close"].iloc[-1] / avg - 1) if pd.notna(avg) and avg > 0 and pd.notna(g["p_close"].iloc[-1]) else g["pnl_pct"].iloc[-1]
        rows.append(dict(pos_id=pid, sym=f.sym, side=int(f.side), t_open=g["t_open"].min(), t_close=g["t_close"].max(),
                         p_first=first_px, p_avg=avg, p_close=g["p_close"].iloc[-1], n_orders=len(g),
                         size=g["size"].sum(), cost=g["cost"].sum(), lev=f.lev, pnl_pct=pnl,
                         size_ratio=float(np.median(ratios)) if ratios else np.nan, add_dir=add_dir))
    p = pd.DataFrame(rows)
    p["hold_h"] = (p.t_close - p.t_open).dt.total_seconds() / 3600
    return p.sort_values("t_open").reset_index(drop=True)
