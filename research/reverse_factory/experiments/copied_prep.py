"""Подготовка личных выгрузок Вадима (данные только локально, inbox/private — в .gitignore).

1. Позиции, скопированные у мастеров (Bybit copy trading): история P&L (мастер, монета, даты, цены, P&L) +
   журнал исполнений (точное время каждого ордера). Order No. истории = Order ID ордера открытия в журнале.
   Время выхода — закрывающие исполнения той же монеты в дату закрытия с ближайшей ценой.
   → inbox/private/copied_positions.parquet, и цели завода по мастерам (targets/copy-<мастер>).
2. Telegram-канал Cryptos_Mx (HTML-выгрузка) → inbox/private/cryptos_tg.json: дата UTC, текст, фото.
"""
from __future__ import annotations

import glob
import html
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory.schema import finalize, save_target, slugify   # noqa: E402

PRIV = ROOT / "inbox" / "private"
TG = Path("/Users/vadim/Downloads/Telegram Desktop/ChatExport_2026-09-26")


def positions() -> pd.DataFrame:
    P = pd.read_csv(glob.glob(str(PRIV / "tradingTools*" / "*.csv"))[0], skiprows=1)
    A = pd.concat([pd.read_csv(f, skiprows=1) for f in glob.glob(str(PRIV / "AssetChange*" / "*.csv"))])
    T = A[A.Type == "Trade"].copy().reset_index(drop=True)
    T["t"] = pd.to_datetime(T.Time)
    opens = T[T.Direction.str.startswith("Open")].groupby("Order ID").agg(t_open=("t", "min"), px_open=("Filled Price", "mean"),
                                                                           dir=("Direction", "first"))
    closes = T[T.Direction.str.startswith("Close")].copy()
    closes["d"] = closes.t.dt.normalize()
    P = P.merge(opens, left_on="Order No.", right_index=True, how="left")
    P["side"] = np.where(P.dir.fillna("Open Long").str.contains("Short"), -1, 1)
    # точное время закрытия: закрывающее исполнение той же монеты в дату закрытия, цена ближе всего
    groups = {k: g for k, g in closes.groupby(["Contract", "d"])}
    t_close = []
    for r in P.to_dict("records"):
        g = groups.get((r["Positions"], pd.Timestamp(r["Closed On"]).normalize()))
        if g is None or g.empty:
            t_close.append(pd.NaT); continue
        j = (g["Filled Price"] - r["Closing Price"]).abs().idxmin()
        t_close.append(g.loc[j, "t"])
    P["t_close"] = t_close
    P["t_close"] = P.t_close.fillna(pd.to_datetime(P["Closed On"]) + pd.Timedelta("23:59:00"))
    P["t_open"] = P.t_open.fillna(pd.to_datetime(P["Opened On"]))
    P = P.rename(columns={"Master Trader": "master", "Positions": "sym", "Qty": "qty", "Entry Price": "entry",
                          "Closing Price": "close", "Close By": "close_by", "Closed P&L": "pnl_usd", "Fees": "fees"})
    P["pnl_pct"] = 100 * P.side * (P.close / P.entry - 1)
    P["exact_open"] = P.px_open.notna()
    return P[["master", "sym", "side", "qty", "t_open", "t_close", "entry", "close", "pnl_pct", "pnl_usd", "fees", "close_by", "exact_open", "Order No."]]


def telegram() -> list[dict]:
    out = []
    for f in sorted(TG.glob("messages*.html"), key=lambda p: (len(p.name), p.name)):
        h = f.read_text(encoding="utf-8")
        for block in re.split(r'(?=<div class="message default clearfix)', h)[1:]:
            m = re.search(r'title="(\d\d\.\d\d\.\d{4} \d\d:\d\d:\d\d) UTC([+-]\d\d):(\d\d)"', block)
            if not m:
                continue
            t = pd.to_datetime(m.group(1), format="%d.%m.%Y %H:%M:%S") - pd.Timedelta(hours=int(m.group(2)))
            txt = re.search(r'<div class="text">(.*?)</div>\s*(?:</div>|<div class="(?:signature|reactions))', block, re.S)
            text = html.unescape(re.sub(r"<br\s*/?>", "\n", txt.group(1))) if txt else ""
            text = re.sub(r"<[^>]+>", "", text).strip()
            photos = [str(TG / p) for p in re.findall(r'href="(photos/[^"]+?\.jpg)"', block)]
            mid = re.search(r'id="message(\d+)"', block)
            out.append(dict(id=int(mid.group(1)) if mid else None, t_utc=str(t), text=text, photos=photos))
    return out


if __name__ == "__main__":
    P = positions()
    P.to_parquet(PRIV / "copied_positions.parquet", index=False)
    print(P.groupby("master").agg(позиций=("sym", "size"), точный_вход=("exact_open", "mean"), с=("t_open", "min"), по=("t_close", "max"),
                                   лонгов=("side", lambda s: (s > 0).mean()), побед=("pnl_pct", lambda s: (s > 0).mean()),
                                   ликвидаций=("close_by", lambda s: (s == "ClosedByLiq").sum()), P_L=("pnl_usd", "sum")).round(2).to_string())
    for master, g in P.groupby("master"):
        if len(g) < 30:
            continue
        df = pd.DataFrame(dict(sym=g.sym, side=g.side, t_open=g.t_open, t_close=g.t_close, p_open=g.entry, order_price=g.entry,
                               p_close=g.close, size=g.qty, cost=g.qty * g.entry, lev=np.nan, pnl_pct=g.pnl_pct, pos_id=g["Order No."]))
        slug = "copy-" + slugify(master, "master")
        save_target(slug, finalize(df), dict(name=f"{master} (мои копии)", source="bybit-copy-export", source_id=master, url="",
                                              description="", time_note="UTC; вход — точное время ордера из журнала, выход — ближайшее закрывающее исполнение",
                                              price_note="цены входа/выхода из истории копирования (средние позиции)",
                                              history_note="только период подписки Вадима"))
        print("цель завода:", slug, len(g))
    msgs = telegram()
    json.dump(msgs, open(PRIV / "cryptos_tg.json", "w"), ensure_ascii=False)
    tg = pd.DataFrame(msgs)
    tg["t"] = pd.to_datetime(tg.t_utc)
    print(f"\nTelegram: {len(tg)} сообщений, {tg.t.min()} → {tg.t.max()}, с фото {(tg.photos.str.len() > 0).sum()}, с текстом {(tg.text.str.len() > 0).sum()}")
    print(tg.groupby(tg.t.dt.year).size().to_dict())
