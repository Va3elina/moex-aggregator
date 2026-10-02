"""CryptosMX: таблица ВСЕХ его сделок из копии с условиями появления.

Для каждой сделки: время до секунды, сторона, объём (копии), цена; позиция и средняя до/после;
ход цены перед сделкой (5м/15м/1ч/4ч/24ч), место в диапазоне минуты, расстояние до хая/лоя 4ч и 24ч,
биткоин за час; шаг от прошлой покупки/продажи и время с неё; круглые уровни; ближайший пост канала до сделки;
тип заявки по ленте (лимитная/рыночная) — из cryptos_tape_classify.py, если уже посчитано.
→ inbox/private/work/cryptosmx/trades_full.parquet (+ .csv для просмотра)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

PRIV = ROOT / "inbox" / "private"
OUT = PRIV / "work" / "cryptosmx"
MANUAL = {("DOGEUSDT", "2025-02-03 05:01"), ("DOGEUSDT", "2025-02-08 11:26")}   # закрытия Вадима руками


def ret(c: pd.Series, t: pd.Timestamp, minutes: int) -> float:
    a = c.asof(t - pd.Timedelta(minutes=1)); b = c.asof(t - pd.Timedelta(minutes=minutes + 1))
    return (a / b - 1) * 100 if a == a and b == b else np.nan


def build() -> pd.DataFrame:
    L = pd.read_parquet(OUT / "ledger.parquet")
    L = L[L.sym.isin(["DOGEUSDT", "1000PEPEUSDT"])].copy()
    L["manual"] = [(s, t.strftime("%Y-%m-%d %H:%M")) in MANUAL and k == "close" for s, t, k in zip(L.sym, L.t, L.kind)]
    K = {s: market.klines(s, "1", "2025-01-10", "2025-02-12") for s in ["DOGEUSDT", "1000PEPEUSDT", "BTCUSDT"]}
    tg = pd.DataFrame(json.load(open(PRIV / "cryptos_tg.json")))
    tg["t"] = pd.to_datetime(tg.t_utc)
    rows = []
    for sym, g in L.sort_values("t", kind="stable").groupby("sym"):
        k = K[sym]; c = k.c.astype(float); h = k.h.astype(float); l = k.l.astype(float)
        bc = K["BTCUSDT"].c.astype(float)
        last = {"open": None, "close": None}
        pos_before = 0.0; avg_before = np.nan
        for r in g.itertuples():
            m = r.t.floor("min")
            bar = k.loc[m] if m in k.index else None
            pre = k.loc[r.t - pd.Timedelta(hours=24): r.t - pd.Timedelta(minutes=1)]
            pre4 = pre.loc[r.t - pd.Timedelta(hours=4):]
            lb, ls = last["open"], last["close"]
            rows.append(dict(
                t=r.t, sym=sym, side="buy" if r.kind == "open" else "sell", manual=r.manual, qty=r.qty, px=r.px,
                pos_before=pos_before, pos_after=r.pos_after, avg_before=avg_before, avg_after=r.avg,
                vs_avg=(r.px / avg_before - 1) * 100 if avg_before == avg_before else np.nan,
                r5=ret(c, r.t, 5), r15=ret(c, r.t, 15), r60=ret(c, r.t, 60), r240=ret(c, r.t, 240), r1440=ret(c, r.t, 1440),
                btc60=ret(bc, r.t, 60),
                from_hi4=(r.px / pre4.h.max() - 1) * 100 if len(pre4) else np.nan,
                from_lo4=(r.px / pre4.l.min() - 1) * 100 if len(pre4) else np.nan,
                from_hi24=(r.px / pre.h.max() - 1) * 100 if len(pre) else np.nan,
                from_lo24=(r.px / pre.l.min() - 1) * 100 if len(pre) else np.nan,
                bar_pos=((r.px - bar.l) / (bar.h - bar.l)) if bar is not None and bar.h > bar.l else np.nan,
                vs_last_buy=(r.px / lb[1] - 1) * 100 if lb else np.nan, min_since_buy=(r.t - lb[0]).total_seconds() / 60 if lb else np.nan,
                vs_last_sell=(r.px / ls[1] - 1) * 100 if ls else np.nan, min_since_sell=(r.t - ls[0]).total_seconds() / 60 if ls else np.nan,
            ))
            last[r.kind] = (r.t, r.px)
            pos_before, avg_before = r.pos_after, r.avg
    T = pd.DataFrame(rows)
    # круглые уровни: расстояние до ближайшего «круглого» числа (DOGE — 0.005, PEPE — 0.0005), в %
    step = np.where(T.sym == "DOGEUSDT", 0.005, 0.0005)
    T["round_dist"] = (np.abs(T.px / step - np.round(T.px / step)) * step / T.px * 100)
    # ближайший пост канала до сделки (48 ч) с упоминанием монеты
    key = {"DOGEUSDT": r"dog|доги|доге|дож", "1000PEPEUSDT": r"pepe|пепе"}
    posts = []
    for r in T.itertuples():
        x = tg[(tg.t <= r.t) & (tg.t >= r.t - pd.Timedelta(hours=48)) & tg.text.str.contains(key[r.sym], case=False, regex=True)]
        posts.append((x.t.iloc[-1], x.text.iloc[-1][:200]) if len(x) else (pd.NaT, ""))
    T["post_t"] = [p[0] for p in posts]; T["post"] = [p[1] for p in posts]
    T["hrs_after_post"] = (T.t - T.post_t).dt.total_seconds() / 3600
    cls = OUT / "tape_class.parquet"
    if cls.exists():
        C = pd.read_parquet(cls)[["t", "sym", "side", "copy_qty", "order", "master_px", "lag_ms", "burst_n", "burst_ratio", "opp_share", "toward", "copy_px"]].rename(columns={"copy_qty": "qty", "copy_px": "px"}).drop_duplicates(["t", "sym", "side", "qty", "px"])
        T = T.merge(C, on=["t", "sym", "side", "qty", "px"], how="left")
    return T


if __name__ == "__main__":
    T = build()
    T.to_parquet(OUT / "trades_full.parquet", index=False)
    T.to_csv(OUT / "trades_full.csv", index=False)
    print(T.groupby(["sym", "side", "manual"]).size())
