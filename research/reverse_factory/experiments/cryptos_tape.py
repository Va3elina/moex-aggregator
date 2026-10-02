"""CryptosMX по ленте Bybit (public.bybit.com/trading): каждая его сделка до долей секунды.

Как видно мастера в ленте: когда исполняется его заявка, биржа тут же ставит рыночные заявки всем подписчикам —
в ленте «всплеск»: сотни–тысячи мелких сделок одной стороны за ~100 мс. Копия Вадима — одна из них.
  * Лимитная заявка мастера: перед всплеском идёт сделка ПРОТИВОПОЛОЖНОЙ стороны по его цене (кто-то «съел» заявку).
  * Рыночная заявка мастера: перед всплеском — крупная сделка той же стороны.
Скрипт: по дням качает ленту DOGE / 1000PEPE, вырезает окна вокруг сделок копии и находит ВСЕ всплески за день
(не только совпавшие с копией), файлы дня удаляет. Итог — inbox/private/work/cryptosmx/tape_*.parquet.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PRIV = ROOT / "inbox" / "private" / "work" / "cryptosmx"
TMP = ROOT / "data" / "ticks"
URL = "https://public.bybit.com/trading/{s}/{s}{d}.csv.gz"
MIN_N = 120          # сделок одной стороны в окне 200 мс — «всплеск»


def day_ticks(sym: str, day: str) -> pd.DataFrame:
    TMP.mkdir(parents=True, exist_ok=True)
    f = TMP / f"{sym}{day}.csv.gz"
    if not f.exists():
        subprocess.run(["curl", "-s", "-o", str(f), URL.format(s=sym, d=day)], check=True)
    T = pd.read_csv(f, usecols=["timestamp", "side", "size", "price"])
    T["ts"] = pd.to_datetime((T.timestamp * 1e6).round().astype("int64"), unit="us")
    return T.drop(columns="timestamp").sort_values("ts", kind="stable").reset_index(drop=True)


def bursts(T: pd.DataFrame) -> pd.DataFrame:
    """Всплески: окна 200 мс, где сделок одной стороны ≥ MIN_N. Соседние окна склеиваются."""
    out = []
    for side, g in T.groupby("side"):
        b = g.ts.dt.floor("200ms")
        agg = g.groupby(b).agg(n=("size", "size"), qty=("size", "sum"), p0=("price", "first"), p1=("price", "last"),
                               pmin=("price", "min"), pmax=("price", "max"), med=("size", "median"))
        agg = agg[agg.n >= MIN_N]
        if agg.empty:
            continue
        grp = (agg.index.to_series().diff() > pd.Timedelta("400ms")).cumsum()
        gi = g.set_index("ts")
        for _, a in agg.groupby(grp.values):
            t0, t1 = a.index[0], a.index[-1] + pd.Timedelta("200ms")
            w = gi.loc[t0:t1]
            orders = w.groupby(level=0)["size"].sum()          # сделки одной заявки-тейкера имеют одно время
            top = np.sort(orders.values)[::-1][:60]
            out.append(dict(t=a.index[0], side=side, n=int(a.n.sum()), n_orders=int(len(orders)), qty=float(a.qty.sum()),
                            p0=a.p0.iloc[0], p1=a.p1.iloc[-1], pmin=a.pmin.min(), pmax=a.pmax.max(), med=float(a.med.median()),
                            top=top.astype("float32").tolist()))
    return pd.DataFrame(out)


if __name__ == "__main__":
    L = pd.read_parquet(PRIV / "ledger.parquet")
    L = L[L.sym.isin(["DOGEUSDT", "1000PEPEUSDT"])]
    days = sorted({(s, t.strftime("%Y-%m-%d")) for s, t in zip(L.sym, L.t)})
    # плюс по дню до и после окна — чтобы видеть его заявки, которые копия не повторила
    extra = []
    for s in ["DOGEUSDT", "1000PEPEUSDT"]:
        ds = sorted(d for ss, d in days if ss == s)
        for d in pd.date_range(pd.Timestamp(ds[0]) - pd.Timedelta(days=2), pd.Timestamp(ds[-1]) + pd.Timedelta(days=2)).strftime("%Y-%m-%d"):
            extra.append((s, d))
    days = sorted(set(days) | set(extra))
    done_b = PRIV / "tape_bursts.parquet"; done_w = PRIV / "tape_windows.parquet"
    B = [pd.read_parquet(done_b)] if done_b.exists() else []
    W = [pd.read_parquet(done_w)] if done_w.exists() else []
    have = set(zip(B[0].sym, B[0].t.dt.strftime("%Y-%m-%d"))) if B else set()
    for sym, day in days:
        if (sym, day) in have:
            continue
        T = day_ticks(sym, day)
        b = bursts(T); b["sym"] = sym; B.append(b)
        mine = L[(L.sym == sym) & (L.t.dt.strftime("%Y-%m-%d") == day)]
        for r in mine.itertuples():
            w = T[(T.ts >= r.t - pd.Timedelta(seconds=12)) & (T.ts <= r.t + pd.Timedelta(seconds=4))].copy()
            w["fill_t"] = r.t; w["sym"] = sym
            W.append(w)
        (TMP / f"{sym}{day}.csv.gz").unlink(missing_ok=True)
        print(sym, day, "сделок в ленте", len(T), "всплесков", len(b), "его исполнений", len(mine), flush=True)
        pd.concat(B, ignore_index=True).to_parquet(done_b)
        if W:
            pd.concat(W, ignore_index=True).to_parquet(done_w)
