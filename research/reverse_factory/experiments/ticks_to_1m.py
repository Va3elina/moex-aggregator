"""Минутные свечи из архива сделок Bybit (public.bybit.com/trading) — для монет, снятых с торгов (минуток в API нет)."""
import subprocess, sys
from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
def build(sym, a, b):
    out = ROOT / f"data/klines_seg/{sym}_ticks_{a}_{b}.parquet"
    if out.exists(): return pd.read_parquet(out)
    parts = []
    for d in pd.date_range(a, b, freq="D").strftime("%Y-%m-%d"):
        f = ROOT / f"data/ticks/{sym}{d}.csv.gz"
        r = subprocess.run(["curl", "-s", "-f", "-o", str(f), f"https://public.bybit.com/trading/{sym}/{sym}{d}.csv.gz"])
        if r.returncode != 0: continue
        T = pd.read_csv(f, usecols=["timestamp", "size", "price"]); f.unlink()
        T["ts"] = pd.to_datetime(T.timestamp, unit="s").dt.floor("min")
        g = T.sort_values("timestamp").groupby("ts")
        parts.append(pd.DataFrame(dict(o=g.price.first(), h=g.price.max(), l=g.price.min(), c=g.price.last(), v=g["size"].sum())))
    k = pd.concat(parts).sort_index()
    k = k.reindex(pd.date_range(k.index.min(), k.index.max(), freq="min")); k["c"] = k.c.ffill()
    for x in "ohl": k[x] = k[x].fillna(k.c)
    k["v"] = k.v.fillna(0); k.index.name = "ts"
    k.to_parquet(out); return k
if __name__ == "__main__":
    for sym, a, b in (("LUNAUSDT", "2022-04-26", "2022-05-12"), ("FTTUSDT", "2022-10-28", "2022-11-13"), ("SRMUSDT", "2022-10-28", "2022-11-15")):
        k = build(sym, a, b); print(sym, len(k), k.index.min(), k.index.max(), f"{k.c.iloc[0]:.4g} → {k.c.iloc[-1]:.4g}", flush=True)
