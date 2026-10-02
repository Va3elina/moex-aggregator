"""Шорты: зеркало не сработало — пробуем шорт только в медвежьем режиме (биткоин ниже дневной EMA50)
и шорт при перегретом финансировании (толпа в лонгах). Для сравнения — лонг в бычьем режиме."""
import multiprocessing as mp, sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grid_engine as ge
PER = {"2021-07-01|2024-03-01": ["DOGEUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "BTCUSDT", "ETHUSDT"],
       "2024-03-01|2026-09-26": ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT", "BTCUSDT", "ETHUSDT"]}
V = {"шорт-зеркало, фильтр": dict(side=-1, btc_filter=True),
     "шорт, только медвежий режим": dict(side=-1, btc_filter=True, regime="медвежий"),
     "шорт, финансирование ≥0.03%": dict(side=-1, btc_filter=True, funding_min=0.0003),
     "шорт, медвежий + финансирование ≥0.01%": dict(side=-1, btc_filter=True, regime="медвежий", funding_min=0.0001),
     "лонг, фильтр": dict(side=1, btc_filter=True),
     "лонг, фильтр, только бычий режим": dict(side=1, btc_filter=True, regime="бычий")}
C = {}
def init():
    pass
def task(a):
    per, sym, name, kw = a
    A, B = per.split("|")
    key = (per, sym)
    if key not in C: C[key] = ge.Coin(sym, A, B)
    doge = C.get((per, "DOGEUSDT")) or ge.Coin("DOGEUSDT", A, B); C[(per, "DOGEUSDT")] = doge
    scale = C[key].daily_range / doge.daily_range if sym in ("BTCUSDT", "ETHUSDT") else 1.0
    E, Cp, liq, tr = ge.run(C[key], ge.P(scale=scale, **kw))
    return dict(период=per, вариант=name, монета=sym, **ge.yearly(E), ликв=int(liq is not None), E=E)
if __name__ == "__main__":
    jobs = [(per, s, n, kw) for per, syms in PER.items() for s in syms for n, kw in V.items()]
    with mp.get_context("fork").Pool(8) as pool:
        res = pool.map(task, jobs, chunksize=3)
    R = pd.DataFrame([{k: v for k, v in r.items() if k != "E"} for r in res])
    pd.set_option("display.width", 250)
    for per in PER:
        x = R[R.период == per]; years = [c for c in x.columns if c.isdigit() and x[c].notna().any()]
        rows = []
        for n in V:
            y = x[x.вариант == n]
            tot = sum(r["E"] for r in res if r["период"] == per and r["вариант"] == n); d = tot.resample("D").last().dropna()
            rows.append(dict(вариант=n, **{c: int(y[c].fillna(0).sum()) for c in years}, просадка=f"{(d / d.cummax() - 1).min() * 100:.0f}%", ликвидаций=int(y.ликв.sum())))
        print(f"\n=== {per.replace('|', ' – ')}"); print(pd.DataFrame(rows).to_string(index=False))
