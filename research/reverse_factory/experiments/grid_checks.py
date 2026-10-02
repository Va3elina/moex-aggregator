"""Перепроверка «спокойного Kamaz» и зеркальные шорты: портфель монет, по годам.

Режимы:
  лонг / шорт / лонг+шорт (два отдельных счёта по $10k на монету, сумма);
  с фильтром по биткоину и без;
  проверки: вход наугад (та же частота сигналов, 5 прогонов), исполнение только при проходе цены на 0.1% + комиссии ×2,
            уровни ×0.8 / ×1.2, тейк 1.2% / 2.0%, таймер 6 / 24 ч.
Запуск: grid_checks.py <начало> <конец> <монеты через запятую>
"""
from __future__ import annotations

import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import grid_engine as ge  # noqa: E402

A, B = sys.argv[1], sys.argv[2]
COINS = sys.argv[3].split(",")
COIN = {}


def init():
    for s in COINS:
        COIN[s] = ge.Coin(s, A, B)
    COIN["_doge_rng"] = COIN["DOGEUSDT"].daily_range if "DOGEUSDT" in COIN else ge.Coin("DOGEUSDT", A, B).daily_range


def task(args):
    name, sym, kw = args
    c = COIN[sym]
    scale = c.daily_range / COIN["_doge_rng"] if sym in ("BTCUSDT", "ETHUSDT") else 1.0
    p = ge.P(scale=scale, **kw)
    E, C, liq, tr = ge.run(c, p)
    y = ge.yearly(E)
    return dict(вариант=name, монета=sym.replace("USDT", ""), **y, ликвидация=liq.strftime("%d.%m.%y") if liq is not None else "", сделок=tr,
                кампаний=len(C), E=E)


VARIANTS = {
    "лонг, без фильтра": dict(side=1, btc_filter=False),
    "лонг, фильтр": dict(side=1, btc_filter=True),
    "шорт, без фильтра": dict(side=-1, btc_filter=False),
    "шорт, фильтр": dict(side=-1, btc_filter=True),
    "лонг, фильтр, x3": dict(side=1, btc_filter=True, L=3.0),
    "шорт, фильтр, x3": dict(side=-1, btc_filter=True, L=3.0),
    "лонг, фильтр: худшее исполнение": dict(side=1, btc_filter=True, through=0.1, maker=0.0004, taker=0.0011),
    "лонг, фильтр: уровни ×0.8": dict(side=1, btc_filter=True, levels=ge.DOGE_LV * 0.8),
    "лонг, фильтр: уровни ×1.2": dict(side=1, btc_filter=True, levels=ge.DOGE_LV * 1.2),
    "лонг, фильтр: тейк 1.2%": dict(side=1, btc_filter=True, tp1=1.5, tpn=1.2),
    "лонг, фильтр: тейк 2.0%": dict(side=1, btc_filter=True, tp1=2.4, tpn=2.0),
    "лонг, фильтр: таймер 6 ч": dict(side=1, btc_filter=True, timer_h=6.0),
    "лонг, фильтр: таймер 24 ч": dict(side=1, btc_filter=True, timer_h=24.0),
}
for sd in range(5):
    VARIANTS[f"лонг, фильтр: вход наугад #{sd}"] = dict(side=1, btc_filter=True, random_entry=True, seed=sd)

if __name__ == "__main__":
    jobs = [(v, s, kw) for v, kw in VARIANTS.items() for s in COINS]
    with mp.get_context("fork").Pool(8, initializer=init) as pool:
        res = pool.map(task, jobs, chunksize=2)
    R = pd.DataFrame([{k: v for k, v in r.items() if k != "E"} for r in res])
    tag = f"{A[:4]}_{B[:4]}"
    R.to_csv(Path(__file__).resolve().parents[1] / f"inbox/private/work/itek/grid_checks_{tag}.csv", index=False)
    years = [c for c in R.columns if c.isdigit()]
    rows = []
    for v in VARIANTS:
        x = R[R.вариант == v]
        tot = sum(r["E"] for r in res if r["вариант"] == v)
        d = tot.resample("D").last()
        rows.append(dict(вариант=v, **{y: int(x[y].fillna(0).sum()) for y in years},
                         просадка_портфеля=f"{(d / d.cummax() - 1).min() * 100:.0f}%", ликвидаций=int((x.ликвидация != "").sum()),
                         монет_в_минусе_по_годам="/".join(str(int((x[y] < 0).sum())) for y in years)))
    S = pd.DataFrame(rows)
    # лонг+шорт вместе
    for f in ("без фильтра", "фильтр"):
        a = S[S.вариант == f"лонг, {f}"].iloc[0]; b = S[S.вариант == f"шорт, {f}"].iloc[0]
        tot = sum(r["E"] for r in res if r["вариант"] in (f"лонг, {f}", f"шорт, {f}")); d = tot.resample("D").last()
        S.loc[len(S)] = dict(вариант=f"лонг+шорт, {f}", **{y: int(a[y] + b[y]) for y in years},
                             просадка_портфеля=f"{(d / d.cummax() - 1).min() * 100:.0f}%", ликвидаций=int(a.ликвидаций + b.ликвидаций), монет_в_минусе_по_годам="")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 100)
    print(f"Период {A} – {B}, монеты: {', '.join(c.replace('USDT', '') for c in COINS)} (по $10 000 на монету и сторону)")
    print(S.to_string(index=False))
