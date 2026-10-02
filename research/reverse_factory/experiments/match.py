"""Жёсткая сверка «сделка в сделку»: наша версия против реальных сделок Algotoria.

1. Позиция в каждый момент: его знак позиции по монете на каждой 15-минутке (из его сделок) против нашего;
   доля совпадения + эталон «всегда в лонг» (совпадение, которое даёт тупая стратегия).
2. Вход в вход: для каждой его сделки — был ли у нас вход в ту же сторону в пределах ±Δ (15 мин…24 ч),
   разница цены входа; и обратно — какая доля наших входов совпала с его.
3. Таблица последних сделок: его вход/выход против нашего ближайшего.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory import market                      # noqa: E402
from factory.schema import load_target           # noqa: E402

DELTAS = ["15min", "1h", "4h", "24h"]


def their_sign(tr: pd.DataFrame, sym: str, grid: pd.DatetimeIndex) -> pd.Series:
    t = tr[tr.sym == sym].dropna(subset=["t_close"])
    s = pd.Series(0.0, grid)
    for _, x in t.iterrows():
        s[(grid >= x.t_open) & (grid < x.t_close)] += x.side
    return np.sign(s)


def entries(sgn: pd.Series) -> pd.DataFrame:
    ch = sgn.ne(sgn.shift()) & (sgn != 0)
    return pd.DataFrame(dict(t=sgn.index[ch], side=sgn[ch].values))


def compare(tr: pd.DataFrame, sym: str, ours_pos: pd.Series, dead: float = 0.05) -> dict:
    t = tr[tr.sym == sym]
    a, b = t.t_open.min().floor("15min"), t.t_close.max()
    grid = ours_pos.loc[a:b].index
    ours = np.sign(ours_pos.loc[a:b].where(ours_pos.loc[a:b].abs() > dead, 0))
    theirs = their_sign(tr, sym, grid)
    m = theirs != 0
    res = dict(sym=sym, bars=int(m.sum()),
               agree=round(float((ours[m] == theirs[m]).mean()), 3),
               always_long=round(float((theirs[m] == 1).mean()), 3),
               opposite=round(float((ours[m] == -theirs[m]).mean()), 3))
    # вход в вход
    E_their = pd.DataFrame(dict(t=t.t_open.values, side=t.side.values, p=t.p_open.values))
    E_our = entries(ours)
    price = market.klines(sym, "15", a - pd.Timedelta(days=1), b).c.astype(float)
    for d in DELTAS:
        dt = pd.Timedelta(d)
        hit_t = [((E_our.side == e.side) & ((E_our.t - e.t).abs() <= dt)).any() for e in E_their.itertuples()]
        hit_o = [((E_their.side == e.side) & ((E_their.t - e.t).abs() <= dt)).any() for e in E_our.itertuples()]
        res[f"его входы, у нас рядом ±{d}"] = round(float(np.mean(hit_t)), 2)
        res[f"наши входы, у него рядом ±{d}"] = round(float(np.mean(hit_o)), 2)
    # эталон: случайные входы с тем же числом и сторонами
    rng = np.random.default_rng(0)
    fake = E_our.copy()
    fake["t"] = pd.to_datetime(rng.uniform(a.value, b.value, len(fake))).floor("15min")
    res["эталон ±4h (случайные входы)"] = round(float(np.mean([((fake.side == e.side) & ((fake.t - e.t).abs() <= pd.Timedelta("4h"))).any()
                                                                for e in E_their.itertuples()])), 2)
    res["наших входов"], res["его входов"] = int(len(E_our)), int(len(E_their))
    # таблица последних сделок
    rows = []
    for e in E_their.tail(12).itertuples():
        same = E_our[E_our.side == e.side]
        if same.empty:
            continue
        j = (same.t - e.t).abs().idxmin()
        o = same.loc[j]
        rows.append(dict(его_вход=str(e.t)[:16], сторона="лонг" if e.side > 0 else "шорт", его_цена=round(e.p, 2),
                         наш_вход=str(o.t)[:16], сдвиг_ч=round((o.t - e.t).total_seconds() / 3600, 1),
                         наша_цена=round(float(price.asof(o.t)), 2)))
    return res, pd.DataFrame(rows)


if __name__ == "__main__":
    import algotoria_like as al
    tr, meta, _ = load_target("algotoria")
    out, _ = al.run(0.0002)
    pd.set_option("display.width", 220)
    for sym in ["BTCUSDT", "ETHUSDT"]:
        res, table = compare(tr, sym, out[sym]["pos"])
        print(f"\n=== {sym}")
        for k, v in res.items():
            print(f"   {k}: {v}")
        print(table.to_string(index=False))
