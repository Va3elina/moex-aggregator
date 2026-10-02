"""v3: стоп-и-разворот (правило, найденное combo_search) + пирамида (доливка по ходу движения).

Направление: знак среднего Supertrend 1ч ATR20×4 и пробоя 96-свечного канала на 15м.
Размер: на развороте 1 доля; доливка +1 доля, когда цена прошла в нашу сторону step×ATR(1ч) от
последней доливки (как «юниты» черепах); максимум U долей. Вариант B — доливка по времени (каждые H часов).
Подбор step/U/H — по связи НАШЕГО размера с ЕГО размером в $ (размер в размер), не по доходности.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al                                        # noqa: E402
from algotoria_v2 import their_size                                # noqa: E402
from combo_search import flips_of                                  # noqa: E402
from factory import market                                         # noqa: E402
from factory.schema import load_target                             # noqa: E402
from sar_search import atr, donchian_dir, match, supertrend_dir    # noqa: E402

FEE = 0.0002


def direction(sym, a, b, grid):
    k60, k15 = market.klines(sym, "60", a, b), market.klines(sym, "15", a, b)
    st = supertrend_dir(k60, 20, 4); st.index = st.index + pd.Timedelta("30min")
    ch = donchian_dir(k15, 96); ch.index = ch.index + pd.Timedelta("7min")
    d = pd.concat([st.reindex(grid, method="ffill"), ch.reindex(grid, method="ffill")], axis=1).fillna(1).mean(axis=1)
    d = np.sign(d).replace(0, np.nan).ffill().fillna(1)
    a1 = atr(k60, 20); a1.index = a1.index + pd.Timedelta("1h")
    return d, a1.reindex(grid, method="ffill")


def pyramid_price(d, price, atr_, step, U):
    u = np.zeros(len(d)); last = price.iloc[0]; cur = 0; dv, pv, av = d.values, price.values, atr_.values
    for i in range(len(d)):
        if i == 0 or dv[i] != dv[i - 1]:
            cur, last = 1, pv[i]
        elif cur < U and dv[i] * (pv[i] - last) >= step * av[i]:
            cur += 1; last = pv[i]
        u[i] = cur
    return pd.Series(u * dv, d.index)


def pyramid_time(d, H, U):
    grp = d.ne(d.shift()).cumsum()
    t_in = d.groupby(grp).cumcount() * 0.25                    # часов в позиции (15-мин шаг)
    return d * np.minimum(U, 1 + np.floor(t_in / H))


if __name__ == "__main__":
    tr, _, eq = load_target("algotoria")
    ya = eq[eq > 0].resample("D").last().pct_change().dropna()
    D = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        t = tr[tr.sym == sym]
        a, b = t.t_open.min() - pd.Timedelta(days=15), t.t_close.max()
        grid = pd.date_range(t.t_open.min().floor("15min"), b, freq="15min")
        d, a1 = direction(sym, a, b, grid)
        price = market.klines(sym, "15", a, b).c.astype(float).reindex(grid, method="ffill")
        D[sym] = dict(d=d, atr=a1, price=price, r=price.pct_change().fillna(0), theirs=their_size(tr, sym, grid),
                      E=pd.DataFrame(dict(t=t.sort_values("t_open").t_open.values, side=t.sort_values("t_open").side.values)))
    variants = {"без пирамиды": lambda x: x["d"]}
    for step in [0.5, 1, 2]:
        for U in [4, 8, 16]:
            variants[f"доливка каждые {step}×ATR, до {U}"] = (lambda s, u: (lambda x: pyramid_price(x["d"], x["price"], x["atr"], s, u)))(step, U)
    for H in [6, 12, 24]:
        for U in [8, 16]:
            variants[f"доливка каждые {H}ч, до {U}"] = (lambda h, u: (lambda x: pyramid_time(x["d"], h, u)))(H, U)
    rows = []
    for name, f in variants.items():
        cs, day = [], []
        for sym, x in D.items():
            pos = f(x)
            cs.append(pos.corr(x["theirs"]))
            ret = pos.shift(1).fillna(0) * x["r"] - pos.diff().abs().fillna(0) * FEE
            day.append(ret.groupby(ret.index.floor("D")).sum())
        port = (day[0] + day[1]).dropna()
        i = port.index.intersection(ya.index)
        pv = port[i] * (ya[i].std() / port[i].std())
        rows.append(dict(вариант=name, связь_размера=round(float(np.mean(cs)), 3), связь_дохода=round(float(pv.corr(ya[i])), 3), наша=al.stats(pv)))
    R = pd.DataFrame(rows).sort_values("связь_размера", ascending=False)
    pd.set_option("display.width", 230); pd.set_option("display.max_colwidth", 100)
    print(R.to_string(index=False))
    print(f"\nAlgotoria: {al.stats(ya[i])}")
    for sym, x in D.items():
        r, p = match(x["E"], flips_of(x["d"]), np.timedelta64(1, "h"))
        m = x["theirs"] != 0
        print(f"{sym}: развороты ±1ч его→наш {r:.2f}, наш→его {p:.2f}; направление {(x['d'][m] == np.sign(x['theirs'][m])).mean():.0%}")
