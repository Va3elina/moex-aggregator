"""v5 = v4 (14 медленных + 4 быстрых разворота) + контртрендовые правила на 1ч + перекос лонга по режиму EMA50.

Контртренд (1ч): возврат после хода за 6ч и 24ч, разворот от полос Боллинджера 20×2, RSI14 от 50.
Перекос лонга: постоянный ×2 (как v4) или по режиму: над дневной EMA50 лонг ×3, под ней ×1.
Сверка — как раньше: развороты ±1ч, направление, размер в размер, связь дневной доходности, по годам.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al                                              # noqa: E402
from algotoria_v2 import their_size                                      # noqa: E402
from combo_search import flips_of                                        # noqa: E402
from factory import market                                               # noqa: E402
from factory.schema import load_target                                   # noqa: E402
from sar_search import donchian_dir, match, supertrend_dir               # noqa: E402

FEE = 0.0002


def mr_rules(k60: pd.DataFrame, grid) -> pd.DataFrame:
    c = k60.c.astype(float)
    sma, sd = c.rolling(20).mean(), c.rolling(20).std()
    d = c.diff()
    rsi = 100 - 100 / (1 + d.clip(lower=0).ewm(alpha=1 / 14).mean() / (-d.clip(upper=0)).ewm(alpha=1 / 14).mean())
    R = {"возврат 6ч": -np.sign(c / c.shift(6) - 1), "возврат 24ч": -np.sign(c / c.shift(24) - 1),
         "Боллинджер 1ч": -((c - sma) / sd / 2).clip(-1, 1), "RSI 1ч": -((rsi - 50) / 30).clip(-1, 1)}
    out = {}
    for name, s in R.items():
        s = s.copy(); s.index = s.index + pd.Timedelta("1h")       # известно на закрытии часа
        out[name] = s.reindex(grid, method="ffill").fillna(0)
    return pd.DataFrame(out)


if __name__ == "__main__":
    tr, _, eq = load_target("algotoria")
    ya = eq[eq > 0].resample("D").last().pct_change().dropna()
    end = pd.Timestamp.now(tz="UTC").tz_localize(None)
    base = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        _, r, P = al.coin(sym, end)
        t = tr[tr.sym == sym]
        grid = P.loc[t.t_open.min().floor("15min"):t.t_close.max()].index
        a, b = grid[0] - pd.Timedelta(days=60), grid[-1]
        k60, k15 = market.klines(sym, "60", a, b), market.klines(sym, "15", a, b)
        fast = {}
        for name, d, off in [("ST1h", supertrend_dir(k60, 20, 4), "30min"), ("ST15", supertrend_dir(k15, 20, 5), "7min"),
                             ("канал15", donchian_dir(k15, 96), "7min"), ("канал1h", donchian_dir(k60, 48), "30min")]:
            d.index = d.index + pd.Timedelta(off); fast[name] = d.reindex(grid, method="ffill").fillna(1)
        kd = market.klines(sym, "D", "2023-01-01", end).c.astype(float)
        above = (kd > kd.ewm(span=50).mean()); above.index = above.index + pd.Timedelta("1D")
        base[sym] = dict(slow=P.loc[grid] / 3, fast=pd.DataFrame(fast), mr=mr_rules(k60, grid), r=r.loc[grid],
                         above=above.reindex(grid, method="ffill").fillna(True).astype(bool), theirs=their_size(tr, sym, grid),
                         E=pd.DataFrame(dict(t=t.sort_values("t_open").t_open.values, side=t.sort_values("t_open").side.values)))
    rows, keep = [], {}
    for wm in [0, 0.5, 1, 2]:
        for lmode in ["лонг ×2 всегда", "лонг ×3 над EMA50, ×1 под"]:
            cs, day, f1s, ag = [], [], [], []
            for sym, x in base.items():
                net = pd.concat([x["slow"], x["fast"], x["mr"] * wm], axis=1).mean(axis=1)
                L = 2.0 if lmode.startswith("лонг ×2") else np.where(x["above"], 3.0, 1.0)
                pos = net.where(net < 0, net * L)
                cs.append(pos.corr(x["theirs"]))
                s = np.sign(net).replace(0, np.nan).ffill().fillna(1)
                rr, pp = match(x["E"], flips_of(s), np.timedelta64(1, "h")); f1s.append(2 * rr * pp / (rr + pp) if rr + pp else 0)
                m = x["theirs"] != 0; ag.append((s[m] == np.sign(x["theirs"][m])).mean())
                ret = pos.shift(1).fillna(0) * x["r"] - pos.diff().abs().fillna(0) * FEE
                day.append(ret.groupby(ret.index.floor("D")).sum())
            port = (day[0] + day[1]).dropna(); i = port.index.intersection(ya.index)
            pv = port[i] * (ya[i].std() / port[i].std())
            rows.append(dict(контртренд=wm, перекос=lmode, развороты_F1=round(float(np.mean(f1s)), 2), направление=round(float(np.mean(ag)), 2),
                             размер=round(float(np.mean(cs)), 2), связь_дохода=round(float(pv.corr(ya[i])), 2), наша=al.stats(pv)))
    pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 100)
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"\nAlgotoria: {al.stats(ya[i])}")
