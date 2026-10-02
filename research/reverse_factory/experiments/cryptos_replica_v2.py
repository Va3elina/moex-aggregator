"""CryptosMX, повтор v2 — по разбору всех сделок (cryptos_explain.py): лесенки привязаны к средней + ручные приёмы.

  П1 лесенка продаж: после каждой покупки — N_s равных кусков (f от позиции) от средней +a0% до +a1%.
  П2 крупная продажа: цена выше средней на b% и за час выросла на c% → продать q2 позиции по рынку (не чаще раза в час).
  К1 лесенка покупок: следующая покупка — не выше «прошлая покупка −sb%» и не выше «средняя −d0%»; кусок g от позиции.
  К2 «ведро»: цена ниже средней на zb% → купить mb позиции, один раз за кампанию (кампания = от нуля до нуля).
Сверка — как в v1 (F1 совпадения ±15/±60 мин, связь позиции), подбор на DOGE 16.01–03.02, проверка DOGE 03–08.02 и PEPE.
"""
from __future__ import annotations

import itertools
import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cryptos_replica as cr  # noqa: E402
import cryptos_replica_fit as cf  # noqa: E402


def simulate2(o, h, l, c, r1h, p: dict, start_px: float):
    a0, a1, Ns, f = p["a0"] / 100, p["a1"] / 100, int(p["Ns"]), p["f"]
    b, cc, q2 = p["b"] / 100, p["c"], p["q2"]
    sb, d0, g = p["sb"] / 100, p["d0"] / 100, p["g"]
    zb, mb = p["zb"] / 100, p["mb"]
    n = len(c)
    Q, cost = 1.0, start_px
    ev, pos = [(0, 1, 1.0, start_px)], np.zeros(n)
    last_buy, bucket_used, last_big = start_px, False, -10 ** 9
    sells: list[list[float]] = []

    def place_sells():
        avg = cost / Q
        lv = np.linspace(a0, a1, Ns) if Ns > 1 else np.array([a0])
        return [[avg * (1 + x), f * Q] for x in lv]

    sells = place_sells()
    for i in range(1, n):
        avg = cost / Q if Q > 1e-12 else np.nan
        # П2: крупная ручная продажа по рынку на ралли
        if Q > 1e-12 and c[i] >= avg * (1 + b) and r1h[i - 1] >= cc and i - last_big >= 60:
            q = q2 * Q; Q -= q; cost = avg * Q; ev.append((i, -1, q, c[i])); last_big = i
            sells = [s for s in sells if s[1] > 0]
        order = ("s", "b") if c[i] < o[i] else ("b", "s")
        for side in order:
            if side == "s" and Q > 1e-12:
                for s in sorted(sells, key=lambda z: z[0]):
                    if h[i] >= s[0] and s[1] > 0 and Q > 1e-12:
                        q = min(s[1], Q); avg = cost / Q
                        Q -= q; cost = avg * Q; s[1] = 0; ev.append((i, -1, q, s[0]))
                sells = [s for s in sells if s[1] > 0]
            elif side == "b":
                avg = cost / Q if Q > 1e-12 else last_buy
                # К2: ведро
                if Q > 1e-12 and not bucket_used and l[i] <= avg * (1 - zb):
                    px = avg * (1 - zb); q = mb * Q
                    Q += q; cost += px * q; ev.append((i, 1, q, px)); bucket_used = True; last_buy = px
                    sells = place_sells(); avg = cost / Q
                # К1: лесенка покупок
                while True:
                    lvl = min(last_buy * (1 - sb), avg * (1 - d0)) if Q > 1e-12 else last_buy * (1 - sb)
                    if l[i] > lvl:
                        break
                    q = max(g * Q, 1.0)
                    Q += q; cost += lvl * q; ev.append((i, 1, q, lvl)); last_buy = lvl
                    avg = cost / Q; sells = place_sells()
        if Q <= 1e-9:
            Q, cost, bucket_used = 0.0, 0.0, False
        pos[i] = Q
    return ev, pos


def grid():
    for a0, a1, Ns, f, (b, cc), q2, sb, d0, g, (zb, mb) in itertools.product(
            [0.3, 1.0, 1.9], [3.5, 5.0], [8, 15], [0.02, 0.04], [(1.5, 1.0), (2.5, 2.0), (99, 99)], [0.25, 0.5],
            [0.3, 0.6], [0.0, 1.0, 3.0], [0.04, 0.06], [(11, 0.5), (11, 1.0), (99, 0)]):
        if (b, cc) == (99, 99) and q2 == 0.5:
            continue
        yield dict(a0=a0, a1=a1, Ns=Ns, f=f, b=b, c=cc, q2=q2, sb=sb, d0=d0, g=g, zb=zb, mb=mb)


def evaluate(p):
    K = cf.K
    kd, rd, Hd, iw1 = K["kd"], K["rd"], K["Hd"], K["iw1"]
    o, h, l, c = (kd[x].values.astype(float) for x in "ohlc")
    ev, pos = simulate2(o, h, l, c, rd, p, float(Hd.px.iloc[0]))
    fit_ev = [e for e in ev if e[0] < iw1]
    s = {k: v for k, v in cr.score(fit_ev, pos[:iw1], kd.index[:iw1], Hd[Hd.t < cr.W1].iloc[1:]).items() if not k.endswith(("_r", "_p"))}
    chk = [(e[0] - iw1, *e[1:]) for e in ev if e[0] >= iw1]
    s2 = cr.score(chk, pos[iw1:], kd.index[iw1:], Hd[Hd.t >= cf.CHK_DOGE[0]]) if chk else {}
    s.update({"DOGE_пров_покупки±15м": s2.get("покупки±15м", 0), "DOGE_пров_продажи±15м": s2.get("продажи±15м", 0)})
    kp, rp, Hp = K["kp"], K["rp"], K["Hp"]
    o, h, l, c = (kp[x].values.astype(float) for x in "ohlc")
    ev, pos = simulate2(o, h, l, c, rp, p, float(Hp.px.iloc[0]))
    s3 = cr.score(ev, pos, kp.index, Hp.iloc[1:])
    s.update({"PEPE_покупки±15м": s3["покупки±15м"], "PEPE_продажи±15м": s3["продажи±15м"]})
    s["цель"] = (s["покупки±15м"] + s["продажи±15м"]) / 2 * 0.7 + 0.3 * max(s["позиция_связь"], 0)
    return {**p, **s}


if __name__ == "__main__":
    P = list(grid())
    with mp.get_context("fork").Pool(8, initializer=cf.init) as pool:
        rows = pool.map(evaluate, P, chunksize=20)
    R = pd.DataFrame(rows).sort_values("цель", ascending=False)
    R.to_csv(cr.PRIV / "work" / "cryptosmx" / "replica_v2_fit.csv", index=False)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    cols = ["a0", "a1", "Ns", "f", "b", "c", "q2", "sb", "d0", "g", "zb", "mb", "покупки±15м", "продажи±15м", "покупки±60м", "продажи±60м",
            "позиция_связь", "покупки_n", "продажи_n", "DOGE_пров_покупки±15м", "DOGE_пров_продажи±15м", "PEPE_покупки±15м", "PEPE_продажи±15м", "цель"]
    print(f"вариантов {len(R)}"); print(R[cols].head(15).round(3).to_string(index=False))
