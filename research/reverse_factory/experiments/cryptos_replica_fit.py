"""CryptosMX: подбор лесенки на DOGE 16.01–03.02.2025 и проверка на том, что в подбор не входило:
  * DOGE 03.02–08.02 (модель продолжает со своего состояния; его покупки — новые ордера мастера);
  * 1000PEPE 15.01–29.01 (модель стартует с его первой покупки; позиция мастера до окна неизвестна — сверяем время).
Параллельно на всех ядрах."""
from __future__ import annotations

import itertools
import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cryptos_replica as cr  # noqa: E402
from factory import market  # noqa: E402

CHK_DOGE = (pd.Timestamp("2025-02-03 05:02"), pd.Timestamp("2025-02-08 11:00"))
PEPE = (pd.Timestamp("2025-01-15 13:01"), pd.Timestamp("2025-01-29 21:30"))


def bars(sym, t0, t1):
    k = market.klines(sym, "1", t0 - pd.Timedelta(hours=2), t1)
    r1h = (k.c / k.c.shift(60) - 1) * 100
    k = k[k.index >= t0.floor("min")]
    return k, r1h.reindex(k.index).values


def his(sym, t0, t1, manual_out=True):
    L = pd.read_parquet(cr.PRIV / "work" / "cryptosmx" / "ledger.parquet")
    L = L[(L.sym == sym) & (L.t >= t0) & (L.t < t1)].copy()
    if manual_out:                                               # ручные закрытия Вадима — не его действия
        L = L[~((L.kind == "close") & (L.t.dt.strftime("%m-%d %H:%M").isin(["02-03 05:01", "02-08 11:26"])))]
    L["q"] = np.where(L.kind == "open", L.qty, -L.qty)
    return L[["t", "kind", "q", "px", "pos_after", "avg"]].reset_index(drop=True)


K = {}


def init():
    kd, rd = bars("DOGEUSDT", cr.W0, CHK_DOGE[1])
    kp, rp = bars("1000PEPEUSDT", *PEPE)
    Hd = his("DOGEUSDT", cr.W0, CHK_DOGE[1]); Hp = his("1000PEPEUSDT", *PEPE)
    K.update(kd=kd, rd=rd, kp=kp, rp=rp, Hd=Hd, Hp=Hp, iw1=int(np.searchsorted(kd.index, cr.W1)))


def slim(s, pre):
    return {pre + k: v for k, v in s.items() if not k.endswith(("_r", "_p"))}


def evaluate(p):
    kd, rd, Hd, iw1 = K["kd"], K["rd"], K["Hd"], K["iw1"]
    o, h, l, c = (kd[x].values.astype(float) for x in "ohlc")
    ev, pos = cr.simulate(o, h, l, c, rd, p, float(Hd.px.iloc[0]))
    fit_ev = [e for e in ev if e[0] < iw1]
    Hfit = Hd[Hd.t < cr.W1].iloc[1:]
    s = slim(cr.score(fit_ev, pos[:iw1], kd.index[:iw1], Hfit), "")
    chk_ev = [(e[0] - iw1, *e[1:]) for e in ev if e[0] >= iw1]
    Hchk = Hd[Hd.t >= CHK_DOGE[0]]
    s2 = cr.score(chk_ev, pos[iw1:], kd.index[iw1:], Hchk) if chk_ev else {}
    s.update({"DOGE_пров_покупки±60м": s2.get("покупки±60м", 0), "DOGE_пров_продажи±60м": s2.get("продажи±60м", 0),
              "DOGE_пров_покупки±15м": s2.get("покупки±15м", 0)})
    kp, rp, Hp = K["kp"], K["rp"], K["Hp"]
    o, h, l, c = (kp[x].values.astype(float) for x in "ohlc")
    ev, pos = cr.simulate(o, h, l, c, rp, p, float(Hp.px.iloc[0]))
    s3 = cr.score(ev, pos, kp.index, Hp.iloc[1:])
    s.update({"PEPE_покупки±15м": s3["покупки±15м"], "PEPE_покупки±60м": s3["покупки±60м"], "PEPE_продажи±15м": s3["продажи±15м"],
              "PEPE_продажи±60м": s3["продажи±60м"]})
    s["цель"] = (s["покупки±15м"] + s["продажи±15м"]) / 2 * 0.7 + 0.3 * max(s["позиция_связь"], 0)
    return {**p, **s}


def grid():
    for sb, a, sd, f, rs, (z, m) in itertools.product([0.4, 0.55, 0.75, 1.0, 1.25], [1.0, 1.5, 2.5, 3.5, 5.0], [0.15, 0.25, 0.35, 0.5],
                                                     [0.02, 0.04, 0.06, 0.1, 0.15], [1.5, 2.0, 2.5, 3.5, 5.0],
                                                     [(99, 1), (3, 5), (5, 5), (5, 15), (5, 25), (8, 15), (8, 25)]):
        yield dict(sb=sb, a=a, sd=sd, f=f, rule="sell", rs=rs, z=z, m=m)
    for sb, a, sd, f, ra, (z, m) in itertools.product([0.4, 0.55, 0.75, 1.0], [1.0, 2.5, 3.5], [0.25, 0.35], [0.04, 0.08],
                                                     [-1.0, 0.0, 1.0, 2.5], [(99, 1), (5, 15)]):
        yield dict(sb=sb, a=a, sd=sd, f=f, rule="avg", ra=ra, z=z, m=m)


if __name__ == "__main__":
    P = list(grid())
    with mp.get_context("fork").Pool(8, initializer=init) as pool:
        rows = pool.map(evaluate, P, chunksize=50)
    R = pd.DataFrame(rows).sort_values("цель", ascending=False)
    R.to_csv(cr.PRIV / "work" / "cryptosmx" / "replica_fit.csv", index=False)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 50)
    cols = ["sb", "a", "sd", "f", "rule", "rs", "ra", "z", "m", "покупки±15м", "продажи±15м", "покупки±60м", "продажи±60м", "позиция_связь",
            "покупки_n", "продажи_n", "DOGE_пров_покупки±15м", "DOGE_пров_покупки±60м", "DOGE_пров_продажи±60м",
            "PEPE_покупки±15м", "PEPE_покупки±60м", "PEPE_продажи±15м", "PEPE_продажи±60м", "цель"]
    print(f"вариантов {len(R)}")
    print(R[cols].head(20).round(3).to_string(index=False))
    print("\nустойчивость: медиана по топ-100 / по всем")
    print(pd.DataFrame({"топ100": R[cols[9:-1]].head(100).median(), "все": R[cols[9:-1]].median()}).round(3).T.to_string())
