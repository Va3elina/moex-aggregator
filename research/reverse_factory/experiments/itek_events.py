"""ITEK (основной счёт, Kamaz, REINVEST): таблица всех сделок из копии Вадима с условиями появления.

Исполнения (fills_all.parquet, мастер определён по цепочке позиции) → события: исполнения одной секунды,
одной стороны и одного мастера по монете склеиваются. Для каждого события: кампания (от нуля до нуля),
номер покупки в кампании, шаг от прошлой покупки, множитель объёма, средняя и позиция до/после,
ход цены перед событием (5м/15м/1ч/4ч/сутки), от хая 4ч, биткоин за час, RSI на 3-мин свечах;
метки: ручное закрытие Вадима, ликвидация копии.
→ inbox/private/work/itek/events.parquet (+ csv)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

PRIV = ROOT / "inbox" / "private"
OUT = PRIV / "work" / "itek"
ACC = {"ITEKCrypto": "основной", "ITEKCrypto Kamaz": "Kamaz", "ITEKCrypto REINVEST": "REINVEST"}


def rsi(c: pd.Series, n=14) -> pd.Series:
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def ret(c: pd.Series, t, minutes):
    a = c.asof(t - pd.Timedelta(minutes=1)); b = c.asof(t - pd.Timedelta(minutes=minutes + 1))
    return (a / b - 1) * 100 if a == a and b == b and b else np.nan


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    F = pd.read_parquet(PRIV / "work" / "itekcrypto-kamaz" / "fills_all.parquet")
    F = F[F.m_res.isin(ACC)].copy()
    F["acc"] = F.m_res.map(ACC)
    F["sym"] = F.Contract
    F["t"] = F.Time
    F["side"] = np.where(F.Direction.str.startswith("Open"), "buy", "sell")
    F["liq"] = F.Type == "Liquidation"
    P = pd.read_parquet(PRIV / "copied_positions.parquet")
    man = P[P.close_by == "ClosedByUserSelf"][["master", "sym", "t_close"]]
    man_keys = {(ACC.get(m), s, t.floor("s")) for m, s, t in zip(man.master, man.sym, man.t_close) if m in ACC}
    # склеиваем исполнения одной секунды
    F["notional"] = F.Quantity * F["Filled Price"]
    E = (F.sort_values(["acc", "sym", "t"], kind="stable")
         .groupby(["acc", "sym", "t", "side"], sort=False)
         .agg(qty=("Quantity", "sum"), notional=("notional", "sum"), pos_after=("Position", "last"),
              pos_min=("Position", "min"), pos_max=("Position", "max"), liq=("liq", "max"), n_fills=("Quantity", "size"))
         .reset_index())
    E["px"] = E.notional / E.qty
    E["manual"] = [(a, s, t.floor("s")) in man_keys and sd == "sell" for a, s, t, sd in zip(E.acc, E.sym, E.t, E.side)]
    rows = []
    kl = {}
    btc = market.klines("BTCUSDT", "1", "2024-10-01", "2025-09-20").c.astype(float)
    for (acc, sym), g in E.groupby(["acc", "sym"], sort=False):
        if sym not in kl:
            k = market.klines(sym, "1", "2024-10-01", "2025-09-20")
            c = k.c.astype(float)
            c3 = c.resample("3min").last()
            kl[sym] = (k, c, rsi(c3), c.rolling(240).max())
        k, c, r3, hi4 = kl[sym]
        Q = cost = 0.0; camp = 0; kbuy = 0; last_buy = None; last_buy_q = None; t_last_add = None; t0 = None; qmax = 0.0
        for r in g.itertuples():
            pos_before = Q
            if r.side == "buy":
                if Q <= 1e-9:
                    camp += 1; kbuy = 0; t0 = r.t; qmax = 0.0
                kbuy += 1
                step = (r.px / last_buy - 1) * 100 if (last_buy and kbuy > 1) else np.nan
                mult = r.qty / last_buy_q if (last_buy_q and kbuy > 1) else np.nan
                avg_before = cost / Q if Q > 1e-9 else np.nan
                Q += r.qty; cost += r.qty * r.px
                last_buy, last_buy_q, t_last_add = r.px, r.qty, r.t
            else:
                step = mult = np.nan
                avg_before = cost / Q if Q > 1e-9 else np.nan
                q = min(r.qty, Q)
                Q -= q; cost = (avg_before if avg_before == avg_before else 0) * Q
            qmax = max(qmax, Q)
            tm = r.t.floor("min") - pd.Timedelta(minutes=1)
            rows.append(dict(acc=acc, sym=sym.replace("USDT", ""), t=r.t, side=r.side, qty=r.qty, px=r.px, usd=r.qty * r.px, manual=r.manual, liq=r.liq,
                             camp=f"{acc}:{sym}:{camp}", k_buy=kbuy if r.side == "buy" else np.nan, step=step, mult=mult,
                             pos_before=pos_before, pos_after=Q, avg_before=avg_before,
                             vs_avg=(r.px / avg_before - 1) * 100 if avg_before == avg_before else np.nan,
                             frac_pos=(r.qty / pos_before * 100) if (r.side == "sell" and pos_before > 0) else np.nan,
                             full_exit=(r.side == "sell" and Q <= 1e-9),
                             min_since_add=(r.t - t_last_add).total_seconds() / 60 if t_last_add is not None else np.nan,
                             hrs_in_camp=(r.t - t0).total_seconds() / 3600 if t0 is not None else np.nan,
                             r5=ret(c, r.t, 5), r15=ret(c, r.t, 15), r60=ret(c, r.t, 60), r240=ret(c, r.t, 240), r1440=ret(c, r.t, 1440),
                             btc60=ret(btc, r.t, 60), from_hi4=(r.px / hi4.asof(tm) - 1) * 100 if hi4.asof(tm) == hi4.asof(tm) else np.nan,
                             rsi3=float(r3.asof(tm)) if len(r3) else np.nan))
            if Q <= 1e-9:
                Q, cost, last_buy, last_buy_q = 0.0, 0.0, None, None
    T = pd.DataFrame(rows)
    T.to_parquet(OUT / "events.parquet", index=False)
    print(T.groupby(["acc", "side"]).agg(n=("qty", "size"), ручных=("manual", "sum"), ликвидаций=("liq", "sum"), кампаний=("camp", "nunique")).to_string())
