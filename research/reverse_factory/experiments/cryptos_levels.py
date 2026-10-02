"""CryptosMX: где он ставит заявки? Проверка десятков систем уровней на разных таймфреймах.

Большинство его сделок — заранее выставленные лимитки: время сделки = момент, когда цена дошла до уровня.
Значит, надо понять, ОТКУДА берутся уровни. Для каждой его сделки считаем расстояние от цены сделки
до ближайшего уровня каждой системы (по данным до сделки, с задержкой lag), для покупок — уровни ниже цены
за lag до сделки, для продаж — выше. То же для «ложных сделок»: случайные минуты, где цена так же пришла сверху
(новый минимум за 15 мин) или снизу (новый максимум). Подъём = доля его сделок у уровня / доля ложных.

Системы уровней (таймфреймы 5м, 15м, 1ч, 4ч, 1д):
  минимумы/максимумы (фракталы), Фибо последнего импульса, скользящие EMA20/50/100/200, полосы Боллинджера,
  круглые цены, дневные уровни (открытие, прошлые хай/лоу/закрытие), VWAP суток, узлы объёма за 24ч,
  средняя его позиции ± k×шаг, прошлая сделка ± k×шаг.
→ levels_hits.csv (по системе), levels_dist.parquet (расстояния для каждой сделки)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"
TF = {"5м": "5min", "15м": "15min", "1ч": "1h", "4ч": "4h", "1д": "1D"}


def resample(k: pd.DataFrame, rule: str) -> pd.DataFrame:
    r = k.resample(rule, label="left", closed="left").agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna()
    r["t_close"] = r.index + pd.Timedelta(rule)
    return r


def fractals(b: pd.DataFrame, n: int):
    """подтверждённые минимумы/максимумы: экстремум среди n баров слева и справа; известен через n баров после."""
    h, l = b.h.values, b.l.values
    hi, lo = [], []
    for i in range(n, len(b) - n):
        if h[i] == h[i - n:i + n + 1].max():
            hi.append((b.t_close.iloc[i + n], h[i]))
        if l[i] == l[i - n:i + n + 1].min():
            lo.append((b.t_close.iloc[i + n], l[i]))
    return pd.DataFrame(hi, columns=["known", "px"]), pd.DataFrame(lo, columns=["known", "px"])


class Levels:
    def __init__(self, sym: str, a: str, b: str):
        k = market.klines(sym, "1", a, b).astype(float)
        self.k = k
        self.bars = {n: resample(k, r) for n, r in TF.items()}
        self.fr = {}
        for n, bb in self.bars.items():
            for w in (2, 5):
                self.fr[(n, w)] = fractals(bb, w)
        self.ema = {}
        for n, bb in self.bars.items():
            for span in (20, 50, 100, 200):
                e = bb.c.ewm(span=span, adjust=False).mean(); e.index = bb.t_close
                self.ema[(n, span)] = e
        self.bb = {}
        for n, bb in self.bars.items():
            m = bb.c.rolling(20).mean(); s = bb.c.rolling(20).std()
            up, dn = m + 2 * s, m - 2 * s; up.index = bb.t_close; dn.index = bb.t_close; m.index = bb.t_close
            self.bb[n] = (dn, m, up)
        d = self.bars["1д"]
        self.daily = d

    def levels(self, t: pd.Timestamp, side: str, ref: float, avg: float | None, last_fill: float | None) -> dict[str, np.ndarray]:
        """Все уровни каждой системы, известные к моменту t, на нужной стороне от цены ref."""
        L: dict[str, np.ndarray] = {}
        below = side == "buy"

        def pick(arr):
            arr = np.asarray(arr, float); arr = arr[np.isfinite(arr)]
            return arr[arr <= ref * 1.0005] if below else arr[arr >= ref * 0.9995]

        for (n, w), (hi, lo) in self.fr.items():
            for look, hrs in (("24ч", 24), ("72ч", 72)):
                src = lo if below else hi
                x = src[(src.known <= t) & (src.known > t - pd.Timedelta(hours=hrs))].px.values
                L[f"экстремумы {n} w{w} {look}"] = pick(x)
        # Фибо последнего импульса: по фракталам w2 на ТФ — последний минимум и последний максимум
        for n in TF:
            hi, lo = self.fr[(n, 2)]
            h = hi[hi.known <= t].tail(1); l = lo[lo.known <= t].tail(1)
            if len(h) and len(l):
                H, Lw = h.px.iloc[0], l.px.iloc[0]
                lv = [Lw + x * (H - Lw) for x in (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0)]
                ext = [H + x * (H - Lw) for x in (0.272, 0.618)] + [Lw - x * (H - Lw) for x in (0.272, 0.618)]
                L[f"Фибо {n}"] = pick(lv + ext)
        for (n, span), e in self.ema.items():
            v = e[e.index <= t]
            L[f"EMA{span} {n}"] = pick(v.tail(1).values)
        for n, (dn, m, up) in self.bb.items():
            L[f"Боллинджер {n}"] = pick([x[x.index <= t].tail(1).values[0] if (x.index <= t).any() else np.nan for x in (dn, m, up)])
        for step, name in ((0.01, "круглые 0.01"), (0.005, "круглые 0.005"), (0.001, "круглые 0.001"), (0.0005, "круглые 0.0005")):
            base = np.round(ref / step) * step
            L[name] = pick(base + np.arange(-30, 31) * step)
        d = self.daily[self.daily.index <= t.floor("D")]
        if len(d) >= 2:
            today, prev = d.iloc[-1], d.iloc[-2]
            L["дневные: открытие, вчера хай/лоу/закрытие"] = pick([today.o, prev.h, prev.l, prev.c])
        kk = self.k.loc[t.floor("D"):t - pd.Timedelta(minutes=1)]
        if len(kk):
            vw = (kk.c * kk.v).sum() / kk.v.sum()
            L["VWAP суток"] = pick([vw])
        k24 = self.k.loc[t - pd.Timedelta(hours=24):t - pd.Timedelta(minutes=1)]
        if len(k24) > 60:
            bins = np.linspace(k24.l.min(), k24.h.max(), 80)
            hist, edges = np.histogram(k24.c, bins=bins, weights=k24.v)
            top = np.argsort(hist)[-5:]
            L["узлы объёма 24ч"] = pick((edges[top] + edges[top + 1]) / 2)
        if avg:
            for st in (0.25, 0.5, 1.0):
                L[f"средняя ± k×{st}%"] = pick(avg * (1 + np.arange(-60, 61) * st / 100))
        if last_fill:
            for st in (0.25, 0.5, 0.75, 1.0):
                L[f"прошлая сделка ± k×{st}%"] = pick(last_fill * (1 + np.arange(-40, 41) * st / 100))
        return L


def nearest(levels: np.ndarray, p: float) -> float:
    return float(np.min(np.abs(levels / p - 1)) * 100) if len(levels) else np.nan


if __name__ == "__main__":
    sym = "DOGEUSDT"
    E = pd.read_csv(OUT / "trades_explained.csv", parse_dates=["t"])
    E = E[(E.sym == sym)].copy()
    lv = Levels(sym, "2024-12-20", "2025-02-12")
    k = lv.k
    lag = pd.Timedelta(minutes=int(sys.argv[1]) if len(sys.argv) > 1 else 30)
    # ложные сделки: минуты, где цена сделала новый минимум (для покупок) / максимум (для продаж) за 15 мин
    lo15 = k.l.rolling(15).min().shift(1); hi15 = k.h.rolling(15).max().shift(1)
    rng = np.random.default_rng(0)
    win = k.loc["2025-01-16 09:10":"2025-02-08"]
    cand_b = win.index[(win.l < lo15.reindex(win.index)).values]
    cand_s = win.index[(win.h > hi15.reindex(win.index)).values]
    fake = pd.concat([pd.DataFrame(dict(t=rng.choice(cand_b, 1500), side="buy")), pd.DataFrame(dict(t=rng.choice(cand_s, 1500), side="sell"))])
    fake["mpx"] = [k.l.loc[t] if s == "buy" else k.h.loc[t] for t, s in zip(fake.t, fake.side)]
    fake["fake"] = True
    # для ложных: средняя и прошлая сделка — как у него в тот момент (его состояние)
    st = E.set_index("t").sort_index()
    rows = []
    for src, df in (("его", E.assign(fake=False)), ("ложные", fake)):
        for r in df.itertuples():
            prev = st[st.index < r.t]
            avg = float(prev.avg_after.iloc[-1]) if len(prev) and prev.avg_after.iloc[-1] == prev.avg_after.iloc[-1] else None
            last = float(prev.mpx.iloc[-1]) if len(prev) else None
            ref = float(k.c.asof(r.t - lag))
            if not ref == ref:
                continue
            Ls = lv.levels(r.t - lag, r.side, ref, avg, last)
            d = {name: nearest(arr, r.mpx) for name, arr in Ls.items()}
            rows.append(dict(src=src, t=r.t, side=r.side, px=r.mpx, **d))
    D = pd.DataFrame(rows)
    D.to_parquet(OUT / f"levels_dist_lag{int(lag.total_seconds() // 60)}.parquet", index=False)
    cols = [c for c in D.columns if c not in ("src", "t", "side", "px")]
    res = []
    for side in ("buy", "sell"):
        for c in cols:
            a = D[(D.src == "его") & (D.side == side)][c]; b = D[(D.src == "ложные") & (D.side == side)][c]
            for tol in (0.03, 0.1):
                pa, pb = (a <= tol).mean(), (b <= tol).mean()
                res.append(dict(сторона=side, система=c, допуск=tol, его=round(pa, 3), ложные=round(pb, 3), подъём=round(pa / pb, 2) if pb > 0 else np.nan))
    R = pd.DataFrame(res)
    R.to_csv(OUT / f"levels_hits_lag{int(lag.total_seconds() // 60)}.csv", index=False)
    pd.set_option("display.width", 200); pd.set_option("display.max_rows", 400)
    for side in ("buy", "sell"):
        for tol in (0.03, 0.1):
            x = R[(R.сторона == side) & (R.допуск == tol)].sort_values("подъём", ascending=False)
            print(f"\n=== {side}, допуск ±{tol}% (задержка {lag}) — лучшие 12 по подъёму")
            print(x.head(12).to_string(index=False))
