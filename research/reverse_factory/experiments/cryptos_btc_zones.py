"""CryptosMX: его собственные уровни по биткоину (из постов, картинок и чата) → режимы для DOGE.

Он сам пишет: докупает, когда биткоин пришёл в зону поддержки и «снял ликвидность»; продаёт, когда биткоин подошёл
к зоне сопротивления / цели. Зоны с моментом публикации (действуют после поста):
  поддержки (подбор): 18.12 — 98–100k, 91.5–93.5k, 83–86k; 03.01 — 91.8–93.5k, 82–85.2k; 09.01 — 91–93k, 88–90k, 84–86k;
                      02.02 (чат) — 91–93k;
  сопротивления / цели: 15.12 — хай 104.1k, цель 107k; 03.01 — 100–107.5k; 09.01 — «красная зона» 97.3–100k;
                      16.01 — 108–110k и 115–120k.
Признаки на каждую минуту: биткоин в зоне поддержки / сопротивления, расстояние до ближайшей, «снятие ликвидности»
(за 4 ч был ниже зоны поддержки и вернулся в неё). Затем — как часто он покупает/продаёт DOGE в каждом режиме.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

SUP = [("2024-12-18 15:08", 98000, 100000), ("2024-12-18 15:08", 91500, 93500), ("2024-12-18 15:08", 83000, 86000),
       ("2025-01-03 17:59", 91800, 93500), ("2025-01-03 17:59", 82000, 85200),
       ("2025-01-09 13:20", 91000, 93000), ("2025-01-09 13:20", 88000, 90000), ("2025-01-09 13:20", 84000, 86000),
       ("2025-02-02 17:31", 91000, 93000)]
RES = [("2024-12-15 16:41", 104100, 104100), ("2024-12-15 16:41", 107000, 107000), ("2025-01-03 17:59", 100000, 107500),
       ("2025-01-09 13:20", 97300, 100000), ("2025-01-16 08:36", 108000, 110000), ("2025-01-16 08:36", 115000, 120000)]


def zone_features(btc: pd.DataFrame, tol=0.3) -> pd.DataFrame:
    idx = btc.index; c = btc.c.values; lo4 = btc.l.rolling(240).min().values
    out = pd.DataFrame(index=idx)
    for name, Z in (("под", SUP), ("сопр", RES)):
        inside = np.zeros(len(idx), bool); dist = np.full(len(idx), np.nan); swept = np.zeros(len(idx), bool)
        for ts, lo, hi in Z:
            act = idx >= pd.Timestamp(ts)
            lo_t, hi_t = lo * (1 - tol / 100), hi * (1 + tol / 100)
            ins = act & (c >= lo_t) & (c <= hi_t)
            inside |= ins
            # расстояние: для поддержки — от цены вниз до верха зоны; для сопротивления — вверх до низа зоны
            dd = (c / hi - 1) * 100 if name == "под" else (lo / c - 1) * 100
            dd = np.where(act, dd, np.nan)
            dd = np.where(dd < -99, np.nan, dd)
            better = act & (np.isnan(dist) | (np.abs(dd) < np.abs(dist)))
            dist = np.where(better, dd, dist)
            if name == "под":
                swept |= act & (lo4 < lo_t) & (c >= lo_t)
        out[f"btc_в_зоне_{name}"] = inside.astype(float)
        out[f"btc_до_зоны_{name}"] = dist
        if name == "под":
            out["btc_снял_ликвидность"] = swept.astype(float)
    return out.shift(1)


if __name__ == "__main__":
    OUT = ROOT / "inbox" / "private" / "work" / "cryptosmx"
    btc = market.klines("BTCUSDT", "1", "2025-01-12", "2025-02-12").astype(float)
    Z = zone_features(btc)
    T = pd.read_parquet(OUT / "trades_full.parquet")
    W0, W1 = pd.Timestamp("2025-01-16 09:10"), pd.Timestamp("2025-02-03 05:00")
    d = T[(T.sym == "DOGEUSDT") & (T.t >= W0) & (T.t < W1) & (~T.manual)].copy()
    d["m"] = d.t.dt.floor("min")
    Zw = Z.loc[W0:W1 - pd.Timedelta(minutes=1)].copy()
    Zw["buy"] = Zw.index.isin(d[d.side == "buy"].m); Zw["sell"] = Zw.index.isin(d[d.side == "sell"].m)
    def mode(r):
        if r.btc_в_зоне_сопр: return "биткоин в зоне сопротивления/цели"
        if r.btc_снял_ликвидность and r.btc_в_зоне_под: return "биткоин снял ликвидность и в зоне поддержки"
        if r.btc_в_зоне_под: return "биткоин в зоне поддержки"
        if r.btc_до_зоны_сопр == r.btc_до_зоны_сопр and r.btc_до_зоны_сопр <= 1.0: return "биткоин ≤1% до сопротивления"
        if r.btc_до_зоны_под == r.btc_до_зоны_под and 0 < r.btc_до_зоны_под <= 1.0: return "биткоин ≤1% над поддержкой"
        return "между зонами"
    Zw["режим"] = [mode(r) for r in Zw.itertuples()]
    t = Zw.groupby("режим").agg(минут=("buy", "size"), покупок_на_1000=("buy", lambda x: round(1000 * x.mean(), 1)),
                                 продаж_на_1000=("sell", lambda x: round(1000 * x.mean(), 1)), покупок=("buy", "sum"), продаж=("sell", "sum"))
    pd.set_option("display.width", 200)
    print(t.to_string())
    Z.to_parquet(OUT / "btc_zone_features.parquet")
