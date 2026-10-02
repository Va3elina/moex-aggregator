"""Наша форма позиции (v4) в рамке ЕГО реального плеча по дням: брутто-плечо его счёта (opl+ops)/mb вчера
делится между BTC и ETH по нашей позиции. Если так доходность по годам сходится — разница была в размере счёта."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import algotoria_like as al
from factory import market
from factory.schema import load_target
from sar_search import donchian_dir, supertrend_dir
FEE = 0.0002
_, _, eq = load_target("algotoria")
ya = eq[eq > 0].resample("D").last().pct_change().dropna()
G = pd.read_csv(Path(__file__).with_name("algotoria_daily.csv"), index_col=0, parse_dates=True)
gross = ((G.opl + G.ops) / G.mb.where(G.mb > 100)).shift(1)          # вчерашнее брутто-плечо
end = pd.Timestamp.now(tz="UTC").tz_localize(None)
legs = {}
for sym in ["BTCUSDT", "ETHUSDT"]:
    _, r, P = al.coin(sym, end)
    grid = P.loc["2023-10-05":].index
    a = grid[0] - pd.Timedelta(days=15)
    k60, k15 = market.klines(sym, "60", a, end), market.klines(sym, "15", a, end)
    fast = []
    for d, off in [(supertrend_dir(k60, 20, 4), "30min"), (supertrend_dir(k15, 20, 5), "7min"), (donchian_dir(k15, 96), "7min"), (donchian_dir(k60, 48), "30min")]:
        d.index = d.index + pd.Timedelta(off); fast.append(d.reindex(grid, method="ffill").fillna(1))
    net = pd.concat([P.loc[grid] / 3] + fast, axis=1).mean(axis=1)
    for L in [2]:
        pos = net.where(net < 0, net * L)
    legs[sym] = (pos / pos.abs().rolling(96 * 30, min_periods=96).max(), r.loc[grid])   # 0..1 доля «полной» позиции за месяц
rows = []
for share in ["пополам", "по силе сигнала"]:
    day = []
    tot = sum(l[0].abs() for l in legs.values())
    for sym, (pos, r) in legs.items():
        env = gross.reindex(pos.index.floor("D")).values
        w = pos * (0.5 if share == "пополам" else (pos.abs() / tot.replace(0, np.nan)).fillna(0.5)) * env * 2
        w = pd.Series(np.nan_to_num(w), pos.index)
        ret = w.shift(1).fillna(0) * r - w.diff().abs().fillna(0) * FEE
        day.append(ret.groupby(ret.index.floor("D")).sum())
    port = (day[0] + day[1]).dropna()
    i = port.index.intersection(ya.index)
    rows.append(dict(доля=share, связь_дохода=round(float(port[i].corr(ya[i])), 2), наша=al.stats(port[i])))
pd.set_option("display.width", 220); pd.set_option("display.max_colwidth", 110)
print(pd.DataFrame(rows).to_string(index=False)); print(f"Algotoria: {al.stats(ya[i])}")
