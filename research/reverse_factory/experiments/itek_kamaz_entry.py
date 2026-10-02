"""ITEK Kamaz, слой A (1000PEPE, DOGE): что именно запускает вход — со строгой сверкой по времени.

Учитываем занятость: пока кампания по монете открыта, нового входа быть не может; сигнал ищем только в «свободное»
время (от выхода до следующего входа). Условия проверяем на закрытии 3-минутной свечи (как в TradingView/боте):
падение за час ≥ X% и RSI(n) на 3-мин ≤ Y (перебор), + варианты: от хая часа, от хая 4ч, биткоин.
Метрика: для каждой свободной паузы — первый сигнал совпал со стартом (±6 мин)? И сколько ложных первых сигналов
(сигнал был, а старта в ±6 мин нет — значит, бот ждал чего-то ещё).
"""
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402

def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)

T = pd.read_parquet(ROOT / "inbox/private/work/itek/events.parquet")
K = T[(T.acc == "Kamaz") & T.sym.isin(["1000PEPE", "DOGE"]) & (T.t >= pd.Timestamp("2024-11-19"))].copy()
res = []
data = {}
for sym in ["1000PEPE", "DOGE"]:
    g = K[K.sym == sym].sort_values("t", kind="stable")
    camps = g.groupby("camp").agg(t0=("t", "min"), t1=("t", "max")).sort_values("t0")
    k = market.klines(sym + "USDT", "1", "2024-11-15", "2025-01-17").astype(float)
    c3 = k.c.resample("3min", label="right", closed="right").last().dropna()
    h3 = k.h.resample("3min", label="right", closed="right").max().reindex(c3.index)
    data[sym] = (camps, k, c3)
rows = []
for n, thr_rsi, thr_drop, mode in itertools.product([6, 7, 9, 14], [20, 25, 30, 35], [1.5, 2.0, 2.5, 3.0], ["ход 60м", "от хая 60м"]):
    hit = fp = tot = 0
    lags = []
    for sym, (camps, k, c3) in data.items():
        r = rsi(c3, n)
        if mode == "ход 60м":
            drop = (c3 / c3.shift(20) - 1) * 100
        else:
            drop = (c3 / k.h.rolling(60).max().reindex(c3.index) - 1) * 100
        sig = (r <= thr_rsi) & (drop <= -thr_drop)
        st = sig[sig].index
        prev_end = pd.Timestamp("2024-11-19")
        for c in camps.itertuples():
            s = st[(st > prev_end) & (st <= c.t0 + pd.Timedelta(minutes=6))]
            tot += 1
            if len(s) and abs((s[0] - c.t0).total_seconds()) <= 360:
                hit += 1; lags.append((s[0] - c.t0).total_seconds() / 60)
            elif len(s):
                fp += 1
            prev_end = c.t1
    rows.append(dict(rsi_n=n, rsi_порог=thr_rsi, падение=thr_drop, как=mode, стартов=tot, первый_сигнал_совпал=hit, ложный_раньше=fp,
                     доля=round(hit / tot, 3), сдвиг_мин=round(float(np.median(lags)), 1) if lags else np.nan))
R = pd.DataFrame(rows).sort_values(["доля", "ложный_раньше"], ascending=[False, True])
pd.set_option("display.width", 200)
print(R.head(15).to_string(index=False))
