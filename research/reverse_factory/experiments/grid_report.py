"""«Спокойный Kamaz»: статистика по годам (сделки, доля прибыльных, средний плюс/минус, доходность, просадка),
сделки сентября 2026 и что бот видит сейчас (открытые позиции, фильтр биткоина, насколько монеты близки к входу).
Лонг, плечо x1 (лесенка целиком = депозит монеты), по $10 000 на монету, таймер 12 ч; с фильтром по биткоину и без."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import grid_engine as ge  # noqa: E402

NOW = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("min") - pd.Timedelta(minutes=5)
SETS = {"2021-07-01": ["DOGEUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "BTCUSDT", "ETHUSDT"],
        "2024-03-01": ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT", "BTCUSDT", "ETHUSDT"]}


def year_stats(C: pd.DataFrame, E: pd.Series, cap: float) -> pd.DataFrame:
    C = C.copy(); C["год"] = C.t1.dt.year
    d = E.resample("D").last().dropna()
    rows = []
    for y, g in C.groupby("год"):
        e = d[d.index.year == y]
        start = d[d.index < e.index[0]].iloc[-1] if (d.index < e.index[0]).any() else cap
        win = g[g.pnl > 0]; loss = g[g.pnl <= 0]
        rows.append(dict(год=y, сделок=len(g), прибыльных=f"{len(win) / len(g) * 100:.0f}%",
                         средний_плюс=f"{win.pnl.mean():.0f}$" if len(win) else "", средний_минус=f"{loss.pnl.mean():.0f}$" if len(loss) else "",
                         худшая=f"{g.pnl.min():.0f}$", итог=f"{g.pnl.sum():+,.0f}$", доходность=f"{(e.iloc[-1] - start) / cap * 100:+.1f}%",
                         просадка_в_году=f"{(e / e.cummax() - 1).min() * 100:.1f}%"))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    for filt in (False, True):
        for start, syms in SETS.items():
            end = "2024-03-01" if start == "2021-07-01" else NOW.strftime("%Y-%m-%d %H:%M")
            coins = {s: ge.Coin(s, start, end) for s in syms}
            doge = coins["DOGEUSDT"].daily_range
            Cs, Es = [], []
            for s, c in coins.items():
                scale = c.daily_range / doge if s in ("BTCUSDT", "ETHUSDT") else 1.0
                E, C, liq, tr = ge.run(c, ge.P(side=1, L=1.0, btc_filter=filt, scale=scale))
                C["монета"] = s.replace("USDT", ""); Cs.append(C); Es.append(E)
            C = pd.concat(Cs); E = sum(Es)
            cap = 10_000 * len(syms)
            print(f"\n=== {'с фильтром' if filt else 'без фильтра'}: {start} – {end[:10]}, {len(syms)} монет, капитал ${cap:,}")
            print(year_stats(C, E, cap).to_string(index=False))
            if start == "2024-03-01":
                sep = C[C.t0 >= pd.Timestamp("2026-09-01")].sort_values("t0")
                print(f"\nсентябрь 2026: сделок {len(sep)}, итог {sep.pnl.sum():+,.0f}$, прибыльных {(sep.pnl > 0).mean() * 100:.0f}%")
                print(sep.assign(t0=sep.t0.dt.strftime("%d.%m %H:%M"), t1=sep.t1.dt.strftime("%d.%m %H:%M"), pnl=sep.pnl.round(0)).to_string(index=False))
                if not filt:
                    # что бот видит сейчас
                    print("\nСЕЙЧАС (", NOW, "UTC):")
                    b = coins["BTCUSDT"]
                    print("  фильтр биткоина (биткоин ниже минимума 12 ч):", bool(b.btc_lo12[-1]))
                    for s, c in coins.items():
                        k = c.k; i = -1
                        r3 = c.r3[c.sel][i]; hi = c.hi60[c.sel][i]; ch = c.ch120[c.sel][i]
                        px = k.c.values[i]
                        print(f"  {s.replace('USDT', ''):8s} цена {px:.6g} | RSI7 3м {r3:5.1f} (вход ≤15) | от хая часа {(px / hi - 1) * 100:+.2f}% (вход ≤−3%) | за 2 ч {ch:+.2f}% (вход ≤−2%)")
