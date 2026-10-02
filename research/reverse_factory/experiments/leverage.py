"""Откуда доходность у стратегий invvo: плечо против качества. Реальное плечо = dl (позиции / счёт) по дням."""
import json, sys, time, urllib.request
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from factory.schema import load_target
rows = []
for slug in ["quant-hill", "algotoria", "knife-catcher", "team-venture", "team-core", "flowai", "syndicate", "syndicate-bybit", "quantex", "donatello-bybit", "low-risk"]:
    _, m, _ = load_target(slug)
    sid, start = m["source_id"], str(m.get("run_date") or "2020-01-01")[:10]
    u = f"https://invvo.com/api/v1/strategy/graph/extended?strategy_id={sid}&scale=day&from_date={start}&to_date=2026-09-26&webp=1&"
    d = json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=30))["result"]["data"]
    tw = np.array(d["tw"], float); lev = np.array(d["dl"], float)
    idx = pd.date_range(d["date"]["start"], periods=len(tw), freq="D")
    eq = pd.Series(1 + tw / 100, idx); eq = eq[eq > 0]; r = eq.pct_change().dropna()
    L = pd.Series(lev, idx).reindex(r.index)
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / yrs) - 1
    inmkt = (L > 0.01).mean()
    rows.append(dict(стратегия=m["name"], годовых=f"{cagr:+.0%}", волатильность=f"{r.std() * np.sqrt(365):.0%}",
                     плечо_среднее=round(float(L[L > 0.01].mean()), 2), плечо_макс=round(float(L.max()), 1),
                     в_рынке=f"{inmkt:.0%}", доходность_на_ед_плеча=f"{cagr / max(L[L > 0.01].mean(), 0.01):+.0%}",
                     Шарп=round(float(r.mean() / r.std() * np.sqrt(365)), 2)))
    time.sleep(0.3)
pd.set_option("display.width", 220)
print(pd.DataFrame(rows).to_string(index=False))
