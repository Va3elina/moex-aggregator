"""Форвардная оценка. Запускать когда угодно, читать выводы — по правилам
PREREGISTRATION.md (не раньше min_forward_days).

Ничего не подбирает и не оптимизирует. Только сравнивает то, что было
заморожено в baseline.csv, с тем, что случилось после.
"""
import datetime
import pathlib
import sys

import numpy as np
import pandas as pd
from scipy import stats

from gridlib import SPEC, grid_frame, slot_prices, cached, build_grid, per_config_stats

ROOT = pathlib.Path(__file__).resolve().parent
base = pd.read_csv(ROOT / "baseline.csv.gz")
base["key"] = (base.sectype + "|" + base.A + "|" + base.B + "|"
               + base.C + "|" + base["dir"])
today = (sys.argv[1] if len(sys.argv) > 1
         else (datetime.date.today() + datetime.timedelta(days=1)).isoformat())
start = SPEC["forward_starts"]

print(f"ФОРВАРД {start} .. {today}   (заморожено {SPEC['freeze_date']})")
print("=" * 72)

# набор слотов берём ИЗ BASELINE, а не пересчитываем — иначе сетки разойдутся
SLOTS = {st: sorted(set(g.A) | set(g.B) | set(g.C))
         for st, g in base.groupby("sectype")}

frames = []
for st in SPEC["instruments"]:
    try:
        df = grid_frame(st, start, today, min_trades=1, slots=SLOTS.get(st))
    except Exception as e:                     # нет данных за период — не падаем
        print(f"  {st}: пропуск ({e})")
        continue
    if len(df):
        frames.append(df)

if not frames:
    raise SystemExit("Форвардных данных ещё нет.")

fwd = pd.concat(frames, ignore_index=True)
m = base.merge(fwd, on="key", suffixes=("_b", "_f"))
n_days = int(fwd.n.max())
print(f"конфигураций сопоставлено: {len(m)}   форвардных сделок максимум: {n_days}")
if n_days < SPEC["min_forward_days"]:
    print(f"⚠️  порог {SPEC['min_forward_days']} сделок не достигнут — "
          f"выводы делать РАНО, смотрим только динамику.\n")

# ── ГЛАВНАЯ ПРОВЕРКА ─────────────────────────────────────────────────────
# Если перебор нашёл настоящую структуру, конфигурации, хорошие на истории,
# должны быть хорошими и вперёд. Положительная ранговая связь = структура
# есть. Ноль = перебор нашёл шум. Минус = активная подгонка.
print("ГЛАВНОЕ: связь «было на истории» -> «стало вперёд», по всей сетке")
print(f"  {'бумага':<8}{'конфигураций':>14}{'ро Спирмена':>14}{'p':>10}")
rows = []
for st, g in m.groupby("sectype_b"):
    ok = g.dropna(subset=["sr_b", "sr_f"])
    if len(ok) < 30:
        continue
    rho, p = stats.spearmanr(ok.sr_b, ok.sr_f)
    rows.append((st, len(ok), rho, p))
    print(f"  {st:<8}{len(ok):>14}{rho:>+14.3f}{p:>10.4f}")
ok = m.dropna(subset=["sr_b", "sr_f"])
rho, p = stats.spearmanr(ok.sr_b, ok.sr_f)
print(f"  {'ВСЕ':<8}{len(ok):>14}{rho:>+14.3f}{p:>10.4f}   <- главный критерий")

# ── КОНТРОЛЬ: не общий ли это дрейф рынка ────────────────────────────────
print(f"\nКОНТРОЛЬ (дрейф): доля конфигураций с плюсом")
print(f"  на истории {(base.total > 0).mean()*100:>5.1f}%     вперёд {(fwd.total > 0).mean()*100:>5.1f}%")
print("  если вперёд плюсуют почти все — это движение рынка, а не отбор")

# ── ИМЕНОВАННАЯ КОНФИГУРАЦИЯ ─────────────────────────────────────────────
nc = SPEC["named_config"]
key = f"{nc['sectype']}|{nc['A']}|{nc['B']}|{nc['C']}|{nc['dir']}"
row = m[m.key == key]
print(f"\nИМЕНОВАННАЯ {nc['sectype']} {nc['A'][:5]}->{nc['B'][:5]}->{nc['C'][:5]} {nc['dir']}:")
if not len(row):
    print("  форвардных сделок пока нет")
else:
    r = row.iloc[0]
    print(f"  история: {int(r.n_b):>4} сделок, {r['mean_b']*100:+.3f}% на сделку")
    print(f"  вперёд:  {int(r.n_f):>4} сделок, {r['mean_f']*100:+.3f}% на сделку")
    sub_b = base[base.sectype == nc["sectype"]]
    sub_f = fwd[fwd.sectype == nc["sectype"]]
    pb = 100 * (sub_b.sr < r.sr_b).mean()
    pf = 100 * (sub_f.sr < r.sr_f).mean()
    print(f"  перцентиль среди своих: было {pb:.1f} -> стало {pf:.1f}")
    print("  (если было 99, а стало ~50 — это и есть «лучшая из 26100»)")

    # доверительный интервал по фактическим сделкам + лестница издержек
    px = slot_prices(cached(nc["sectype"], start, today), SPEC["min_slot_coverage"])
    labels, R = build_grid(px, SPEC["signal_threshold"], SPEC["max_gap_days"])
    lk = (nc["A"], nc["B"], nc["C"], nc["dir"])
    if lk in labels:
        v = R[:, labels.index(lk)]
        v = v[v != 0]
        if len(v) > 2:
            lo, hi = stats.t.interval(0.95, len(v) - 1, v.mean(), stats.sem(v))
            print(f"  95% интервал на валовую: [{lo*100:+.3f}%; {hi*100:+.3f}%]")
            print(f"  {'издержки':>10}{'на сделку':>12}{'итого':>10}")
            for c in SPEC["cost_ladder"]:
                net = v - c
                print(f"  {c*100:>9.2f}%{net.mean()*100:>+11.3f}%{net.sum()*100:>+9.1f}%")

print("\nЧитать выводы по разделу «Правила остановки» в PREREGISTRATION.md.")
