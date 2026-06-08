"""Сравнение окон ATR (7/14/20/30) для аномалий позиций. НЕ прод.

Два критерия, которые реально различают окна (не просто частота):
  1. КЛАСТЕРНОСТЬ: % аномалий «одиночных» (нет аномалии в предыдущие 5 дней) vs
     «продолжение серии». Короткое окно после большого дня поднимает базу → больше
     одиночных (глотает серии). Длинное → ловит продолжение.
  2. УСТОЙЧИВОСТЬ во времени: разброс годовой частоты ≥3× (std по годам). Низкий
     разброс = окно даёт стабильный смысл «аномалии» в разных режимах рынка.

Запуск (хост): /opt/frame/signals/.venv/bin/python -m signals.research.oi_atr_windows
"""
from __future__ import annotations
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text
from signals import config

WINDOWS = [7, 14, 20, 30]
MIN_PART = 50
MIN_REL = 0.02
GAP_MAX = 5
ATR_FLOOR_REL = 0.001
MIN_MED_NET = 500
TIER = 3.0          # «сильно» — на нём сравниваем


def load(conn, sectype, clgroup):
    rows = conn.execute(text("""
        SELECT tradedate,
               (COALESCE(pos_long,0)+COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0)) AS npart
        FROM open_interest WHERE sectype=:s AND clgroup=:g AND interval=24
        ORDER BY tradedate
    """), {"s": sectype, "g": clgroup}).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    valid = defaultdict(int)                      # W -> валидных точек
    flags = defaultdict(int)                      # W -> срабатываний ≥TIER
    isolated = defaultdict(int)                   # W -> одиночных
    by_year = defaultdict(lambda: defaultdict(lambda: [0, 0]))  # W -> year -> [valid, flag]
    Wmax = max(WINDOWS)
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            for clg in ("FIZ", "YUR"):
                try:
                    s = load(conn, sectype, clg)
                except Exception:
                    continue
                if len(s) < Wmax + 80:
                    continue
                if st.median([abs(x[1]) for x in s]) < MIN_MED_NET:
                    continue
                dates = [x[0] for x in s]; nets = [x[1] for x in s]; npart = [x[2] for x in s]
                adiff = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
                flagged_days = {W: set() for W in WINDOWS}
                for i in range(Wmax, len(adiff)):
                    di = i + 1
                    if npart[di] < MIN_PART:
                        continue
                    if (dates[di] - dates[di - 1]).days > GAP_MAX:
                        continue
                    net = nets[di]
                    if adiff[i] / max(abs(net), 1) < MIN_REL:
                        continue
                    yr = dates[di].year
                    for W in WINDOWS:
                        atr = st.fmean(adiff[i - W:i])
                        if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
                            continue
                        valid[W] += 1
                        by_year[W][yr][0] += 1
                        if adiff[i] / atr >= TIER:
                            flags[W] += 1
                            by_year[W][yr][1] += 1
                            flagged_days[W].add(di)
                # кластерность: одиночная аномалия = нет аномалии в предыдущие 5 дней
                for W in WINDOWS:
                    fs = flagged_days[W]
                    for d in fs:
                        if not any((d - k) in fs for k in range(1, 6)):
                            isolated[W] += 1

    print(f"\n=== Сравнение окон ATR (порог ≥{TIER:.0f}×, ликвидные) ===\n")
    print(f"{'окно':6}{'частота ≥3×':>13}{'% одиночных':>14}{'устойчивость(σ годов)':>24}")
    for W in WINDOWS:
        v = valid[W] or 1
        freq = 100 * flags[W] / v
        iso = 100 * isolated[W] / (flags[W] or 1)
        # годовая частота → std (берём годы с достаточной выборкой)
        yearly = []
        for yr, (vy, fy) in sorted(by_year[W].items()):
            if vy >= 200:
                yearly.append(100 * fy / vy)
        sd = st.pstdev(yearly) if len(yearly) >= 2 else 0
        print(f"{str(W)+'д':6}{freq:>11.1f}%{iso:>12.1f}%{sd:>20.2f} пп")
    print("\n  Частота — насколько часто срабатывает (спам/пропуск).")
    print("  % одиночных — выше = окно даёт «по 1 алерту на событие» (не спамит серией).")
    print("  Устойчивость σ — НИЖЕ = стабильнее в разные годы (надёжнее смысл порога).")


if __name__ == "__main__":
    run()
